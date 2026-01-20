import asyncio
import logging

from core.bot_management.event_bus import EventBus, Events
from core.order_handling.order import Order, OrderStatus
from core.order_handling.order_book import OrderBook


class OrderStatusTracker:
    """
    Tracks the status of pending orders and publishes events
    when their states change (e.g., open, filled, canceled).
    """

    def __init__(
        self,
        order_book: OrderBook,
        order_execution_strategy,
        event_bus: EventBus,
        trading_pair: str,
        polling_interval: float = 15.0,
    ):
        """
        Initializes the OrderStatusTracker.

        Args:
            order_book: OrderBook instance to manage and query orders.
            order_execution_strategy: Strategy for querying order statuses from the exchange.
            event_bus: EventBus instance for publishing state change events.
            polling_interval: Time interval (in seconds) between status checks.
        """
        self.order_book = order_book
        self.order_execution_strategy = order_execution_strategy
        self.event_bus = event_bus
        self.polling_interval = polling_interval
        self.trading_pair = trading_pair
        self._monitoring_task = None
        self._active_tasks = set()
        self.logger = logging.getLogger(self.__class__.__name__)
        # Deduplication: Track processed fill events to prevent duplicates
        self._processed_fills: set[str] = set()

    async def _track_open_order_statuses(self) -> None:
        """
        DEPRECATED: Polling loop.
        Kept for fallback if needed, but start_streaming matches the plan.
        We will remove the actual loop logic to prevent usage.
        """
        self.logger.warning("_track_open_order_statuses called but polling is disabled.")
        return

    async def start_streaming(self) -> None:
        """
        Starts the WebSocket stream for order updates.
        """
        if self._monitoring_task and not self._monitoring_task.done():
            self.logger.warning("OrderStatusTracker stream is already running.")
            return

        # Start the integrity/reconciliation once before streaming
        await self._process_open_orders()

        self._monitoring_task = asyncio.create_task(
            self.order_execution_strategy.exchange_service.start_user_stream(self._on_order_update)
        )
        self.logger.info("OrderStatusTracker has started WebSocket stream.")

    async def _on_order_update(self, order_data: dict) -> None:
        """
        Callback for WebSocket order updates.
        """
        try:
            # order_data is a dict consistent with ccxt structure
            # We construct a partial Order object or just handle the ID and status directly.
            # Assuming order_data has 'id', 'status', 'filled', 'remaining'

            order_id = order_data.get("id")
            status = order_data.get("status")

            if not order_id or not status:
                self.logger.warning(f"Incomplete order data received: {order_data}")
                return

            await self._handle_order_status_change(order_data)

        except Exception as e:
            self.logger.error(f"Error handling WebSocket order update: {e}", exc_info=True)

    async def _process_open_orders(self) -> None:
        """
        Processes open orders by querying their statuses and handling state changes.
        """
        open_orders = self.order_book.get_open_orders()
        tasks = [self._create_task(self._query_and_handle_order(order)) for order in open_orders]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Error during order processing: {result}", exc_info=True)

    async def _query_and_handle_order(self, local_order: Order):
        """
        Query order and handling state changes if needed.
        """
        try:
            remote_order = await self.order_execution_strategy.get_order(local_order.identifier, local_order.symbol)
            await self._handle_order_status_change(remote_order)

        except Exception as error:
            self.logger.error(
                f"Failed to query remote order with identifier {local_order.identifier}: {error}",
                exc_info=True,
            )

    async def _handle_order_status_change(
        self,
        order_data: dict | Order,
    ) -> None:
        """
        Handles changes in the status of the order data (from WebSocket or Polling).

        Args:
            order_data: Dictionary containing order details OR Order object.
        """
        try:
            if isinstance(order_data, dict):
                order_id = order_data.get("id")
                status = order_data.get("status")
                filled = float(order_data.get("filled", 0.0))
                remaining = float(order_data.get("remaining", 0.0))
            else:
                # Assume Order object
                order_id = order_data.identifier
                status = order_data.status
                filled = order_data.filled
                remaining = order_data.remaining

            if not status:
                self.logger.error(f"Missing 'status' in order data: {order_data}")
                return  # Can't raise here as it might break the loop, just return

            # Normalize status to enum if possible or string check
            if status == OrderStatus.CLOSED or status == "closed":
                # DEDUPLICATION: Skip if already processed
                if order_id in self._processed_fills:
                    self.logger.debug(f"Order {order_id} already processed. Skipping duplicate fill event.")
                    return

                self.order_book.update_order_status(order_id, OrderStatus.CLOSED)
                # Ideally we want an Order object, but for now we might need to fetch it or construct it
                # If we have it in order_book, we can get it
                local_order = self.order_book.get_order(order_id)
                if local_order:
                    # Update local order with filled amount
                    local_order.filled = filled
                    local_order.remaining = remaining
                    local_order.status = OrderStatus.CLOSED

                    # FIX: Update price information if available
                    avg_price = (
                        float(order_data.get("average", 0.0)) if isinstance(order_data, dict) else order_data.average
                    )
                    ord_price = (
                        float(order_data.get("price", 0.0)) if isinstance(order_data, dict) else order_data.price
                    )

                    if avg_price and avg_price > 0:
                        local_order.average = avg_price
                    if ord_price and ord_price > 0:
                        local_order.price = ord_price

                    # Mark as processed BEFORE publishing to prevent race conditions
                    self._processed_fills.add(order_id)
                    self.event_bus.publish_sync(Events.ORDER_FILLED, local_order)
                    self.logger.info(f"Order {order_id} filled.")
                else:
                    self.logger.warning(f"Filled order {order_id} not found in local OrderBook. Attempting recovery...")
                    # Attempt to fetch remote order to get full details (symbol, amount, etc)
                    try:
                        # We guess symbol from order_data if available, or we might need to rely on the strategy knowing the default
                        # If order_data is a dict from WS, it usually has 'symbol' e.g. 'SOL/USDT'
                        symbol = (
                            order_data.get("symbol")
                            if isinstance(order_data, dict)
                            else getattr(order_data, "pair", None)
                        )

                        # Fallback to known trading pair if missing in update (common in some WS streams)
                        if not symbol:
                            symbol = self.trading_pair

                        if not symbol:
                            self.logger.error(f"Cannot recover orphan order {order_id}: Symbol missing in update data.")
                        else:
                            remote_order = await self.order_execution_strategy.get_order(order_id, symbol)
                            if remote_order and remote_order.status == OrderStatus.CLOSED:
                                # Mark as processed BEFORE publishing to prevent race conditions
                                self._processed_fills.add(order_id)
                                self.logger.info(f"Recovered orphan order {order_id}. Publishing FILLED event.")
                                self.event_bus.publish_sync(Events.ORDER_FILLED, remote_order)
                    except Exception as rec_error:
                        self.logger.error(f"Failed to recover orphan order {order_id}: {rec_error}")

            elif status == OrderStatus.CANCELED or status == "canceled":
                self.order_book.update_order_status(order_id, OrderStatus.CANCELED)
                local_order = self.order_book.get_order(order_id)
                if local_order:
                    self.event_bus.publish_sync(Events.ORDER_CANCELLED, local_order)
                self.logger.warning(f"Order {order_id} was canceled.")

            elif status == OrderStatus.OPEN or status == "open":  # Still open
                if filled > 0:
                    self.logger.info(
                        f"Order {order_id} partially filled. Filled: {filled}, Remaining: {remaining}.",
                    )
                else:
                    self.logger.debug(f"Order {order_id} is still open. No fills yet.")
            else:
                self.logger.warning(
                    f"Unhandled order status '{status}' for order {order_id}.",
                )

        except Exception as e:
            self.logger.error(f"Error handling order status change: {e}", exc_info=True)

    def _create_task(self, coro):
        """
        Creates a managed asyncio task and adds it to the active task set.

        Args:
            coro: Coroutine to be scheduled as a task.
        """
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    async def _cancel_active_tasks(self):
        """
        Cancels all active tasks tracked by the tracker.
        """
        for task in self._active_tasks:
            task.cancel()
        await asyncio.gather(*self._active_tasks, return_exceptions=True)
        self._active_tasks.clear()

    # NOTE: start_tracking is deprecated/removed in favor of start_streaming
    # but kept as alias if needed, or we just remove it.
    # The plan said "Remove polling loop", so we replacing start_tracking logic.

    def start_tracking(self) -> None:
        """
        Starts the order tracking task.
        """
        if self._monitoring_task and not self._monitoring_task.done():
            self.logger.warning("OrderStatusTracker is already running.")
            return
        self._monitoring_task = asyncio.create_task(self._track_open_order_statuses())
        self.logger.info("OrderStatusTracker has started tracking open orders.")

    async def stop_tracking(self) -> None:
        """
        Stops the order tracking task.
        """
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                self.logger.info("OrderStatusTracker monitoring task was cancelled.")
            await self._cancel_active_tasks()
            self._monitoring_task = None
            self.logger.info("OrderStatusTracker has stopped tracking open orders.")
