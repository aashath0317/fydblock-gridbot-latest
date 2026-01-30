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
        bot_id: str | int | None = None,
    ):
        """
        Initializes the OrderStatusTracker.

        Args:
            order_book: OrderBook instance to manage and query orders.
            order_execution_strategy: Strategy for querying order statuses from the exchange.
            event_bus: EventBus instance for publishing state change events.
            trading_pair: The trading pair (e.g., "BTC/USDT") this tracker is monitoring.
            polling_interval: Time interval (in seconds) between status checks.
            bot_id: Optional identifier for the bot, used for ownership validation of orphan orders.
        """
        self.order_book = order_book
        self.order_execution_strategy = order_execution_strategy
        self.event_bus = event_bus
        self.polling_interval = polling_interval
        self.trading_pair = trading_pair
        self.bot_id = bot_id  # Store for ownership validation
        self._monitoring_task = None
        self._active_tasks = set()
        # FIX: Include bot_id and pair in logger name to distinguish instances
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{bot_id}.{trading_pair}")
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

            # --- ISOLATION FIX: Filter by Trading Pair ---
            # Every bot sees every order update on the account. We must ignore other symbols.
            msg_symbol = (
                order_data.get("symbol") if isinstance(order_data, dict) else getattr(order_data, "symbol", None)
            )
            # Normalization (e.g. OKX sometimes returns 'BTC-USDT-SWAP' vs 'BTC/USDT')
            if msg_symbol and msg_symbol != self.trading_pair:
                # Silently ignore orders for other symbols
                return

            # Normalize status to enum if possible or string check
            # Normalize ID for deduplication
            order_id_str = str(order_id)

            if status == OrderStatus.CLOSED or status == "closed":
                # CRITICAL DEDUPLICATION: Check BEFORE any logic
                if order_id_str in self._processed_fills:
                    self.logger.debug(f"Order {order_id} already processed. Skipping duplicate fill event.")
                    return

                # Mark as processed IMMEDIATELY
                self._processed_fills.add(order_id_str)

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

                    self.event_bus.publish_sync(Events.ORDER_FILLED, local_order)
                    self.logger.info(f"Order {order_id} filled.")
                else:
                    # --- EARLY OWNERSHIP CHECK (BEFORE LOGGING/RECOVERY) ---
                    # If this order doesn't belong to us, skip silently without logging warnings
                    # This prevents cross-bot log spam when multiple bots share an exchange account
                    if self.bot_id:
                        # Get clientOrderId from the incoming order_data if available
                        info = (
                            order_data.get("info", {})
                            if isinstance(order_data, dict)
                            else getattr(order_data, "info", {})
                        )
                        client_oid = (
                            info.get("clientOrderId") or info.get("clOrdId") or info.get("client_oid") if info else None
                        )

                        # If we have a client order ID that starts with "G" (a bot order) but NOT our prefix, skip silently
                        expected_prefix = f"G{self.bot_id}x"
                        if (
                            client_oid
                            and str(client_oid).startswith("G")
                            and not str(client_oid).startswith(expected_prefix)
                        ):
                            # This order belongs to another bot - skip without logging
                            return
                    # -------------------------------------------------------

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

                        # Fallback to known trading pair ONLY if we can verify ownership via clientOrderId
                        if not symbol:
                            info = (
                                order_data.get("info", {})
                                if isinstance(order_data, dict)
                                else getattr(order_data, "info", {})
                            )
                            client_oid = (
                                info.get("clientOrderId") or info.get("clOrdId") or info.get("client_oid")
                                if info
                                else None
                            )
                            expected_prefix = f"G{self.bot_id}x"

                            # checks if the order belongs to this bot
                            if client_oid and str(client_oid).startswith(expected_prefix):
                                symbol = self.trading_pair
                            else:
                                # If we can't verify it's ours, DO NOT assume it is.
                                # This prevents 'Order does not exist' errors when another bot's order comes in without a symbol.
                                return

                        if not symbol:
                            self.logger.error(f"Cannot recover orphan order {order_id}: Symbol missing in update data.")
                        else:
                            remote_order = await self.order_execution_strategy.get_order(order_id, symbol)
                            if remote_order and remote_order.status == OrderStatus.CLOSED:
                                # OWNERSHIP VALIDATION: Check clientOrderId if bot_id is set (double-check after fetch)
                                if self.bot_id:
                                    info = remote_order.info or {}
                                    # Check common fields for client ID (CCXT standardizes to 'clientOrderId', exchange specific might be 'clOrdId')
                                    client_oid = (
                                        info.get("clientOrderId") or info.get("clOrdId") or info.get("client_oid")
                                    )

                                    # Expected prefix: "G<bot_id>x"
                                    expected_prefix = f"G{self.bot_id}x"

                                    if not client_oid or not str(client_oid).startswith(expected_prefix):
                                        self.logger.debug(
                                            f"Ignored orphan order {order_id}. Ownership mismatch. "
                                            f"Expected prefix '{expected_prefix}', got CID '{client_oid}'."
                                        )
                                        return

                                self.logger.info(f"Recovered orphan order {order_id}. Publishing FILLED event.")
                                self.event_bus.publish_sync(Events.ORDER_FILLED, remote_order)
                    except Exception as rec_error:
                        error_msg = str(rec_error)
                        # Check for "Ghost" order indicators
                        if "Order does not exist" in error_msg or "not found" in error_msg or "51603" in error_msg:
                            self.logger.warning(
                                f"Market order {order_id} already closed or not found (Ghost Order), skipping recovery. Error: {error_msg}"
                            )
                            return

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
