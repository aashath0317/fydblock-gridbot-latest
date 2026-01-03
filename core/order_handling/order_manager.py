import asyncio  # Added for sleep
import logging
import math

import aiohttp
import pandas as pd

from config.trading_mode import TradingMode
from core.bot_management.event_bus import EventBus, Events
from core.bot_management.notification.notification_content import NotificationType
from core.bot_management.notification.notification_handler import NotificationHandler
from core.storage.bot_database import BotDatabase
from strategies.strategy_type import StrategyType

from ..grid_management.grid_level import GridLevel
from ..grid_management.grid_manager import GridManager
from ..order_handling.balance_tracker import BalanceTracker
from ..order_handling.order_book import OrderBook
from ..validation.order_validator import OrderValidator
from .execution_strategy.order_execution_strategy_interface import (
    OrderExecutionStrategyInterface,
)
from .order import Order, OrderSide, OrderStatus


class OrderManager:
    def __init__(
        self,
        grid_manager: GridManager,
        order_validator: OrderValidator,
        balance_tracker: BalanceTracker,
        order_book: OrderBook,
        event_bus: EventBus,
        order_execution_strategy: OrderExecutionStrategyInterface,
        notification_handler: NotificationHandler,
        trading_mode: TradingMode,
        trading_pair: str,
        strategy_type: StrategyType,
        bot_id: int | None = None,
        backend_url: str = "http://localhost:5000/api/user/bot-trade",
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.grid_manager = grid_manager
        self.order_validator = order_validator
        self.balance_tracker = balance_tracker
        self.order_book = order_book
        self.event_bus = event_bus
        self.order_execution_strategy = order_execution_strategy
        self.notification_handler = notification_handler
        self.trading_mode: TradingMode = trading_mode
        self.trading_pair = trading_pair
        self.strategy_type: StrategyType = strategy_type
        self.bot_id = bot_id
        self.backend_url = backend_url
        self.initializing = False

        # --- Initialize Database ---
        self.db = BotDatabase()
        # ---------------------------

        self.event_bus.subscribe(Events.ORDER_FILLED, self._on_order_filled)
        self.event_bus.subscribe(Events.ORDER_CANCELLED, self._on_order_cancelled)

    # ==============================================================================
    #  METHOD 3: SAFE CLEAN START (DB-DRIVEN)
    # ==============================================================================
    async def perform_clean_start(self):
        """
        Aggressively cancels orders until the exchange reports 0 open orders for this bot.
        """
        self.logger.info(f"🧹 Clean Start: Enforcing Zero State for Bot {self.bot_id}...")

        if not self.bot_id:
            return

        # Max attempts to clean up (avoid infinite loops if exchange is broken)
        for attempt in range(5):
            # 1. Fetch ALL open orders (using the new pagination logic in service)
            open_orders = await self.order_execution_strategy.exchange_service.fetch_open_orders(self.trading_pair)

            # Filter for THIS bot
            my_orders = [o for o in open_orders if o.get("clientOrderId", "").startswith(f"G_{self.bot_id}_")]

            # Also check DB for legacy orders we might track
            db_orders = self.db.get_all_active_orders(self.bot_id)

            # Combine lists
            orders_to_cancel = {o["id"] for o in my_orders}
            orders_to_cancel.update(db_orders.keys())

            if not orders_to_cancel:
                self.logger.info("✅ Clean Start Verified: 0 Open Orders.")

                # Double check DB is wiped
                self.db.clear_all_orders(self.bot_id)
                return

            self.logger.info(f"🧹 Attempt {attempt + 1}: Found {len(orders_to_cancel)} lingering orders. Cancelling...")

            # Cancel them all
            for order_id in orders_to_cancel:
                try:
                    await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)
                except Exception:
                    pass  # Ignore errors, just try to kill it

            # Wait for exchange to process cancellations
            await asyncio.sleep(2)

        self.logger.warning("⚠️ Clean Start Warning: Could not verify 0 orders after 5 attempts. Proceeding anyway.")
        self.db.clear_all_orders(self.bot_id)

    async def initialize_grid_orders(self, current_price: float):
        # 1. Block the Watchdog
        self.initializing = True

        try:
            # 2. Enforce Clean Start (Wait until 0 orders)
            await self.perform_clean_start()

            self.logger.info("Initializing Grid Orders (DB-Aware)...")

            # 3. Place Orders (Now safely into an empty account)
            # FIX: Use Dynamic Dead Zone from GridManager
            safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

            for price in self.grid_manager.sorted_buy_grids:
                if price >= safe_buy_limit:
                    continue
                await self._place_limit_order_safe(price, OrderSide.BUY)

            for price in self.grid_manager.sorted_sell_grids:
                if price <= safe_sell_limit:
                    continue
                await self._place_limit_order_safe(price, OrderSide.SELL)

        finally:
            self.initializing = False
            self.logger.info("✅ Initialization Complete. Watchdog released.")

    async def resume_existing_orders(self, current_price: float):
        """
        Hot Boot Logic:
        Resumes control of existing orders on the exchange matching our DB.
        Rebuilds internal GridLevel state WITHOUT cancelling orders.
        """
        self.initializing = True
        self.logger.info("🔥 Hot Boot: Resuming existing orders...")

        try:
            # 1. Fetch Orders from Exchange
            open_orders = await self.order_execution_strategy.exchange_service.fetch_open_orders(self.trading_pair)

            # 2. Fetch Orders from DB
            db_orders = self.db.get_all_active_orders(self.bot_id)

            # 3. Reconcile & Rebuild State
            matched_count = 0

            for order_data in open_orders:
                order_id = order_data["id"]
                price = float(order_data["price"])
                side_str = order_data["side"].upper()  # buy/sell
                side = OrderSide.BUY if side_str == "BUY" else OrderSide.SELL

                # Check if it belongs to us (by ID or Price match)
                # Strict: Must match DB ID?
                # SRS: "Matches them against the database records"

                if order_id in db_orders:
                    # Verified match
                    pass
                else:
                    # Check if it's a grid order by price logic?
                    # If we have a Grid Level at this price, we claim it.
                    closest_grid = min(self.grid_manager.price_grids, key=lambda x: abs(x - price))
                    if abs(closest_grid - price) / price < 0.001:  # 0.1% tolerance
                        # It matches a grid line.
                        # Upsert to DB if missing?
                        self.logger.info(f"   Claiming orphaned order {order_id} at {price}")
                        self.db.add_order(self.bot_id, order_id, price, side.value, float(order_data["amount"]))
                    else:
                        self.logger.info(f"   Ignoring unrelated order {order_id} at {price}")
                        continue

                # 4. update Grid Level State
                grid_level = self.grid_manager.grid_levels.get(price) or self.grid_manager.grid_levels.get(closest_grid)

                if grid_level:
                    # Reconstruct Order Object
                    # We need a proper Order object to add to OrderBook
                    order_obj = Order(
                        identifier=order_id,
                        side=side,
                        pair=self.trading_pair,
                        amount=float(order_data["amount"]),
                        price=price,
                        status=OrderStatus.OPEN,
                        timestamp=int(pd.Timestamp.now().timestamp()),
                    )

                    self.order_book.add_order(order_obj, grid_level)
                    self.grid_manager.mark_order_pending(grid_level, order_obj)
                    matched_count += 1

            self.logger.info(f"✅ Resumed {matched_count} orders from Hot Boot.")

            # 5. Fill Gaps (If any grids are empty but should have orders)
            # We call reconcile once to plug holes
            await self.reconcile_grid_orders(current_price)

        finally:
            self.initializing = False

    # ==============================================================================
    #  CORE EVENT HANDLERS
    # ==============================================================================

    async def _on_order_cancelled(self, order: Order) -> None:
        if self.bot_id:
            self.db.update_order_status(order.identifier, "CANCELLED")

        await self.notification_handler.async_send_notification(
            NotificationType.ORDER_CANCELLED,
            order_details=str(order),
        )

    async def _on_order_filled(self, order: Order) -> None:
        try:
            # 1. Update DB Status immediately
            if self.bot_id:
                self.db.update_order_status(order.identifier, "FILLED")

            # 2. Update Memory / Balances
            await self.balance_tracker.update_balance_on_order_completion(order)
            grid_level = self.order_book.get_grid_level_for_order(order)

            if not grid_level:
                self.logger.warning(
                    f"No grid level found by ID for filled order {order.identifier}. Attempting price match..."
                )
                # Try to fuzzy match by price
                closest_grid_price = min(self.grid_manager.grid_levels.keys(), key=lambda x: abs(x - order.price))
                if abs(closest_grid_price - order.price) / order.price < 0.001:  # 0.1% tolerance
                    grid_level = self.grid_manager.grid_levels[closest_grid_price]
                    self.logger.info(f"Orphan order matched to grid level {closest_grid_price}. Adopting...")
                    self.order_book.add_order(order, grid_level)
                else:
                    self.logger.warning(f"Could not match orphan order {order} to any grid level.")
                    return

            await self._handle_order_completion(order, grid_level)

        except Exception as e:
            self.logger.error(f"Error handling filled order {order.identifier}: {e}", exc_info=True)

    async def _handle_order_completion(self, order: Order, grid_level: GridLevel) -> None:
        if order.side == OrderSide.BUY:
            await self._handle_buy_order_completion(order, grid_level)
        elif order.side == OrderSide.SELL:
            await self._handle_sell_order_completion(order, grid_level)

    async def _handle_buy_order_completion(self, order: Order, grid_level: GridLevel) -> None:
        self.logger.info(f"Buy order completed at grid level {grid_level}.")
        self.grid_manager.complete_order(grid_level, OrderSide.BUY)

        paired_sell_level = self.grid_manager.get_paired_sell_level(grid_level)
        if paired_sell_level and self.grid_manager.can_place_order(paired_sell_level, OrderSide.SELL):
            await self._place_sell_order(grid_level, paired_sell_level, order.filled)
        else:
            self.logger.warning(f"No valid sell grid level found for buy grid level {grid_level}.")

    async def _handle_sell_order_completion(self, order: Order, grid_level: GridLevel) -> None:
        self.logger.info(f"Sell order completed at grid level {grid_level}.")
        self.grid_manager.complete_order(grid_level, OrderSide.SELL)

        # --- FIX: Determine Buy Level FIRST (with fallback) ---
        # Try to find the paired buy level (explicit or theoretical)
        paired_buy_level = self._get_or_create_paired_buy_level(grid_level)
        # ------------------------------------------------------

        # --- PROFIT SYNC ---
        if paired_buy_level:
            # Calculate Profit: (Sell Price - Buy Grid Price) * Amount
            gross_profit = (order.average - paired_buy_level.price) * order.filled

            self.logger.info(
                f"💰 PROFIT SECURED: +{gross_profit:.4f} {self.trading_pair.split('/')[1]} "
                f"(Buy @ {paired_buy_level.price:.2f} -> Sell @ {order.average:.2f})"
            )

            if self.bot_id:
                await self._sync_profit_to_backend(order, gross_profit)
        else:
            self.logger.warning("Could not calculate profit: No paired buy level found.")
        # -------------------

        # --- REPLENISHMENT ---
        if paired_buy_level:
            await self._place_buy_order(grid_level, paired_buy_level, order.filled)
        else:
            self.logger.error(f"Failed to find or create a paired buy grid level for grid level {grid_level}.")

    def _get_or_create_paired_buy_level(self, sell_grid_level: GridLevel) -> GridLevel | None:
        paired_buy_level = sell_grid_level.paired_buy_level
        if paired_buy_level and self.grid_manager.can_place_order(paired_buy_level, OrderSide.BUY):
            return paired_buy_level

        fallback_buy_level = self.grid_manager.get_grid_level_below(sell_grid_level)
        if fallback_buy_level:
            return fallback_buy_level
        return None

    # ==============================================================================
    #  SAFE ORDER PLACEMENT
    # ==============================================================================

    async def _place_limit_order_safe(self, price: float, side: OrderSide, quantity_override: float = 0.0):
        """
        Places an order and immediately saves it to DB.
        Calculates quantity if not provided.
        """
        if self.bot_id:
            existing_order = self.db.get_active_order_at_price(self.bot_id, price)
            if existing_order:
                self.logger.info(f"⏭️ Skipping {side} at {price}: Order already exists in DB.")
                return None

        grid_level = self.grid_manager.grid_levels[price]
        total_balance_value = self.balance_tracker.get_total_balance_value(price)
        raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)

        quantity = quantity_override if quantity_override > 0 else raw_quantity

        try:
            if side == OrderSide.BUY:
                qty = self.order_validator.adjust_and_validate_buy_quantity(
                    self.balance_tracker.balance, quantity, price
                )
            else:
                qty = self.order_validator.adjust_and_validate_sell_quantity(
                    self.balance_tracker.crypto_balance, quantity
                )

            order = await self.order_execution_strategy.execute_limit_order(side, self.trading_pair, qty, price)

            if order:
                if self.bot_id:
                    self.db.add_order(self.bot_id, order.identifier, price, side.value, order.amount)

                if side == OrderSide.BUY:
                    self.balance_tracker.reserve_funds_for_buy(order.amount * order.price)
                else:
                    self.balance_tracker.reserve_funds_for_sell(order.amount)

                self.grid_manager.mark_order_pending(grid_level, order)
                self.order_book.add_order(order, grid_level)
                self.logger.info(f"Placed & Saved {side.name} order at {price}")

                if order.status == OrderStatus.CLOSED:
                    await self.event_bus.publish(Events.ORDER_FILLED, order)

                return order

        except Exception as e:
            msg = str(e).lower()
            if "insufficient" in msg:
                self.logger.warning(f"❌ Insufficient funds for {side.name} at {price}")
            else:
                self.logger.error(f"Failed to place safe order: {e}")

        return None

    # --- Wrappers that use the Safe Method ---
    async def _place_buy_order(self, sell_level, buy_level, quantity):
        await self._place_limit_order_safe(buy_level.price, OrderSide.BUY, quantity)

    async def _place_sell_order(self, buy_level, sell_level, quantity):
        await self._place_limit_order_safe(sell_level.price, OrderSide.SELL, quantity)

    # ==============================================================================
    #  ROBUST RECONCILIATION
    # ==============================================================================

    async def reconcile_grid_orders(self, current_price: float):
        if self.initializing:
            self.logger.info("⏳ Bot is initializing. Watchdog sleeping...")
            return

        # self.logger.info("🛡️ Database Integrity Check...") (Avoid Spamming)

        await self.balance_tracker.sync_balances(self.order_execution_strategy.exchange_service, current_price)

        # Fetch orders
        exchange_orders = await self.order_execution_strategy.exchange_service.fetch_open_orders(self.trading_pair)

        # --- SAFETY CHECK: PAGINATION WARNING ---
        if len(exchange_orders) >= 100:
            self.logger.warning(
                "⚠️ API returned 100+ orders. Pagination limit might be hit! Skipping integrity check to prevent duplicates."
            )
            return  # Stop here. Do not mark things as missing.
        # ----------------------------------------

        exchange_order_ids = set(o["id"] for o in exchange_orders)

        if self.bot_id:
            db_orders = self.db.get_all_active_orders(self.bot_id)
            for order_id, _ in db_orders.items():
                if order_id not in exchange_order_ids:
                    self.logger.warning(f"⚠️ Order {order_id} missing from exchange open orders. Verifying status...")
                    try:
                        # Attempt to fetch the specific order to see if it was filled or canceled
                        verified_order = await self.order_execution_strategy.get_order(order_id, self.trading_pair)

                        if verified_order:
                            if verified_order.status == OrderStatus.CLOSED:
                                self.logger.info(f"✅ Found missing order {order_id} as FILLED. Replaying fill event.")
                                await self._on_order_filled(verified_order)
                            elif verified_order.status == OrderStatus.CANCELED:
                                self.logger.info(f"ℹ️ Found missing order {order_id} as CANCELED. syncing...")
                                await self._on_order_cancelled(verified_order)
                            else:
                                # It exists but not Open/Closed/Canceled? Weird. Assume closed if not in open list.
                                self.logger.warning(
                                    f"Order {order_id} status is {verified_order.status} but not in open list. Marking CLOSED_UNKNOWN."
                                )
                                self.db.update_order_status(order_id, "CLOSED_UNKNOWN")
                        else:
                            # returned None
                            self.logger.warning(f"Order {order_id} returned None from fetch. Marking CLOSED_UNKNOWN.")
                            self.db.update_order_status(order_id, "CLOSED_UNKNOWN")

                    except Exception as e:
                        self.logger.error(f"Failed to verify missing order {order_id}: {e}. Marking CLOSED_UNKNOWN.")
                        self.db.update_order_status(order_id, "CLOSED_UNKNOWN")

        # Balance Check before recovery loop to stop spam
        MIN_FIAT_THRESHOLD = 5.0
        MIN_CRYPTO_THRESHOLD = 0.05
        has_fiat = self.balance_tracker.balance > MIN_FIAT_THRESHOLD
        has_crypto = self.balance_tracker.crypto_balance > MIN_CRYPTO_THRESHOLD

        # Dynamic Dead Zone: Use the GridManager's logic
        safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

        # Check BUY Grids
        if has_fiat:
            for price in self.grid_manager.sorted_buy_grids:
                if price >= safe_buy_limit:
                    continue

                # Check DB first
                if self.bot_id and self.db.get_active_order_at_price(self.bot_id, price):
                    continue

                # Check Exchange (Secondary)
                is_active = any(
                    math.isclose(p, price, rel_tol=1e-3) for p in [float(o["price"]) for o in exchange_orders]
                )
                if is_active:
                    continue

                # --- FIX: Check for Dust/Insufficient Funds ---
                total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
                raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
                required_value = raw_quantity * price

                # 5% tolerance to avoid skipping due to tiny fee mismatches, but prevent massive partial "dust" fills
                # FIX: Use BalanceTracker's attempt_fee_recovery to auto-heal "dust" shortfalls
                if not self.balance_tracker.attempt_fee_recovery(required_value * 0.95):
                    self.logger.warning(
                        f"Skipping BUY reconciliation for level {price}: Insufficient funds "
                        f"(Available: {self.balance_tracker.balance:.2f}, Required: {required_value:.2f})"
                    )
                    continue
                # ----------------------------------------------

                success = await self._place_limit_order_safe(price, OrderSide.BUY)
                if not success:
                    break
        else:
            self.logger.info("ℹ️ Skipping Buy Recovery: Insufficient Fiat.")

        # Check SELL Grids
        if has_crypto:
            for price in self.grid_manager.sorted_sell_grids:
                if price <= safe_sell_limit:
                    continue

                if self.bot_id and self.db.get_active_order_at_price(self.bot_id, price):
                    continue

                is_active = any(
                    math.isclose(p, price, rel_tol=1e-3) for p in [float(o["price"]) for o in exchange_orders]
                )
                if is_active:
                    continue

                # --- FIX: Check for Dust/Insufficient Funds ---
                total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
                raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)

                # For SELL, the "value" needed is the crypto amount itself
                required_crypto = raw_quantity

                if self.balance_tracker.crypto_balance < (required_crypto * 0.95):
                    self.logger.warning(
                        f"Skipping SELL reconciliation for level {price}: Insufficient crypto "
                        f"(Available: {self.balance_tracker.crypto_balance:.4f}, Required: {required_crypto:.4f})"
                    )
                    continue
                # ----------------------------------------------

                success = await self._place_limit_order_safe(price, OrderSide.SELL)
                if not success:
                    break
        else:
            self.logger.info("ℹ️ Skipping Sell Recovery: Insufficient Crypto.")

    # --- Misc Methods ---

    async def perform_initial_purchase(self, current_price: float) -> None:
        grid_prices = self.grid_manager.grid_levels.keys()
        total_grid_count = len(grid_prices)
        if total_grid_count == 0:
            return

        sell_grids = [p for p in grid_prices if p > current_price]
        sell_grid_count = len(sell_grids)
        total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
        value_per_grid = total_balance_value / total_grid_count
        target_crypto_value = value_per_grid * sell_grid_count
        current_crypto_value = self.balance_tracker.crypto_balance * current_price
        shortfall_value = target_crypto_value - current_crypto_value

        if shortfall_value > 10.0:
            amount_to_buy = (shortfall_value / current_price) * 1.01
            try:
                adjusted_quantity = self.order_validator.adjust_and_validate_buy_quantity(
                    self.balance_tracker.balance, amount_to_buy, current_price
                )
                buy_order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.BUY,
                    self.trading_pair,
                    amount=adjusted_quantity,
                    price=current_price,
                )
                self.order_book.add_order(buy_order)
                if self.trading_mode != TradingMode.BACKTEST:
                    self.balance_tracker.update_after_initial_purchase(initial_order=buy_order)
                else:
                    await self._simulate_fill(buy_order, buy_order.timestamp)

                await self.notification_handler.async_send_notification(
                    NotificationType.ORDER_PLACED,
                    order_details=f"Rebalanced Portfolio: Bought {amount_to_buy:.4f} SOL",
                )
            except Exception as e:
                self.logger.error(f"Failed to execute initial purchase: {e}")
                raise e

    async def execute_take_profit_or_stop_loss_order(
        self, current_price: float, take_profit_order: bool = False, stop_loss_order: bool = False
    ) -> None:
        if not (take_profit_order or stop_loss_order):
            return
        event = "Take profit" if take_profit_order else "Stop loss"
        try:
            quantity = self.balance_tracker.crypto_balance
            order = await self.order_execution_strategy.execute_market_order(
                OrderSide.SELL, self.trading_pair, quantity, current_price
            )
            if not order:
                raise Exception("Order execution returned None")
            self.order_book.add_order(order)
            await self.notification_handler.async_send_notification(
                NotificationType.TAKE_PROFIT_TRIGGERED if take_profit_order else NotificationType.STOP_LOSS_TRIGGERED,
                order_details=str(order),
            )
            self.logger.info(f"{event} triggered at {current_price} and sell order executed.")
        except Exception as e:
            self.logger.error(f"Failed to execute {event}: {e}")

    async def simulate_order_fills(self, high_price: float, low_price: float, timestamp: int | pd.Timestamp) -> None:
        timestamp_val = int(timestamp.timestamp()) if isinstance(timestamp, pd.Timestamp) else int(timestamp)
        pending_orders = self.order_book.get_open_orders()
        crossed_buy = [l for l in self.grid_manager.sorted_buy_grids if low_price <= l <= high_price]
        crossed_sell = [l for l in self.grid_manager.sorted_sell_grids if low_price <= l <= high_price]

        for order in pending_orders:
            if (order.side == OrderSide.BUY and order.price in crossed_buy) or (
                order.side == OrderSide.SELL and order.price in crossed_sell
            ):
                await self._simulate_fill(order, timestamp_val)

    async def _simulate_fill(self, order: Order, timestamp: int) -> None:
        order.filled = order.amount
        order.remaining = 0.0
        order.status = OrderStatus.CLOSED
        order.timestamp = timestamp

        # execution price (limit order = price, market order ~ price)
        # For simplicity in backtest, we assume Limit fill at price.
        order.average = order.price

        # Calculate Mock Fee
        # We rely on BalanceTracker's fee calculator to ensure consistency
        if self.balance_tracker and self.balance_tracker.fee_calculator:
            trade_value = order.filled * order.average
            fee_cost = self.balance_tracker.fee_calculator.calculate_fee(trade_value)
            # Standard exchange format (dict)
            order.fee = {
                "cost": fee_cost,
                "currency": self.trading_pair.split("/")[1],  # Quote currency fees usually
            }

        await self.event_bus.publish(Events.ORDER_FILLED, order)

    async def cancel_all_open_orders(self) -> None:
        """
        Safe cancellation: Only cancels orders that belong to this bot (found in DB).
        """
        self.logger.info(f"Cancelling open orders for Bot {self.bot_id}...")

        if self.bot_id:
            my_orders = self.db.get_all_active_orders(self.bot_id)
            for order_id in my_orders.keys():
                try:
                    await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)
                    self.db.update_order_status(order_id, "CANCELLED")
                    self.logger.info(f"Cancelled order {order_id}")
                except Exception as e:
                    self.logger.warning(f"Failed to cancel {order_id}: {e}")
        else:
            # Legacy fallback
            open_orders = self.order_book.get_open_orders()
            for order in open_orders:
                try:
                    await self.order_execution_strategy.cancel_order(order.identifier, self.trading_pair)
                except Exception as e:
                    self.logger.error(f"Failed: {e}")

    async def liquidate_positions(self, current_price: float) -> None:
        crypto_balance = self.balance_tracker.crypto_balance
        if crypto_balance > 0:
            self.logger.info(f"Liquidating {crypto_balance} {self.trading_pair} at market price...")
            try:
                await self.order_execution_strategy.execute_market_order(
                    OrderSide.SELL, self.trading_pair, crypto_balance, current_price
                )
                self.logger.info("Liquidation successful.")
            except Exception as e:
                self.logger.error(f"Failed to liquidate positions: {e}")

    async def _sync_profit_to_backend(self, order: Order, profit: float):
        payload = {
            "bot_id": self.bot_id,
            "side": "sell",
            "price": order.average,
            "amount": order.filled,
            "profit": profit,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.backend_url, json=payload) as response:
                    if response.status == 200:
                        self.logger.info("✅ Profit synced.")
                    else:
                        self.logger.warning(f"⚠️ Profit Sync Failed: {response.status}")
        except Exception as e:
            self.logger.error(f"❌ Error syncing profit: {e}")
