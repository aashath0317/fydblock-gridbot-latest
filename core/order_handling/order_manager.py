import asyncio
import logging
import math
import uuid

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
from .order import Order, OrderSide, OrderStatus, OrderType  # Added OrderStatus/OrderType, OrderStatus


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
        self._log_throttle = {}
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

        # Deduplication: Track processed fill events for defense-in-depth
        self._processed_order_fills: set[str] = set()

        self.event_bus.subscribe(Events.ORDER_FILLED, self._on_order_filled)
        self.event_bus.subscribe(Events.ORDER_CANCELLED, self._on_order_cancelled)

    def _throttled_warning(self, msg: str, key: str, interval: int = 60):
        """Logs a warning only if 'interval' seconds have passed since the last log for 'key'."""
        import time

        now = time.time()
        last_time = self._log_throttle.get(key, 0)
        if now - last_time > interval:
            self.logger.warning(msg)
            self._log_throttle[key] = now

    def has_active_orders(self) -> bool:
        """
        Checks if the bot has any active orders in the persistent database.
        Used for Smart Resume (Hot Boot).
        """
        if not self.bot_id:
            return False

        active_orders = self.db.get_all_active_orders(self.bot_id)
        return len(active_orders) > 0

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
            open_orders = await self.order_execution_strategy.exchange_service.refresh_open_orders(self.trading_pair)

            # Filter for THIS bot
            my_orders = [o for o in open_orders if o.get("clientOrderId", "").startswith(f"G{self.bot_id}x")]

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

            # 3. Smart Rebalance: Ensure we have the funds BEFORE we try to place orders
            # This handles the "Auto-Buy/Sell" requirement to fix deficits.
            await self.ensure_funds_for_grid(current_price)

            # 4. Place Orders (Now safely into an empty account)
            # FIX: Use Dynamic Dead Zone from GridManager
            safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

            for price in self.grid_manager.sorted_buy_grids:
                if price >= safe_buy_limit:
                    continue
                await self._place_limit_order_safe(price, OrderSide.BUY)

            for price in self.grid_manager.sorted_sell_grids:
                if price <= safe_sell_limit:
                    continue
                # Phase 4 Update: Capture order and record stock
                order = await self._place_limit_order_safe(price, OrderSide.SELL)
                if order:
                    # We successfully locked stock in a sell order.
                    # According to the plan, we should record this as stock "allocated" to the key.
                    # Even though it's locked, it is the Tracking Field for this grid.
                    # If we cancel it later, we know how much we had.
                    self.grid_manager.grid_levels[price].stock_on_hand = order.amount
                    if self.bot_id:
                        self.db.update_grid_stock(self.bot_id, price, order.amount)
                    self.logger.info(f"🏁 Init: Recorded initial stock {order.amount} for grid {price}")

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
            open_orders = await self.order_execution_strategy.exchange_service.refresh_open_orders(self.trading_pair)

            # 2. Fetch Orders from DB
            db_orders = self.db.get_all_active_orders(self.bot_id)

            # 3. Reconcile & Rebuild State
            matched_count = 0
            resumed_grid_prices = {}

            prefix = f"G{self.bot_id}x" if self.bot_id else None

            for order_data in open_orders:
                # SAFETY: Only process orders that belong to THIS bot instance
                if prefix:
                    cid = order_data.get("clientOrderId") or ""
                    if not str(cid).startswith(prefix):
                        continue

                order_id = order_data["id"]
                price = float(order_data["price"])
                side_str = order_data["side"].upper()  # buy/sell
                side = OrderSide.BUY if side_str == "BUY" else OrderSide.SELL

                # Check if it belongs to us (by ID or Price match)
                closest_grid = None  # Initialize to avoid UnboundLocalError

                if order_id in db_orders:
                    # Verified match
                    pass
                else:
                    # Check if it's a grid order by price logic
                    closest_grid = min(self.grid_manager.price_grids, key=lambda x: abs(x - price))
                    if abs(closest_grid - price) / price < 0.001:  # 0.1% tolerance
                        # FIX: Check if we already have it to avoid UNIQUE constraint error
                        if not self.db.get_order(order_id):
                            self.logger.info(f"   Claiming orphaned order {order_id} at {price}")
                            self.db.add_order(self.bot_id, order_id, price, side.value, float(order_data["amount"]))
                        else:
                            self.logger.info(f"   Order {order_id} at {price} already in DB. Resuming...")
                    else:
                        self.logger.info(f"   Ignoring unrelated order {order_id} at {price}")
                        continue

                # 4. update Grid Level State
                grid_level = self.grid_manager.grid_levels.get(price)

                # If exact match failed, try closest grid
                if not grid_level:
                    if closest_grid is None:
                        closest_grid = min(self.grid_manager.price_grids, key=lambda x: abs(x - price))
                    grid_level = self.grid_manager.grid_levels.get(closest_grid)

                if grid_level:
                    # --- DUPLICATE CHECK ---
                    # --- DUPLICATE CHECK ---
                    if grid_level.price in resumed_grid_prices:
                        existing_order_id = resumed_grid_prices[grid_level.price]

                        # Fix: Check if it's the SAME order ID (Duplicate entry in list)
                        if existing_order_id == order_id:
                            self.logger.warning(
                                f"⚠️ Identical order entry found for {grid_level.price} (Order {order_id}). Ignoring duplicate entry."
                            )
                            continue

                        self.logger.warning(
                            f"⚠️ Duplicate order found for grid {grid_level.price} (Order {order_id} vs Existing {existing_order_id}). Cancelling..."
                        )
                        try:
                            await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)
                        except Exception as e:
                            self.logger.error(f"Failed to cancel duplicate order {order_id}: {e}")
                        continue

                    resumed_grid_prices[grid_level.price] = order_id
                    # -----------------------
                    # -----------------------

                    # Reconstruct Order Object
                    order_obj = Order(
                        identifier=order_id,
                        status=OrderStatus.OPEN,
                        order_type=OrderType.LIMIT,
                        side=side,
                        price=price,
                        average=None,
                        amount=float(order_data["amount"]),
                        filled=0.0,
                        remaining=float(order_data["amount"]),
                        timestamp=int(pd.Timestamp.now().timestamp() * 1000),
                        datetime=pd.Timestamp.now().isoformat(),
                        last_trade_timestamp=None,
                        symbol=self.trading_pair,
                        time_in_force="GTC",
                    )

                    self.order_book.add_order(order_obj, grid_level)
                    self.grid_manager.mark_order_pending(grid_level, order_obj)

                    # FIX: Sync with BalanceTracker
                    # deduct_from_balance=False because these funds are ALREADY locked on exchange
                    # and therefore missing from the 'free balance' we initialized with.
                    self.balance_tracker.register_open_order(order_obj, deduct_from_balance=False)
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

        # FIX: Release Reserved Funds
        if order.side == OrderSide.BUY:
            # For BUY, we reserved (Price * Amount)
            # Use remaining amount to calculate what to release
            release_amount = order.remaining * order.price
            self.balance_tracker.release_reserve_for_buy(release_amount)
        elif order.side == OrderSide.SELL:
            # For SELL, we reserved (Amount) of crypto
            self.balance_tracker.release_reserve_for_sell(order.remaining)

        await self.notification_handler.async_send_notification(
            NotificationType.ORDER_CANCELLED,
            order_details=str(order),
        )

    async def _on_order_filled(self, order: Order) -> None:
        try:
            # DEDUPLICATION: Skip if already processed (defense-in-depth)
            # FIX: Enforce string type for consistent deduplication
            order_id_str = str(order.identifier)
            if order_id_str in self._processed_order_fills:
                self.logger.debug(f"Order {order.identifier} already handled. Ignoring duplicate fill event.")
                return

            # Mark as processed immediately
            self._processed_order_fills.add(order_id_str)

            # OWNERSHIP VALIDATION: Defense-in-depth
            if self.bot_id:
                info = order.info or {}
                # Check common fields for client ID
                client_oid = info.get("clientOrderId") or info.get("clOrdId") or info.get("client_oid")
                expected_prefix = f"G{self.bot_id}x"

                if client_oid and str(client_oid).startswith("G") and not str(client_oid).startswith(expected_prefix):
                    # It starts with G (likely a bot order) but not OUR bot prefix
                    self.logger.warning(
                        f"Ignored foreign bot order {order.identifier}. CID '{client_oid}' does not match prefix '{expected_prefix}'."
                    )
                    return

            # 1. Update DB Status immediately
            if self.bot_id:
                self.db.update_order_status(order.identifier, "FILLED")

            # 2. Update Memory / Balances
            # Pass fee data if available
            await self.balance_tracker.update_balance_on_order_completion(order)
            grid_level = self.order_book.get_grid_level_for_order(order)

            if not grid_level:
                self.logger.warning(
                    f"No grid level found by ID for filled order {order.identifier}. Attempting price match..."
                )
                # --- ORPHAN RECOVERY: Approximate Price Match ---
                # Fallback: Find a grid level with a price close to order.price
                tolerance = 0.001  # 0.1% tolerance
                matched_level = None
                for price, level in self.grid_manager.grid_levels.items():
                    if abs(price - order.price) < (price * tolerance):
                        # Ensure logic validity:
                        # If BUY filled, level should be WAITING_FOR_BUY_FILL or similar.
                        # If SELL filled, level should be WAITING_FOR_SELL_FILL.
                        # We accept it even if state is "READY_..." if we trust the fill more.
                        matched_level = level
                        break

                if matched_level:
                    self.logger.info(
                        f"✅ Recovered orphan order {order.identifier} by matching price {order.price} to grid level {matched_level.price}"
                    )
                    grid_level = matched_level
                    # Auto-adopt: Update order book mapping so future lookups work
                    self.order_book.add_order(order, grid_level)
                else:
                    self.logger.warning(
                        f"Ignored unknown order {order.identifier} (Not in OrderBook/DB and no price match)."
                    )
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

        # --- PRECISE COIN TRACKING: Calculate Net Coin FIRST ---
        # 1. Get the exact fill data
        executed_qty = order.filled
        fee_paid_in_coin = 0.0

        if order.fee and order.fee.get("currency") == self.trading_pair.split("/")[0]:
            # Fee was paid in Base Currency (e.g. SOL)
            fee_paid_in_coin = float(order.fee.get("cost", 0.0))
        elif not order.fee:
            pass  # Fallback handled elsewhere or assumed 0

        # 2. Calculate Net Coin Received
        net_coin_received = executed_qty - fee_paid_in_coin

        # 3. Save to Buy Level Initially (Safety)
        grid_level.stock_on_hand = net_coin_received
        if self.bot_id:
            self.db.update_grid_stock(self.bot_id, grid_level.price, net_coin_received)

        self.logger.info(
            f"🧬 Precise Tracking: Bought {executed_qty} - Fee {fee_paid_in_coin} = Stock {net_coin_received:.6f}"
        )
        # -------------------------------------------------------

        # --- Determine Sell Level & Transfer Stock ---
        paired_sell_level = self._get_or_create_paired_sell_level(grid_level)

        if paired_sell_level:
            # TRANSFER STOCK: Buy Level -> Sell Level
            # This ensures the Sell Order sees the stock and uses it.
            paired_sell_level.stock_on_hand = net_coin_received
            if self.bot_id:
                self.db.update_grid_stock(self.bot_id, paired_sell_level.price, net_coin_received)

            # Clear Buy Level
            grid_level.stock_on_hand = 0.0
            if self.bot_id:
                self.db.update_grid_stock(self.bot_id, grid_level.price, 0.0)

            self.logger.info(
                f"🚚 Stock Transferred: {net_coin_received:.6f} moved from {grid_level.price} to {paired_sell_level.price}"
            )

            if self.grid_manager.can_place_order(paired_sell_level, OrderSide.SELL):
                # FIX: Pass the EXACT net coin quantity
                await self._place_sell_order(grid_level, paired_sell_level, net_coin_received)
        else:
            self.logger.warning(
                f"No valid sell grid level found for buy grid level {grid_level}. Stock remains on Buy Level."
            )

        # --- SAVE TRADE HISTORY (BUY) ---
        if self.bot_id:
            trade_record = {
                "bot_id": self.bot_id,
                "order_id": order.identifier,
                "pair": self.trading_pair,
                "side": "buy",
                "price": order.average or order.price,
                "quantity": order.filled,
                "fee_amount": 0.0,
                "fee_currency": self.trading_pair.split("/")[0],
                "realized_pnl": 0.0,
            }
            if order.fee:
                trade_record["fee_amount"] = float(order.fee.get("cost", 0.0))
                currency = order.fee.get("currency", "")
                if currency and currency.upper() != "UNKNOWN":
                    trade_record["fee_currency"] = currency
            else:
                # Fallback: Estimate Fee in Base Currency
                rate = self.balance_tracker.fee_calculator.trading_fee
                trade_record["fee_amount"] = order.filled * rate

            self.db.add_trade_history(trade_record)
        # --------------------------------

    async def _handle_sell_order_completion(self, order: Order, grid_level: GridLevel) -> None:
        self.logger.info(f"Sell order completed at grid level {grid_level}.")
        self.grid_manager.complete_order(grid_level, OrderSide.SELL)

        # --- FIX: Determine Buy Level FIRST (with fallback) ---
        # Try to find the paired buy level (explicit or theoretical)
        paired_buy_level = self._get_or_create_paired_buy_level(grid_level)
        # ------------------------------------------------------

        # --- PROFIT SYNC ---
        if paired_buy_level:
            # --- PROFIT CALCULATION ---
            # Determine logic based on Order Size Type (Base vs Quote)
            # Base = Fixed Coin Amount (Standard Grid) -> Profit is Cash Flow (Realized PnL)
            # Quote = Fixed USDT Amount (Accumulate Coin) -> Profit is Total Value Gain (Realized + Unrealized Dust)
            order_size_type = self.grid_manager.config_manager.get_order_size_type()

            if order_size_type == "quote":
                # Formula: Revenue * (SellPrice - BuyPrice) / BuyPrice
                # This calculates the value of the 'extra coin' gained at the current sell price
                revenue = order.average * order.filled
                gross_profit = revenue * (order.average - paired_buy_level.price) / paired_buy_level.price
                self.logger.info(f"🧮 Quote Mode Profit: {gross_profit:.4f} (Based on Val. Gain)")
            else:
                # Standard: Price Diff * Quantity Sold
                gross_profit = (order.average - paired_buy_level.price) * order.filled

            # --------------------------

            # Calculate Fees (Estimate)
            # Buy Fee: based on the original buy price
            buy_fee = self.balance_tracker.fee_calculator.calculate_fee(paired_buy_level.price * order.filled)
            # Sell Fee: based on the sell price (or use actual fee if available, but estimate for consistency)
            sell_fee = self.balance_tracker.fee_calculator.calculate_fee(order.average * order.filled)

            total_estimated_fees = buy_fee + sell_fee
            net_profit = gross_profit - total_estimated_fees

            # --- RESERVE ALLOCATION (10%) ---
            reserve_amount = 0.0
            if net_profit > 0:
                reserve_amount = net_profit * 0.10
                self.balance_tracker.allocate_profit_to_reserve(reserve_amount)

            self.logger.info(
                f"💰 PROFIT SECURED: +{net_profit:.4f} {self.trading_pair.split('/')[1]} "
                f"[Sold {order.filled:.6f} {self.trading_pair.split('/')[0]}] "
                f"(Gross: {gross_profit:.4f}, Fees: {total_estimated_fees:.4f}, Reserve: {reserve_amount:.4f})"
            )

            if self.bot_id:
                # Sync Net Profit to backend
                await self._sync_profit_to_backend(order, net_profit)

                # --- SAVE TRADE HISTORY (SELL) ---
                trade_record = {
                    "bot_id": self.bot_id,
                    "order_id": order.identifier,
                    "pair": self.trading_pair,
                    "side": "sell",
                    "price": order.average or order.price,
                    "quantity": order.filled,
                    "fee_amount": total_estimated_fees,
                    "fee_currency": self.trading_pair.split("/")[1],
                    "realized_pnl": net_profit,
                }
                self.db.add_trade_history(trade_record)
                # ---------------------------------
        else:
            self.logger.warning("Could not calculate profit: No paired buy level found.")
        # -------------------

        # --- REPLENISHMENT ---
        if paired_buy_level:
            # FIX: Pass 0.0 quantity to force recalculation based on config (Fixed USDT vs Fixed Coin)
            await self._place_buy_order(grid_level, paired_buy_level, 0.0)
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

    def _get_or_create_paired_sell_level(self, buy_grid_level: GridLevel) -> GridLevel | None:
        paired_sell_level = buy_grid_level.paired_sell_level
        if paired_sell_level and self.grid_manager.can_place_order(paired_sell_level, OrderSide.SELL):
            return paired_sell_level

        fallback_sell_level = self.grid_manager.get_grid_level_above(buy_grid_level)
        if fallback_sell_level:
            return fallback_sell_level
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

        # --- PRECISE COIN TRACKING (Phase 3) ---
        # If Selling, check if we have specific stock tracking for this grid
        dust_remainder = 0.0
        if side == OrderSide.SELL and grid_level.stock_on_hand > 0:
            self.logger.info(f"📉 Using Stock-On-Hand for Sell Order: {grid_level.stock_on_hand:.6f}")
            quantity = grid_level.stock_on_hand
        # ---------------------------------------

        try:
            if side == OrderSide.BUY:
                qty = self.order_validator.adjust_and_validate_buy_quantity(
                    self.balance_tracker.balance, quantity, price
                )
            else:
                qty = self.order_validator.adjust_and_validate_sell_quantity(
                    self.balance_tracker.crypto_balance, quantity
                )

                # (Dust management moved to after successful placement)

            # --- ISOLATION FIX: Generate Client Order ID ---
            client_order_id = None
            if self.bot_id:
                # Format: G<bot_id>x<short_uuid> (Alphanumeric only for OKX)
                short_uuid = uuid.uuid4().hex[:8]
                client_order_id = f"G{self.bot_id}x{short_uuid}"

            params = {"clientOrderId": client_order_id} if client_order_id else None

            order = await self.order_execution_strategy.execute_limit_order(
                side, self.trading_pair, qty, price, params=params
            )

            if order:
                # --- PRECISE COIN TRACKING UPDATE (Phase 3) ---
                # Update stock ONLY after successful placement to prevent data loss.
                if side == OrderSide.SELL and grid_level.stock_on_hand > 0:
                    if qty < quantity:
                        dust_remainder = quantity - qty
                        grid_level.stock_on_hand = dust_remainder
                        if self.bot_id:
                            self.db.update_grid_stock(self.bot_id, price, dust_remainder)
                        self.logger.info(f"🧹 Dust Management: Keeping {dust_remainder:.6f} as residue.")
                    else:
                        grid_level.stock_on_hand = 0.0
                        if self.bot_id:
                            self.db.update_grid_stock(self.bot_id, price, 0.0)
                # ----------------------------------------------

                if self.bot_id:
                    self.db.add_order(self.bot_id, order.identifier, price, side.value, order.amount)

                if side == OrderSide.BUY:
                    self.balance_tracker.reserve_funds_for_buy(order.amount * order.price)
                else:
                    self.balance_tracker.reserve_funds_for_sell(order.amount)

                self.grid_manager.mark_order_pending(grid_level, order)
                self.order_book.add_order(order, grid_level)
                self.logger.info(f"Placed & Saved {side.name} order at {price} (CID: {client_order_id})")

                if order.status == OrderStatus.CLOSED:
                    await self.event_bus.publish(Events.ORDER_FILLED, order)

                return order

        except Exception as e:
            msg = str(e).lower()
            if "insufficient" in msg:
                self.logger.warning(f"❌ Insufficient funds for {side.name} at {price}")

                # --- PHASE 5: Sync Wallet Logic ---
                if side == OrderSide.SELL and grid_level.stock_on_hand > 0:
                    self.logger.warning("🔄 Syncing wallet to resolve insufficient funds...")

                    # 1. Force Sync Wallet
                    # sync_balances returns the ACTUAL free funds in the shared wallet
                    _, actual_crypto = await self.balance_tracker.sync_balances(
                        self.order_execution_strategy.exchange_service, price
                    )

                    # 2. Check Discrepancy
                    if actual_crypto is not None and actual_crypto < grid_level.stock_on_hand:
                        self.logger.warning(
                            f"📉 Stock Mismatch (Manual Sell?): Wanted {grid_level.stock_on_hand:.6f}, "
                            f"Found {actual_crypto:.6f}. Adjusting stock down."
                        )

                        # 3. Adjust Stock
                        grid_level.stock_on_hand = actual_crypto
                        if self.bot_id:
                            self.db.update_grid_stock(self.bot_id, price, actual_crypto)

                        # 4. Retry Order (Recursive One-Shot)
                        # We only retry if we have meaningful balance left (e.g. > dust)
                        if actual_crypto > 0.0001:
                            self.logger.info("🔁 Retrying Sell Order with synced balance...")
                            return await self._place_limit_order_safe(price, side)
                        else:
                            self.logger.warning("🚫 Balance too low to retry.")
                # ----------------------------------
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

        exchange_orders_raw = await self.order_execution_strategy.exchange_service.refresh_open_orders(
            self.trading_pair
        )

        # --- ISOLATION FIX: Strict Filter by Client Order ID ---
        exchange_orders = []
        if self.bot_id:
            prefix = f"G{self.bot_id}x"
            for o in exchange_orders_raw:
                cid = o.get("clientOrderId") or ""
                # Strict check: Must start with my prefix
                if str(cid).startswith(prefix) or self.db.get_order(o["id"]):
                    exchange_orders.append(o)
        else:
            # No bot_id (e.g. backtest), take everything
            exchange_orders = exchange_orders_raw

        # --- SAFETY CHECK: PAGINATION WARNING ---
        if len(exchange_orders_raw) >= 2000:
            self.logger.warning(
                "⚠️ API returned 2000+ orders. Pagination limit might be hit! Skipping integrity check to prevent duplicates."
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
                                # It exists but not Open/Closed/Canceled? Weird. Assume closed if not in open list.
                                # FIX: Do not mark CLOSED_UNKNOWN if status is valid (OPEN/PARTIALLY_FILLED).
                                if verified_order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                                    self.logger.debug(
                                        f"Order {order_id} is {verified_order.status} on exchange but missing from open list. "
                                        f"Trusting specific fetch and keeping it alive."
                                    )
                                    # Ideally we should re-add it to our open list if missing?
                                    # But for now, just don't kill it.
                                else:
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

        # Balance Check Limits (soft check only needed later)
        # We don't block the loop on this anymore.
        min_fiat_threshold = 5.0
        min_crypto_threshold = 0.05
        has_fiat = self.balance_tracker.balance > min_fiat_threshold
        has_crypto = self.balance_tracker.crypto_balance > min_crypto_threshold

        # Dynamic Dead Zone: Use the GridManager's logic
        safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

        # --- Count Active Grid Orders ---
        # Only count orders that match a valid grid level.
        # This prevents orphaned orders or manual trades from blocking the grid logic.
        active_grid_order_count = 0
        valid_grid_prices = list(self.grid_manager.grid_levels.keys())
        # FIX: Ensure we only count OUR orders towards the limit
        prefix = f"G{self.bot_id}x" if self.bot_id else None

        for o in exchange_orders:
            try:
                # Filter by Bot ID if available
                if prefix:
                    cid = o.get("clientOrderId") or ""
                    if not str(cid).startswith(prefix):
                        continue

                op = float(o["price"])
                if any(math.isclose(op, gp, rel_tol=1e-3) for gp in valid_grid_prices):
                    active_grid_order_count += 1
            except (ValueError, KeyError):
                pass
        # --------------------------------

        # Check BUY Grids
        for price in self.grid_manager.sorted_buy_grids:
            if price >= safe_buy_limit:
                continue

            # Check DB first
            if self.bot_id and self.db.get_active_order_at_price(self.bot_id, price):
                continue

            # Check Exchange (Secondary)
            is_active = any(math.isclose(p, price, rel_tol=1e-3) for p in [float(o["price"]) for o in exchange_orders])
            if is_active:
                continue

            # --- STRICT ORDER COUNT GUARD ---
            # If we are already at (or above) capacity, do NOT place new orders.
            # This prevents the "46th order" bug.
            if active_grid_order_count >= self.grid_manager.num_grids:
                self._throttled_warning(
                    f"🛑 Max orders reached ({active_grid_order_count}/{self.grid_manager.num_grids}). Skipping BUY at {price}.",
                    f"max_orders_buy_{self.bot_id}",
                )
                break
            # --------------------------------

            # --- FUND CHECK MOVED HERE ---
            # Only check funds if we actually NEED to place an order
            if not has_fiat:
                self.logger.debug(f"Skipping BUY at {price}: No Fiat (Throttle Log)")
                # If we have 0 fiat and no order, we can't do anything. Stop this loop cycle.
                break

            # Check for Dust/Insufficient Funds per order
            total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
            raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            required_value = raw_quantity * price

            if not self.balance_tracker.attempt_fee_recovery(required_value * 0.95):
                self._throttled_warning(
                    f"Skipping BUY reconciliation for level {price}: Insufficient funds "
                    f"(Available: {self.balance_tracker.balance:.2f}, Required: {required_value:.2f})",
                    f"insufficient_funds_buy_{price}",
                )
                continue
            # -----------------------------

            success = await self._place_limit_order_safe(price, OrderSide.BUY)
            if not success:
                break
            else:
                # CRITICAL FIX: Increment count so subsequent loops don't overfill
                active_grid_order_count += 1

        # Check SELL Grids
        for price in self.grid_manager.sorted_sell_grids:
            if price <= safe_sell_limit:
                continue

            if self.bot_id and self.db.get_active_order_at_price(self.bot_id, price):
                continue

            is_active = any(math.isclose(p, price, rel_tol=1e-3) for p in [float(o["price"]) for o in exchange_orders])
            if is_active:
                continue

            # --- STRICT ORDER COUNT GUARD ---
            if active_grid_order_count >= self.grid_manager.num_grids:
                self._throttled_warning(
                    f"🛑 Max orders reached ({active_grid_order_count}/{self.grid_manager.num_grids}). Skipping SELL at {price}.",
                    f"max_orders_sell_{self.bot_id}",
                )
                break
            # --------------------------------

            # --- FUND CHECK MOVED HERE ---
            if not has_crypto:
                self.logger.debug(f"Skipping SELL at {price}: No Crypto (Throttle Log)")
                break

            # Check for Dust/Insufficient Funds
            total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
            raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            required_crypto = raw_quantity

            if self.balance_tracker.crypto_balance < (required_crypto * 0.95):
                self._throttled_warning(
                    f"Skipping SELL reconciliation for level {price}: Insufficient crypto "
                    f"(Available: {self.balance_tracker.crypto_balance:.4f}, Required: {required_crypto:.4f})",
                    f"insufficient_crypto_sell_{price}",
                )
                continue
            # ------------------------------

            success = await self._place_limit_order_safe(price, OrderSide.SELL)
            if not success:
                break
            else:
                # CRITICAL FIX: Increment count so subsequent loops don't overfill
                active_grid_order_count += 1

    async def ensure_funds_for_grid(self, current_price: float) -> None:
        """
        Checks if sufficient funds exist for the active grid.
        If not, attempts to rebalance (market buy/sell) to fix the deficit.
        """
        required_fiat, required_crypto = self.calculate_grid_requirements(current_price)

        # Check against BalanceTracker
        deficit = self.balance_tracker.check_rebalance_needs(required_fiat, required_crypto, current_price)

        if deficit:
            self.logger.info(f"⚖️ Deficit Detected: {deficit}")
            await self.execute_rebalance(deficit, current_price)
            # We assume rebalance works. The next iteration/step will use the new funds.

    def calculate_grid_requirements(self, current_price: float) -> tuple[float, float]:
        """
        Calculates the total Fiat and Crypto needed to support the current grid levels.
        Returns: (required_fiat, required_crypto)
        """
        required_fiat = 0.0
        required_crypto = 0.0

        total_balance_value = self.balance_tracker.get_total_balance_value(current_price)

        # Dynamic Dead Zone
        safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

        # 1. Sum up BUY requirements
        for price in self.grid_manager.sorted_buy_grids:
            if price >= safe_buy_limit:
                continue
            # Calculate cost for this level
            raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            required_fiat += raw_quantity * price

        # FIX: Add 2% buffer to the fiat requirement to cover rounding/dust issues
        required_fiat *= 1.02

        # 2. Sum up SELL requirements
        for price in self.grid_manager.sorted_sell_grids:
            if price <= safe_sell_limit:
                continue

            raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            required_crypto += raw_quantity

        # FIX: Add 2% buffer to the crypto requirement to cover rounding/dust issues
        # This prevents the last sell order placement from failing after rebalancing.
        required_crypto *= 1.02

        return required_fiat, required_crypto

    # ==============================================================================
    #  MISC METHODS
    # ==============================================================================

    async def execute_rebalance(self, deficit: dict, current_price: float) -> bool:
        """
        Executes a MARKET order to fix a balance deficit.
        """
        if not deficit:
            return False

        action_type = deficit.get("type")
        required_amount = deficit.get("amount", 0.0)
        reason = deficit.get("reason", "Unknown")

        self.logger.warning(
            f"⚖️ REBALANCING: Attempting to fix deficit: {reason} (Action: {action_type}, Amount: {required_amount:.6f})"
        )

        try:
            order = None
            if action_type == "BUY_CRYPTO":
                # We need to buy `amount` crypto.
                # Check fiat balance first (redundant but safe)
                cost = required_amount * current_price
                if self.balance_tracker.balance < cost:
                    self.logger.error(
                        f"❌ CANNOT REBALANCE: Need {required_amount} crypto (Cost: {cost:.2f}) "
                        f"but only have {self.balance_tracker.balance:.2f} fiat. Manual intervention required."
                    )
                    return False

                # Convert crypto amount to fiat cost for 'quote' based market buy?
                # Usually execute_market_order takes 'amount' as crypto quantity for BUY/SELL if side is base.
                # Let's assume quantity is "Amount of Base Currency".
                # Adjust quantity for precision
                # FIX: Add small buffer (0.5%) to cover rounding/dust issues which cause "Insufficient Funds" for the last order
                adjusted_qty = self.order_validator.adjust_and_validate_buy_quantity(
                    self.balance_tracker.balance, required_amount * 1.005, current_price
                )

                order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.BUY,
                    self.trading_pair,
                    amount=adjusted_qty,
                    price=current_price,
                )

            elif action_type == "SELL_CRYPTO":
                # We need to sell `amount` crypto to raise fiat.
                if self.balance_tracker.crypto_balance < required_amount:
                    self.logger.error(
                        f"❌ CANNOT REBALANCE: Need to sell {required_amount} crypto "
                        f"but only have {self.balance_tracker.crypto_balance:.6f}. Manual intervention required."
                    )
                    return False

                adjusted_qty = self.order_validator.adjust_and_validate_sell_quantity(
                    self.balance_tracker.crypto_balance, required_amount
                )

                order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.SELL,
                    self.trading_pair,
                    amount=adjusted_qty,
                    price=current_price,
                )

            if order:
                self.logger.info(f"✅ Rebalance Order Placed: {order.side} {order.amount} @ {order.average}")
                self.order_book.add_order(order)
                if self.bot_id:
                    self.db.add_order(self.bot_id, order.identifier, current_price, order.side.value, order.amount)

                # NOTE: We do not add this to 'grid_levels' pending. It's a structural adjustment.
                # However, we MUST event bus it so balance tracker updates.
                if order.status == OrderStatus.CLOSED:
                    await self.event_bus.publish(Events.ORDER_FILLED, order)
                    await self.notification_handler.async_send_notification(
                        NotificationType.ORDER_PLACED,
                        order_details=f"⚖️ Smart Rebalance Triggered: {order.side.name} {order.amount:.4f} {self.trading_pair.split('/')[0]}",
                    )
                return True

        except Exception as e:
            self.logger.error(f"Failed to execute rebalance: {e}")
            return False

        return False

    async def perform_initial_purchase(self, current_price: float) -> None:
        # DELEGATE TO GRID MANAGER
        # This ensures we use the correct logic for "Fixed USDT" vs "Fixed Coin"
        # and respect the "Projected Holdings" allocation from the frontend.
        target_quantity = self.grid_manager.get_initial_order_quantity(
            current_price, self.balance_tracker.crypto_balance, self.balance_tracker.balance
        )

        if target_quantity > 0:
            amount_to_buy = target_quantity

            # --- ISOLATION FIX: Generate Client Order ID for Initial Buy ---
            client_order_id = None
            if self.bot_id:
                short_uuid = uuid.uuid4().hex[:8]
                client_order_id = f"G{self.bot_id}x{short_uuid}"

            params = {"clientOrderId": client_order_id} if client_order_id else None
            # ---------------------------------------------------------------

            try:
                adjusted_quantity = self.order_validator.adjust_and_validate_buy_quantity(
                    self.balance_tracker.balance, amount_to_buy, current_price
                )
                buy_order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.BUY,
                    self.trading_pair,
                    amount=adjusted_quantity,
                    price=current_price,
                    params=params,
                )
                self.order_book.add_order(buy_order)
                # FIX: Redundant update removed. Event bus handles it via _on_order_filled.
                await self.notification_handler.async_send_notification(
                    NotificationType.ORDER_PLACED,
                    order_details=f"Rebalanced Portfolio: Bought {amount_to_buy:.4f} SOL",
                )

                # --- SAVE TRADE HISTORY (INITIAL BUY) ---
                if self.bot_id:
                    trade_record = {
                        "bot_id": self.bot_id,
                        "order_id": buy_order.identifier,
                        "pair": self.trading_pair,
                        "side": "buy",
                        "price": buy_order.average or buy_order.price,
                        "quantity": buy_order.filled,
                        "fee_amount": 0.0,  # TODO: Parse fee
                        "fee_currency": self.trading_pair.split("/")[0],
                        "realized_pnl": 0.0,
                    }
                    if buy_order.fee:
                        trade_record["fee_amount"] = float(buy_order.fee.get("cost", 0.0))
                        trade_record["fee_currency"] = buy_order.fee.get("currency", "")

                    self.db.add_trade_history(trade_record)
                # ----------------------------------------
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
        Robust cancellation: Fetches ALL open orders from Exchange and cancels those belonging to this bot.
        Uses a dual-matching strategy:
        1. Match by Exchange ID against local DB (Primary).
        2. Match by clientOrderId prefix (Secondary).
        """
        self.logger.info(f"🛑 Stopping: Force-cancelling confirmed open orders for Bot {self.bot_id}...")

        if not self.bot_id:
            # Legacy fallback
            open_orders = self.order_book.get_open_orders()
            for order in open_orders:
                try:
                    await self.order_execution_strategy.cancel_order(order.identifier, self.trading_pair)
                except Exception as e:
                    self.logger.error(f"Failed: {e}")
            return

        # 1. Fetch Source of Truth from Exchange
        try:
            exchange_orders = await self.order_execution_strategy.exchange_service.refresh_open_orders(
                self.trading_pair
            )

            # 2. Fetch Source of Truth from DB
            db_orders = self.db.get_all_active_orders(self.bot_id)
            db_order_ids = set(db_orders.keys())

            # 3. Identify Orders to Cancel
            orders_to_cancel = []
            prefix = f"G{self.bot_id}x"

            for o in exchange_orders:
                oid = o["id"]
                client_oid = o.get("clientOrderId", "")

                # Match A: ID is in our DB
                if oid in db_order_ids:
                    orders_to_cancel.append(o)
                    continue

                # Match B: Prefix matches (even if not in DB, we own it)
                if str(client_oid).startswith(prefix):
                    orders_to_cancel.append(o)
                    continue

                # Match C: REMOVED.
                # Do NOT match by price/grid level. It causes cross-bot cancellation
                # if two bots share similar grid ranges on the same pair.
                # If it's not in DB and doesn't have our prefix, leave it alone.
                # continue

            self.logger.info(
                f"🔎 Found {len(orders_to_cancel)} active orders on exchange to cancel (DB or Prefix Match)."
            )

            for order_data in orders_to_cancel:
                order_id = order_data["id"]
                try:
                    await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)
                    self.logger.info(f"✅ Cancelled order {order_id}")

                    # Update DB to keep it consistent
                    self.db.update_order_status(order_id, "CANCELLED")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to cancel {order_id}: {e}")

            # 4. Final DB Cleanup (Force mark all 'OPEN' as 'CANCELLED' in DB to remove zombies)
            # Even if exchange didn't return them (maybe they closed 1ms ago), we shouldn't think they are open.
            # Rrefetch active orders to see what's left
            remaining_db_orders = self.db.get_all_active_orders(self.bot_id)
            if remaining_db_orders:
                self.logger.info(f"🧹 Cleanup: Marking {len(remaining_db_orders)} local DB orders as CANCELLED.")
                for oid in remaining_db_orders.keys():
                    self.db.update_order_status(oid, "CANCELLED")

        except Exception as e:
            self.logger.error(f"❌ Critical Error during cancel_all_open_orders: {e}", exc_info=True)

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

    async def force_nuclear_cleanup(self) -> None:
        """
        ☢️ NUCLEAR OPTION (SCOPED): Cancels open orders for this bot.
        Uses Client Order ID Prefix or DB Matching to ensure we don't kill other bots' orders.
        """
        self.logger.warning(f"☢️ NUCLEAR CLEANUP TRIGGERED for {self.trading_pair} (Bot {self.bot_id}).")

        if not self.bot_id:
            self.logger.error("❌ Cannot run nuclear cleanup without Bot ID.")
            return

        try:
            # 1. Fetch EVERYTHING
            open_orders = await self.order_execution_strategy.exchange_service.refresh_open_orders(self.trading_pair)

            if not open_orders:
                self.logger.info("✅ Nuclear Cleanup Verified: 0 Open Orders found.")
                # Ensure DB is wiped too
                self.db.clear_all_orders(self.bot_id)
                return

            self.logger.info(f"🔎 Scanning {len(open_orders)} orders for Bot {self.bot_id}...")

            # 2. Filter Orders (Safety First)
            # We cancel if:
            # A) Order ID is in our DB for this bot.
            # B) clientOrderId starts with our prefix (G_{bot_id}_)

            db_orders = self.db.get_all_active_orders(self.bot_id)
            db_ids = set(db_orders.keys())
            prefix = f"G{self.bot_id}x"

            orders_to_nuke = []

            for order in open_orders:
                oid = order["id"]
                cid = order.get("clientOrderId", "")

                if oid in db_ids or str(cid).startswith(prefix):
                    orders_to_nuke.append(order)
                else:
                    # Optional: Check for orphaned price match if strictly needed,
                    # but typically prefix is enough and safer.
                    pass

            if not orders_to_nuke:
                self.logger.info("✅ No matching orders found to nuke. Safe.")
                self.db.clear_all_orders(self.bot_id)
                return

            self.logger.info(f"☢️ Nuking {len(orders_to_nuke)} confirmed orders for Bot {self.bot_id}...")

            # 3. Cancel Filtered Orders
            for order in orders_to_nuke:
                order_id = order["id"]
                try:
                    await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)
                    self.logger.info(f"💥 Nuclear: Cancelled {order_id}")
                except Exception as e:
                    self.logger.error(f"Failed to nuke order {order_id}: {e}")

            # 4. Wipe DB
            self.db.clear_all_orders(self.bot_id)

            self.logger.info("✅ Nuclear Cleanup Complete.")

        except Exception as e:
            self.logger.error(f"Nuclear Cleanup Failed: {e}", exc_info=True)
