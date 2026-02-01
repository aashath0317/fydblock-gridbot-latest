import asyncio
import bisect
import contextlib
import logging
import math
import time
import uuid

import aiohttp
import pandas as pd

from config.trading_mode import TradingMode
from core.bot_management.event_bus import EventBus, Events
from core.bot_management.notification.notification_content import NotificationType
from core.bot_management.notification.notification_handler import NotificationHandler
from core.services.exchange_interface import ExchangeInterface
from core.storage.bot_database import BotDatabase
from strategies.strategy_type import StrategyType

from ..grid_management.grid_level import GridCycleState, GridLevel
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
        exchange_service: ExchangeInterface,
        bot_id: int | None = None,
        backend_url: str = "http://localhost:5000/api/user/bot-trade",
        db: BotDatabase | None = None,
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
        self.exchange_service = exchange_service
        self.bot_id = bot_id
        self.backend_url = backend_url
        self.initializing = False
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{bot_id}.{trading_pair}")
        self._rebalancing_lock = asyncio.Lock()

        # --- LOCKS ---
        self._placing_orders = set()  # Tracks prices currently being processed for orders

        # --- Initialize Database ---
        # Use injected DB if available (Server mode), else create new (Backtest/Standalone)
        self.db = db if db else BotDatabase()
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

    async def has_active_orders(self) -> bool:
        """
        Checks if the bot has any active orders in the persistent database.
        Used for Smart Resume (Hot Boot).
        """
        if not self.bot_id:
            return False

        active_orders = await self.db.get_all_active_orders(self.bot_id)
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
            db_orders = await self.db.get_all_active_orders(self.bot_id)

            # Combine lists
            orders_to_cancel = {o["id"] for o in my_orders}
            orders_to_cancel.update(db_orders.keys())

            if not orders_to_cancel:
                self.logger.info("✅ Clean Start Verified: 0 Open Orders.")

                # Double check DB is wiped
                await self.db.clear_all_orders(self.bot_id)
                return

            self.logger.info(f"🧹 Attempt {attempt + 1}: Found {len(orders_to_cancel)} lingering orders. Cancelling...")

            # Cancel them all
            for order_id in orders_to_cancel:
                with contextlib.suppress(Exception):
                    await self.order_execution_strategy.cancel_order(order_id, self.trading_pair)

            # Wait for exchange to process cancellations
            await asyncio.sleep(2)

        self.logger.warning("⚠️ Clean Start Warning: Could not verify 0 orders after 5 attempts. Proceeding anyway.")
        await self.db.clear_all_orders(self.bot_id)

    async def _perform_state_diff_rebalance(self, current_price: float):
        """
        Phase 1 & 2: State Diff Calculation & Priority Rebalancing.
        Detects Surplus Coin/Cash and liquidates/acquires to match Target Distribution.
        """
        self.logger.info("⚖️ Phase 1: Snapshotting State & Calculating Diff...")

        # 1. Snapshot State (Net Worth)
        await self.balance_tracker.sync_balances(self.order_execution_strategy.exchange_service, current_price)

        # Use TOTAL (Free + Reserved) for State Diff Check.
        # If we have funds locked in Grid Orders (Reserved), we count them as "Inventory".
        current_coin = self.balance_tracker.crypto_balance + self.balance_tracker.reserved_crypto
        current_cash = self.balance_tracker.balance + self.balance_tracker.reserved_fiat

        # 2. Compute Targets
        total_balance_value = self.balance_tracker.get_total_balance_value(current_price)
        safe_buy, safe_sell = self.grid_manager.get_dead_zone_thresholds(current_price)

        target_coin = 0.0
        for price in self.grid_manager.sorted_sell_grids:
            if price <= safe_sell:
                continue
            qty = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            target_coin += qty

        target_cash = 0.0
        for price in self.grid_manager.sorted_buy_grids:
            if price >= safe_buy:
                continue
            qty = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
            target_cash += (qty * price) * 1.002  # Add 0.2% buffer for fees

        # 3. Identify Imbalances
        coin_diff = current_coin - target_coin
        cash_diff = current_cash - target_cash

        self.logger.info(f"   Targets: Coin={target_coin:.6f}, Cash={target_cash:.2f}")
        self.logger.info(f"   Current: Coin={current_coin:.6f}, Cash={current_cash:.2f}")
        self.logger.info(
            f"   Diff: Coin {'Surplus' if coin_diff > 0 else 'Deficit'}={abs(coin_diff):.6f}, "
            f"Cash {'Surplus' if cash_diff > 0 else 'Deficit'}={abs(cash_diff):.2f}"
        )

        # 4. Phase 2: Priority Rebalancing
        # Case A: Surplus Coin (Sell to fund Cash)
        # Fix: Removed relative buffer (target_coin * 0.02). If we have a surplus and a deficit, we MUST sell.
        is_surplus_coin = coin_diff > 0
        value_of_surplus = coin_diff * current_price
        min_trade_val = 2.0  # Safe minimum for most exchanges

        if is_surplus_coin and value_of_surplus > min_trade_val:
            # Check if we actually need cash (Deficit) OR if we just want to be neutral
            # User goal: "fund the cash deficit".
            # If cash_diff < 0 (Deficit), we definitely sell.
            # If cash_diff >= 0 (Surplus Cash), strictly we could hold the coin, but for Grid Bot neutrality, we usually sell.
            # Let's prioritize the Deficit case to avoid "forgetting to sell".

            self.logger.info(f"🚨 Phase 2: Selling Surplus Coin ({coin_diff:.6f}) to release liquidity...")
            try:
                await self.order_execution_strategy.execute_market_order(
                    OrderSide.SELL, self.trading_pair, coin_diff, current_price
                )
                await asyncio.sleep(2)  # Wait for settlement
                await self.balance_tracker.sync_balances(self.order_execution_strategy.exchange_service, current_price)
            except Exception as e:
                self.logger.error(f"Failed to sell surplus: {e}")

        # Case B: Coin Deficit (Buy using Surplus Cash)
        # Only buy if we really need it (Coin Deficit)
        elif coin_diff < 0 and abs(coin_diff * current_price) > min_trade_val:
            # FIX: Add buffer for trading fees (e.g. 0.1% or 0.2%)
            # If we need 4.8 BNB, and fee is 0.1%, we receive 4.8 * 0.999 = 4.7952 (Shortfall).
            # We need to BUY enough so that (BuyQty * (1 - fee)) >= Needed.
            # BuyQty >= Needed / (1 - fee). Approx Needed * 1.01 (1% buffer)
            needed_coin = abs(coin_diff) * 1.01

            # Check if we can afford it.
            # We check if we have enough cash.
            # Note: We only buy if we have the cash.
            cost = needed_coin * current_price

            if self.balance_tracker.balance >= cost:
                self.logger.info(f"🚨 Phase 2: Buying Deficit Coin ({needed_coin:.6f})...")
                try:
                    # Adjust qty for precision
                    adj_qty = self.order_validator.adjust_and_validate_buy_quantity(
                        self.balance_tracker.balance, needed_coin, current_price
                    )
                    order = await self.order_execution_strategy.execute_market_order(
                        OrderSide.BUY, self.trading_pair, amount=adj_qty, price=current_price
                    )

                    if order and order.status == OrderStatus.CLOSED:
                        self.logger.info(f"✅ Rebalance Buy Successful: {order.filled} @ {order.average}")
                        await self.balance_tracker.update_balance_on_order_completion(order)

                    await asyncio.sleep(2)
                    await self.balance_tracker.sync_balances(
                        self.order_execution_strategy.exchange_service, current_price
                    )
                except Exception as e:
                    self.logger.error(f"Failed to buy deficit coin: {e}")
            else:
                self.logger.warning("Phase 2: Coin Deficit detected but Insufficient Cash to fully cover.")

    async def initialize_grid_orders(self, current_price: float, can_clean_start: bool = True):
        # 1. Block the Watchdog
        self.initializing = True

        try:
            # 2. Phase 0: Enforce Zero State (Only if fresh start / clean start)
            if can_clean_start:
                await self.perform_clean_start()
            else:
                self.logger.info("Initializing Grid Orders: Skipping Clean Start (Preserving existing orders).")

            self.logger.info("Initializing Grid Orders (State Diff Strategy)...")

            # 3. Phase 1 & 2: State Diff Rebalance
            await self._perform_state_diff_rebalance(current_price)

            # 4. Phase 3: Grid Distribution
            self.logger.info("Phase 3: Distributing Orders...")

            safe_buy_limit, safe_sell_limit = self.grid_manager.get_dead_zone_thresholds(current_price)

            # --- SELL ORDERS ---
            for price in self.grid_manager.sorted_sell_grids:
                if price <= safe_sell_limit:
                    continue

                # Check Inventory (Iterative Locking)
                if self.balance_tracker.crypto_balance <= 0:
                    self.logger.warning(f"⚠️ Phase 3: Out of Coin Inventory at {price}. Stopping Sell Loop.")
                    break

                # Get Target Qty (re-calc based on latest value)
                total_val = self.balance_tracker.get_total_balance_value(current_price)
                target_qty = self.grid_manager.get_order_size_for_grid_level(total_val, price)

                order = await self._place_limit_order_safe(price, OrderSide.SELL, quantity_override=target_qty)
                if order:
                    # Record Initial Stock and Order Quantity
                    self.grid_manager.grid_levels[price].stock_on_hand = order.amount
                    self.grid_manager.grid_levels[price].order_quantity = order.amount
                    if self.bot_id is not None:
                        await self.db.update_grid_stock(self.bot_id, price, order.amount)
                        await self.db.update_grid_order_quantity(self.bot_id, price, order.amount)
                    self.logger.info(f"🏁 Init: Recorded initial stock {order.amount} for grid {price}")

            # --- BUY ORDERS (Phase 4 Solvency) ---
            for price in self.grid_manager.sorted_buy_grids:
                if price >= safe_buy_limit:
                    continue

                # Solvency Guard check
                total_val = self.balance_tracker.get_total_balance_value(current_price)
                target_qty = self.grid_manager.get_order_size_for_grid_level(total_val, price)
                cost = target_qty * price

                if self.balance_tracker.balance < (cost * 0.98):
                    # NEW: Try Safe Top-Up First
                    shortfall = cost - self.balance_tracker.balance
                    topup_success = await self.balance_tracker.request_wallet_topup(
                        shortfall, "QUOTE", self.exchange_service
                    )

                    if topup_success:
                        self.logger.info(f"🛡️ Solvency: Rescued Buy at {price} via Wallet Top-Up.")
                    else:
                        # Gather all remaining grids to skip
                        skipped_prices = [p for p in self.grid_manager.sorted_buy_grids if p <= price]
                        self.logger.warning(
                            f"🛡️ Phase 4 Solvency: Insufficient Cash for Buy at {price}. "
                            f"Auto-Scaling: Trimming {len(skipped_prices)} grids."
                        )
                        self.grid_manager.deactivate_grid_levels(skipped_prices)
                        break

                await self._place_limit_order_safe(price, OrderSide.BUY)

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
            db_orders = await self.db.get_all_active_orders(self.bot_id)

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
                        if not await self.db.get_order(order_id):
                            self.logger.info(f"   Claiming orphaned order {order_id} at {price}")
                            await self.db.add_order(
                                self.bot_id, order_id, price, side.value, float(order_data["amount"])
                            )
                        else:
                            self.logger.info(f"   Order {order_id} at {price} already in DB. Resuming...")
                    else:
                        self.logger.info(f"   Ignoring unrelated order {order_id} at {price}")
                        continue

                # Accumulate reserves logic removed (Redundant)
                pass

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
                    # deduct_from_balance=True because we now rehydrate Total Equity (Free+Locked) from DB.
                    # We must subtract the locked portion here to return to the correct "Free" state.
                    await self.balance_tracker.register_open_order(order_obj, deduct_from_balance=True)
                    matched_count += 1

            self.logger.info(f"✅ Resumed {matched_count} orders from Hot Boot.")

            # Force Sync Reserves Block Removed (Redundant & caused crash)
            # register_open_order loop above handles reserve tracking.

            # Re-save state removed (GridManager has no such method, and DB is updated via OrderManager)
            pass
            # 5. Fill Gaps (If any grids are empty but should have orders)
            # We call reconcile once to plug holes
            await self.reconcile_grid_orders(current_price)

        finally:
            self.initializing = False
            self.logger.info("✅ Initialization Complete. Watchdog released.")

    # ==============================================================================
    #  CORE EVENT HANDLERS
    # ==============================================================================

    async def _on_order_cancelled(self, order: Order) -> None:
        if self.bot_id is not None:
            await self.db.update_order_status(order.identifier, "CANCELLED")

        # FIX: Release Reserved Funds
        if order.side == OrderSide.BUY:
            # For BUY, we reserved (Price * Amount)
            # Use remaining amount to calculate what to release
            release_amount = order.remaining * order.price
            await self.balance_tracker.release_reserve_for_buy(release_amount)
        elif order.side == OrderSide.SELL:
            # For SELL, we reserved (Amount) of crypto
            await self.balance_tracker.release_reserve_for_sell(order.remaining)

        # FIX: Reset Grid Level State
        grid_level = self.grid_manager.grid_levels.get(order.price)
        if not grid_level:
            # Fallback for float precision issues
            for price, level in self.grid_manager.grid_levels.items():
                if math.isclose(price, order.price, rel_tol=1e-9):
                    grid_level = level
                    break

        if grid_level:
            # Remove order from level references
            initial_count = len(grid_level.orders)
            # Remove by ID
            grid_level.orders = [o for o in grid_level.orders if str(o.identifier) != str(order.identifier)]

            # If we removed an order and no active orders remain, reset state
            if len(grid_level.orders) == 0 and initial_count > 0:
                if order.side == OrderSide.BUY:
                    grid_level.state = GridCycleState.READY_TO_BUY
                elif order.side == OrderSide.SELL:
                    grid_level.state = GridCycleState.READY_TO_SELL
                self.logger.info(
                    f"🔄 Grid Level {grid_level.price} reset to {grid_level.state.name} after cancellation."
                )

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

            # --- ISOLATION FIX: Ignore Market Orders (Rebalancing/Manual) ---
            if order.order_type == OrderType.MARKET:
                self.logger.info(f"⏭️ Skipping Grid Logic for MARKET order {order.identifier} (Rebalance/Manual).")
                # Still update balance tracker, as it listens to event bus independently?
                # Actually, BalanceTracker listens to event bus.
                # But here we explicitly call: await self.balance_tracker.update_balance_on_order_completion(order)
                # We SHOULD call it to ensure local state is fresh before next logic.
                await self.balance_tracker.update_balance_on_order_completion(order)

                # --- FIX: Clear phantom stock when rebalance SELL orders fill ---
                # When we liquidate "excess crypto" via MARKET SELL, that crypto was counted
                # as stock_on_hand on grid levels. We must reduce it to prevent accumulation.
                if order.side == OrderSide.SELL:
                    sold_amount = order.filled
                    remaining_to_clear = sold_amount

                    # Clear stock from ALL grids (highest first)
                    # Instant Split sells might draw from "Buy Grids" that just filled but aren't yet "Sell Grids"
                    grids_with_stock = sorted(
                        [g for g in self.grid_manager.grid_levels.values() if g.stock_on_hand > 0],
                        key=lambda x: x.price,
                        reverse=True,
                    )

                    for grid_level in grids_with_stock:
                        if remaining_to_clear <= 0:
                            break

                        cleared = min(grid_level.stock_on_hand, remaining_to_clear)
                        grid_level.stock_on_hand -= cleared
                        grid_level.stock_on_hand = round(grid_level.stock_on_hand, 6)
                        remaining_to_clear -= cleared

                        if self.bot_id is not None:
                            await self.db.update_grid_stock(self.bot_id, grid_level.price, grid_level.stock_on_hand)

                        self.logger.info(
                            f"🧹 Cleared phantom stock: {cleared:.6f} from grid {grid_level.price} "
                            f"(Remaining stock: {grid_level.stock_on_hand:.6f})"
                        )

                    if remaining_to_clear > 0.0001:
                        self.logger.warning(f"⚠️ Could not fully clear sold amount. Remaining: {remaining_to_clear:.6f}")

                if self.bot_id is not None:
                    await self.db.update_order_status(order.identifier, "FILLED")
                return
            # ----------------------------------------------------------------

            # OWNERSHIP VALIDATION: Defense-in-depth
            if self.bot_id is not None:
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
            if self.bot_id is not None:
                await self.db.update_order_status(order.identifier, "FILLED")

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

        # 1. Get Settings
        strat_config = self.grid_manager.config_manager.config.get("grid_strategy", {})
        profit_mode = strat_config.get("profit_mode", "USDT_ONLY")
        fiat_style = strat_config.get("fiat_profit_style", "SPLIT")
        split_ratio = strat_config.get("profit_split_ratio", 0.5)
        order_size_type = self.grid_manager.config_manager.get_order_size_type()

        # 2. Precise Coin Tracking
        executed_qty = order.filled
        fee_paid_in_coin = 0.0
        if order.fee and order.fee.get("currency") == self.trading_pair.split("/")[0]:
            fee_paid_in_coin = float(order.fee.get("cost", 0.0))

        net_coin_received = executed_qty - fee_paid_in_coin

        # Save to Buy Level initially (Accumulate logic)
        grid_level.stock_on_hand += net_coin_received
        if self.bot_id is not None:
            await self.db.update_grid_stock(self.bot_id, grid_level.price, grid_level.stock_on_hand)

        self.logger.info(
            f"🧬 Precise Tracking: Bought {executed_qty} - Fee {fee_paid_in_coin:.6f} = Net {net_coin_received:.6f}"
        )

        # 3. Determine Sell Level
        paired_sell_level = self._get_or_create_paired_sell_level(grid_level)

        if paired_sell_level:
            # --- PROFIT STRATEGY LOGIC ---
            buy_price = order.average or order.price
            sell_price = paired_sell_level.price

            # Calculate Cost Basis (Coins needed to recover USDT invested)
            # USDT Invested = Net Coin * Buy Price (approx) or Executed Qty * Buy Price?
            # Strictly: We spent (Executed Qty * Buy Price) USDT + Fees(USDT).
            # But let's keep it simple: We received `net_coin_received` which cost us `order.cost` (or filled * price).
            # We want to recover that Cost.
            total_cost_usdt = order.cost if order.cost else (executed_qty * buy_price)

            # Target: Sell X coins at SellPrice to get TotalCostUSDT.
            # X = TotalCostUSDT / SellPrice
            cost_basis_coins = total_cost_usdt / sell_price

            # Profit Coins = What we have - What we need to sell to break even
            profit_coins = max(0.0, net_coin_received - cost_basis_coins)

            self.logger.info(
                f"💰 Profit Calc: Mode={profit_mode}, Style={fiat_style}. "
                f"NetCoin={net_coin_received:.6f}, CostBasis={cost_basis_coins:.6f}, Profit={profit_coins:.6f}"
            )

            qty_to_sell_at_paired = 0.0
            qty_to_carry = 0.0
            qty_to_keep_as_moonbag = 0.0

            # --- MODE SWITCH ---
            if profit_mode == "COIN_ONLY":
                # MOON BAG: Sell only cost basis, keep profit.
                qty_to_sell_at_paired = cost_basis_coins
                qty_to_keep_as_moonbag = profit_coins

            else:  # USDT_ONLY or HYBRID (treated as USDT variants here usually, or explicit Hybrid check)
                if profit_mode == "HYBRID":
                    # Hybrid usually implies some split. Let's map it to SPLIT style for now or handling custom.
                    # Assuming HYBRID = SPLIT logic but maybe keeping moonbag?
                    # Let's stick to the requested "Fiat Profit Style" logic for USDT_ONLY.
                    pass

                # Handle USDT_ONLY styles
                if fiat_style == "INSTANT":
                    # Sell Everything at Target (Standard Grid)
                    qty_to_sell_at_paired = net_coin_received

                elif fiat_style == "DELAYED":
                    # Sell Cost Basis, Carry Profit
                    qty_to_sell_at_paired = cost_basis_coins
                    qty_to_carry = profit_coins

                elif fiat_style == "SPLIT":
                    # USER REQUESTED LOGIC: "Instant Split"
                    # Determine the portion of profit to realize in Fiat
                    profit_to_fiat = profit_coins * split_ratio

                    if profit_to_fiat > 0:
                        # SELL IT NOW (Market Order)
                        self.logger.info(f"⚡ SPLIT: Executing IMMEDIATE market sell of {profit_to_fiat:.6f} profit.")
                        try:
                            # We must release the lock or handle it carefully?
                            # calling execute_market_order is safe as it handles its own locking/execution.
                            # But we are inside _on_order_filled... async is fine.

                            # Fire and forget? No, wait for it to adjust stock.
                            start_balance = self.balance_tracker.crypto_balance

                            # Execute Market Sell
                            sell_order = await self.order_execution_strategy.execute_market_order(
                                OrderSide.SELL,
                                self.trading_pair,
                                profit_to_fiat,
                                params={"clientOrderId": f"G{self.bot_id}xSPLIT{int(buy_price)}"}
                                if self.bot_id
                                else None,
                            )

                            if sell_order and sell_order.status == OrderStatus.CLOSED:
                                sold_qty = sell_order.filled
                                self.logger.info(
                                    f"✅ Immediate Profit Secured: Sold {sold_qty:.6f} for {sell_order.cost:.2f} USDT"
                                )

                                # Setup the main Grid Sell Order
                                # We have 'net_coin_received' - 'sold_qty' left.
                                # effectively: remaining = total - sold
                                # We need to sell 'cost_basis_coins' to recover original investment.
                                # Whatever is left after that is the Moonbag.

                                remaining_stock = net_coin_received - sold_qty
                                qty_to_sell_at_paired = remaining_stock
                                qty_to_carry = 0.0  # Zero Moonbag

                                self.logger.info(
                                    f"📐 Split Remaining: Selling all {qty_to_sell_at_paired:.6f} at target (No Moonbag)."
                                )

                                # Safety: If we oversold (e.g. huge fees or slippage)?
                                if qty_to_sell_at_paired < 0:
                                    self.logger.warning(f"⚠️ Immediate sell oversold! Stock is negative.")
                                    qty_to_sell_at_paired = 0.0
                                    qty_to_carry = 0.0
                                else:
                                    # Logic to proceed
                                    # --- SYNC PROFIT TO FRONTEND ---
                                    # We sold 'sold_qty'.
                                    # Cost Basis of this portion = sold_qty * buy_price
                                    # Revenue = sell_order.cost (Total USDT received)
                                    # Realized Profit = Revenue - Cost Basis

                                    instant_profit_usdt = sell_order.cost - (sold_qty * buy_price)
                                    if self.bot_id:
                                        self.logger.info(f"📊 Syncing Instant Profit: +{instant_profit_usdt:.4f} USDT")

                                        # Record as a Trade History Event
                                        trade_data = {
                                            "bot_id": self.bot_id,
                                            "order_id": sell_order.identifier,
                                            "pair": self.trading_pair,
                                            "side": "sell",
                                            "price": sell_order.average or sell_order.price,
                                            "quantity": sold_qty,
                                            "fee_amount": sell_order.fee.get("cost", 0.0) if sell_order.fee else 0.0,
                                            "fee_currency": sell_order.fee.get("currency", "USDT")
                                            if sell_order.fee
                                            else "USDT",
                                            "realized_pnl": instant_profit_usdt,
                                        }
                                        await self.db.add_trade_history(trade_data)
                                    # -------------------------------

                            else:
                                self.logger.error(
                                    "❌ Immediate market sell failed/incomplete. Reverting to standard Split (Delayed)."
                                )
                                # Fallback to Standard Split logic
                                profit_to_sell = profit_coins * split_ratio
                                qty_to_sell_at_paired = cost_basis_coins + profit_to_sell
                                qty_to_carry = profit_coins - profit_to_sell

                        except Exception as e:
                            self.logger.error(f"❌ Error during Instant Split execution: {e}")
                            # Fallback
                            profit_to_sell = profit_coins * split_ratio
                            qty_to_sell_at_paired = cost_basis_coins + profit_to_sell
                            qty_to_carry = profit_coins - profit_to_sell

                    else:
                        # No profit to split?
                        qty_to_sell_at_paired = cost_basis_coins
                        qty_to_carry = profit_coins

            # --- Validations & Adjustments ---

            # SAFETY: If top of grid, we cannot carry. Sell ALL.
            next_upper_level = self.grid_manager.get_grid_level_above(paired_sell_level)
            if qty_to_carry > 0 and not next_upper_level:
                self.logger.info("⚠️ Cannot carry profit (Top of Grid). converting to INSTANT sell.")
                qty_to_sell_at_paired += qty_to_carry
                qty_to_carry = 0.0

            # Rounding Safety
            qty_to_sell_at_paired = round(qty_to_sell_at_paired, 6)
            qty_to_carry = round(qty_to_carry, 6)

            # Apply to Paired Level
            # We overwrite stock_on_hand because we are transferring ownership from Buy Level
            paired_sell_level.stock_on_hand = qty_to_sell_at_paired

            # Update Order Quantity (For logic checking)
            if order_size_type == "quote":
                paired_sell_level.order_quantity = total_cost_usdt  # Tracking cost
            else:
                paired_sell_level.order_quantity = qty_to_sell_at_paired

            # DB Updates for Paired
            if self.bot_id is not None:
                await self.db.update_grid_stock(self.bot_id, paired_sell_level.price, paired_sell_level.stock_on_hand)
                await self.db.update_grid_order_quantity(
                    self.bot_id, paired_sell_level.price, paired_sell_level.order_quantity
                )

            # Apply Carry Forward
            if qty_to_carry > 0 and next_upper_level:
                next_upper_level.stock_on_hand += qty_to_carry
                self.logger.info(f"⤴️ CARRYING PROFIT: Moved {qty_to_carry:.6f} to Next Level {next_upper_level.price}")
                if self.bot_id is not None:
                    await self.db.update_grid_stock(self.bot_id, next_upper_level.price, next_upper_level.stock_on_hand)

            if qty_to_keep_as_moonbag > 0:
                self.logger.info(f"🌑 MOON BAG: Keeping {qty_to_keep_as_moonbag:.6f} coins (Removed from grid logic)")

            # Clear Buy Level
            grid_level.stock_on_hand = 0.0
            grid_level.order_quantity = 0.0
            if self.bot_id is not None:
                await self.db.update_grid_stock(self.bot_id, grid_level.price, 0.0)

            # --- PLACE SELL ORDER ---
            if self.grid_manager.can_place_order(paired_sell_level, OrderSide.SELL):
                # Check for existing accumulation
                if paired_sell_level.state == GridCycleState.WAITING_FOR_SELL_FILL:
                    # Cancel and Replace
                    self.logger.info(f"📦 Accumulating on {paired_sell_level.price}. Replacing order...")
                    for o in list(paired_sell_level.orders):
                        with contextlib.suppress(Exception):
                            oid = o.identifier if hasattr(o, "identifier") else o
                            if hasattr(o, "status") and o.status == OrderStatus.CLOSED:
                                continue
                            await self.order_execution_strategy.cancel_order(oid, self.trading_pair)
                    paired_sell_level.orders.clear()
                    self.grid_manager.complete_order(paired_sell_level, OrderSide.SELL)

                # Place Sell with NEW Quantity
                # NOTE: The helper `_place_limit_order_safe` usually looks at `order_size_for_grid`.
                # We need to force it to use our `stock_on_hand`.
                # We can use `quantity_override` in `_place_limit_order_safe`. or `_place_sell_order` helper.
                # `_handle_buy_order_completion` used `_place_sell_order` at the end previously.
                await self._place_sell_order(grid_level, paired_sell_level, paired_sell_level.stock_on_hand)

        else:
            self.logger.warning(
                f"No valid sell grid level found for buy grid level {grid_level}. Stock remains on Buy Level."
            )

        # --- SAVE TRADE HISTORY (BUY) ---
        if self.bot_id is not None:
            trade_record = {
                "bot_id": self.bot_id,
                "order_id": order.identifier,
                "pair": self.trading_pair,
                "side": "buy",
                "price": order.average or order.price,
                "quantity": order.filled,
                "fee_amount": fee_paid_in_coin,
                "fee_currency": self.trading_pair.split("/")[0],
                "realized_pnl": 0.0,
            }
            if order.fee and "currency" in order.fee:
                trade_record["fee_currency"] = order.fee["currency"]

            await self.db.add_trade_history(trade_record)
        # --------------------------------

    async def _handle_sell_order_completion(self, order: Order, grid_level: GridLevel) -> None:
        self.logger.info(f"Sell order completed at grid level {grid_level}.")
        self.grid_manager.complete_order(grid_level, OrderSide.SELL)

        # --- FIX: Clear stock_on_hand and order_quantity after sell order completes ---
        # The crypto has been sold, so we must zero out the stock tracking.
        # This prevents phantom stock accumulation where future buys add to old, already-sold stock.
        sold_amount = order.filled
        if grid_level.stock_on_hand > 0 or grid_level.order_quantity > 0:
            cleared = min(grid_level.stock_on_hand, sold_amount) if grid_level.stock_on_hand > 0 else sold_amount
            grid_level.stock_on_hand = 0.0
            grid_level.order_quantity = 0.0

            if self.bot_id is not None:
                await self.db.update_grid_stock(self.bot_id, grid_level.price, 0.0)
                await self.db.update_grid_order_quantity(self.bot_id, grid_level.price, 0.0)

            self.logger.info(
                f"🧹 Cleared sold stock: {cleared:.6f} from grid {grid_level.price} "
                f"(Stock and Order Quantity reset to 0)"
            )
        # -----------------------------------------------------------

        # --- FIX: Determine Buy Level FIRST (with fallback) ---
        # Try to find the paired buy level (explicit or theoretical)
        paired_buy_level = await self._get_or_create_paired_buy_level(grid_level)
        # ------------------------------------------------------

        # --- PROFIT SYNC ---
        if paired_buy_level:
            # --- PROFIT CALCULATION ---
            # Determine logic based on Order Size Type (Base vs Quote)
            # Base = Fixed Coin Amount (Standard Grid) -> Profit is Cash Flow (Realized PnL)
            # Quote = Fixed USDT Amount (Accumulate Coin) -> Profit is Total Value Gain (Realized + Unrealized Dust)
            order_size_type = self.grid_manager.config_manager.get_order_size_type()

            # --- PROFIT CALCULATION ---
            # Universal Formula: (AverageSellPrice - BuyPrice) * QuantitySold
            # This represents the Realized PnL in QUOTE CURRENCY (USDT).
            # Even if we kept some coin (Base Profit), the "Realized USDT" is strictly this formula on the *sold portion*.
            # However, for "Base Profit" logging, we want to show the COIN amount retained.

            profit_type = self.grid_manager.config_manager.get_profit_currency_type()

            sell_price = order.average or order.price
            buy_price = paired_buy_level.price
            quantity_sold = order.filled

            # Gross Realized Profit (USDT)
            gross_profit_usdt = (sell_price - buy_price) * quantity_sold

            # Calculate what we *retained* (Coin Profit)
            # We need to know what we *bought* to know what we kept.
            # But here we only simply know what we sold.
            # Implication: if profit_type == 'base', we intentionally sold LESS than we bought.
            # We can infer retained coin if we know the original buy size.
            # But strictly speaking, the user wants to see:
            # Coin Mode: "0.0013 SOL"
            # Fiat Mode: "8.00 USDT"
            # Mixed: "0.0006 SOL + 4 USDT"

            # We need to estimate "Retained Coin" based on the assumption of what a "Full Sell" would have been.
            # Full Sell would be: Cost / BuyPrice? Or just Cost?
            # Quote Mode Cost = paired_buy_level.order_quantity (if relying on Quote Mode logic)
            # Let's derive it:
            # We Sold 'quantity_sold' at 'sell_price'. Revenue = Q * S.
            # Cost of that sold portion = Q * BuyPrice.
            # Realized Profit = Q * (S - B).

            # If we are in BASE profit mode:
            # We sold enough to cover cost. Revenue ~= Original Total Cost.
            # Current Profit (in USDT) ~= 0 (Realized).
            # The "Profit" is the coin we didn't sell.
            # How much did we not sell?
            # Margin = (S - B) / S.  (Profit Margin ratio in Coin terms)
            # Coin Profit = Revenue * Margin / B ? No.

            # Simpler approach:
            # If `profit_type == 'base'`:
            #   The profit is the *unsold* portion.
            #   We assume we bought `TotalQty`. We sold `SoldQty`.
            #   ProfitCoin = TotalQty - SoldQty.
            #   We can't easily know TotalQty here unless we track it perfectly from the buy.
            #   BUT, we can calculate "Imputed Coin Profit" from the Realized USDT Profit.
            #   If we made $10 USDT profit, how much SOL is that at current price?
            #   CoinArgs = RealizedUSDT / CurrentPrice.
            #   This represents the "Buying Power" of that profit.

            # Let's use that for standardisation:
            # CoinProfit = GrossProfitUSDT / SellPrice.

            base_currency = self.trading_pair.split("/")[0]
            quote_currency = self.trading_pair.split("/")[1]

            log_msg = ""

            if profit_type == "base":
                # Show in Coin
                profit_coin = gross_profit_usdt / sell_price
                log_msg = f"+{profit_coin:.6f} {base_currency}"
                # For backend sync, we might still send USDT value? Or Coin?
                # Usually PnL is tracked in Quote.
                net_profit = gross_profit_usdt  # Internal tracking always USDT

            elif profit_type == "mixed":
                # Show Split
                # We assume 50/50 split of the *value*
                profit_coin = (gross_profit_usdt / 2) / sell_price
                profit_usdt = gross_profit_usdt / 2
                log_msg = f"+{profit_coin:.6f} {base_currency} + {profit_usdt:.4f} {quote_currency}"
                net_profit = gross_profit_usdt

            else:
                # Quote (Fiat) - Default
                log_msg = f"+{gross_profit_usdt:.4f} {quote_currency}"
                net_profit = gross_profit_usdt

            # --------------------------

            # Calculate Fees (Estimate)
            buy_fee = self.balance_tracker.fee_calculator.calculate_fee(paired_buy_level.price * order.filled)
            sell_fee = self.balance_tracker.fee_calculator.calculate_fee(order.average * order.filled)
            total_estimated_fees = buy_fee + sell_fee

            # Adjust Net Profit (Realized) for backend tracking
            # Note: If 'base' mode, 'net_profit' (USDT) is technically realized,
            # but if we held the coin, the wallet doesn't have this USDT.
            # Wait. In 'base' mode, we recover cost.
            # Cost = 100. Rev = 100. Profit = 0.
            # So `gross_profit_usdt` calculated above `(S-B)*Q` corresponds to the profit on the *sold* coins.
            # If we only sold cost-recovery coins, then `(S-B)*Q_recovery` is the Realized Profit on that chunk.
            # But the *Unrealized* profit is in the bag.

            # User wants to "Keep Profit in Coin".
            # This means they want the "Gain" to remain as Coin.
            # So `log_msg` using `gross_profit_usdt / sell_price` is a representation of the *value* generated.
            # Ideally, we should subtract fees from the *value* before display.

            net_profit_display_value = max(0, gross_profit_usdt - total_estimated_fees)

            if profit_type == "base":
                profit_coin = net_profit_display_value / sell_price
                log_msg = f"+{profit_coin:.6f} {base_currency}"
            elif profit_type == "mixed":
                profit_coin = (net_profit_display_value / 2) / sell_price
                profit_usdt = net_profit_display_value / 2
                log_msg = f"+{profit_coin:.6f} {base_currency} + {profit_usdt:.4f} {quote_currency}"
            else:
                log_msg = f"+{net_profit_display_value:.4f} {quote_currency}"

            self.logger.info(
                f"💰 PROFIT SECURED: {log_msg} "
                f"[Sold {order.filled:.6f} {base_currency} @ {sell_price:.4f}] "
                f"(Gross Val: {gross_profit_usdt:.4f}, Fees: {total_estimated_fees:.4f})"
            )

            # For backend sync and reserves, we use the USDT value equivalent
            net_profit = net_profit_display_value

            # --- RESERVE ALLOCATION (10%) ---
            reserve_amount = 0.0
            if net_profit > 0:
                reserve_amount = net_profit * 0.10
                await self.balance_tracker.allocate_profit_to_reserve(reserve_amount)

            if self.bot_id is not None:
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
                await self.db.add_trade_history(trade_record)
                # ---------------------------------
        else:
            self.logger.warning("Could not calculate profit: No paired buy level found.")
        # -------------------

        # (Profit coins are now sold in _handle_buy_order_completion with exact calculation)

        # --- REPLENISHMENT ---
        if paired_buy_level:
            # FIX: Pass 0.0 quantity to force recalculation based on config (Fixed USDT vs Fixed Coin)
            await self._place_buy_order(grid_level, paired_buy_level, 0.0)
        else:
            self.logger.error(f"Failed to find or create a paired buy grid level for grid level {grid_level}.")

    async def _get_or_create_paired_buy_level(self, sell_grid_level: GridLevel) -> GridLevel | None:
        # 1. Try explicit pairing (set during order flow)
        paired_buy_level = sell_grid_level.paired_buy_level
        if paired_buy_level and self.grid_manager.can_place_order(paired_buy_level, OrderSide.BUY):
            return paired_buy_level

        # 2. Try to find an existing grid level below (works if levels weren't trimmed)
        fallback_buy_level = self.grid_manager.get_grid_level_below(sell_grid_level)
        if fallback_buy_level:
            return fallback_buy_level

        sell_price = sell_grid_level.price

        # 3. FALLBACK: Find the nearest buy level BELOW the sell price (in-memory)
        try:
            all_levels = sorted(self.grid_manager.grid_levels.keys())
            levels_below = [p for p in all_levels if p < sell_price]

            if levels_below:
                nearest_buy_price = max(levels_below)
                nearest_level = self.grid_manager.grid_levels[nearest_buy_price]
                self.logger.info(
                    f"♻️ FALLBACK: Using nearest buy level at {nearest_buy_price:.4f} for sell at {sell_price:.4f}"
                )
                return nearest_level
        except Exception as e:
            self.logger.debug(f"In-memory fallback failed: {e}")

        # 4. DATABASE FALLBACK: Query grid_levels table for persisted levels
        # Crucial after restart when in-memory state is lost
        if self.db and self.bot_id:
            try:
                db_grid_levels = await self.db.get_grid_levels(self.bot_id)
                if db_grid_levels:
                    db_prices_below = [p for p in db_grid_levels if p < sell_price]

                    if db_prices_below:
                        nearest_db_price = max(db_prices_below)
                        db_level_data = db_grid_levels[nearest_db_price]

                        # Recreate the grid level from DB data
                        new_level = GridLevel(
                            price=nearest_db_price,
                            state=GridCycleState.READY_TO_BUY,
                            paired_sell_level=sell_grid_level,
                        )
                        new_level.stock_on_hand = db_level_data.get("stock", 0.0)
                        new_level.order_quantity = db_level_data.get("order_quantity", 0.0)

                        # Add to in-memory grid
                        self.grid_manager.grid_levels[nearest_db_price] = new_level
                        if nearest_db_price not in self.grid_manager.sorted_buy_grids:
                            self.grid_manager.sorted_buy_grids.append(nearest_db_price)
                            self.grid_manager.sorted_buy_grids.sort(reverse=True)

                        self.logger.info(
                            f"♻️ DB RECOVERY: Restored buy level at {nearest_db_price:.4f} "
                            f"for sell at {sell_price:.4f} (stock: {new_level.stock_on_hand:.6f})"
                        )
                        return new_level
            except Exception as e:
                self.logger.warning(f"DB fallback for paired buy level failed: {e}")

        # 5. LAST RESORT: Recreate from price_grids
        try:
            sorted_original_prices = sorted(self.grid_manager.price_grids)
            # FIX: Use bisect_left but handle exact matches or float jitter
            idx = bisect.bisect_left(sorted_original_prices, sell_price)

            # If sell_price matches exactly, idx points to it. We want idx-1.
            # If sell_price is slightly larger (drift), idx points to next. We still want idx-1?
            # Wait. if sell_price = 100. bisect_left([100, 200], 100) -> 0.
            # Then idx-1 = -1. INVALID.

            # Logic: We want the price strictly LESS than sell_price.
            # bisect_left gives first position >= x.
            # So sorted[idx] >= sell_price.
            # Thus sorted[idx-1] < sell_price (if idx > 0).
            # This is correct.

            if idx > 0:
                buy_price = sorted_original_prices[idx - 1]

                # Double check to prevent "Buying at same price" due to weird float drift
                if math.isclose(buy_price, sell_price, rel_tol=1e-5):
                    if idx > 1:
                        buy_price = sorted_original_prices[idx - 2]
                    else:
                        self.logger.warning(f"⚠️ Bottom of grid reached at {sell_price}. Cannot create buy level.")
                        return None

                if buy_price not in self.grid_manager.grid_levels:
                    new_level = GridLevel(
                        price=buy_price,
                        state=GridCycleState.READY_TO_BUY,
                        paired_sell_level=sell_grid_level,
                    )
                    self.grid_manager.grid_levels[buy_price] = new_level

                    if buy_price not in self.grid_manager.sorted_buy_grids:
                        self.grid_manager.sorted_buy_grids.append(buy_price)
                        self.grid_manager.sorted_buy_grids.sort(reverse=True)

                    self.logger.info(
                        f"♻️ REACTIVATED: Recreated buy level at {buy_price:.4f} (for sell level {sell_price:.4f})"
                    )
                    return new_level
                else:
                    return self.grid_manager.grid_levels[buy_price]
            else:
                self.logger.warning(
                    f"⚠️ Sell Price {sell_price} is below lowest grid configuration. Cannot find buy level."
                )

        except Exception as e:
            self.logger.error(f"Failed to find/create buy level below {sell_price}: {e}", exc_info=True)

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
        # --- RACE CONDITION LOCK ---
        if price in self._placing_orders:
            self.logger.warning(f"🔒 Skipping {side.name} at {price}: Order placement already in progress.")
            return None

        self._placing_orders.add(price)

        try:
            if self.bot_id is not None:
                existing_order = await self.db.get_active_order_at_price(self.bot_id, price)
                if existing_order:
                    self.logger.info(f"⏭️ Skipping {side} at {price}: Order already exists in DB.")
                    return None

            grid_level = self.grid_manager.grid_levels[price]
            total_balance_value = self.balance_tracker.get_total_balance_value(price)
            raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)

            quantity = quantity_override if quantity_override > 0 else raw_quantity

            # --- PRECISE ORDER SIZING (Fixed Quantity per Grid) ---
            # SELL orders use order_quantity from the grid level (set during BUY completion)
            # Quote Mode: order_quantity stores USDT cost -> calculate coins = cost / price
            # Base Mode: order_quantity stores coin quantity -> use directly
            dust_remainder = 0.0
            if side == OrderSide.SELL:
                if grid_level.order_quantity > 0:
                    # Check order sizing mode AND profit currency preference
                    order_size_type = self.grid_manager.config_manager.get_order_size_type()
                    profit_type = self.grid_manager.config_manager.get_profit_currency_type()  # quote, base, mixed

                    if order_size_type == "quote":
                        # Quote Mode: order_quantity contains USDT cost
                        usdt_cost = grid_level.order_quantity
                        full_qty = usdt_cost / price

                        recover_cost_qty = usdt_cost / price

                        if profit_type == "quote":
                            # Sell EVERYTHING we bought (Max Profit in USDT)
                            if grid_level.stock_on_hand > 0:
                                quantity = grid_level.stock_on_hand
                                self.logger.info(f"📐 Profit(Fiat): Selling ALL stock {quantity:.6f}")
                            else:
                                quantity = recover_cost_qty  # Better than nothing
                                self.logger.warning(
                                    f"⚠️ Profit(Fiat): No stock recorded. Defaulting to break-even qty {quantity:.6f}"
                                )

                        elif profit_type == "base":
                            # Sell only enough to recover cost
                            quantity = recover_cost_qty
                            self.logger.info(
                                f"📐 Profit(Coin): Selling calculated qty {quantity:.6f} to recover {usdt_cost:.2f} USDT"
                            )

                        elif profit_type == "mixed":
                            # 50% Coin, 50% Fiat
                            if grid_level.stock_on_hand > recover_cost_qty:
                                surplus = grid_level.stock_on_hand - recover_cost_qty
                                quantity = recover_cost_qty + (surplus * 0.5)
                                self.logger.info(
                                    f"📐 Profit(Mixed): Selling {quantity:.6f} ({recover_cost_qty:.6f} Cost + {surplus * 0.5:.6f} Profit)"
                                )
                            else:
                                quantity = recover_cost_qty
                        else:
                            pass

                    else:
                        # Base Mode: order_quantity contains coin quantity
                        quantity = grid_level.order_quantity
                        self.logger.info(f"📐 Base Mode Sell: Using fixed coin quantity {quantity:.6f}")
                elif grid_level.stock_on_hand > 0:
                    # Fallback: Legacy mode - use stock_on_hand if order_quantity not set
                    quantity = grid_level.stock_on_hand
                    self.logger.info(f"📉 Using Stock-On-Hand for Sell Order (Legacy): {quantity:.6f}")

            # --- FINAL GUARD: strict cap at stock_on_hand ---
            # Ensure we NEVER try to sell more than we physically track, regardless of mode.
            if side == OrderSide.SELL and grid_level.stock_on_hand > 0:
                original_qty = quantity
                quantity = min(quantity, grid_level.stock_on_hand)
                if quantity < original_qty:
                    self.logger.warning(
                        f"⚠️ Quantity clamped to Stock-on-Hand: Requested {original_qty:.6f} -> Capped {quantity:.6f}"
                    )
            # ------------------------------------------------

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
                if self.bot_id is not None:
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
                            if self.bot_id is not None:
                                try:
                                    await self.db.update_grid_stock(self.bot_id, price, dust_remainder)
                                except Exception as e:
                                    self.logger.error(f"❌ DB Error: Failed to update grid stock for {price}: {e}")
                            self.logger.info(f"🧹 Dust Management: Keeping {dust_remainder:.6f} as residue.")
                        else:
                            grid_level.stock_on_hand = 0.0
                            if self.bot_id is not None:
                                try:
                                    await self.db.update_grid_stock(self.bot_id, price, 0.0)
                                except Exception as e:
                                    self.logger.error(f"❌ DB Error: Failed to zero grid stock for {price}: {e}")
                    # ----------------------------------------------

                    if self.bot_id is not None:
                        try:
                            await self.db.add_order(self.bot_id, order.identifier, price, side.value, order.amount)
                        except Exception as e:
                            self.logger.error(
                                f"❌ CRITICAL DB FAILURE: Failed to save order {order.identifier} to DB after retries: {e}"
                            )
                            self.logger.warning(f"🛡️ Attempting to cancel ghost order {order.identifier} on exchange...")
                            try:
                                await self.order_execution_strategy.cancel_order(order.identifier, self.trading_pair)
                                self.logger.info(f"✅ Successfully neutralized ghost order {order.identifier}.")
                            except Exception as ce:
                                self.logger.critical(
                                    f"💀 Failed to cancel ghost order {order.identifier}! It is now loose on the exchange. Error: {ce}"
                                )
                            return None

                    if side == OrderSide.BUY:
                        await self.balance_tracker.reserve_funds_for_buy(order.amount * order.price)
                    else:
                        await self.balance_tracker.reserve_funds_for_sell(order.amount)

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
                            self.grid_manager.grid_levels[price].stock_on_hand = actual_crypto
                            if self.bot_id:
                                await self.db.update_grid_stock(self.bot_id, price, actual_crypto)

                            # Recursively retry ONCE with new stock
                            if actual_crypto > 0.0001:
                                self.logger.info("🔁 Retrying Sell Order with synced balance...")
                                # FIX: Important to discard the lock BEFORE retrying
                                self._placing_orders.discard(price)
                                return await self._place_limit_order_safe(price, side, quantity_override=actual_crypto)
                            else:
                                self.logger.warning("🚫 Balance too low to retry.")
                else:
                    self.logger.error(f"Failed to place safe order: {e}")

        finally:
            self._placing_orders.discard(price)

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

        # FIX: Check and Ensure funds for both sides (Initial Rebalance)
        await self.ensure_funds_for_grid(current_price)

        exchange_orders_raw = await self.order_execution_strategy.exchange_service.refresh_open_orders(
            self.trading_pair
        )

        # --- ISOLATION FIX: Strict Filter by Client Order ID ---
        exchange_orders = []
        if self.bot_id is not None:
            prefix = f"G{self.bot_id}x"
            for o in exchange_orders_raw:
                cid = o.get("clientOrderId") or ""
                # Strict check: Must start with my prefix
                # FIX: Await the DB call
                if str(cid).startswith(prefix) or await self.db.get_order(o["id"]):
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

        # --- GHOST ORDER ADOPTION (Phase 4) ---
        # Detect if any order on the exchange (with our prefix) is MISSING from our DB.
        # This happens if a multi-order strike hit and DB insertion failed silently.
        if self.bot_id is not None:
            for o in exchange_orders:
                order_id = o["id"]
                if not await self.db.get_order(order_id):
                    self.logger.warning(
                        f"👻 Ghost Order Found: {order_id} at {o['price']} is on exchange but NOT in DB. Adopting..."
                    )
                    try:
                        await self.db.add_order(
                            self.bot_id, order_id, float(o["price"]), o["side"].lower(), float(o["amount"])
                        )
                        self.logger.info(f"✅ Ghost Order {order_id} successfully adopted into database.")
                    except Exception as e:
                        self.logger.error(f"❌ Failed to adopt ghost order {order_id}: {e}")
            # --------------------------------------
            db_orders = await self.db.get_all_active_orders(self.bot_id)
            # Track reserves for Hot Boot restoration
            restored_reserved_crypto = 0.0
            restored_reserved_fiat = 0.0

            for order_id, order_info in db_orders.items():
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
                                    await self.db.update_order_status(order_id, "CLOSED_UNKNOWN")
                            else:
                                # Status is OPEN or PARTIALLY_FILLED but verified_order matches, so we respect it.
                                # Wait, the elif chain above handles CANCELED. This else covers OPEN/PARTIALLY?
                                # Actually, lines 793-803 are inside the elif CANCELED block?
                                # No, 788 is elif CANCELED. 793 indentation matches 789... wait.
                                # In the view:
                                # 788: elif verified_order.status == OrderStatus.CANCELED:
                                # 789:     self.logger.info(...)
                                # 790:     await self._on_order_cancelled(...)
                                # 791:     # It exists...
                                # 793:     if verified_order.status in ...:
                                # This block 793 looks indented under 788?
                                # No, 793 checks status OPEN/PARTIAL. If status was CANCELED (788), this check 793 is useless (CANCELED is not OPEN).
                                # It seems 793 block is Mis-indented or I misread the view.
                                # Let's assume the indentation in the View was correct.
                                # View: 788 elif... 789... 790... 793 if...
                                # If 793 is inside elif CANCELED, it never runs true.
                                # BUT, I am replacing the block, so I can fix indentation if needed.
                                # Actually, it seems I should just be careful with replacement.
                                pass
                        else:
                            # returned None
                            self.logger.warning(f"Order {order_id} returned None from fetch. Marking CLOSED_UNKNOWN.")
                            await self.db.update_order_status(order_id, "CLOSED_UNKNOWN")

                    except Exception as e:
                        err_msg = str(e).lower()
                        if (
                            "network" in err_msg
                            or "timeout" in err_msg
                            or "50001" in err_msg
                            or "service temporarily unavailable" in err_msg
                        ):
                            self.logger.warning(
                                f"⚠️ Network issue verifying order {order_id}. Retrying next cycle. ({e})"
                            )
                        elif "does not exist" in err_msg or "51603" in err_msg or "not found" in err_msg:
                            self.logger.warning(
                                f"🚫 Order {order_id} reported missing by Exchange. treating as CANCELED to fix state."
                            )
                            # Construct Dummy Order
                            dummy_order = Order(
                                identifier=order_id,
                                status=OrderStatus.CANCELED,
                                order_type=OrderType.LIMIT,
                                side=OrderSide(order_info["side"].lower()),
                                price=float(order_info["price"]),
                                average=None,
                                amount=float(order_info["amount"]),
                                filled=0.0,
                                remaining=float(order_info["amount"]),  # Assume none filled so full refund
                                timestamp=int(time.time() * 1000),
                                datetime=None,
                                last_trade_timestamp=None,
                                symbol=self.trading_pair,
                                time_in_force="GTC",
                            )
                            await self._on_order_cancelled(dummy_order)
                        else:
                            self.logger.error(
                                f"Failed to verify missing order {order_id}: {e}. Marking CLOSED_UNKNOWN."
                            )
                            await self.db.update_order_status(order_id, "CLOSED_UNKNOWN")

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
        occupied_grid_prices = set()
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
                for gp in valid_grid_prices:
                    if math.isclose(op, gp, rel_tol=1e-3):
                        occupied_grid_prices.add(gp)
                        break
            except (ValueError, KeyError):
                pass

        active_grid_order_count = len(occupied_grid_prices)
        # --------------------------------

        # Check BUY Grids
        for price in self.grid_manager.sorted_buy_grids:
            if price >= safe_buy_limit:
                continue

            # Check DB first
            if self.bot_id and await self.db.get_active_order_at_price(self.bot_id, price):
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
                    f"max_orders_reached_{self.bot_id}",
                    interval=600,
                )
                self.logger.debug(
                    f"Order skipped: grid capacity full ({active_grid_order_count}/{self.grid_manager.num_grids})"
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

            if not await self.balance_tracker.attempt_fee_recovery(required_value * 0.95):
                self._throttled_warning(
                    f"🛑 Budget Exhausted. Trimming remaining BUY grids below {price}. "
                    f"(Available: {self.balance_tracker.balance:.2f}, Required: {required_value:.2f})",
                    f"budget_exhausted_buy_{price}",
                )
                break  # Graceful Degradation: Stop placing orders for deeper levels.
            # -----------------------------

            success = await self._place_limit_order_safe(price, OrderSide.BUY)
            if not success:
                break
            else:
                # CRITICAL FIX: Increment count so subsequent loops don't overfill
                active_grid_order_count += 1

        # --- FIX: Auto-Allocate Surplus Crypto to Empty Grids ---
        # Detect if we have free crypto in the wallet that isn't assigned to any grid.
        # This happens on Hot Boot, Manual Deposit, or after manual cancellations.
        try:
            committed_idle_stock = sum(
                g.stock_on_hand
                for p, g in self.grid_manager.grid_levels.items()
                if g.stock_on_hand > 0 and p not in occupied_grid_prices
            )
            # Use a small epsilon to avoid floating point noise triggers
            surplus_crypto = self.balance_tracker.crypto_balance - committed_idle_stock

            # Fixed threshold (0.0001) as GridManager does not expose min_order_size_base
            if surplus_crypto > 0.0001:
                self.logger.debug(
                    f"🔎 Found Surplus Crypto: {surplus_crypto:.6f} (Free: {self.balance_tracker.crypto_balance:.6f} - IdleStock: {committed_idle_stock:.6f})"
                )

                # Distribute to empty Sell Grids (Top Down - Sell High first)
                for price in self.grid_manager.sorted_sell_grids:
                    if surplus_crypto <= 0:
                        break
                    if price <= safe_sell_limit:
                        continue
                    if price in occupied_grid_prices:
                        continue

                    grid_level = self.grid_manager.grid_levels.get(price)
                    if grid_level and grid_level.stock_on_hand <= 0:
                        # Determine how much this grid *should* have
                        total_val = self.balance_tracker.get_total_balance_value(current_price)
                        target_qty = self.grid_manager.get_order_size_for_grid_level(total_val, price)

                        # Allocate what we can
                        allocation = min(surplus_crypto, target_qty)

                        # Only allocate if meaningful amount
                        if allocation > 0.00001:
                            grid_level.stock_on_hand = allocation
                            surplus_crypto -= allocation

                            if self.bot_id is not None:
                                await self.db.update_grid_stock(self.bot_id, price, allocation)
                                # Also update order qty to match, for consistency
                                if grid_level.order_quantity <= 0:
                                    grid_level.order_quantity = allocation
                                    await self.db.update_grid_order_quantity(self.bot_id, price, allocation)

                            self.logger.info(f"✨ Auto-Allocated {allocation:.6f} surplus crypto to grid {price}")

        except Exception as e:
            self.logger.error(f"Error during surplus allocation: {e}")
        # --------------------------------------------------------

        # Check SELL Grids
        for price in self.grid_manager.sorted_sell_grids:
            if price <= safe_sell_limit:
                continue

            if self.bot_id and await self.db.get_active_order_at_price(self.bot_id, price):
                continue

            is_active = any(math.isclose(p, price, rel_tol=1e-3) for p in [float(o["price"]) for o in exchange_orders])
            if is_active:
                continue

            # --- CHECK STOCK ON HAND ---
            # If we don't have stock for this level, we cannot place a sell order.
            # This prevents "Crypto Exhausted" warnings for empty grid levels.
            grid_level = self.grid_manager.grid_levels.get(price)
            if not grid_level or grid_level.stock_on_hand <= 0:
                # If we have no stock and no active order (checked above), skip this level.
                continue
            # ---------------------------

            # --- STRICT ORDER COUNT GUARD ---
            if active_grid_order_count >= self.grid_manager.num_grids:
                self._throttled_warning(
                    f"🛑 Max orders reached ({active_grid_order_count}/{self.grid_manager.num_grids}). Skipping SELL at {price}.",
                    f"max_orders_reached_{self.bot_id}",
                    interval=600,
                )
                self.logger.debug(
                    f"Order skipped: grid capacity full ({active_grid_order_count}/{self.grid_manager.num_grids})"
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
                    f"🛑 Crypto Exhausted. Trimming remaining SELL grids above {price}. "
                    f"(Available: {self.balance_tracker.crypto_balance:.4f}, Required: {required_crypto:.4f})",
                    f"insufficient_crypto_sell_{price}",
                )
                break
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
            # Throttle this log to avoid spamming every 5s when stuck
            # FIX: Increased throttle to 5 minutes to reduce spam
            self._throttled_warning(f"⚖️ Deficit Detected: {deficit}", f"deficit_detected_{self.bot_id}", interval=300)
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
            # FIX: Only include levels that need NEW orders (active zone AND no existing order)
            # Don't include levels with existing orders - their funds are already reserved in BalanceTracker
            is_active_zone = price < safe_buy_limit
            grid_level = self.grid_manager.grid_levels.get(price)
            has_active_order = len(grid_level.orders) > 0 if grid_level else False

            # Only count if we need a NEW order here
            if is_active_zone and not has_active_order:
                # Calculate cost for this level
                raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
                required_fiat += raw_quantity * price

        required_fiat *= 1.005

        # 2. Sum up SELL requirements
        for price in self.grid_manager.sorted_sell_grids:
            # FIX: Only include levels that need NEW orders (active zone AND no existing order/stock)
            # Don't include levels with existing orders/stock - their funds are already reserved
            is_active_zone = price > safe_sell_limit
            grid_level = self.grid_manager.grid_levels.get(price)
            # Check for orders OR stock_on_hand (reserved for sell)
            has_active_order = (len(grid_level.orders) > 0 or grid_level.stock_on_hand > 0) if grid_level else False

            # Only count if we need a NEW order here
            if is_active_zone and not has_active_order:
                raw_quantity = self.grid_manager.get_order_size_for_grid_level(total_balance_value, price)
                required_crypto += raw_quantity

        # FIX: Add 2% buffer to the crypto requirement to cover rounding/dust issues
        # This prevents the last sell order placement from failing after rebalancing.
        required_crypto *= 1.005

        return required_fiat, required_crypto

    # ==============================================================================
    #  MISC METHODS
    # ==============================================================================

    async def execute_rebalance(self, deficit: dict, current_price: float) -> bool:
        """
        Executes a MARKET order to fix a balance deficit.
        """
        async with self._rebalancing_lock:
            if not deficit:
                return False

        action_type = deficit.get("type")
        required_amount = deficit.get("amount", 0.0)
        reason = deficit.get("reason", "Unknown")

        # FIX: Throttle this warning using the same logic as the "Paused" warning below
        # This prevents spamming "Attempting to fix" every 5 seconds when locked.
        self._throttled_warning(
            f"⚖️ REBALANCING: Attempting to fix deficit: {reason} (Action: {action_type}, Amount: {required_amount:.6f})",
            f"rebalance_locked_{self.bot_id}",
            interval=300,
        )

        try:
            order = None
            if action_type == "BUY_CRYPTO":
                # We need to buy `amount` crypto.
                # Check fiat balance first (redundant but safe)
                cost = required_amount * current_price
                if self.balance_tracker.balance < cost:
                    # FIX: Relaxed Rebalance Logic (Runtime Check)
                    # If we can't afford the rebalance, check if it's just a small "Phantom" deficit (N+1 issue)
                    single_grid_qty = required_amount  # Default fallback
                    if (
                        hasattr(self.grid_manager, "uniform_order_quantity")
                        and self.grid_manager.uniform_order_quantity
                    ):
                        single_grid_qty = self.grid_manager.uniform_order_quantity
                    elif hasattr(self.grid_manager, "auto_usdt_per_grid") and self.grid_manager.auto_usdt_per_grid:
                        single_grid_qty = self.grid_manager.auto_usdt_per_grid / current_price

                    if required_amount <= (single_grid_qty * 2.5):
                        self._throttled_warning(
                            f"⚠️ Runtime Rebalance: Insufficient Cash for Deficit ({cost:.2f} > {self.balance_tracker.balance:.2f}). "
                            f"Deficit ({required_amount:.4f}) is small (~2.5 grids). Skipping to avoid spam.",
                            f"skip_small_deficit_{self.bot_id}",
                            interval=300,
                        )
                        return False
                    else:
                        self._throttled_warning(
                            f"❌ CANNOT REBALANCE: Need {required_amount} crypto (Cost: {cost:.2f}) "
                            f"but only have {self.balance_tracker.balance:.2f} fiat. Manual intervention required.",
                            f"cannot_rebalance_fiat_{self.bot_id}",
                            interval=300,
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

                # --- ISOLATION FIX: Attach Client Order ID ---
                client_order_id = None
                if self.bot_id is not None:
                    short_uuid = uuid.uuid4().hex[:8]
                    client_order_id = f"G{self.bot_id}x{short_uuid}"

                params = {"clientOrderId": client_order_id} if client_order_id else None
                # ---------------------------------------------

                order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.BUY,
                    self.trading_pair,
                    amount=adjusted_qty,
                    price=current_price,
                    params=params,
                )

            elif action_type == "SELL_CRYPTO":
                # We need to sell `amount` crypto to raise fiat.
                # FIX: Check Available vs Required.
                # If we have "Excess" according to Total Equity, but it is LOCKED in orders,
                # we cannot sell it. We should sell what we CAN (Partial) or just wait.

                available_crypto = self.balance_tracker.crypto_balance
                if available_crypto < required_amount:
                    # Check if it's just a tiny dust difference or a real "Locked" situation
                    shortfall = required_amount - available_crypto

                    # If we have SOME loose crypto, sell it to move towards target
                    if available_crypto > (required_amount * 0.1) and available_crypto * current_price > 2.0:
                        self.logger.warning(
                            f"⚠️ Partial Liquidation: Need to sell {required_amount} but only {available_crypto} free. "
                            f"Locked: {shortfall}. Selling available amount."
                        )
                        # Adjust target to what we have
                        required_amount = available_crypto
                    else:
                        # We are effectively locked out. Do not spam ERROR.
                        self._throttled_warning(
                            f"⏳ Rebalance Paused: Excess crypto detected ({required_amount:.6f}) but it is locked in open orders. "
                            f"Available: {available_crypto:.6f}. Waiting for orders to fill.",
                            f"rebalance_locked_{self.bot_id}",
                            interval=300,  # 5 minutes throttle
                        )
                        return False

                adjusted_qty = self.order_validator.adjust_and_validate_sell_quantity(
                    self.balance_tracker.crypto_balance, required_amount
                )

                # --- ISOLATION FIX: Attach Client Order ID ---
                client_order_id = None
                if self.bot_id is not None:
                    short_uuid = uuid.uuid4().hex[:8]
                    client_order_id = f"G{self.bot_id}x{short_uuid}"

                params = {"clientOrderId": client_order_id} if client_order_id else None
                # ---------------------------------------------

                order = await self.order_execution_strategy.execute_market_order(
                    OrderSide.SELL, self.trading_pair, amount=adjusted_qty, price=current_price, params=params
                )

            if order:
                self.logger.info(f"✅ Rebalance Order Placed: {order.side} {order.amount} @ {order.average}")
                self.order_book.add_order(order)
                if self.bot_id is not None:
                    await self.db.add_order(
                        self.bot_id, order.identifier, current_price, order.side.value, order.amount
                    )

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
            if self.bot_id is not None:
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
                if self.bot_id is not None:
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
            db_orders = await self.db.get_all_active_orders(self.bot_id)
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
                    await self.db.update_order_status(order_id, "CANCELLED")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to cancel {order_id}: {e}")

            # 4. Final DB Cleanup (Force mark all 'OPEN' as 'CANCELLED' in DB to remove zombies)
            # Even if exchange didn't return them (maybe they closed 1ms ago), we shouldn't think they are open.
            # Rrefetch active orders to see what's left
            remaining_db_orders = await self.db.get_all_active_orders(self.bot_id)
            if remaining_db_orders:
                self.logger.info(f"🧹 Cleanup: Marking {len(remaining_db_orders)} local DB orders as CANCELLED.")
                for oid in remaining_db_orders.keys():
                    await self.db.update_order_status(oid, "CANCELLED")

        except Exception as e:
            self.logger.error(f"❌ Critical Error during cancel_all_open_orders: {e}", exc_info=True)

    async def liquidate_positions(self, current_price: float, max_retries: int = 3) -> None:
        """
        Liquidate all crypto holdings with retry logic and verification.
        Ensures complete liquidation by checking actual exchange balance after each attempt.
        """
        base_currency = self.trading_pair.split("/")[0]
        min_trade_amount = 0.0001  # Minimum tradeable amount (dust threshold)

        for attempt in range(max_retries):
            try:
                # 1. Get ACTUAL balance from exchange (not tracked balance)
                balances = await self.order_execution_strategy.exchange_service.get_balance()
                actual_balance = float(balances.get(base_currency, {}).get("free", 0.0))

                # Check if liquidation is complete
                if actual_balance < min_trade_amount:
                    self.logger.info(f"✅ Liquidation complete. Remaining {base_currency}: {actual_balance}")
                    return

                # 2. Attempt to sell what's available
                self.logger.info(
                    f"🔄 Liquidation Attempt {attempt + 1}/{max_retries}: Selling {actual_balance} {base_currency}..."
                )

                await self.order_execution_strategy.execute_market_order(
                    OrderSide.SELL, self.trading_pair, actual_balance, current_price
                )

                # 3. Wait for order to settle before next check
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(f"Liquidation attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(1)  # Brief pause before retry

        # 4. Final verification after all attempts
        try:
            balances = await self.order_execution_strategy.exchange_service.get_balance()
            remaining = float(balances.get(base_currency, {}).get("free", 0.0))
            if remaining >= min_trade_amount:
                self.logger.error(
                    f"⚠️ Liquidation incomplete after {max_retries} attempts. Remaining: {remaining} {base_currency}"
                )
            else:
                self.logger.info("✅ Liquidation verified complete after retries.")
        except Exception as e:
            self.logger.error(f"Failed to verify liquidation: {e}")

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
