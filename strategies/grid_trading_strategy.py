import logging

import numpy as np
import pandas as pd

from config.config_manager import ConfigManager
from config.trading_mode import TradingMode
from core.bot_management.event_bus import EventBus, Events
from core.grid_management.grid_manager import GridManager
from core.order_handling.balance_tracker import BalanceTracker
from core.order_handling.order import OrderSide
from core.order_handling.order_manager import OrderManager
from core.services.exchange_interface import ExchangeInterface
from strategies.plotter import Plotter
from strategies.trading_performance_analyzer import TradingPerformanceAnalyzer

from .trading_strategy_interface import TradingStrategyInterface


class GridTradingStrategy(TradingStrategyInterface):
    # Set to 0 for Real-Time WebSocket streaming
    TICKER_REFRESH_INTERVAL = 0

    def __init__(
        self,
        config_manager: ConfigManager,
        event_bus: EventBus,
        exchange_service: ExchangeInterface,
        grid_manager: GridManager,
        order_manager: OrderManager,
        balance_tracker: BalanceTracker,
        trading_performance_analyzer: TradingPerformanceAnalyzer,
        trading_mode: TradingMode,
        trading_pair: str,
        plotter: Plotter | None = None,
    ):
        super().__init__(config_manager, balance_tracker)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event_bus = event_bus
        self.exchange_service = exchange_service
        self.grid_manager = grid_manager
        self.order_manager = order_manager
        self.trading_performance_analyzer = trading_performance_analyzer
        self.trading_mode = trading_mode
        self.trading_pair = trading_pair
        self.plotter = plotter
        self.data = self._initialize_historical_data()
        # FIX: Use deque with maxlen to cap memory usage (prevents unbounded growth)
        from collections import deque

        self.live_trading_metrics = deque(maxlen=1000)  # Keep last ~17 mins of 1-sec data
        self._running = True

        # Subscribe to Order Fills for Dynamic Trailing
        self.event_bus.subscribe(Events.ORDER_FILLED, self._handle_order_filled)

    def _initialize_historical_data(self) -> pd.DataFrame | None:
        if self.trading_mode != TradingMode.BACKTEST:
            return None
        try:
            timeframe, start_date, end_date = self._extract_config()
            return self.exchange_service.fetch_ohlcv(self.trading_pair, timeframe, start_date, end_date)
        except Exception as e:
            self.logger.error(f"Failed to initialize data for backtest: {e}")
            return None

    def _extract_config(self) -> tuple[str, str, str]:
        timeframe = self.config_manager.get_timeframe()
        start_date = self.config_manager.get_start_date()
        end_date = self.config_manager.get_end_date()
        return timeframe, start_date, end_date

    def initialize_strategy(self):
        self.grid_manager.initialize_grids_and_levels()

    async def stop(self, sell_assets: bool = False, cancel_orders: bool = True):
        self._running = False

        if sell_assets and self.trading_mode != TradingMode.BACKTEST:
            self.logger.info("?? Emergency stop triggered: Cancelling orders and liquidating assets.")
            try:
                # 1. Get current price for liquidation estimate
                current_price = await self.exchange_service.get_current_price(self.trading_pair)

                # 2. Cancel all pending grid orders (Releases locked funds)
                # Note: Emergency stop implies we MUST cancel orders to liquidate.
                await self.order_manager.cancel_all_open_orders()

                # 3. SYNC BALANCE (Crucial Fix)
                # Fetch fresh balances from exchange to account for fees/dust drift
                balances = await self.exchange_service.get_balance()
                base_currency = self.trading_pair.split("/")[0]  # e.g., 'SOL'

                actual_crypto = float(balances.get(base_currency, {}).get("free", 0.0))

                # Update the tracker with the REAL amount to sell
                self.balance_tracker.crypto_balance = actual_crypto
                self.logger.info(f"?? Balance Synced for Liquidation: {actual_crypto} {base_currency}")

                # 4. Sell everything to Quote Currency
                await self.order_manager.liquidate_positions(current_price)

            except Exception as e:
                self.logger.error(f"Error during emergency cleanup: {e}", exc_info=True)

        # Close connection only after operations are done
        await self.exchange_service.close_connection()
        self.logger.info("Trading execution stopped.")

    async def restart(self):
        if not self._running:
            self.logger.info("Restarting trading session.")
            await self.run()

    async def run(self):
        self._running = True
        trigger_price = self.grid_manager.get_trigger_price()

        if self.trading_mode == TradingMode.BACKTEST:
            await self._run_backtest(trigger_price)
            self.logger.info("Ending backtest simulation")
            self._running = False
        else:
            await self._run_live_or_paper_trading(trigger_price)

    async def _run_live_or_paper_trading(self, trigger_price: float):
        self.logger.info(f"Starting {'live' if self.trading_mode == TradingMode.LIVE else 'paper'} trading")

        # --- MODIFIED BALANCE SYNC LOGIC ---
        self.logger.info("?? Synchronizing Wallet Balances with Exchange...")
        try:
            balances = await self.exchange_service.get_balance()
            base_currency, quote_currency = self.trading_pair.split("/")

            # 1. Get Actual Wallet Balances (Free & Total)
            # Free: Available for new orders
            # Total: Free + Locked in Open Orders (Net Worth)
            free_crypto_balance = float(balances.get(base_currency, {}).get("free", 0.0))
            free_fiat_balance = float(balances.get(quote_currency, {}).get("free", 0.0))

            total_crypto_balance = float(balances.get(base_currency, {}).get("total", 0.0))
            total_fiat_balance = float(balances.get(quote_currency, {}).get("total", 0.0))

            # 2. Get User's Investment Limit
            investment_amount = self.config_manager.get_investment_amount()
            self.logger.info(f"   ?? Wallet Free: {free_fiat_balance} {quote_currency}")
            self.logger.info(
                f"   ?? Wallet Net Worth: {total_fiat_balance} {quote_currency} (Fiat) + {total_crypto_balance} {base_currency} (Crypto)"
            )
            self.logger.info(f"   ?? User Allocated: {investment_amount} {quote_currency}")

            # 3. Check for Active Orders (Hot Boot detection)
            # FIX: Moved Logic Up to determine Balance Initialization
            has_active_orders = self.order_manager.has_active_orders()

            if has_active_orders:
                self.logger.info("🔥 Found active orders in Database. Enabling Smart Resume (Hot Boot).")
                self.use_hot_boot = True
            else:
                self.logger.info("✨ No active orders found. Proceeding with Clean Start.")
                self.use_hot_boot = False

            # 4. Validate Funds (Equity Check uses NET WORTH)
            # Calculate Total Equity (Total Fiat + Total Crypto Value)
            current_price = await self.exchange_service.get_current_price(self.trading_pair)
            crypto_value = total_crypto_balance * current_price
            total_equity = total_fiat_balance + crypto_value

            # Tolerance for price fluctuations/fees (e.g. 98% of investment is okay)
            required_equity = investment_amount * 0.98

            self.logger.info(
                f"   💰 Total Equity: {total_equity:.2f} {quote_currency} "
                f"(Fiat: {total_fiat_balance:.2f}, Crypto Value: {crypto_value:.2f})"
            )

            if total_equity < required_equity:
                error_msg = (
                    f"❌ INSUFFICIENT FUNDS: Total Equity ({total_equity:.2f}) < "
                    f"Investment ({investment_amount:.2f}). "
                    f"Wallet Total: {total_fiat_balance:.2f} {quote_currency} + "
                    f"{total_crypto_balance:.4f} {base_currency}."
                )
                self.logger.error(error_msg)
                raise Exception(error_msg)

            # If Equity is sufficient but Fiat is low, we warn but PROCEED.
            if total_fiat_balance < investment_amount * 0.1:  # warn if very low fiat
                self.logger.warning(
                    f"⚠️ Low Fiat Balance ({total_fiat_balance:.2f}). "
                    f"Bot will rely heavily on existing Crypto ({total_crypto_balance:.4f}) for Sell orders."
                )

            # 5. Initialize Tracker with FREE/AVAILABLE values
            # FIX: Removed incorrect initialization with global wallet balance.
            # self.balance_tracker.initialize_balances(free_fiat_balance, free_crypto_balance)

            # --- FIX: ISOLATE BOT ASSETS ---
            # If Clean Start (New Bot), we ignore existing crypto in the wallet.
            # If Hot Boot (Resume), we RESTORE balances from DB (not reset to investment).
            if self.use_hot_boot:
                # FIX: Load persisted balances from DB - this includes the correct FREE balance
                if await self.balance_tracker.load_persisted_balances():
                    # Balances are now restored from DB - use them as-is
                    # The FREE balance is correct (what's left after orders were placed)
                    # The LOCKED balance is calculated from grid_orders in server.py
                    self.logger.info(
                        f"   🔥 Hot Boot: Restored from DB: "
                        f"{self.balance_tracker.balance:.2f} {quote_currency} (Free), "
                        f"{self.balance_tracker.crypto_balance:.4f} {base_currency}"
                    )
                    # Just set investment_cap for profit calculations (don't reset balances!)
                    self.balance_tracker.investment_cap = investment_amount
                    # DO NOT call setup_balances - it would overwrite the restored FREE balance!
                else:
                    # DB load failed, fall back to fresh start with investment amount
                    effective_fiat_balance = investment_amount
                    effective_crypto_balance = 0.0
                    self.logger.warning(
                        f"   🔥 Hot Boot: DB Load Failed. Starting fresh with {effective_fiat_balance} {quote_currency}"
                    )
                    res = await self.balance_tracker.setup_balances(
                        effective_fiat_balance, effective_crypto_balance, self.exchange_service
                    )
            else:
                # Clean Start - initialize with investment amount
                effective_crypto_balance = 0.0
                effective_fiat_balance = investment_amount
                self.logger.info(f"   ✨ Clean Start: Initializing with {effective_fiat_balance} {quote_currency}")
                res = await self.balance_tracker.setup_balances(
                    effective_fiat_balance, effective_crypto_balance, self.exchange_service
                )

        except Exception as e:
            self.logger.error(f"Failed to refresh balances: {e}", exc_info=True)
            self._running = False
            return
        # -----------------------------------

        last_price: float | None = None
        grid_orders_initialized = False

        async def on_ticker_update(current_price):
            nonlocal last_price, grid_orders_initialized
            try:
                if not self._running:
                    self.logger.info("Trading stopped; halting price updates.")
                    return

                account_value = self.balance_tracker.get_total_balance_value(current_price)
                self.live_trading_metrics.append((pd.Timestamp.now(), account_value, current_price))

                grid_orders_initialized = await self._initialize_grid_orders_once(
                    current_price,
                    trigger_price,
                    grid_orders_initialized,
                    last_price,
                    hot_boot=getattr(self, "use_hot_boot", False),
                )

                # Reset the flag after first use so subsequent re-inits (e.g. auto tuner) are fresh
                if grid_orders_initialized and getattr(self, "use_hot_boot", False):
                    self.use_hot_boot = False

                if not grid_orders_initialized:
                    last_price = current_price
                    return

                # --- AUTO-TUNER CHECK ---
                # Check for Market Phase changes (Reset Up / Expand Down)
                if await self._handle_auto_tuning(current_price):
                    # If auto-tuned, we essentially restarted the grid.
                    # Last price should be reset or handled carefully.
                    last_price = current_price
                    return

                if await self._handle_take_profit_stop_loss(current_price):
                    return

                last_price = current_price

            except Exception as e:
                self.logger.error(f"Error during ticker update: {e}", exc_info=True)

        try:
            await self.exchange_service.listen_to_ticker_updates(
                self.trading_pair,
                on_ticker_update,
                self.TICKER_REFRESH_INTERVAL,
            )
        except Exception as e:
            self.logger.error(f"Error in live/paper trading loop: {e}", exc_info=True)
        finally:
            self.logger.info("Exiting live/paper trading loop.")

    async def _handle_auto_tuning(self, current_price: float) -> bool:
        """
        Checks if the price has moved out of bounds and triggers Auto-Tuner logic if enabled.
        Returns True if an adjustment was made (which requires a loop reset), False otherwise.
        """
        # Determine if we are in "Auto" mode.
        # Currently, we infer "Auto" if the spacing is Geometric or explicitly set.
        # For this implementation, we assume ALL Geometric Grids have this behavior enabled
        # OR we check a flag in config. SRS implies "Auto Mode" defaults to Geometric.
        # We can check specific config flags if they exist, otherwise default to "smart" behavior.

        bottom_range = self.config_manager.get_bottom_range()
        top_range = self.config_manager.get_top_range()

        # 1. Reset Up (Bullish Breakout)
        if current_price > top_range:
            self.logger.info(f"?? Price ({current_price}) broke above Top Range ({top_range}). Triggering RESET UP.")

            # Cancel all existing orders
            await self.order_manager.cancel_all_open_orders()

            # Recalculate Grid
            self.grid_manager.reset_grid_up(current_price)

            # Re-initialize Orders (Immediate)
            # We can either return True and let the next ticker update handle it (safer?)
            # or force re-init here.
            # Given `_initialize_grid_orders_once` checks state, we can reset the state flag?
            # Actually, `grid_manager` has changed, so we need to rebuild orders.

            # Force logic:
            # The next tick will see `grid_orders_initialized=True` (local var in run loop).
            # We need to signal the run loop to re-init.
            # Returning True signals "Skip rest of logic, loop will likely continue."

            # But wait, `grid_orders_initialized` is a local variable in `run`.
            # We can't change it from here easily without refactoring.
            # FIX: We will handle re-initialization HERE.

            self.logger.info("?? Placing new grid orders after Reset Up...")
            await self.order_manager.initialize_grid_orders(current_price)
            return True

        # 2. Expand Down (Bearish Drop)
        elif current_price < bottom_range:
            # Check Cooldown (TODO: Add timestamp check)
            # For now, we assume we can expand.

            self.logger.info(
                f"?? Price ({current_price}) broke below Bottom Range ({bottom_range}). Triggering EXPAND DOWN."
            )

            # Cancel all (SRS Requirement to be safe? Or just add?)
            # SRS says "Grid count remains the same, resulting in wider gaps".
            # This implies we MUST move the lines. So yes, cancel all.
            await self.order_manager.cancel_all_open_orders()

            self.grid_manager.expand_grid_down(current_price)

            self.logger.info("?? Placing new grid orders after Expand Down...")
            await self.order_manager.initialize_grid_orders(current_price)
            return True

        return False

    async def _initialize_grid_orders_once(
        self,
        current_price: float,
        trigger_price: float,
        grid_orders_initialized: bool,
        last_price: float | None = None,
        hot_boot: bool = False,
    ) -> bool:
        if grid_orders_initialized:
            return True

        self.logger.info(f"🔄 Immediate Start Triggered! Current Price: {current_price} (Grid Center: {trigger_price})")

        # Define callback for GridManager to release funds/assets when trailing
        async def cancel_order_callback(grid_level):
            try:
                # Find the active order for this grid price
                # We use a tolerance because Order price might be slightly different handling floats
                target_order = self.order_manager.order_book.get_order_at_price(grid_level.price)

                if target_order and target_order.status == "OPEN":
                    self.logger.info(
                        f"   [Callback] Cancelling Order {target_order.identifier} at {grid_level.price}..."
                    )
                    await self.order_manager.order_execution_strategy.cancel_order(
                        target_order.identifier, self.trading_pair
                    )

                    # Manual Release of Funds (Crucial for immediate re-use in same tick)
                    # OrderCancelled event might be too slow if we depend on WebSocket
                    if target_order.side == OrderSide.BUY:
                        cost = target_order.remaining * target_order.price
                        await self.balance_tracker.release_reserve_for_buy(cost)
                    else:
                        await self.balance_tracker.release_reserve_for_sell(target_order.remaining)

                    # Mark locally as Cancelled so we don't try again
                    target_order.status = "CANCELLED"
                else:
                    self.logger.warning(f"   [Callback] No OPEN order found at {grid_level.price} to cancel.")

            except Exception as e:
                self.logger.error(f"   [Callback] Failed to cancel order: {e}")

        # Pass callback and AWAIT the async update
        await self.grid_manager.update_zones_based_on_price(current_price, cancel_order_callback)

        try:
            if hot_boot:
                self.logger.info("🔥 Hot Boot detected: Resuming existing orders...")
                await self.order_manager.resume_existing_orders(current_price)
            else:
                self.logger.info("✨ Clean Start: Initializing fresh grid...")
                await self.order_manager.perform_initial_purchase(current_price)
                self.logger.info("Initial purchase complete. Placing grid orders...")
                await self.order_manager.initialize_grid_orders(current_price)

            await self.event_bus.publish(Events.INITIALIZATION_COMPLETE)
            return True

        except Exception as e:
            self.logger.error(f"?? CRITICAL: Initialization Failed. Stopping Strategy. Error: {e}")
            self._running = False
            return False

    async def _run_backtest(self, trigger_price: float) -> None:
        if self.data is None:
            self.logger.error("No data available for backtesting.")
            return
        self.logger.info("Starting backtest simulation")
        self.data["account_value"] = np.nan
        self.close_prices = self.data["close"].values
        high_prices = self.data["high"].values
        low_prices = self.data["low"].values
        timestamps = self.data.index
        self.data.loc[timestamps[0], "account_value"] = self.balance_tracker.get_total_balance_value(
            price=self.close_prices[0]
        )
        grid_orders_initialized = False
        last_price = None
        for i, (current_price, high_price, low_price, timestamp) in enumerate(
            zip(self.close_prices, high_prices, low_prices, timestamps, strict=False)
        ):
            grid_orders_initialized = await self._initialize_grid_orders_once(
                current_price, trigger_price, grid_orders_initialized, last_price
            )
            if not grid_orders_initialized:
                self.data.loc[timestamps[i], "account_value"] = self.balance_tracker.get_total_balance_value(
                    price=current_price
                )
                last_price = current_price
                continue
            await self.order_manager.simulate_order_fills(high_price, low_price, timestamp)
            if await self._handle_take_profit_stop_loss(current_price):
                break
            self.data.loc[timestamp, "account_value"] = self.balance_tracker.get_total_balance_value(current_price)
            last_price = current_price

    def generate_performance_report(self) -> tuple[dict, list]:
        if self.trading_mode == TradingMode.BACKTEST:
            return self.trading_performance_analyzer.generate_performance_summary(
                self.data,
                self.close_prices[0],
                self.balance_tracker.get_adjusted_fiat_balance(),
                self.balance_tracker.get_adjusted_crypto_balance(),
                self.close_prices[-1],
                self.balance_tracker.total_fees,
            )
        else:
            if not self.live_trading_metrics:
                return {}, []
            live_data = pd.DataFrame(self.live_trading_metrics, columns=["timestamp", "account_value", "price"])
            live_data.set_index("timestamp", inplace=True)
            return self.trading_performance_analyzer.generate_performance_summary(
                live_data,
                live_data.iloc[0]["price"],
                self.balance_tracker.get_adjusted_fiat_balance(),
                self.balance_tracker.get_adjusted_crypto_balance(),
                live_data.iloc[-1]["price"],
                self.balance_tracker.total_fees,
            )

    def plot_results(self) -> None:
        if self.trading_mode == TradingMode.BACKTEST:
            self.plotter.plot_results(self.data)
        else:
            self.logger.info("Plotting is not available for live/paper trading mode.")

    async def _handle_take_profit_stop_loss(self, current_price: float) -> bool:
        if await self._evaluate_tp_or_sl(current_price):
            self.logger.info("Take-profit or stop-loss triggered, ending trading session.")
            await self.event_bus.publish(Events.STOP_BOT, "TP or SL hit.")
            return True
        return False

    async def _evaluate_tp_or_sl(self, current_price: float) -> bool:
        if self.balance_tracker.crypto_balance == 0:
            return False
        return await self._handle_take_profit(current_price) or await self._handle_stop_loss(current_price)

    async def _handle_take_profit(self, current_price: float) -> bool:
        if (
            self.config_manager.is_take_profit_enabled()
            and current_price >= self.config_manager.get_take_profit_threshold()
        ):
            self.logger.info(f"Take-profit triggered at {current_price}. Executing TP order...")
            await self.order_manager.execute_take_profit_or_stop_loss_order(
                current_price=current_price, take_profit_order=True
            )
            return True
        return False

    async def _handle_stop_loss(self, current_price: float) -> bool:
        if (
            self.config_manager.is_stop_loss_enabled()
            and current_price <= self.config_manager.get_stop_loss_threshold()
        ):
            self.logger.info(f"Stop-loss triggered at {current_price}. Executing SL order...")
            await self.order_manager.execute_take_profit_or_stop_loss_order(
                current_price=current_price, stop_loss_order=True
            )
            return True
        return False

    async def _handle_order_filled(self, order) -> None:
        """
        Handles order fills to trigger Dynamic Trailing (Infinity Grid).
        """
        if not self._running:
            return

        # 1. Identify if this order is an "Edge" order (Top Sell or Bottom Buy)
        # We check against the CURRENT grid state (before this fill potentially changed it?)
        # Actually, if the order fills, the current price is at that level.
        # We need to know if it was the Highest Sell or Lowest Buy.

        sorted_grids = sorted(self.grid_manager.price_grids)
        if not sorted_grids:
            return

        highest_price = sorted_grids[-1]
        lowest_price = sorted_grids[0]

        # Use a small tolerance for floating point comparison
        is_highest_sell = order.side == OrderSide.SELL and abs(order.price - highest_price) < 1e-6
        is_lowest_buy = order.side == OrderSide.BUY and abs(order.price - lowest_price) < 1e-6

        if is_highest_sell:
            await self._trail_up(order)
        elif is_lowest_buy:
            await self._trail_down(order)

    async def _trail_up(self, filled_order) -> None:
        self.logger.info(f"🚀 Trailing UP triggered by fill at {filled_order.price}")

        # 1. Inventory Adjustment (Re-buy Base Currency)
        # "The bot immediately performs a Market Buy... for the same quantity... that was just sold."
        try:
            # We use the raw amount from the filled order
            quantity_to_buy = filled_order.amount

            # Use OrderManager strategies to execute market buy
            # We treat this as a helper operation, not a grid order
            # Note: We must ensure we have enough USDT. But we just sold, so we should have it.
            buy_order = await self.order_manager.order_execution_strategy.execute_market_order(
                OrderSide.BUY, self.trading_pair, quantity_to_buy, filled_order.price
            )

            if buy_order:
                self.logger.info(f"   Inventory Re-buy Successful: {buy_order.filled} @ {buy_order.average}")
                # Update Balance Tracker (Deduct cost, Add crypto)
                # cost = amount * price + fee
                # BalanceTracker expects an Order object.
                # We can manually register it or use register_open_order logic (but it's filled)
                # Let's manually trigger update
                await self.balance_tracker.update_balance_on_order_completion(buy_order)
            else:
                self.logger.error("   Inventory Re-buy Failed: No order returned.")

        except Exception as e:
            self.logger.error(f"   Failed to re-buy inventory: {e}", exc_info=True)
            # Proceed? SRS says logic continues.

        # 2. Grid Shift (Remove Low, Add High)
        removed_price, new_top_price = self.grid_manager.extend_grid_up()

        # 3. Cancel the Lowest Buy Order (at removed_price)
        # We must release the funds locked by this order
        try:
            # Find the order in OrderBook
            orders = self.order_manager.order_book.get_open_orders()
            target_order = None
            for o in orders:
                if abs(o.price - removed_price) < 1e-6 and o.side == OrderSide.BUY:
                    target_order = o
                    break

            if target_order:
                self.logger.info(f"   Cancelling Lowest Buy Order at {removed_price}...")
                await self.order_manager.order_execution_strategy.cancel_order(
                    target_order.identifier, self.trading_pair
                )

                # Manually Release Reserved Funds (Since OrderManager event doesn't do it)
                cost = target_order.remaining * target_order.price
                await self.balance_tracker.release_reserve_for_buy(cost)
                self.logger.info(f"   Released {cost:.2f} reserved fiat.")

                # Force status update locally (OrderManager event will eventually confirm)
                target_order.status = "CANCELLED"
            else:
                self.logger.warning(f"   Could not find active order at removed grid {removed_price} to cancel.")

        except Exception as e:
            self.logger.error(f"   Failed to cancel lowest buy order: {e}", exc_info=True)

        # 4. Place New Sell Order at Top
        self.logger.info(f"   Placing New Top Sell Order at {new_top_price}...")
        await self.order_manager._place_limit_order_safe(new_top_price, OrderSide.SELL)

    async def _trail_down(self, filled_order) -> None:
        self.logger.info(f"📉 Trailing DOWN triggered by fill at {filled_order.price}")

        # 1. Grid Shift First? No, SRS says: "Identify Highest Sell -> Cancel -> Market Sell"

        # Get the Highest Sell Price BEFORE shifting?
        # GridManager hasn't shifted yet.
        highest_price = sorted(self.grid_manager.price_grids)[-1]

        # 2. Cancel Highest Sell Order
        quantity_released = 0.0
        try:
            orders = self.order_manager.order_book.get_open_orders()
            target_order = None
            for o in orders:
                if abs(o.price - highest_price) < 1e-6 and o.side == OrderSide.SELL:
                    target_order = o
                    break

            if target_order:
                self.logger.info(f"   Cancelling Highest Sell Order at {highest_price}...")
                await self.order_manager.order_execution_strategy.cancel_order(
                    target_order.identifier, self.trading_pair
                )

                # Manually Release Reserved Funds
                quantity_released = target_order.remaining
                await self.balance_tracker.release_reserve_for_sell(quantity_released)
                self.logger.info(f"   Released {quantity_released:.6f} reserved crypto.")

                target_order.status = "CANCELLED"
            else:
                self.logger.warning(f"   Could not find active order at highest grid {highest_price} to cancel.")

        except Exception as e:
            self.logger.error(f"   Failed to cancel highest sell order: {e}", exc_info=True)

        # 3. Inventory Adjustment (Liquidation)
        # "Immediately performs a Market Sell of that released Base Currency"
        if quantity_released > 0:
            try:
                sell_order = await self.order_manager.order_execution_strategy.execute_market_order(
                    OrderSide.SELL, self.trading_pair, quantity_released, filled_order.price
                )
                if sell_order:
                    self.logger.info(f"   Inventory Liquidation Successful: {sell_order.filled} @ {sell_order.average}")
                    await self.balance_tracker.update_balance_on_order_completion(sell_order)
            except Exception as e:
                self.logger.error(f"   Failed to liquidate inventory: {e}", exc_info=True)

        # 4. Grid Shift (Remove High, Add Low)
        removed_price, new_bottom_price = self.grid_manager.extend_grid_down()

        # 5. Place New Buy Order at Bottom
        self.logger.info(f"   Placing New Bottom Buy Order at {new_bottom_price}...")
        await self.order_manager._place_limit_order_safe(new_bottom_price, OrderSide.BUY)

    def get_formatted_orders(self):
        return self.trading_performance_analyzer.get_formatted_orders()
