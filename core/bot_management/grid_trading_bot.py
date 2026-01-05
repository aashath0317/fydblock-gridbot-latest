import asyncio
import logging
import traceback
from typing import TYPE_CHECKING, Any

from config.config_manager import ConfigManager
from config.trading_mode import TradingMode
from core.bot_management.event_bus import EventBus, Events
from core.grid_management.grid_manager import GridManager
from core.order_handling.balance_tracker import BalanceTracker
from core.order_handling.execution_strategy.order_execution_strategy_factory import (
    OrderExecutionStrategyFactory,
)
from core.order_handling.fee_calculator import FeeCalculator
from core.order_handling.order_book import OrderBook
from core.order_handling.order_manager import OrderManager
from core.order_handling.order_status_tracker import OrderStatusTracker
from core.services.exceptions import (
    DataFetchError,
    UnsupportedExchangeError,
    UnsupportedTimeframeError,
)
from core.services.exchange_service_factory import ExchangeServiceFactory
from core.validation.order_validator import OrderValidator
from strategies.grid_trading_strategy import GridTradingStrategy
from strategies.plotter import Plotter
from strategies.trading_performance_analyzer import TradingPerformanceAnalyzer

if TYPE_CHECKING:
    from strategies.strategy_type import StrategyType

from .notification.notification_handler import NotificationHandler


class GridTradingBot:
    def __init__(
        self,
        config_path: str,
        config_manager: ConfigManager,
        notification_handler: NotificationHandler,
        event_bus: EventBus,
        save_performance_results_path: str | None = None,
        no_plot: bool = False,
        bot_id: int | None = None,
    ):
        try:
            self.bot_id = bot_id
            self.logger = logging.getLogger(self.__class__.__name__)
            self.config_path = config_path
            self.config_manager = config_manager
            self.notification_handler = notification_handler
            self.event_bus = event_bus
            self.event_bus.subscribe(Events.STOP_BOT, self._handle_stop_bot_event)
            self.event_bus.subscribe(Events.START_BOT, self._handle_start_bot_event)
            self.save_performance_results_path = save_performance_results_path
            self.no_plot = no_plot
            self.trading_mode: TradingMode = self.config_manager.get_trading_mode()
            base_currency: str = self.config_manager.get_base_currency()
            quote_currency: str = self.config_manager.get_quote_currency()

            self.trading_pair = f"{base_currency}/{quote_currency}"

            strategy_type: StrategyType = self.config_manager.get_strategy_type()
            self.logger.info(
                f"Starting Grid Trading Bot in {self.trading_mode.value} mode with strategy: {strategy_type.value}",
            )
            self.is_running = False

            # --- FIX: Track the background task ---
            self.integrity_task = None
            # --------------------------------------

            self.exchange_service = ExchangeServiceFactory.create_exchange_service(
                self.config_manager,
                self.trading_mode,
            )
            order_execution_strategy = OrderExecutionStrategyFactory.create(self.config_manager, self.exchange_service)
            grid_manager = GridManager(self.config_manager, strategy_type)
            order_validator = OrderValidator()
            fee_calculator = FeeCalculator(self.config_manager)

            self.balance_tracker = BalanceTracker(
                event_bus=self.event_bus,
                fee_calculator=fee_calculator,
                trading_mode=self.trading_mode,
                base_currency=base_currency,
                quote_currency=quote_currency,
            )
            order_book = OrderBook()

            self.order_status_tracker = OrderStatusTracker(
                order_book=order_book,
                order_execution_strategy=order_execution_strategy,
                event_bus=self.event_bus,
                polling_interval=5.0,
            )

            order_manager = OrderManager(
                grid_manager,
                order_validator,
                self.balance_tracker,
                order_book,
                self.event_bus,
                order_execution_strategy,
                self.notification_handler,
                self.trading_mode,
                self.trading_pair,
                strategy_type,
                self.bot_id,
            )

            trading_performance_analyzer = TradingPerformanceAnalyzer(self.config_manager, order_book)
            plotter = Plotter(grid_manager, order_book) if self.trading_mode == TradingMode.BACKTEST else None
            self.strategy = GridTradingStrategy(
                self.config_manager,
                self.event_bus,
                self.exchange_service,
                grid_manager,
                order_manager,
                self.balance_tracker,
                trading_performance_analyzer,
                self.trading_mode,
                self.trading_pair,
                plotter,
            )

        except (UnsupportedExchangeError, DataFetchError, UnsupportedTimeframeError) as e:
            self.logger.error(f"{type(e).__name__}: {e}")
            raise

        except Exception:
            self.logger.error("An unexpected error occurred.")
            self.logger.error(traceback.format_exc())
            raise

    async def run(self) -> dict[str, Any] | None:
        try:
            self.is_running = True
            investment_amount = self.config_manager.get_initial_balance()

            await self.balance_tracker.setup_balances(
                initial_balance=investment_amount,
                initial_crypto_balance=0.0,
                exchange_service=self.exchange_service,
            )

            await self.order_status_tracker.start_streaming()
            self.strategy.initialize_strategy()

            # --- FIX: Store the task in self.integrity_task ---
            if self.trading_mode in {TradingMode.LIVE, TradingMode.PAPER_TRADING}:
                self.integrity_task = asyncio.create_task(self._start_integrity_loop())
            # --------------------------------------------------

            await self.strategy.run()

            if not self.no_plot:
                self.strategy.plot_results()

            return self._generate_and_log_performance()

        except Exception as e:
            self.logger.error(f"An unexpected error occurred {e}")
            self.logger.error(traceback.format_exc())
            raise

        finally:
            self.is_running = False
            # Ensure task is cancelled if we exit via exception
            if self.integrity_task and not self.integrity_task.done():
                self.integrity_task.cancel()

    async def _handle_stop_bot_event(self, reason: str) -> None:
        self.logger.info(f"Handling STOP_BOT event: {reason}")
        await self._stop()

    async def _handle_start_bot_event(self, reason: str) -> None:
        self.logger.info(f"Handling START_BOT event: {reason}")
        await self.restart()

    async def _stop(self, sell_assets: bool = False, cancel_orders: bool = True) -> None:
        if not self.is_running:
            self.logger.info("Bot is not running. Nothing to stop.")
            return

        self.logger.info(f"Stopping Grid Trading Bot... (Cancel Orders: {cancel_orders})")

        try:
            # --- FIX: Kill the Integrity Watchdog FIRST ---
            if self.integrity_task:
                self.integrity_task.cancel()
                try:
                    await self.integrity_task
                except asyncio.CancelledError:
                    self.logger.info("Integrity Watchdog task cancelled.")
                self.integrity_task = None
            # ----------------------------------------------

            self.is_running = False
            await self.order_status_tracker.stop_tracking()

            # Cancel orders (Clean up exchange) if requested
            if cancel_orders:
                self.logger.info("🛑 Stopping: Force-cancelling confirmed open orders...")
                await self.strategy.order_manager.cancel_all_open_orders()
            else:
                self.logger.info("⏸️ Soft Stop: Leaving orders active on exchange for resume.")

            await self.strategy.stop(sell_assets=sell_assets, cancel_orders=cancel_orders)

        except Exception as e:
            self.logger.error(f"Error while stopping components: {e}", exc_info=True)

        self.logger.info("Grid Trading Bot has been stopped.")

    async def restart(self) -> None:
        if self.is_running:
            self.logger.info("Bot is already running. Restarting...")
            await self._stop()

        self.logger.info("Restarting Grid Trading Bot...")
        self.is_running = True

        try:
            await self.order_status_tracker.start_streaming()
            await self.strategy.restart()

            # Restart integrity check if needed
            if self.trading_mode in {TradingMode.LIVE, TradingMode.PAPER_TRADING}:
                self.integrity_task = asyncio.create_task(self._start_integrity_loop())

        except Exception as e:
            self.logger.error(f"Error while restarting components: {e}", exc_info=True)

        self.logger.info("Grid Trading Bot has been restarted.")

    def _generate_and_log_performance(self) -> dict[str, Any] | None:
        performance_summary, formatted_orders = self.strategy.generate_performance_report()
        return {
            "config": self.config_path,
            "performance_summary": performance_summary,
            "orders": formatted_orders,
        }

    async def get_bot_health_status(self) -> dict:
        health_status = {
            "strategy": await self._check_strategy_health(),
            "exchange_status": await self._get_exchange_status(),
        }

        health_status["overall"] = all(health_status.values())
        return health_status

    async def _check_strategy_health(self) -> bool:
        if not self.is_running:
            self.logger.warning("Bot has stopped unexpectedly.")
            return False
        return True

    async def _get_exchange_status(self) -> str:
        exchange_status = await self.exchange_service.get_exchange_status()
        return exchange_status.get("status", "unknown")

    async def _start_integrity_loop(self):
        self.logger.info("🛡️ Starting Grid Integrity Watchdog (5s interval)...")
        # Initial delay to let startup complete
        await asyncio.sleep(5)

        while self.is_running:
            try:
                # Check price and reconcile
                current_price = await self.exchange_service.get_current_price(self.trading_pair)
                await self.strategy.order_manager.reconcile_grid_orders(current_price)

                # Wait 5 seconds
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                self.logger.info("Integrity Loop Cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Integrity check failed: {e}")
                # Wait before retrying to avoid spamming logs on persistent error
                await asyncio.sleep(5)

    def get_balances(self) -> dict[str, float]:
        return {
            "fiat": self.balance_tracker.balance,
            "reserved_fiat": self.balance_tracker.reserved_fiat,
            "crypto": self.balance_tracker.crypto_balance,
            "reserved_crypto": self.balance_tracker.reserved_crypto,
        }
