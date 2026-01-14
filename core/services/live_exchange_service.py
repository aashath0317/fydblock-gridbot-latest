import asyncio
from collections.abc import Callable
import logging
import os
from typing import Any

from ccxt.base.errors import BaseError, ExchangeError, NetworkError, OrderNotFound
import ccxt.pro as ccxtpro
import pandas as pd

from config.config_manager import ConfigManager

from .exceptions import (
    DataFetchError,
    MissingEnvironmentVariableError,
    OrderCancellationError,
    UnsupportedExchangeError,
)
from .exchange_interface import ExchangeInterface


class LiveExchangeService(ExchangeInterface):
    def __init__(self, config_manager: ConfigManager, is_paper_trading_activated: bool):
        self.config_manager = config_manager
        self.is_paper_trading_activated = is_paper_trading_activated
        self.logger = logging.getLogger(self.__class__.__name__)
        self.exchange_name = self.config_manager.get_exchange_name()

        # --- FIXED: Use getattr to safely get keys from config adapter ---
        self.api_key = getattr(config_manager, "get_api_key", lambda: None)() or os.getenv("EXCHANGE_API_KEY")
        self.secret_key = getattr(config_manager, "get_api_secret", lambda: None)() or os.getenv("EXCHANGE_SECRET_KEY")

        # --- FIXED: Added retrieval of password ---
        self.password = getattr(config_manager, "get_api_password", lambda: None)() or os.getenv("EXCHANGE_PASSWORD")

        if not self.api_key or not self.secret_key:
            raise MissingEnvironmentVariableError("API Key or Secret missing.")

        self.exchange = self._initialize_exchange()
        self.connection_active = False

    def _get_env_variable(self, key: str) -> str:
        value = os.getenv(key)
        if value is None:
            raise MissingEnvironmentVariableError(f"Missing required environment variable: {key}")
        return value

    def _initialize_exchange(self) -> None:
        try:
            exchange_options = {
                "apiKey": self.api_key,
                "secret": self.secret_key,
                "enableRateLimit": True,
            }

            # --- FIXED: Inject password if available (Critical for OKX/KuCoin) ---
            if self.password:
                exchange_options["password"] = self.password

            exchange = getattr(ccxtpro, self.exchange_name)(exchange_options)

            if self.is_paper_trading_activated:
                self._enable_sandbox_mode(exchange)
            return exchange
        except AttributeError:
            raise UnsupportedExchangeError(f"The exchange '{self.exchange_name}' is not supported.") from None

    def _enable_sandbox_mode(self, exchange) -> None:
        if self.exchange_name == "binance":
            exchange.urls["api"] = "https://testnet.binance.vision/api"
        elif self.exchange_name == "kraken":
            exchange.urls["api"] = "https://api.demo-futures.kraken.com"
        elif self.exchange_name == "bitmex":
            exchange.urls["api"] = "https://testnet.bitmex.com"
        elif self.exchange_name == "bybit" or self.exchange_name == "okx":
            exchange.set_sandbox_mode(True)
        # ------------------------------
        else:
            self.logger.warning(f"No sandbox mode available for {self.exchange_name}. Running in live mode.")

    async def _subscribe_to_ticker_updates(
        self,
        pair: str,
        on_ticker_update: Callable[[float], None],
        update_interval: float,
        max_retries: int = 5,
    ) -> None:
        self.connection_active = True
        retry_count = 0

        while self.connection_active:
            try:
                ticker = await self.exchange.watch_ticker(pair)
                current_price: float = ticker["last"]
                if not self.connection_active:
                    break

                await on_ticker_update(current_price)
                await asyncio.sleep(update_interval)
                retry_count = 0  # Reset retry count after a successful operation

            except (NetworkError, ExchangeError) as e:
                retry_count += 1
                retry_interval = min(retry_count * 5, 60)
                self.logger.error(
                    f"Error connecting to WebSocket for {pair}: {e}. "
                    f"Retrying in {retry_interval} seconds ({retry_count}/{max_retries}).",
                )

                if retry_count >= max_retries:
                    self.logger.error("Max retries reached. Stopping WebSocket connection.")
                    self.connection_active = False
                    break

                await asyncio.sleep(retry_interval)

            except asyncio.CancelledError:
                self.logger.error(f"WebSocket subscription for {pair} was cancelled.")
                self.connection_active = False
                break

            except Exception as e:
                self.logger.error(f"WebSocket connection error: {e}. Reconnecting...")
                await asyncio.sleep(5)

            finally:
                if not self.connection_active:
                    try:
                        self.logger.info("Connection to Websocket no longer active.")
                        await self.exchange.close()

                    except Exception as e:
                        self.logger.error(f"Error while closing WebSocket connection: {e}", exc_info=True)

    async def listen_to_ticker_updates(
        self,
        pair: str,
        on_price_update: Callable[[float], None],
        update_interval: float,
    ) -> None:
        await self._subscribe_to_ticker_updates(pair, on_price_update, update_interval)

    async def start_user_stream(
        self,
        on_order_update: Callable[[dict], None],
    ) -> None:
        """
        Connects to the private user data stream (orders) via WebSocket.
        Uses ccxt.pro's watch_orders().
        """
        self.connection_active = True
        self.logger.info(f"Starting user data stream for {self.exchange_name}...")
        retry_count = 0
        max_retries = 10

        while self.connection_active:
            try:
                # Watch for ANY order updates (symbol=None means all symbols, or catch-all)
                # Note: Some exchanges require a symbol. For a single-pair bot, we can pass it if needed.
                # However, watch_orders() usually returns a list of orders.
                orders = await self.exchange.watch_orders()

                if not self.connection_active:
                    break

                # orders is a list of order objects. We process each.
                for order in orders:
                    # Clean/normalize data if needed, but ccxt unified structure is good.
                    # We pass the raw ccxt order object (dict) to the callback.
                    try:
                        normalized_order = {
                            "id": order["id"],
                            "price": float(order["price"]) if order["price"] else 0.0,
                            "average": float(order["average"]) if order.get("average") else 0.0,
                            "amount": float(order["amount"]) if order["amount"] else 0.0,
                            "side": order["side"],
                            "status": order["status"],
                            "filled": float(order.get("filled", 0.0)),
                            "remaining": float(order.get("remaining", 0.0)),
                            "clientOrderId": order.get("clientOrderId") or order.get("info", {}).get("clOrdId"),
                            "raw": order,  # Keep raw just in case
                        }
                        # Invoke callback
                        if asyncio.iscoroutinefunction(on_order_update):
                            await on_order_update(normalized_order)
                        else:
                            on_order_update(normalized_order)

                    except Exception as e:
                        self.logger.error(f"Error processing order update: {e}", exc_info=True)

                retry_count = 0  # Reset on success

            except (NetworkError, ExchangeError) as e:
                retry_count += 1
                retry_interval = min(retry_count * 5, 60)
                self.logger.error(
                    f"Error in user stream: {e}. Retrying in {retry_interval}s ({retry_count}/{max_retries})."
                )
                if retry_count >= max_retries:
                    self.logger.error("Max retries reached for user stream. Stopping.")
                    self.connection_active = False
                    break
                await asyncio.sleep(retry_interval)

            except asyncio.CancelledError:
                self.logger.info("User stream task cancelled.")
                # FIX: Do NOT set self.connection_active = False here.
                # This shared flag controls the Ticker Stream too. If we kill it here,
                # the Ticker Stream exits and closes the Exchange Session while we still need it.
                break

            except Exception as e:
                self.logger.error(f"Unexpected error in user stream: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def close_connection(self) -> None:
        self.connection_active = False
        self.logger.info("Closing WebSocket connection...")

    async def get_balance(self) -> dict[str, Any]:
        try:
            balance = await self.exchange.fetch_balance()
            return balance

        except BaseError as e:
            raise DataFetchError(f"Error fetching balance: {e!s}") from e

    async def get_current_price(self, pair: str) -> float:
        try:
            ticker = await self.exchange.fetch_ticker(pair)
            return ticker["last"]

        except BaseError as e:
            raise DataFetchError(f"Error fetching current price: {e!s}") from e

    async def place_order(
        self,
        pair: str,
        order_type: str,
        order_side: str,
        amount: float,
        price: float | None = None,
    ) -> dict[str, str | float]:
        try:
            order = await self.exchange.create_order(pair, order_type, order_side, amount, price)
            return order

        except NetworkError as e:
            raise DataFetchError(f"Network issue occurred while placing order: {e!s}") from e

        except BaseError as e:
            raise DataFetchError(f"Error placing order: {e!s}") from e

        except Exception as e:
            raise DataFetchError(f"Unexpected error placing order: {e!s}") from e

    async def fetch_order(
        self,
        order_id: str,
        pair: str,
    ) -> dict[str, str | float]:
        try:
            return await self.exchange.fetch_order(order_id, pair)

        except NetworkError as e:
            raise DataFetchError(f"Network issue occurred while fetching order status: {e!s}") from e

        except BaseError as e:
            raise DataFetchError(f"Exchange-specific error occurred: {e!s}") from e

        except Exception as e:
            raise DataFetchError(f"Failed to fetch order status: {e!s}") from e

    async def cancel_order(
        self,
        order_id: str,
        pair: str,
    ) -> dict:
        try:
            self.logger.info(f"Attempting to cancel order {order_id} for pair {pair}")
            cancellation_result = await self.exchange.cancel_order(order_id, pair)

            if cancellation_result["status"] in ["canceled", "closed"]:
                self.logger.info(f"Order {order_id} successfully canceled.")
                return cancellation_result
            else:
                self.logger.warning(f"Order {order_id} cancellation status: {cancellation_result['status']}")
                return cancellation_result

        except OrderNotFound:
            raise OrderCancellationError(
                f"Order {order_id} not found for cancellation. It may already be completed or canceled.",
            ) from None

        except NetworkError as e:
            raise OrderCancellationError(f"Network error while canceling order {order_id}: {e!s}") from e

        except BaseError as e:
            raise OrderCancellationError(f"Exchange error while canceling order {order_id}: {e!s}") from e

        except Exception as e:
            raise OrderCancellationError(f"Unexpected error while canceling order {order_id}: {e!s}") from e

    async def get_exchange_status(self) -> dict:
        try:
            status = await self.exchange.fetch_status()
            return {
                "status": status.get("status", "unknown"),
                "updated": status.get("updated"),
                "eta": status.get("eta"),
                "url": status.get("url"),
                "info": status.get("info", "No additional info available"),
            }

        except AttributeError:
            return {"status": "unsupported", "info": "fetch_status not supported by this exchange."}

        except Exception as e:
            return {"status": "error", "info": f"Failed to fetch exchange status: {e}"}

    async def refresh_open_orders(self, pair: str) -> list[dict]:
        if not self.exchange:
            raise UnsupportedExchangeError("Exchange has not been initialized")

        try:
            all_orders = []
            limit = 100  # OKX Max Limit per page
            params = {}

            while True:
                # Use the last order ID as a cursor for the next page
                if all_orders:
                    # For OKX/CCXT, 'after' tells it to give orders starting AFTER this ID
                    params["after"] = all_orders[-1]["id"]

                # Fetch one batch
                orders = await self.exchange.fetch_open_orders(symbol=pair, limit=limit, params=params)

                # If we get nothing, we are done
                if not orders:
                    break

                all_orders.extend(orders)

                # If we got fewer than the limit, it means we reached the last page
                if len(orders) < limit:
                    break

                # Safety break to prevent infinite loops (e.g. API bug)
                if len(all_orders) > 2000:
                    self.logger.warning("Fetched >2000 orders. Stopping pagination to prevent overflow.")
                    break

            # self.logger.info(f"Fetched total {len(all_orders)} open orders from exchange.") (Avoid Spamming)

            return [
                {
                    "id": order["id"],
                    "price": float(order["price"]),
                    "amount": float(order["amount"]),
                    "side": order["side"],
                    "status": order["status"],
                    "filled": float(order.get("filled", 0.0)),
                    "remaining": float(order.get("remaining", 0.0)),
                    "clientOrderId": order.get("clientOrderId") or order.get("info", {}).get("clOrdId"),
                }
                for order in all_orders
            ]
        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
            return []

    def fetch_ohlcv(
        self,
        pair: str,
        timeframe: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        raise NotImplementedError("fetch_ohlcv is not used in live or paper trading mode.")
