from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class ExchangeInterface(ABC):
    @abstractmethod
    async def get_balance(self) -> dict[str, Any]:
        """Fetches the account balance, returning a dictionary with fiat and crypto balances."""
        pass

    @abstractmethod
    async def place_order(
        self,
        pair: str,
        order_side: str,
        order_type: str,
        amount: float,
        price: float | None = None,
        params: dict = None,
    ) -> dict[str, str | float]:
        """Places an order, returning a dictionary with order details including id and status."""
        pass

    @abstractmethod
    def fetch_ohlcv(
        self,
        pair: str,
        timeframe: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetches historical OHLCV data as a list of dictionaries, each containing open, high, low,
        close, and volume for the specified time period.
        """
        pass

    @abstractmethod
    async def get_current_price(
        self,
        pair: str,
    ) -> float:
        """Fetches the current market price for the specified trading pair."""
        pass

    @abstractmethod
    async def cancel_order(
        self,
        order_id: str,
        pair: str,
    ) -> dict[str, str | float]:
        """Attempts to cancel an order by ID, returning the result of the cancellation."""
        pass

    @abstractmethod
    async def get_exchange_status(self) -> dict:
        """Fetches current exchange status."""
        pass

    @abstractmethod
    async def close_connection(self) -> None:
        """Close current exchange connection."""
        pass

    @abstractmethod
    async def refresh_open_orders(self, pair: str) -> list[dict]:
        """Fetches all currently open orders for the pair."""
        pass

    @abstractmethod
    async def start_user_stream(
        self,
        on_order_update: Any,
    ) -> None:
        """
        Starts a WebSocket stream for user data (orders).

        Args:
            on_order_update: Callback function to invoke when an order update is received.
        """
        pass
