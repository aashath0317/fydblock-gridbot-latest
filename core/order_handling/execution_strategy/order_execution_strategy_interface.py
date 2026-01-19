from abc import ABC, abstractmethod

from ..order import Order, OrderSide


class OrderExecutionStrategyInterface(ABC):
    @abstractmethod
    async def execute_market_order(
        self, order_side: OrderSide, pair: str, amount: float, price: float = None, params: dict = None
    ) -> Order:
        pass

    @abstractmethod
    async def execute_limit_order(
        self,
        order_side: OrderSide,
        pair: str,
        amount: float,
        price: float,
        params: dict = None,
    ) -> Order | None:
        pass

    @abstractmethod
    async def get_order(
        self,
        order_id: str,
        pair: str,
    ) -> Order | None:
        pass

    @abstractmethod
    async def cancel_order(
        self,
        order_id: str,
        pair: str,
    ) -> bool:
        pass
