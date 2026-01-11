import asyncio
import logging
import time

from core.services.exceptions import DataFetchError
from core.services.exchange_interface import ExchangeInterface

from ..exceptions import OrderExecutionFailedError
from ..order import Order, OrderSide, OrderStatus, OrderType
from .order_execution_strategy_interface import OrderExecutionStrategyInterface


class LiveOrderExecutionStrategy(OrderExecutionStrategyInterface):
    def __init__(
        self,
        exchange_service: ExchangeInterface,
        max_retries: int = 3,
        retry_delay: int = 1,
        max_slippage: float = 0.01,
    ) -> None:
        self.exchange_service = exchange_service
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_slippage = max_slippage
        self.logger = logging.getLogger(self.__class__.__name__)

    async def execute_market_order(
        self,
        order_side: OrderSide,
        pair: str,
        amount: float,
        price: float = None,
    ) -> Order:
        try:
            raw_order = await self.exchange_service.place_order(
                pair, OrderType.MARKET.value.lower(), order_side.value.lower(), amount, price
            )

            # --- SAFE PARSING START ---
            parsed_amount = float(raw_order.get("amount") or amount)
            parsed_filled = float(raw_order.get("filled") or parsed_amount)

            parsed_price = float(raw_order.get("price") or (price if price is not None else 0.0))

            parsed_average = raw_order.get("average")
            if parsed_average is None:
                parsed_average = parsed_price
            else:
                parsed_average = float(parsed_average)

            parsed_cost = raw_order.get("cost")
            if parsed_cost is None:
                parsed_cost = parsed_price * parsed_filled
            else:
                parsed_cost = float(parsed_cost)

            parsed_timestamp = raw_order.get("timestamp")
            if parsed_timestamp is None:
                parsed_timestamp = int(time.time() * 1000)
            else:
                parsed_timestamp = int(parsed_timestamp)

            parsed_fee = raw_order.get("fee")
            if parsed_fee is None:
                parsed_fee = {"cost": 0.0, "currency": pair.split("/")[1] if "/" in pair else "USDT"}
            # --------------------------

            return Order(
                identifier=str(raw_order["id"]),
                symbol=pair,
                side=order_side,
                order_type=OrderType.MARKET,
                status=OrderStatus.CLOSED,
                timestamp=parsed_timestamp,
                datetime=raw_order.get("datetime"),
                last_trade_timestamp=raw_order.get("lastTradeTimestamp"),
                amount=parsed_amount,
                filled=parsed_filled,
                remaining=0.0,
                price=parsed_price,
                average=parsed_average,
                time_in_force=raw_order.get("timeInForce"),
                trades=raw_order.get("trades", []),
                fee=parsed_fee,
                cost=parsed_cost,
                info=raw_order,
            )

        except Exception as e:
            self.logger.error(f"Failed to execute Market order on {pair}: {e}")
            raise OrderExecutionFailedError(
                f"Failed to execute Market order on {pair}: {e}",
                order_side,
                OrderType.MARKET,
                pair,
                amount,
                price if price is not None else 0.0,
            ) from e

    async def execute_limit_order(
        self,
        order_side: OrderSide,
        pair: str,
        amount: float,
        price: float,
    ) -> Order | None:
        try:
            raw_order = await self.exchange_service.place_order(
                pair,
                OrderType.LIMIT.value.lower(),
                order_side.value.lower(),
                amount,
                price,
            )
            # FIX: Pass amount and price as fallback
            order_result = await self._parse_order_result(raw_order, fallback_amount=amount, fallback_price=price)
            return order_result

        except DataFetchError as e:
            self.logger.error(f"DataFetchError during order execution for {pair} - {e}")
            raise OrderExecutionFailedError(
                f"Failed to execute Limit order on {pair}: {e}",
                order_side,
                OrderType.LIMIT,
                pair,
                amount,
                price,
            ) from e

        except Exception as e:
            self.logger.error(f"Unexpected error in execute_limit_order: {e}")
            raise OrderExecutionFailedError(
                f"Unexpected error during order execution: {e}",
                order_side,
                OrderType.LIMIT,
                pair,
                amount,
                price,
            ) from e

    async def get_order(
        self,
        order_id: str,
        pair: str,
    ) -> Order | None:
        try:
            raw_order = await self.exchange_service.fetch_order(order_id, pair)
            order_result = await self._parse_order_result(raw_order)
            return order_result

        except DataFetchError as e:
            raise e

        except Exception as e:
            raise DataFetchError(f"Unexpected error during order status retrieval: {e!s}") from e

    async def _parse_order_result(
        self,
        raw_order_result: dict,
        fallback_amount: float = 0.0,
        fallback_price: float = 0.0,
    ) -> Order:
        """
        Parses the raw order response from the exchange into an Order object.
        """
        ts = raw_order_result.get("timestamp")
        if ts is None:
            ts = int(time.time() * 1000)
        else:
            ts = int(ts)

        # FIX: Use fallback for price if missing/zero
        price = float(raw_order_result.get("price") or fallback_price)
        # FIX: Use fallback if amount is missing/zero (common in Sandbox/Async)
        amount = float(raw_order_result.get("amount") or fallback_amount)
        filled = float(raw_order_result.get("filled") or 0.0)

        cost = raw_order_result.get("cost")
        if cost is None:
            cost = price * filled
        else:
            cost = float(cost)

        avg = raw_order_result.get("average")
        if avg is None:
            avg = price
        else:
            avg = float(avg)

        fee = raw_order_result.get("fee")
        if fee is None:
            fee = {"cost": 0.0, "currency": "UNKNOWN"}

        # --- CRITICAL FIX: Safe String Parsing ---
        status_val = raw_order_result.get("status")
        if status_val is None:
            status_val = "open"  # Default for new limit orders

        type_val = raw_order_result.get("type")
        if type_val is None:
            type_val = "limit"  # Default if missing

        side_val = raw_order_result.get("side")
        if side_val is None:
            side_val = "buy"  # Should not happen, but safe fallback
        # -----------------------------------------

        return Order(
            identifier=raw_order_result.get("id", ""),
            status=OrderStatus(status_val.lower()),
            order_type=OrderType(type_val.lower()),
            side=OrderSide(side_val.lower()),
            price=price,
            average=avg,
            amount=amount,
            filled=filled,
            remaining=float(raw_order_result.get("remaining") or 0.0),
            timestamp=ts,
            datetime=raw_order_result.get("datetime"),
            last_trade_timestamp=raw_order_result.get("lastTradeTimestamp"),
            symbol=raw_order_result.get("symbol", ""),
            time_in_force=raw_order_result.get("timeInForce"),
            trades=raw_order_result.get("trades", []),
            fee=fee,
            cost=cost,
            info=raw_order_result.get("info", raw_order_result),
        )

    async def _adjust_price(
        self,
        order_side: OrderSide,
        price: float,
        attempt: int,
    ) -> float:
        adjustment = self.max_slippage / self.max_retries * attempt
        return price * (1 + adjustment) if order_side == OrderSide.BUY else price * (1 - adjustment)

    async def _handle_partial_fill(
        self,
        order: Order,
        pair: str,
    ) -> dict | None:
        self.logger.info(f"Order partially filled with {order.filled}. Attempting to cancel and retry full quantity.")

        if not await self._retry_cancel_order(order.identifier, pair):
            self.logger.error(f"Unable to cancel partially filled order {order.identifier} after retries.")

    async def _retry_cancel_order(
        self,
        order_id: str,
        pair: str,
    ) -> bool:
        for cancel_attempt in range(self.max_retries):
            try:
                cancel_result = await self.exchange_service.cancel_order(order_id, pair)

                if cancel_result["status"] == "canceled":
                    self.logger.info(f"Successfully canceled order {order_id}.")
                    return True

                self.logger.warning(f"Cancel attempt {cancel_attempt + 1} for order {order_id} failed.")

            except Exception as e:
                self.logger.warning(f"Error during cancel attempt {cancel_attempt + 1} for order {order_id}: {e!s}")

            await asyncio.sleep(self.retry_delay)
        return False

    async def cancel_order(
        self,
        order_id: str,
        pair: str,
    ) -> bool:
        try:
            result = await self.exchange_service.cancel_order(order_id, pair)
            return result.get("status") == "canceled"
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
