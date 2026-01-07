import logging

from config.trading_mode import TradingMode
from core.bot_management.event_bus import EventBus
from core.services.exchange_interface import ExchangeInterface

from ..validation.exceptions import (
    InsufficientBalanceError,
    InsufficientCryptoBalanceError,
)
from .fee_calculator import FeeCalculator
from .order import Order, OrderSide, OrderStatus


class BalanceTracker:
    def __init__(
        self,
        event_bus: EventBus,
        fee_calculator: FeeCalculator,
        trading_mode: TradingMode,
        base_currency: str,
        quote_currency: str,
    ):
        """
        Initializes the BalanceTracker.

        Args:
            event_bus: The event bus instance for subscribing to events.
            fee_calculator: The fee calculator instance for calculating trading fees.
            trading_mode: "BACKTEST", "LIVE" or "PAPER_TRADING".
            base_currency: The base currency symbol.
            quote_currency: The quote currency symbol.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event_bus: EventBus = event_bus
        self.fee_calculator: FeeCalculator = fee_calculator
        self.trading_mode: TradingMode = trading_mode
        self.base_currency: str = base_currency
        self.quote_currency: str = quote_currency

        self.balance: float = 0.0
        self.crypto_balance: float = 0.0
        self.total_fees: float = 0
        self.reserved_fiat: float = 0.0
        self.reserved_crypto: float = 0.0
        self.investment_cap: float = float("inf")
        self.operational_reserve: float = 0.0  # Dynamic Fee Stabilization Reserve

        # REMOVED: Automatic subscription. OrderManager will call update manually to ensure sequence.
        # self.event_bus.subscribe(Events.ORDER_FILLED, self._update_balance_on_order_completion)

    async def setup_balances(
        self,
        initial_balance: float,
        initial_crypto_balance: float,
        exchange_service=ExchangeInterface,
    ):
        """
        Sets up the balances using the investment cap passed from the Strategy.
        """
        self.balance = initial_balance
        self.crypto_balance = initial_crypto_balance
        self.investment_cap = initial_balance

        self.logger.info(
            f"Balance Tracker Initialized: {self.balance} {self.quote_currency} (Investment Cap) / {self.crypto_balance} {self.base_currency} / Reserve: {self.operational_reserve}"
        )

    def attempt_fee_recovery(self, required_amount: float) -> bool:
        """
        Checks if the main balance is sufficient. If not, attempts to auto-heal
        using the Operational Reserve ("Dust Shortfall" logic).
        """
        if self.balance >= required_amount:
            return True

        deficit = required_amount - self.balance

        # FR-03/04: Auto-Injection if reserve covers deficit
        # We assume ANY deficit covered by reserve is valid "Fee Drag" recovery.
        if self.operational_reserve >= deficit:
            self.operational_reserve -= deficit
            self.balance += deficit
            self.logger.info(f"?? RESCUE: Auto-injected {deficit:.6f} from Operational Reserve to cover shortfall.")
            return True

        return False

    async def _fetch_live_balances(
        self,
        exchange_service: ExchangeInterface,
    ) -> tuple[float, float]:
        """
        Fetches live balances from the exchange asynchronously.

        Args:
            exchange_service: The exchange instance.

        Returns:
            tuple: The quote and base currency balances.
        """
        balances = await exchange_service.get_balance()

        if not balances or "free" not in balances:
            raise ValueError(f"Unexpected balance structure: {balances}")

        quote_balance = float(balances.get("free", {}).get(self.quote_currency, 0.0))
        base_balance = float(balances.get("free", {}).get(self.base_currency, 0.0))
        self.logger.info(
            f"Fetched balances - Quote: {self.quote_currency}: {quote_balance}, "
            f"Base: {self.base_currency}: {base_balance}",
        )
        return quote_balance, base_balance

    async def sync_balances(self, exchange_service: ExchangeInterface, current_price: float):
        """
        Forces an update of the available (free) balance from the exchange.
        This ensures the bot doesn't try to spend funds it doesn't actually have.
        """
        try:
            balances = await exchange_service.get_balance()
            if balances and "free" in balances and "total" in balances:
                # Update Available Fiat
                free_fiat = float(balances["free"].get(self.quote_currency, 0.0))
                free_crypto = float(balances["free"].get(self.base_currency, 0.0))

                total_fiat = float(balances["total"].get(self.quote_currency, 0.0))
                total_crypto = float(balances["total"].get(self.base_currency, 0.0))

                crypto_equity_value = total_crypto * current_price

                locked_fiat = total_fiat - free_fiat
                target_total_fiat = max(0.0, self.investment_cap - crypto_equity_value)

                max_allowed_free_fiat = max(0.0, target_total_fiat - locked_fiat)

                new_fiat = min(free_fiat, max_allowed_free_fiat)
                new_crypto = free_crypto

                # Log only if there's a significant drift
                if abs(new_fiat - self.balance) > 1.0 or abs(new_crypto - self.crypto_balance) > 0.01:
                    self.logger.debug(
                        f"⚖️ Balance Synced: {self.balance:.2f} -> {new_fiat:.2f} {self.quote_currency} "
                        f"(Wallet Free: {free_fiat:.2f}, Total Crypto Val: {crypto_equity_value:.2f}) | "
                        f"{self.crypto_balance:.4f} -> {new_crypto:.4f} {self.base_currency}"
                    )

                self.balance = new_fiat
                self.crypto_balance = new_crypto
        except Exception as e:
            self.logger.error(f"Failed to sync balances: {e}")

    # CHANGED: Renamed from _update_balance_on_order_completion to public method
    async def update_balance_on_order_completion(self, order: Order) -> None:
        """
        Updates the account balance and crypto balance when an order is filled.

        This method is called when an `ORDER_FILLED` event is received. It determines
        whether the filled order is a buy or sell order and updates the balances
        accordingly.

        Args:
            order: The filled `Order` object containing details such as the side
            (BUY/SELL), filled quantity, and price.
        """
        if order.side == OrderSide.BUY:
            self._update_after_buy_order_filled(order.filled, order.price)
        elif order.side == OrderSide.SELL:
            self._update_after_sell_order_filled(order.filled, order.price)

    def _update_after_buy_order_filled(
        self,
        quantity: float,
        price: float,
    ) -> None:
        """
        Updates the balances after a buy order is completed, including handling reserved funds.

        Deducts the total cost (price * quantity + fee) from the reserved fiat balance,
        releases any unused reserved fiat back to the main balance, adds the purchased
        crypto quantity to the crypto balance, and tracks the fees incurred.

        Args:
            quantity: The quantity of crypto purchased.
            price: The price at which the crypto was purchased (per unit).
        """
        fee = self.fee_calculator.calculate_fee(quantity * price)
        total_cost = quantity * price + fee

        self.reserved_fiat -= total_cost
        if self.reserved_fiat < 0:
            self.balance += self.reserved_fiat  # Adjust with excess reserved fiat
            self.reserved_fiat = 0

        self.crypto_balance += quantity
        self.total_fees += fee
        self.logger.info(f"Buy order completed: {quantity} crypto purchased at {price}.")

    def _update_after_sell_order_filled(
        self,
        quantity: float,
        price: float,
    ) -> None:
        """
        Updates the balances after a sell order is completed, including handling reserved funds.
        Implements Profit Recycling to specific Operational Reserve.
        """
        fee = self.fee_calculator.calculate_fee(quantity * price)
        sale_proceeds = quantity * price - fee
        self.reserved_crypto -= quantity

        if self.reserved_crypto < 0:
            self.crypto_balance += abs(self.reserved_crypto)  # Adjust with excess reserved crypto
            self.reserved_crypto = 0

        # --- Dynamic Fee Stabilization (FR-01/02) ---
        # 1. Calculate Replacement Cost (Fee * Safety Multiplier)
        # We assume we need to buy back roughly same amount, so fee will be similar.
        safety_multiplier = 1.1
        replacement_cost = fee * safety_multiplier

        # 2. Allocate to Reserve
        # Ensure we don't take more than the proceeds (sanity check)
        allocation = min(replacement_cost, sale_proceeds)

        self.operational_reserve += allocation
        net_proceeds = sale_proceeds - allocation

        self.balance += net_proceeds
        self.total_fees += fee

        self.logger.info(
            f"Sell order completed: {quantity} crypto sold at {price}. "
            f"Recycled {allocation:.4f} to Reserve. Net Proceeds: {net_proceeds:.4f}"
        )

    def update_after_initial_purchase(self, initial_order: Order):
        """
        Updates balances after an initial crypto purchase.

        Args:
            initial_order: The Order object containing details of the completed purchase.
        """
        if initial_order.status != OrderStatus.CLOSED:
            raise ValueError(f"Order {initial_order.id} is not CLOSED. Cannot update balances.")

        total_cost = initial_order.filled * initial_order.average
        fee = self.fee_calculator.calculate_fee(initial_order.amount * initial_order.average)

        self.crypto_balance += initial_order.filled
        self.balance -= total_cost + fee
        self.total_fees += fee
        self.logger.info(
            f"Updated balances. Crypto balance: {self.crypto_balance}, "
            f"Fiat balance: {self.balance}, Total fees: {self.total_fees}",
        )

    def reserve_funds_for_buy(
        self,
        amount: float,
    ) -> None:
        """
        Reserves fiat for a pending buy order.

        Args:
            amount: The amount of fiat to reserve.
        """
        if self.balance < amount:
            raise InsufficientBalanceError(f"Insufficient fiat balance to reserve {amount}. Available: {self.balance}")

        self.reserved_fiat += amount
        self.balance -= amount
        self.logger.info(f"Reserved {amount} fiat for a buy order. Remaining fiat balance: {self.balance}.")

    def reserve_funds_for_sell(
        self,
        quantity: float,
    ) -> None:
        """
        Reserves crypto for a pending sell order.

        Args:
            quantity: The quantity of crypto to reserve.
        """
        if self.crypto_balance < quantity:
            raise InsufficientCryptoBalanceError(
                f"Insufficient crypto balance to reserve {quantity}. Available: {self.crypto_balance}"
            )

        self.reserved_crypto += quantity
        self.crypto_balance -= quantity
        self.logger.info(
            f"Reserved {quantity} crypto for a sell order. Remaining crypto balance: {self.crypto_balance}.",
        )

    def get_adjusted_fiat_balance(self) -> float:
        """
        Returns the fiat balance, including reserved funds.

        Returns:
            float: The total fiat balance including reserved funds.
        """
        return self.balance + self.reserved_fiat

    def get_adjusted_crypto_balance(self) -> float:
        """
        Returns the crypto balance, including reserved funds.

        Returns:
            float: The total crypto balance including reserved funds.
        """
        return self.crypto_balance + self.reserved_crypto

    def get_total_balance_value(self, price: float) -> float:
        """
        Calculates the total account value in fiat, including reserved funds.

        Args:
            price: The current market price of the crypto asset.

        Returns:
            float: The total account value in fiat terms.
        """
        return self.get_adjusted_fiat_balance() + self.get_adjusted_crypto_balance() * price
