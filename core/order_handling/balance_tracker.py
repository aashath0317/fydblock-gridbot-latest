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
        db=None,  # BotDatabase
        bot_id: int | None = None,
    ):
        """
        Initializes the BalanceTracker.

        Args:
            event_bus: The event bus instance for subscribing to events.
            fee_calculator: The fee calculator instance for calculating trading fees.
            trading_mode: "BACKTEST", "LIVE" or "PAPER_TRADING".
            base_currency: The base currency symbol.
            quote_currency: The quote currency symbol.
            db: The persistent database instance.
            bot_id: The ID of the bot owning this tracker.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event_bus: EventBus = event_bus
        self.fee_calculator: FeeCalculator = fee_calculator
        self.trading_mode: TradingMode = trading_mode
        self.base_currency: str = base_currency
        self.quote_currency: str = quote_currency
        self.db = db
        self.bot_id = bot_id

        self.balance: float = 0.0
        self.crypto_balance: float = 0.0
        self.total_fees: float = 0
        self.reserved_fiat: float = 0.0
        self.reserved_crypto: float = 0.0
        self.investment_cap: float = float("inf")
        self.operational_reserve: float = 0.0  # Dynamic Fee Stabilization Reserve

    def _persist_balances(self):
        """Saves current balance state to DB."""
        if self.db and self.bot_id:
            # We persist what we OWN (Total Fiat, Total Crypto, Reserve)
            # Reserve usually refers to the 'Operational Reserve' (Fee Reserve), not locked order funds.
            self.db.update_balances(self.bot_id, self.balance, self.crypto_balance, self.operational_reserve)

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
        1% is moved to Operational Reserve for fees.
        """
        reservation_amount = initial_balance * 0.01
        self.operational_reserve = reservation_amount
        self.balance = initial_balance - reservation_amount

        self.crypto_balance = initial_crypto_balance
        self.investment_cap = initial_balance

        self.logger.info(
            f"Balance Tracker Initialized: {self.balance:.2f} {self.quote_currency} (Trading) / "
            f"{self.operational_reserve:.2f} {self.quote_currency} (Fee Reserve) / "
            f"{self.crypto_balance} {self.base_currency}"
        )
        self._persist_balances()

    def initialize_balances(self, fiat_balance: float, crypto_balance: float):
        """
        Manually initializes the balance state.
        Called by the Strategy during startup after validating real-world funds.
        """
        self.balance = fiat_balance
        self.crypto_balance = crypto_balance
        self.logger.info(
            f"✅ Balances synced with Wallet: {self.balance:.2f} {self.quote_currency}, "
            f"{self.crypto_balance} {self.base_currency}"
        )
        self._persist_balances()

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
            self._persist_balances()
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
        if self.trading_mode != TradingMode.LIVE:
            return

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
                self._persist_balances()
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
            self._update_after_buy_order_filled(order.filled, order.price, order.fee)
        elif order.side == OrderSide.SELL:
            self._update_after_sell_order_filled(order.filled, order.price)

    def _update_after_buy_order_filled(self, quantity: float, price: float, fee_data: dict | None = None) -> None:
        """
        Updates the balances after a buy order is completed.
        Handles fee deduction from either Reserve (Fiat) or Base (Crypto).
        """
        # Default Fee Calculation (if no actual data provided)
        estimated_fee = self.fee_calculator.calculate_fee(quantity * price)

        # 1. Total Cost in Fiat (Price * Qty)
        # Note: If fee is in Quote, usually the exchange *deducts* it from the acquired amount
        # OR takes it from the Quote balance.
        # For Spot Grid, usually:
        # Buy ETH/USDT -> Pay USDT, Receive ETH.
        # Fee is often taken from ETH received (Base) OR USDT paid (Quote).

        cost_in_fiat = quantity * price

        # Check actual fee data if available
        fee_in_base = 0.0
        fee_in_quote = 0.0

        if fee_data and fee_data.get("cost") is not None:
            # Use real fee info
            cost = float(fee_data["cost"])
            currency = fee_data.get("currency", "")

            if currency == self.base_currency:
                fee_in_base = cost
                self.logger.info(f"🧾 Fee paid in Base Currency ({self.base_currency}): {fee_in_base}")
            elif currency == self.quote_currency:
                fee_in_quote = cost
                self.logger.info(f"🧾 Fee paid in Quote Currency ({self.quote_currency}): {fee_in_quote}")
            else:
                # BNB Deduct or other
                self.logger.info(f"🧾 Fee paid in external currency ({currency}): {cost}")
        else:
            # Fallback: Assume fee is in Quote (old behavior)
            fee_in_quote = estimated_fee

        # 2. Update Fiat Reserve
        # We reserved 'quantity * price' roughly.
        # If fee is in Quote, cost is Price*Qty + Fee (if additive) or included.
        # usually Buy Cost = Price * Qty. Fee is separate or deducted.
        # Safe assumption: We release the locked amount.
        self.reserved_fiat -= cost_in_fiat

        # Handle "Fee in Quote" extra cost if handled that way
        if fee_in_quote > 0:
            if self.reserved_fiat >= fee_in_quote:
                self.reserved_fiat -= fee_in_quote
            else:
                self.balance -= fee_in_quote

        if self.reserved_fiat < 0:
            self.balance += self.reserved_fiat  # Release excess or cover small deficit
            self.reserved_fiat = 0

        # 3. Update Crypto Balance
        # Net Crypto = Quantity - Fee (if fee is in Base)
        net_crypto = quantity - fee_in_base
        self.crypto_balance += net_crypto

        self.total_fees += fee_in_quote + (fee_in_base * price)
        self.logger.info(f"Buy filled: +{net_crypto:.6f} {self.base_currency} (Gross: {quantity}, Fee: {fee_in_base}).")
        self._persist_balances()

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
            f"Sell order completed. Recycled {allocation:.4f} to Reserve. Net Proceeds: {net_proceeds:.4f}"
        )
        self._persist_balances()

    def allocate_profit_to_reserve(self, amount: float) -> None:
        """
        Allocates a specific amount of fiat profit to the operational reserve.
        This effectively 'banks' the profit, preventing it from being reinvested.
        """
        if amount <= 0:
            return

        if self.balance < amount:
            self.logger.warning(
                f"⚠️ Cannot allocate {amount} profit to reserve. "
                f"Available balance {self.balance} is less than profit (Funds likely reused). "
                f"Allocating max available."
            )
            amount = self.balance

        self.balance -= amount
        self.operational_reserve += amount
        self.logger.info(f"🏦 Banked Profit: Moved {amount:.4f} {self.quote_currency} to Reserve.")
        self._persist_balances()

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
        self._persist_balances()

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
        self._persist_balances()

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
        self._persist_balances()

    def register_open_order(self, order: Order, deduct_from_balance: bool = True) -> None:
        """
        Registers an existing open order (e.g. from Hot Boot) with the tracker,
        moving funds from 'balance' to 'reserved'.
        If deduct_from_balance is False, we assume funds were already deducted (e.g. not in free balance).
        """
        if order.side == OrderSide.BUY:
            cost = order.remaining * order.price
            if deduct_from_balance:
                if self.balance >= cost:
                    self.balance -= cost
                else:
                    self.logger.warning(
                        f"⚠️ Inconsistency during Hot Boot: registered buy order cost {cost} > available balance {self.balance}. "
                        f"Forcing reserve (Balance may go negative)."
                    )
                    self.balance -= cost

            self.reserved_fiat += cost
            self.logger.info(
                f"Registered Open BUY {order.identifier}: Reserved {cost:.2f} {self.quote_currency} (Deducted: {deduct_from_balance})"
            )
            self._persist_balances()

        elif order.side == OrderSide.SELL:
            amount = order.remaining
            if deduct_from_balance:
                self.crypto_balance -= amount

            self.reserved_crypto += amount
            self.logger.info(
                f"Registered Open SELL {order.identifier}: Reserved {amount:.6f} {self.base_currency} (Deducted: {deduct_from_balance})"
            )
            self._persist_balances()

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

    def check_rebalance_needs(self, required_fiat: float, required_crypto: float, current_price: float) -> dict | None:
        """
        Checks if the current balances are sufficient for the required grid.
        Returns a 'deficit' dictionary if rebalancing is needed, else None.

        Deficit format:
        {
            "type": "SELL_CRYPTO" | "BUY_CRYPTO",  # Action needed to fix deficit
            "amount": float,                       # Amount of CRYPTO to buy/sell
            "reason": str
        }
        """
        # 1. Check Crypto Deficit (for Sell Orders)
        # We need `required_crypto` available.
        if self.crypto_balance < required_crypto:
            shortfall = required_crypto - self.crypto_balance
            # Ensure shortfall is significant (> 1% of required or some min value)
            if shortfall > (required_crypto * 0.01):
                self.logger.warning(
                    f"⚠️ Rebalance Needed: Crypto Shortfall. Have {self.crypto_balance}, Need {required_crypto}."
                )
                return {
                    "type": "BUY_CRYPTO",
                    "amount": shortfall,
                    "reason": f"Insufficient {self.base_currency} for grid.",
                }

        # 2. Check Fiat Deficit (for Buy Orders)
        # We need `required_fiat` available.
        if self.balance < required_fiat:
            shortfall = required_fiat - self.balance
            if shortfall > (required_fiat * 0.01):
                # We need to raise 'shortfall' USDT.
                # Amount of crypto to SELL = shortfall / current_price
                crypto_to_sell = shortfall / current_price if current_price > 0 else 0
                if crypto_to_sell > 0:
                    self.logger.warning(
                        f"⚠️ Rebalance Needed: Fiat Shortfall. Have {self.balance}, Need {required_fiat}."
                    )
                    return {
                        "type": "SELL_CRYPTO",
                        "amount": crypto_to_sell,
                        "reason": f"Insufficient {self.quote_currency} for grid.",
                    }

        return None
