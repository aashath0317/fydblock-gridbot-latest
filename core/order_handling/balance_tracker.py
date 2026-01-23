import logging
import time

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
        self._last_drift_warning_time = 0

    async def _persist_balances(self):
        """Saves current balance state to DB."""
        if self.db and self.bot_id:
            # We persist what we OWN (Total Fiat, Total Crypto, Reserve)
            await self.db.update_balances(self.bot_id, self.balance, self.crypto_balance, self.operational_reserve)

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
        await self._persist_balances()

    async def initialize_balances(self, fiat_balance: float, crypto_balance: float):
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
        await self._persist_balances()

    async def load_persisted_balances(self) -> bool:
        """
        Loads the last known balance state from the database.
        Returns True if successful, False otherwise.
        """
        if not self.db or not self.bot_id:
            return False

        try:
            saved = await self.db.get_balances(self.bot_id)
            if saved:
                # { "fiat_balance": X, "crypto_balance": Y, "reserve_amount": Z }
                self.balance = float(saved.get("fiat_balance", 0.0))
                self.crypto_balance = float(saved.get("crypto_balance", 0.0))
                self.operational_reserve = float(saved.get("reserve_amount", 0.0))

                self.logger.info(
                    f"💾 Restored Isolated Balances from DB: Fiat={self.balance:.2f}, "
                    f"Crypto={self.crypto_balance:.4f}, Reserve={self.operational_reserve:.2f}"
                )
                return True
        except Exception as e:
            self.logger.error(f"Failed to load persisted balances: {e}")

        return False

    async def attempt_fee_recovery(self, required_amount: float) -> bool:
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
            await self._persist_balances()
            return True

        return False

    async def _fetch_live_balances(
        self,
        exchange_service: ExchangeInterface,
    ) -> tuple[float, float]:
        """
        Fetches live balances from the exchange asynchronously.
        For PAPER_TRADING, it attempts to load from the DB to preserve simulation state.

        Args:
            exchange_service: The exchange instance.

        Returns:
            tuple: The quote and base currency balances.
        """
        # FIX: For Paper Trading, we must NOT overwrite with Real Wallet Balance
        # We should load from DB if available, or default to Investment amount if fresh.
        if self.trading_mode == TradingMode.PAPER_TRADING:
            saved = await self.db.get_balances(self.bot_id) if self.db else None
            if saved:
                # { "fiat_balance": X, "crypto_balance": Y, "reserve_amount": Z }
                fiat = float(saved.get("fiat_balance", 0.0))
                crypto = float(saved.get("crypto_balance", 0.0))
                self.logger.info(
                    f"💾 Loaded PAPER Balances from DB: {fiat} {self.quote_currency}, {crypto} {self.base_currency}"
                )
                return fiat, crypto
            else:
                # If no DB entry, it might be a fresh start.
                # However, the caller usually initializes with investment_amount.
                # Returning 0, 0 here prevents overwriting with Real Wallet,
                # but we usually rely on 'initialize_balances' for the initial set.
                # Let's return the current self.balance (in-memory) if non-zero, or just 0.0
                return self.balance, self.crypto_balance

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
        CLAMPS funds to the 'User Allocated' Investment Cap to ensure strict isolation.
        This prevents the bot from seeing or spending the User's personal HODL stack.
        """
        if self.trading_mode not in [TradingMode.LIVE, TradingMode.PAPER_TRADING]:
            return None, None

        try:
            balances = await exchange_service.get_balance()
            if balances and "free" in balances and "total" in balances:
                # 1. Get Live Wallet State
                wallet_free_fiat = float(balances["free"].get(self.quote_currency, 0.0))
                wallet_free_crypto = float(balances["free"].get(self.base_currency, 0.0))

                wallet_total_fiat = float(balances["total"].get(self.quote_currency, 0.0))
                wallet_total_crypto = float(balances["total"].get(self.base_currency, 0.0))

                # 2. Determine Bot's "Locked" State (What we KNOW we own in orders)
                # These are funds we MUST account for.
                my_locked_fiat = self.reserved_fiat
                my_locked_crypto = self.reserved_crypto

                # 3. Calculate "Effective Cap" (Investment - Fee Reserves)
                # This is the max net value the bot is allowed to manage.
                effective_cap = self.investment_cap - self.operational_reserve

                # ---------------------------------------------------------
                # ISOLATION LOGIC: FIAT
                # ---------------------------------------------------------
                # Max Fiat we can possibly have = Cap - (My Locked Crypto Value) - (My Free Crypto Value)
                # But My Free Crypto is unknown yet.
                # Alternative: Clamp Fiat based on simple "Cap - Current Crypto Value"?
                # No, because Current Crypto Value might include User's 5 SOL.

                # We need to trust our Internal Ledger but "reconcile" downwards if Wallet is empty.
                # If Wallet has LESS than we think, we must shrink.
                # If Wallet has MORE than we think, we must IGNORE the excess.

                # Sanity Check 1: We cannot have more free fiat than the wallet has.
                checked_free_fiat = min(self.balance, wallet_free_fiat)

                # Sanity Check 2: We cannot have more free crypto than the wallet has.
                checked_free_crypto = min(self.crypto_balance, wallet_free_crypto)

                # 4. Check for External Deposit "Drift" (User added funds?)
                # If we suddenly see MORE funds, we do NOT auto-claim them unless explicitly resized.
                # So the `min` check above implicitly handles "User added 5 SOL".
                # self.crypto_balance (0.4) vs Wallet (5.4) -> min serves up 0.4. Correct.

                # 5. Check for External Withdrawal "Drift" (User took funds?)
                # self.crypto_balance (0.4) vs Wallet (0.2) -> min serves up 0.2.
                # We must accept this loss to avoid order errors.

                if checked_free_fiat != self.balance:
                    self.logger.warning(
                        f"📉 Fiat Sync: Internal {self.balance:.2f} -> Wallet Limit {checked_free_fiat:.2f} "
                        f"(User withdrew funds?)"
                    )
                    self.balance = checked_free_fiat

                if checked_free_crypto != self.crypto_balance:
                    self.logger.warning(
                        f"📉 Crypto Sync: Internal {self.crypto_balance:.6f} -> Wallet Limit {checked_free_crypto:.6f} "
                        f"(User withdrew funds?)"
                    )
                    self.crypto_balance = checked_free_crypto

                # Re-Persist (only if changed, but safe to call)
                # await self._persist_balances()
                # (Caller typically handles logic, but let's persist here to be safe)
                await self._persist_balances()

                return self.balance, self.crypto_balance

        except Exception as e:
            self.logger.error(f"Failed to sync balances: {e}")

        return None, None

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
            await self._update_after_buy_order_filled(order.filled, order.price, order.fee)
        elif order.side == OrderSide.SELL:
            await self._update_after_sell_order_filled(order.filled, order.price)

    async def _update_after_buy_order_filled(self, quantity: float, price: float, fee_data: dict | None = None) -> None:
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

        # 2. Update Fiat Reserve & Operational Reserve
        # Buy Cost usually comes from 'reserved_fiat' (Funds locked for this Grid).
        self.reserved_fiat -= cost_in_fiat

        # Handle "Fee in Quote"
        # Logic: Try to pay fee from Operational Reserve (Fee Reserve) first.
        # If not enough, pay from Balance or Reserved Fiat.
        if fee_in_quote > 0:
            if self.operational_reserve >= fee_in_quote:
                self.operational_reserve -= fee_in_quote
                self.logger.info(f"Paid {fee_in_quote:.4f} fee from Operational Reserve.")
            else:
                # Reserve empty, fallback to balance
                remaining_fee = fee_in_quote - self.operational_reserve
                self.operational_reserve = 0.0

                # Deduct from general balance (or remaining reserved fiat if applicable)
                self.balance -= remaining_fee
                self.logger.info(f"Paid fee partially/fully from Balance ({remaining_fee:.4f}).")

        if self.reserved_fiat < 0:
            self.balance += self.reserved_fiat  # Release excess or cover small deficit
            self.reserved_fiat = 0

        # 3. Update Crypto Balance
        # Net Crypto = Quantity - Fee (if fee is in Base)
        net_crypto = quantity - fee_in_base
        self.crypto_balance += net_crypto

        self.total_fees += fee_in_quote + (fee_in_base * price)
        self.logger.info(f"Buy filled: +{net_crypto:.6f} {self.base_currency} (Gross: {quantity}, Fee: {fee_in_base}).")
        await self._persist_balances()

    async def _update_after_sell_order_filled(
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

        if self.reserved_crypto < 0:
            self.crypto_balance += abs(self.reserved_crypto)  # Adjust with excess reserved crypto
            self.reserved_crypto = 0

        # --- Standard Sell Completion ---
        # Proceeds go to Balance (Fiat)
        # Note: Profit Tax (10%) is handled separately by OrderManager -> allocate_profit_to_reserve
        # So here we just free up the *entire* proceeds to 'balance'.
        # If OrderManager takes 10% later, it calls allocate_profit_to_reserve, which
        # MOVES it from 'balance' to 'operational_reserve'.

        self.balance += sale_proceeds
        self.total_fees += fee

        self.logger.info(f"Sell order completed. Proceeds: {sale_proceeds:.4f} {self.quote_currency} added to balance.")
        await self._persist_balances()

    async def allocate_profit_to_reserve(self, amount: float) -> None:
        """
        Allocates a specific amount of fiat profit to the operational reserve.
        Subject to a Cap: Stop allocating if Reserve >= 1% of Investment.
        """
        if amount <= 0:
            return

        # 1. Check Cap (1% of Investment)
        target_cap = self.investment_cap * 0.01
        if self.operational_reserve >= target_cap:
            return  # Reserve is full

        # 2. Partial Allocation if near cap
        space_remaining = target_cap - self.operational_reserve
        to_allocate = min(amount, space_remaining)

        if self.balance < to_allocate:
            self.logger.warning(
                f"⚠️ Cannot allocate {to_allocate} profit to reserve. Available balance {self.balance} is low."
            )
            to_allocate = self.balance  # Take what we can

        if to_allocate > 0:
            self.balance -= to_allocate
            self.operational_reserve += to_allocate
            self.logger.info(
                f"🏦 Banked Profit: Moved {to_allocate:.4f} {self.quote_currency} to Reserve (Cap: {target_cap:.2f})."
            )
            await self._persist_balances()

    async def update_after_initial_purchase(self, initial_order: Order):
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
        await self._persist_balances()

    async def reserve_funds_for_buy(
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
        await self._persist_balances()

    async def reserve_funds_for_sell(
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
        await self._persist_balances()

    async def release_reserve_for_buy(self, amount: float) -> None:
        """
        Releases reserved fiat when a buy order is cancelled.

        Args:
            amount: The amount of fiat to release.
        """
        # Ensure we don't release more than reserved (sanity check)
        amount_to_release = min(amount, self.reserved_fiat)

        self.reserved_fiat -= amount_to_release
        self.balance += amount_to_release
        self.logger.info(f"Released {amount_to_release} fiat from reserve (Cancelled Buy). Balance: {self.balance}.")
        await self._persist_balances()

    async def release_reserve_for_sell(self, quantity: float) -> None:
        """
        Releases reserved crypto when a sell order is cancelled.

        Args:
            quantity: The quantity of crypto to release.
        """
        # Ensure we don't release more than reserved
        qty_to_release = min(quantity, self.reserved_crypto)

        self.reserved_crypto -= qty_to_release
        self.crypto_balance += qty_to_release
        self.logger.info(
            f"Released {qty_to_release} crypto from reserve (Cancelled Sell). Crypto Balance: {self.crypto_balance}."
        )
        await self._persist_balances()

    async def register_open_order(self, order: Order, deduct_from_balance: bool = True) -> None:
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
            await self._persist_balances()

        elif order.side == OrderSide.SELL:
            amount = order.remaining
            if deduct_from_balance:
                self.crypto_balance -= amount

            self.reserved_crypto += amount
            self.logger.info(
                f"Registered Open SELL {order.identifier}: Reserved {amount:.6f} {self.base_currency} (Deducted: {deduct_from_balance})"
            )
            await self._persist_balances()

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
        # FIX: Check TOTAL owned crypto (Free + Reserved), not just free.
        # If funds are reserved, they are already supporting the grid orders, so we are good.
        total_crypto = self.get_adjusted_crypto_balance()
        if total_crypto < required_crypto:
            shortfall = required_crypto - total_crypto
            # Ensure shortfall is significant (> 1% of required or some min value)
            if shortfall > (required_crypto * 0.01):
                self.logger.warning(
                    f"⚠️ Rebalance Needed: Crypto Shortfall. Have {total_crypto} (Free+Reserved), Need {required_crypto}."
                )
                return {
                    "type": "BUY_CRYPTO",
                    "amount": shortfall,
                    "reason": f"Insufficient {self.base_currency} for grid.",
                }

        # 2. Check Fiat Deficit (for Buy Orders)
        # FIX: Check TOTAL owned fiat (Free + Reserved).
        total_fiat = self.get_adjusted_fiat_balance()
        if total_fiat < required_fiat:
            shortfall = required_fiat - total_fiat
            if shortfall > (required_fiat * 0.01):
                # We need to raise 'shortfall' USDT.
                # Amount of crypto to SELL = shortfall / current_price
                crypto_to_sell = shortfall / current_price if current_price > 0 else 0
                if crypto_to_sell > 0:
                    self.logger.warning(
                        f"⚠️ Rebalance Needed: Fiat Shortfall. Have {total_fiat} (Free+Reserved), Need {required_fiat}."
                    )
                    return {
                        "type": "SELL_CRYPTO",
                        "amount": crypto_to_sell,
                        "reason": f"Insufficient {self.quote_currency} for grid.",
                    }

        return None

    def adjust_capital_allocation(self, ratio: float) -> None:
        """
        Global Solvency Mechanism:
        Reduces the specific bot's liquid capital and reserves by a ratio < 1.0.
        Used when the user withdraws funds externally, causing a global deficit.
        """
        if ratio >= 1.0 or ratio <= 0.0:
            return

        old_bal = self.balance
        old_res = self.operational_reserve

        # Apply Haircut
        self.balance *= ratio
        self.operational_reserve *= ratio

        # We do NOT touch 'reserved_fiat' or 'reserved_crypto' as those back REAL open orders.
        # If the user withdrew funds backing open orders, those orders will just fail on execution.
        # But we validly shrink the 'buffer' (Balance + Fee Reserve) to match reality.

        self.logger.warning(
            f"📉 Solvency Adjustment (Ratio {ratio:.4f}): "
            f"Balance {old_bal:.4f} -> {self.balance:.4f}, "
            f"Reserve {old_res:.4f} -> {self.operational_reserve:.4f}"
        )
        self._persist_balances()
