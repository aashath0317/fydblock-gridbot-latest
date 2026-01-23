import logging

import numpy as np

from config.config_manager import ConfigManager
from core.storage.bot_database import BotDatabase
from strategies.spacing_type import SpacingType
from strategies.strategy_type import StrategyType
from typing import Any

from ..order_handling.order import Order, OrderSide
from .grid_level import GridCycleState, GridLevel


class GridManager:
    def __init__(
        self,
        config_manager: ConfigManager,
        strategy_type: StrategyType,
        db: Any = None,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_manager: ConfigManager = config_manager
        self.strategy_type: StrategyType = strategy_type

        # Inject bot_id if available in config, needed for DB persistence
        self.bot_id = self.config_manager.config.get("bot_id")
        self.db = db
        self.price_grids: list[float]
        self.central_price: float
        self.sorted_buy_grids: list[float]
        self.sorted_sell_grids: list[float]
        self.grid_levels: dict[float, GridLevel] = {}
        self.uniform_order_quantity: float | None = None  # Store the fixed quantity (Base)
        self.auto_usdt_per_grid: float | None = None  # Store auto-calculated USDT per grid (Quote)

    @property
    def num_grids(self) -> int:
        return self.config_manager.get_num_grids()

    def initialize_grids_and_levels(self) -> None:
        """
        Initializes the grid levels and assigns their respective states.
        Calculates the Uniform Order Quantity ensuring 1% reserve for fees.
        """
        self.price_grids, self.central_price = self._calculate_price_grids_and_central_price()

        # --- UNIFORM QUANTITY CALCULATION ---
        # Formula: (Investment * 0.99) / (Total Grids) / Central Price
        # This ensures every buy order is identical and we have a reserve for fees.
        investment = self.config_manager.get_investment_amount()
        amount_per_grid = self.config_manager.get_amount_per_grid()
        order_size_type = self.config_manager.get_order_size_type()

        # If explicit amount is set, we prioritize that logic in get_order_size_for_grid_level
        # But for 'base' mode, we can pre-set uniform_order_quantity here.
        if amount_per_grid > 0 and order_size_type == "base":
            self.uniform_order_quantity = amount_per_grid
            self.logger.info(f"?? Using Configured Fixed Coin Quantity: {self.uniform_order_quantity}")
        elif amount_per_grid > 0 and order_size_type == "quote":
            self.uniform_order_quantity = None  # Dynamic calculation needed
            self.logger.info(f"?? Using Configured Fixed USDT Amount: {amount_per_grid}")
        else:
            # Legacy / Auto-Calculate Mode (Standard Grid)
            # Legacy / Auto-Calculate Mode (Standard Grid)
            usable_investment = investment * 0.99
            total_lines = len(self.price_grids)

            if total_lines > 0:
                if order_size_type == "quote":
                    # Fixed USDT (Quote) Auto-Calculation
                    self.auto_usdt_per_grid = usable_investment / total_lines
                    self.uniform_order_quantity = None
                    self.logger.info(
                        f"?? Auto-Calculated Fixed USDT: {self.auto_usdt_per_grid:.2f} "
                        f"(Inv: {investment}, Usable: {usable_investment:.2f}, Lines: {total_lines})"
                    )
                else:
                    # Fixed Coin (Base) Auto-Calculation - Legacy Behavior
                    if self.central_price > 0:
                        raw_quantity = usable_investment / total_lines / self.central_price
                        self.uniform_order_quantity = raw_quantity
                        self.logger.info(
                            f"?? Uniform Order Quantity Calculated: {self.uniform_order_quantity:.6f} "
                            f"(Inv: {investment}, Usable: {usable_investment:.2f}, Lines: {total_lines}, Entry: {self.central_price})"
                        )
                    else:
                        self.uniform_order_quantity = 0.0
            else:
                self.uniform_order_quantity = 0.0
                self.logger.warning("?? Could not calculate Uniform Quantity (Lines=0)")
        # ------------------------------------

        if self.strategy_type == StrategyType.SIMPLE_GRID:
            # Initial static assignment
            # FIX: Use "<" instead of "<=" to exclude the central price from BUY orders.
            # This creates the "blank in center" requested by the user.
            self.sorted_buy_grids = [price_grid for price_grid in self.price_grids if price_grid < self.central_price]
            self.sorted_sell_grids = [price_grid for price_grid in self.price_grids if price_grid > self.central_price]
            self.grid_levels = {
                price: GridLevel(
                    price,
                    # FIX: Center Grid Logic
                    # If price is at or above central price, we treat it as "Already Bought" (Initial Position).
                    # So its state is READY_TO_SELL.
                    # Only prices strictly BELOW central are READY_TO_BUY.
                    GridCycleState.READY_TO_BUY if price < self.central_price else GridCycleState.READY_TO_SELL,
                )
                for price in self.price_grids
            }

        elif self.strategy_type == StrategyType.HEDGED_GRID:
            self.sorted_buy_grids = self.price_grids[:-1]  # All except the top grid
            self.sorted_sell_grids = self.price_grids[1:]  # All except the bottom grid
            self.grid_levels = {
                price: GridLevel(
                    price,
                )
                for price in self.price_grids
            }

        # 4. FIX: Enforce Absolute Max Order Count (Startup)
        max_allowed_orders = self.config_manager.get_num_grids()
        if not max_allowed_orders or max_allowed_orders < 1:
            max_allowed_orders = len(self.price_grids) - 1

        while (len(self.sorted_buy_grids) + len(self.sorted_sell_grids)) > max_allowed_orders:
            # Strategies to remove excess orders:
            # Remove furthest from central price (initial state).
            all_candidates = self.sorted_buy_grids + self.sorted_sell_grids
            furthest_grid = max(all_candidates, key=lambda p: abs(p - self.central_price))

            if furthest_grid in self.sorted_buy_grids:
                self.sorted_buy_grids.remove(furthest_grid)
            elif furthest_grid in self.sorted_sell_grids:
                self.sorted_sell_grids.remove(furthest_grid)

            self.logger.info(f"Startup: Trimming excess grid {furthest_grid} to match limit {max_allowed_orders}")

        self.logger.info(f"Grids and levels initialized. Central price: {self.central_price}")
        self.logger.info(f"Final Count: {len(self.sorted_buy_grids)} Buys, {len(self.sorted_sell_grids)} Sells")

    def get_dead_zone_thresholds(self, current_price: float) -> tuple[float, float]:
        """
        Calculates the dynamic dead zone thresholds based on the local grid gap.
        Rule: threshold_margin = grid_gap / 2.
        Returns (buy_threshold, sell_threshold).
        """
        if len(self.price_grids) < 2:
            # Fallback for insufficient grids (should not happen in valid state)
            return current_price * 0.995, current_price * 1.005

        sorted_grids = sorted(self.price_grids)
        gap = 0.0

        # Find where current_price fits
        if current_price < sorted_grids[0]:
            gap = sorted_grids[1] - sorted_grids[0]
        elif current_price >= sorted_grids[-1]:
            gap = sorted_grids[-1] - sorted_grids[-2]
        else:
            # Find the interval containing current_price
            for i in range(len(sorted_grids) - 1):
                if sorted_grids[i] <= current_price < sorted_grids[i + 1]:
                    gap = sorted_grids[i + 1] - sorted_grids[i]
                    break

        # If still 0 for some reason (e.g. strict equality edge cases), default to some logic
        if gap == 0:
            # Try to find *any* gap?
            gap = sorted_grids[1] - sorted_grids[0]

        threshold_margin = gap / 2.0
        return current_price - threshold_margin, current_price + threshold_margin

    async def update_zones_based_on_price(self, current_price: float, cancel_order_callback=None):
        """
        Re-aligns the Buy/Sell zones based on the actual Current Market Price.
        Triggers Smart Trailing (Infinity Grid) if price exits the grid range.
        """
        if self.strategy_type != StrategyType.SIMPLE_GRID:
            return

        self.logger.info(f"🔄 Re-aligning grid zones to Current Price: {current_price}")

        # 1. Smart Trailing Check (Infinity Grid)
        lowest_grid = min(self.price_grids)
        highest_grid = max(self.price_grids)

        # Gap calculation (Rough estimate for trigger check)
        # We need the gap to ensure we trail at the right intervals
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) > 1:
            # Assume Arithmetic for simple gap check or use average
            gap = sorted_grids[1] - sorted_grids[0]
            # Safety: Ensure gap is positive
            gap = max(gap, 0.000001)

            # TRAIL DOWN
            # If price drops below lowest_grid - gap (meaning it crossed a virtual new grid line below)
            if current_price <= lowest_grid - gap:
                await self.trail_grid_down(current_price, cancel_order_callback)
                return

            # TRAIL UP
            # If price goes above highest_grid + gap (meaning it crossed a virtual new grid line above)
            if current_price >= highest_grid + gap:
                await self.trail_grid_up(current_price, cancel_order_callback)
                return

        # ... Rest of the logic for Normal Zone Update remains mostly same but we skip it if we trailed ...
        # Actually, standard zone logic is still good to keep internal states tidy.

        self.sorted_buy_grids = []
        self.sorted_sell_grids = []

        # Dynamic Buffer Thresholds
        safe_buy_limit, safe_sell_limit = self.get_dead_zone_thresholds(current_price)
        self.logger.info(f"   Dead Zone: {safe_buy_limit:.4f} - {safe_sell_limit:.4f}")

        for price in self.price_grids:
            grid_level = self.grid_levels[price]

            # 1. Determine Zone
            if price <= safe_buy_limit:
                # Clearly in Buy Zone
                self.sorted_buy_grids.append(price)
                ideal_state = GridCycleState.READY_TO_BUY

            elif price >= safe_sell_limit:
                # Clearly in Sell Zone
                self.sorted_sell_grids.append(price)
                ideal_state = GridCycleState.READY_TO_SELL

            else:
                # Dead Zone
                continue

            # 3. FIX: Only update state if the grid is IDLE.
            if grid_level.state in [GridCycleState.READY_TO_BUY, GridCycleState.READY_TO_SELL]:
                # In Infinite Grid, we trust the DB/Trail state mostly, but this auto-correction is okay for startup.
                # However, we should be careful not to flip states that are "holding bags".
                pass
                # grid_level.state = ideal_state
                # COMMENTED OUT: We stop auto-flipping states here because Infinite Grid manages states explicitly.
                # If we flip a "READY_TO_SELL" (Holding Bag) back to "READY_TO_BUY" just because price moved, we lose the bag logic!
            else:
                self.logger.info(f"   Skipping state update for busy grid {price} (State: {grid_level.state})")

        self.logger.info(f"   📊 Active Buy Grids: {len(self.sorted_buy_grids)}")
        self.logger.info(f"   📊 Active Sell Grids: {len(self.sorted_sell_grids)}")

    def get_trigger_price(self) -> float:
        return self.central_price

    def get_order_size_for_grid_level(
        self,
        total_balance: float,
        current_price: float,
    ) -> float:
        # 1. Infinity Grid / Explicit Config Support
        amount_per_grid = self.config_manager.get_amount_per_grid()
        order_size_type = self.config_manager.get_order_size_type()

        if amount_per_grid > 0:
            if order_size_type == "quote":
                # Fixed USDT -> Calculate Base Quantity dynamically
                # amount / price
                return amount_per_grid / current_price
            elif order_size_type == "base":
                # Fixed Coin -> Constant
                return amount_per_grid

        # 2. STRICT: Return the pre-calculated Uniform Quantity if available (Legacy/Standard)
        if self.uniform_order_quantity and self.uniform_order_quantity > 0:
            return self.uniform_order_quantity

        # 3. Auto Quote (Fixed USDT)
        if self.auto_usdt_per_grid and self.auto_usdt_per_grid > 0:
            return self.auto_usdt_per_grid / current_price

        # 3. Fallback (Should not be reached if init works)
        total_grids = len(self.grid_levels)
        if total_grids == 0:
            return 0.0
        investment = self.config_manager.get_investment_amount()
        if investment <= 0:
            investment = total_balance

        # Apply 1% reserve even in fallback
        usable = investment * 0.99
        order_size = usable / total_grids / self.central_price
        return order_size

    def get_initial_order_quantity(
        self,
        current_fiat_balance: float,
        current_crypto_balance: float,
        current_price: float,
    ) -> float:
        current_crypto_value_in_fiat = current_crypto_balance * current_price
        total_portfolio_value = current_fiat_balance + current_crypto_value_in_fiat

        # FIX: Check for explicit "Frontend Projected" allocation first
        # hierarchy:
        # 1. explicit 'initial_base_balance_allocation' (The USDT value we WANT in Base)
        # 2. calculated requirement based on grid summation

        grid_strategy_config = self.config_manager.config.get("grid_strategy", {})
        initial_base_budget = grid_strategy_config.get("initial_base_balance_allocation", 0.0)

        if initial_base_budget > 0:
            # The frontend explicitly told us: "We want $X worth of Base coin"
            # We just need to buy enough to reach that target, accounting for what we already have.

            # If we already have crypto, subtract its value
            needed_fiat_value = initial_base_budget - current_crypto_value_in_fiat

            # Cap at available balance
            # Also cap at investment amount if set? The 'initial_base_budget' is usually part of the investment.
            # But let's respect available fiat.
            # FIX: Add 0.5% buffer for rounding/dust to ensure we have enough for all grids
            fiat_to_allocate = max(0, min(needed_fiat_value * 1.005, current_fiat_balance))

            self.logger.info(
                f"🎯 Using Explicit Base Allocation: Target=${initial_base_budget:.2f}, Need=${needed_fiat_value:.2f}"
            )
            return fiat_to_allocate / current_price

        # Fallback to calculated logic
        # FIX: Use configured investment if available, else total balance
        investment = self.config_manager.get_investment_amount()
        if investment <= 0:
            investment = total_portfolio_value

        # Calculate TOTAL required crypto value for all SELL grids
        # (This is more accurate than simple ratio if we have varying grid sizes/prices)
        required_crypto_value = 0.0

        # We need to cover all grids ABOVE current price
        sell_grids = [p for p in self.price_grids if p > current_price]

        # If we can calculate exact requirements per grid:
        total_balance_for_calc = investment  # Use investment as the 'total' context

        for p in sell_grids:
            # How much crypto is needed for this grid?
            # Fixed USDT: size = USDT / p. Value = (USDT/p) * p = USDT.
            # Fixed Coin: size = Coin. Value = Coin * p.
            qty = self.get_order_size_for_grid_level(total_balance_for_calc, p)
            required_crypto_value += qty * current_price  # Current value of that future sell obligation

        # Add 1% buffer for fee safety
        target_crypto_value = required_crypto_value * 1.01

        fiat_to_allocate = target_crypto_value - current_crypto_value_in_fiat
        fiat_to_allocate = max(0, min(fiat_to_allocate, current_fiat_balance, investment))

        return fiat_to_allocate / current_price

    def pair_grid_levels(
        self,
        source_grid_level: GridLevel,
        target_grid_level: GridLevel,
        pairing_type: str,
    ) -> None:
        if pairing_type == "buy":
            source_grid_level.paired_buy_level = target_grid_level
            target_grid_level.paired_sell_level = source_grid_level
            self.logger.info(
                f"Paired sell grid level {source_grid_level.price} with buy grid level {target_grid_level.price}.",
            )
        elif pairing_type == "sell":
            source_grid_level.paired_sell_level = target_grid_level
            target_grid_level.paired_buy_level = source_grid_level
            self.logger.info(
                f"Paired buy grid level {source_grid_level.price} with sell grid level {target_grid_level.price}.",
            )
        else:
            raise ValueError(f"Invalid pairing type: {pairing_type}. Must be 'buy' or 'sell'.")

    def get_paired_sell_level(
        self,
        buy_grid_level: GridLevel,
    ) -> GridLevel | None:
        if self.strategy_type == StrategyType.SIMPLE_GRID:
            sorted_prices = sorted(self.price_grids)
            try:
                idx = sorted_prices.index(buy_grid_level.price)
                if idx + 1 < len(sorted_prices):
                    sell_price = sorted_prices[idx + 1]
                    return self.grid_levels[sell_price]
            except ValueError:
                pass
            return None

        elif self.strategy_type == StrategyType.HEDGED_GRID:
            sorted_prices = sorted(self.price_grids)
            current_index = sorted_prices.index(buy_grid_level.price)
            if current_index + 1 < len(sorted_prices):
                paired_sell_price = sorted_prices[current_index + 1]
                return self.grid_levels[paired_sell_price]
            return None
        return None

    def get_grid_level_below(self, grid_level: GridLevel) -> GridLevel | None:
        sorted_levels = sorted(self.grid_levels.keys())
        current_index = sorted_levels.index(grid_level.price)
        if current_index > 0:
            lower_price = sorted_levels[current_index - 1]
            return self.grid_levels[lower_price]
        return None

    def get_grid_level_above(self, grid_level: GridLevel) -> GridLevel | None:
        sorted_levels = sorted(self.grid_levels.keys())
        current_index = sorted_levels.index(grid_level.price)
        if current_index < len(sorted_levels) - 1:
            higher_price = sorted_levels[current_index + 1]
            return self.grid_levels[higher_price]
        return None

    def mark_order_pending(
        self,
        grid_level: GridLevel,
        order: Order,
    ) -> None:
        grid_level.add_order(order)
        if order.side == OrderSide.BUY:
            grid_level.state = GridCycleState.WAITING_FOR_BUY_FILL
            self.logger.info(f"Buy order placed and marked as pending at grid level {grid_level.price}.")
        elif order.side == OrderSide.SELL:
            grid_level.state = GridCycleState.WAITING_FOR_SELL_FILL
            self.logger.info(f"Sell order placed and marked as pending at grid level {grid_level.price}.")

    def complete_order(
        self,
        grid_level: GridLevel,
        order_side: OrderSide,
    ) -> None:
        if self.strategy_type == StrategyType.SIMPLE_GRID:
            if order_side == OrderSide.BUY:
                grid_level.state = GridCycleState.READY_TO_SELL
                self.logger.info(
                    f"Buy order completed at grid level {grid_level.price}. Transitioning to READY_TO_SELL.",
                )
                if grid_level.paired_sell_level:
                    grid_level.paired_sell_level.state = GridCycleState.READY_TO_SELL

            elif order_side == OrderSide.SELL:
                # FIX: Prevent race condition.
                if grid_level.state == GridCycleState.WAITING_FOR_BUY_FILL:
                    self.logger.info(
                        f"Sell order completed at {grid_level.price}, but level is already "
                        f"WAITING_FOR_BUY_FILL (claimed by neighbor). Keeping existing state."
                    )
                else:
                    grid_level.state = GridCycleState.READY_TO_BUY
                    self.logger.info(
                        f"Sell order completed at grid level {grid_level.price}. Transitioning to READY_TO_BUY.",
                    )

                if grid_level.paired_buy_level:
                    grid_level.paired_buy_level.state = GridCycleState.READY_TO_BUY

        elif self.strategy_type == StrategyType.HEDGED_GRID:
            if order_side == OrderSide.BUY:
                grid_level.state = GridCycleState.READY_TO_BUY_OR_SELL
                self.logger.info(
                    f"Buy order completed at grid level {grid_level.price}. Transitioning to READY_TO_BUY_OR_SELL.",
                )
                if grid_level.paired_sell_level:
                    grid_level.paired_sell_level.state = GridCycleState.READY_TO_SELL

            elif order_side == OrderSide.SELL:
                grid_level.state = GridCycleState.READY_TO_BUY_OR_SELL
                self.logger.info(
                    f"Sell order completed at grid level {grid_level.price}. Transitioning to READY_TO_BUY_OR_SELL.",
                )
                if grid_level.paired_buy_level:
                    grid_level.paired_buy_level.state = GridCycleState.READY_TO_BUY
        else:
            self.logger.error("Unexpected strategy type")

    def can_place_order(
        self,
        grid_level: GridLevel,
        order_side: OrderSide,
    ) -> bool:
        if self.strategy_type == StrategyType.SIMPLE_GRID:
            if order_side == OrderSide.BUY:
                # 1. Self Check: Must be ready to buy
                if grid_level.state != GridCycleState.READY_TO_BUY:
                    return False

                # 2. NEIGHBOR CHECK (The Fix)
                # Ensure the grid level ABOVE (where we would sell) is completely empty.
                # If the level above is busy buying, selling, or holding a bag,
                # we cannot buy here because we won't be able to place the sell order.
                paired_sell = self.get_paired_sell_level(grid_level)
                if paired_sell and paired_sell.state != GridCycleState.READY_TO_BUY:
                    # If the neighbor is not empty, block this trade to prevent "Grid Overlap"
                    return False

                return True

            elif order_side == OrderSide.SELL:
                # FIX: Allow placement if we have a bag (READY_TO_SELL)
                # OR if the level is empty (READY_TO_BUY) and we are pushing a bag from below.
                return grid_level.state in [GridCycleState.READY_TO_SELL, GridCycleState.READY_TO_BUY]

        elif self.strategy_type == StrategyType.HEDGED_GRID:
            if order_side == OrderSide.BUY:
                return grid_level.state in {GridCycleState.READY_TO_BUY, GridCycleState.READY_TO_BUY_OR_SELL}
            elif order_side == OrderSide.SELL:
                return grid_level.state in {GridCycleState.READY_TO_SELL, GridCycleState.READY_TO_BUY_OR_SELL}

        return False

    def _extract_grid_config(self) -> tuple[float, float, int, str]:
        bottom_range = self.config_manager.get_bottom_range()
        top_range = self.config_manager.get_top_range()
        num_grids = self.config_manager.get_num_grids()
        spacing_type = self.config_manager.get_spacing_type()
        return bottom_range, top_range, num_grids, spacing_type

    def _calculate_price_grids_and_central_price(self) -> tuple[list[float], float]:
        bottom_range, top_range, num_grids, spacing_type = self._extract_grid_config()

        self.logger.info(f"   ?? Lower Band: {bottom_range}")
        self.logger.info(f"   ?? Upper Band: {top_range}")

        # --- N+1 GRID LOGIC RESTORED ---
        # User goal: 30 Grids setting => 30 Pending Orders + 1 Active Position.
        # Total lines needed = 31.
        # So we MUST generate N+1 points.
        points_to_generate = num_grids + 1

        if spacing_type == SpacingType.ARITHMETIC:
            grids = np.linspace(bottom_range, top_range, points_to_generate)
            central_price = (top_range + bottom_range) / 2

        elif spacing_type == SpacingType.GEOMETRIC:
            grids = []
            if points_to_generate <= 1:
                grids = [bottom_range]
                central_price = bottom_range
            else:
                ratio = (top_range / bottom_range) ** (1 / (points_to_generate - 1))
                current_price = bottom_range
                for _ in range(points_to_generate):
                    grids.append(current_price)
                    current_price *= ratio
                central_index = len(grids) // 2
                central_price = grids[central_index]
        else:
            raise ValueError(f"Unsupported spacing type: {spacing_type}")

        # Remove duplicate assignments if any (unlikely with np.linspace but good safety)
        grids = sorted(list(set(grids)))

        return grids, central_price

    async def trail_grid_up(self, current_price: float, cancel_order_callback=None):
        """
        Infinite Grid Logic: Shift the grid interval UP one by one.
        """
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) < 2:
            return

        gap = sorted_grids[1] - sorted_grids[0]
        highest_grid = sorted_grids[-1]

        # Iteratively shift while price is far enough above
        while current_price >= highest_grid + gap:
            self.logger.info(f"🚀 TRAILING UP: Price {current_price} reached next level. Shifting Grid...")

            # --- SIMPLIFIED LOGIC (User Request) ---
            # We assume the config validation has blocked unsafe combinations (Arithmetic + Fixed Coin).
            # So we proceed without complex balance checks.

            # 1. Identify and Remove Lowest Grid
            # In Trail UP, the lowest grid (Buy) is dropped to make room for new Top (Sell).
            lowest_grid_price = sorted(self.price_grids)[0]
            lowest_grid_level = self.grid_levels.get(lowest_grid_price)

            if lowest_grid_level:
                # A) Cancel Order on Exchange (Release USDT)
                if cancel_order_callback:
                    self.logger.info(f"   Cancelling lowest grid order at {lowest_grid_price}...")
                    await cancel_order_callback(lowest_grid_level)

                if self.db and self.bot_id:
                    await self.db.delete_grid_level(self.bot_id, lowest_grid_price)

            # C) Shift Grid Structure (Remove Low, Add High)
            removed_price, new_top_price = self.extend_grid_up()

            # 2. Update Previous Top Grid -> READY_TO_BUY
            # The *old* highest grid was a Sell/Resistance. Now it's below us, so it becomes a Buy code.
            # Grid Manager extend_grid_up() adds the new top. The old top is now 2nd highest.
            prev_highest_grid = sorted(self.price_grids)[-2]  # New 2nd highest
            if prev_highest_grid in self.grid_levels:
                self.grid_levels[prev_highest_grid].state = GridCycleState.READY_TO_BUY
                if self.db and self.bot_id:
                    await self.db.update_grid_level_status(
                        self.bot_id, prev_highest_grid, GridCycleState.READY_TO_BUY.value
                    )

            # 3. New Top Grid is READY_TO_SELL (Default from extend_grid_up)
            # Add to DB
            if self.db and self.bot_id:
                await self.db.add_grid_level(self.bot_id, new_top_price, GridCycleState.READY_TO_SELL.value)

            # Update loop var
            highest_grid = new_top_price

            # Persist Config Range
            new_bottom = sorted(self.price_grids)[0]
            await self._update_and_persist_config(new_bottom, new_top_price)

    async def trail_grid_down(self, current_price: float, available_balance: float = 0.0, cancel_order_callback=None):
        """
        Infinite Grid Logic: Shift the grid interval DOWN one by one.
        Includes Reserve Check and Virtual Order Logic.
        """
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) < 2:
            return

        gap = sorted_grids[1] - sorted_grids[0]
        lowest_grid = sorted_grids[0]

        # Iteratively shift while price is far enough below
        while current_price <= lowest_grid - gap:
            self.logger.info(f"📉 TRAILING DOWN: Price {current_price} reached next level. Shifting Grid...")

            # 1. Reserve Check
            # We need enough USDT to place the new BUY order at the bottom.
            new_bottom_price = self.calculate_next_price_down(lowest_grid)
            needed_amount = self.get_order_size_for_grid_level(0, new_bottom_price)  # total_balance 0 to force calc

            # Note: available_balance must be passed in by the caller (OrderManager/Bot)
            if available_balance < needed_amount:
                self.logger.warning(
                    f"⚠️ Trailing Down PAUSED: Insufficient Reserve (Have: {available_balance:.2f}, Need: {needed_amount:.2f})"
                )
                return

            # 2. Virtual Order Logic
            # Cancel Highest Sell -> Mark as VIRTUAL_HOLD -> Place New Buy
            highest_grid_price = sorted(self.price_grids)[-1]
            highest_grid_level = self.grid_levels.get(highest_grid_price)

            if highest_grid_level:
                # A) Cancel Order on Exchange
                if cancel_order_callback:
                    self.logger.info(
                        f"   Cancelling highest grid order at {highest_grid_price} (Converting to Virtual)..."
                    )
                    await cancel_order_callback(highest_grid_level)

                # B) Mark as VIRTUAL_HOLD (Do NOT delete from DB, just update status)
                highest_grid_level.state = GridCycleState.VIRTUAL_HOLD
                highest_grid_level.state = GridCycleState.VIRTUAL_HOLD
                if self.db and self.bot_id:
                    await self.db.update_grid_level_status(
                        self.bot_id, highest_grid_price, GridCycleState.VIRTUAL_HOLD.value
                    )
                    self.logger.info(f"   MARKED {highest_grid_price} AS VIRTUAL_HOLD")

            # 3. Shift Grid Structure (Active Window)
            # We remove the Highest Grid from 'price_grids' (Active Trading Window)
            # but keep it in 'grid_levels' (Virtual Memory).
            if highest_grid_price in self.price_grids:
                self.price_grids.remove(highest_grid_price)

            # Add New Bottom
            self.price_grids.append(new_bottom_price)
            self.grid_levels[new_bottom_price] = GridLevel(new_bottom_price, GridCycleState.READY_TO_BUY)

            # DB Add New Bottom
            # DB Add New Bottom
            if self.db and self.bot_id:
                await self.db.add_grid_level(self.bot_id, new_bottom_price, GridCycleState.READY_TO_BUY.value)

            # 4. Check if we are re-entering a previously Virtual Level?
            # (User Logic: "If Price rises back... Re-activate")
            # This logic typically runs in 'update_zones' or 'trail_grid_up', not trail_grid_down.
            # Here we are just creating the new bottom.

            # Update loop vars
            sorted_grids = sorted(self.price_grids)  # Re-sort after mutation
            lowest_grid = sorted_grids[0]
            # Verify gap consistency if needed, but we rely on loop condition

            # Persist Config Range
            new_top_active = sorted_grids[-1]
            await self._update_and_persist_config(lowest_grid, new_top_active)

            # Decrement available balance simulation for the loop (approximate)
            available_balance -= needed_amount

    async def _update_and_persist_config(self, new_bottom: float, new_top: float):
        """
        Updates the in-memory config and persists it to the DB.
        """
        # 1. Update ConfigManager state
        self.config_manager.config["grid_strategy"]["range"]["top"] = new_top
        self.config_manager.config["grid_strategy"]["range"]["bottom"] = new_bottom

        # 2. Persist to DB
        if self.db and self.bot_id:
            try:
                import json

                # serializing the FULL config might be heavy, but necessary.
                config_json = json.dumps(self.config_manager.config)
                await self.db.update_bot_status(self.bot_id, "RUNNING", config_json)
                self.logger.info("💾 Grid Configuration Persisted to DB.")
            except Exception as e:
                self.logger.error(f"Failed to persist config: {e}")

    def calculate_next_price_up(self, current_highest: float) -> float:
        """Calculates the next grid price ABOVE the current highest."""
        _, _, num_grids, spacing_type = self._extract_grid_config()
        grid_gap = self.config_manager.get_grid_gap()

        # Infinity Grid: Use explicit Gap if available
        if grid_gap > 0:
            if spacing_type == SpacingType.ARITHMETIC:
                return current_highest + grid_gap
            else:  # GEOMETRIC
                # Gap is percentage (e.g., 2.0 = 2%)
                return current_highest * (1 + grid_gap / 100.0)

        # Legacy / Auto-derived logic
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) < 2:
            return current_highest * 1.01  # Fallback

        if spacing_type == SpacingType.ARITHMETIC:
            gap = sorted_grids[-1] - sorted_grids[-2]
            return current_highest + gap
        else:  # GEOMETRIC
            if sorted_grids[-2] == 0:
                ratio = 1.01
            else:
                ratio = sorted_grids[-1] / sorted_grids[-2]
            return current_highest * ratio

    def calculate_next_price_down(self, current_lowest: float) -> float:
        """Calculates the next grid price BELOW the current lowest."""
        _, _, num_grids, spacing_type = self._extract_grid_config()
        grid_gap = self.config_manager.get_grid_gap()

        # Infinity Grid: Use explicit Gap if available
        if grid_gap > 0:
            if spacing_type == SpacingType.ARITHMETIC:
                return current_lowest - grid_gap
            else:  # GEOMETRIC
                # Gap is percentage (e.g., 2.0 = 2%)
                # Using the user's explicit formula logic: (1 - Percentage)
                return current_lowest * (1 - grid_gap / 100.0)

        # Legacy / Auto-derived logic
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) < 2:
            return current_lowest * 0.99

        if spacing_type == SpacingType.ARITHMETIC:
            gap = sorted_grids[1] - sorted_grids[0]
            return current_lowest - gap
        else:  # GEOMETRIC
            if sorted_grids[0] == 0:
                ratio = 1.01
            else:
                ratio = sorted_grids[1] / sorted_grids[0]
            return current_lowest / ratio

    def extend_grid_up(self) -> tuple[float, float]:
        """
        Shifts the grid UP by one level (Infinity Grid).
        Removes Lowest -> Adds New Highest.
        Returns (removed_price, added_price)
        """
        sorted_grids = sorted(self.price_grids)
        lowest_price = sorted_grids[0]
        highest_price = sorted_grids[-1]

        # 1. Calculate New Top
        new_top = self.calculate_next_price_up(highest_price)

        # 2. Modify State
        if lowest_price in self.price_grids:
            self.price_grids.remove(lowest_price)
        if lowest_price in self.grid_levels:
            del self.grid_levels[lowest_price]

        self.price_grids.append(new_top)
        # Create new level as READY_TO_SELL (It's above price)
        self.grid_levels[new_top] = GridLevel(new_top, GridCycleState.READY_TO_SELL)

        # 3. Update Persisted Config (Shift Range)
        # We set new bottom to the *new* lowest grid (which was the 2nd lowest)
        new_bottom_config = sorted(self.price_grids)[0]
        self._update_and_persist_config(new_bottom_config, new_top)

        self.logger.info(f"🕸️ Infinity Grid Shift UP: Removed {lowest_price:.4f}, Added {new_top:.4f}")
        return lowest_price, new_top

    def extend_grid_down(self) -> tuple[float, float]:
        """
        Shifts the grid DOWN by one level (Infinity Grid).
        Removes Highest -> Adds New Lowest.
        Returns (removed_price, added_price)
        """
        sorted_grids = sorted(self.price_grids)
        lowest_price = sorted_grids[0]
        highest_price = sorted_grids[-1]

        # 1. Calculate New Bottom
        new_bottom = self.calculate_next_price_down(lowest_price)

        # 2. Modify State
        if highest_price in self.price_grids:
            self.price_grids.remove(highest_price)
        if highest_price in self.grid_levels:
            del self.grid_levels[highest_price]

        self.price_grids.append(new_bottom)
        # Create new level as READY_TO_BUY (It's below price)
        self.grid_levels[new_bottom] = GridLevel(new_bottom, GridCycleState.READY_TO_BUY)

        # 3. Update Config
        new_top_config = sorted(self.price_grids)[-1]
        self._update_and_persist_config(new_bottom, new_top_config)

        self.logger.info(f"🕸️ Infinity Grid Shift DOWN: Removed {highest_price:.4f}, Added {new_bottom:.4f}")
        return highest_price, new_bottom
