import logging

import numpy as np

from config.config_manager import ConfigManager
from strategies.spacing_type import SpacingType
from strategies.strategy_type import StrategyType

from ..order_handling.order import Order, OrderSide
from .grid_level import GridCycleState, GridLevel


class GridManager:
    def __init__(
        self,
        config_manager: ConfigManager,
        strategy_type: StrategyType,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_manager: ConfigManager = config_manager
        self.strategy_type: StrategyType = strategy_type

        # Inject bot_id if available in config, needed for DB persistence
        self.bot_id = self.config_manager.config.get("bot_id")
        self.price_grids: list[float]
        self.central_price: float
        self.sorted_buy_grids: list[float]
        self.sorted_sell_grids: list[float]
        self.grid_levels: dict[float, GridLevel] = {}
        self.uniform_order_quantity: float | None = None  # Store the fixed quantity

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
        reserve_factor = 0.99  # 1% Fee Reserve
        usable_investment = investment * reserve_factor
        total_lines = len(self.price_grids)

        if total_lines > 0 and self.central_price > 0:
            raw_quantity = usable_investment / total_lines / self.central_price
            # TODO: Apply exchange step size floor precision if available.
            # For now, we assume standard precision or rely on OrderValidator to floor it safely.
            # We want to be slightly conservative, so 4-6 decimal places is usually safe for most coins except very low value ones.
            # Let's trust proper rounding in OrderValidator, but here we store the raw target.
            self.uniform_order_quantity = raw_quantity
            self.logger.info(
                f"?? Uniform Order Quantity Calculated: {self.uniform_order_quantity:.6f} "
                f"(Inv: {investment}, Usable: {usable_investment:.2f}, Lines: {total_lines}, Entry: {self.central_price})"
            )
        else:
            self.uniform_order_quantity = 0.0
            self.logger.warning("?? Could not calculate Uniform Quantity (Lines=0 or Price=0)")
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

    def update_zones_based_on_price(self, current_price: float):
        """
        Re-aligns the Buy/Sell zones based on the actual Current Market Price.
        Triggers Smart Trailing if price exits the grid range.
        """
        if self.strategy_type != StrategyType.SIMPLE_GRID:
            return

        self.logger.info(f"?? Re-aligning grid zones to Current Price: {current_price}")

        # 1. Smart Trailing Check
        lowest_grid = min(self.price_grids)
        highest_grid = max(self.price_grids)

        if current_price < lowest_grid:
            self.trail_grid_down(current_price)
            return  # Re-init happens inside, so we exit this cycle

        if current_price > highest_grid:
            self.trail_grid_up(current_price)
            return

        # 2. Normal Zone Update
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
                # Dead Zone (Ghost Area)
                # We do NOT add to sorted lists, so OrderManager won't place new orders here.
                # self.logger.debug(f"   Grid {price} in Dead Zone ({buy_threshold:.2f} - {sell_threshold:.2f})")
                continue

            # 3. FIX: Only update state if the grid is IDLE.
            # If it's WAITING_FOR_FILL, it has a live order. Do NOT touch it.
            if grid_level.state in [GridCycleState.READY_TO_BUY, GridCycleState.READY_TO_SELL]:
                grid_level.state = ideal_state
            else:
                self.logger.info(f"   Skipping state update for busy grid {price} (State: {grid_level.state})")

        # 4. FIX: Enforce Absolute Max Order Count
        # This must be done AFTER the loop has populated all candidates.
        max_allowed_orders = self.config_manager.get_num_grids()

        if not max_allowed_orders or max_allowed_orders < 1:
            max_allowed_orders = len(self.price_grids) - 1

        while (len(self.sorted_buy_grids) + len(self.sorted_sell_grids)) > max_allowed_orders:
            # Strategies to remove excess orders:
            # Remove furthest from current price.
            all_candidates = self.sorted_buy_grids + self.sorted_sell_grids
            furthest_grid = max(all_candidates, key=lambda p: abs(p - current_price))

            if furthest_grid in self.sorted_buy_grids:
                self.sorted_buy_grids.remove(furthest_grid)
            elif furthest_grid in self.sorted_sell_grids:
                self.sorted_sell_grids.remove(furthest_grid)

            # self.logger.info(f"DEBUG: Removed excess grid {furthest_grid} to offset count.")

        self.logger.info(f"   ? New Buy Grids: {len(self.sorted_buy_grids)}")
        self.logger.info(f"   ? New Sell Grids: {len(self.sorted_sell_grids)}")

    def get_trigger_price(self) -> float:
        return self.central_price

    def get_order_size_for_grid_level(
        self,
        total_balance: float,
        current_price: float,
    ) -> float:
        # STRICT: Return the pre-calculated Uniform Quantity if available
        if self.uniform_order_quantity and self.uniform_order_quantity > 0:
            return self.uniform_order_quantity

        # Fallback (Should not be reached if init works)
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

        sell_grid_count = len([p for p in self.price_grids if p > current_price])
        total_grid_count = len(self.price_grids)

        if total_grid_count == 0:
            return 0.0

        target_crypto_ratio = sell_grid_count / total_grid_count
        target_crypto_value = total_portfolio_value * target_crypto_ratio

        fiat_to_allocate = target_crypto_value - current_crypto_value_in_fiat
        fiat_to_allocate = max(0, min(fiat_to_allocate, current_fiat_balance))

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

    def trail_grid_up(self, current_price: float):
        """
        Smart Trailing Logic: Shift the ENTIRE grid window UP.
        Maintains the same number of grids and spacing/ratio.
        """
        self.logger.info(f"🚀 TRAILING UP: Shifting grid window UP to cover {current_price}")

        old_bottom, old_top, num_grids, spacing_type = self._extract_grid_config()

        if spacing_type == SpacingType.ARITHMETIC:
            # Shift by one grid interval? Or shift such that current price is inside?
            # Standard trailing: Top moves up, Bottom moves up.
            # We calculate the Shift Amount needed.
            # If we just want to expand, that's different.
            # "Trail Up" usually means following the price.
            # Let's shift so that the NEW Top is slightly above current price?
            # Or just Add 1 Grid to Top and Remove 1 from Bottom?
            grid_gap = (old_top - old_bottom) / num_grids
            new_bottom = old_bottom + grid_gap
            new_top = old_top + grid_gap

            # Consistency check: Ensure current_price is within (new_bottom, new_top) roughly?
            # If price jumped massively, one shift might not be enough.
            # Recursively shift? Or calculate target.
            if current_price > new_top:
                # Big jump: Shift window to center? No, user said "Trail".
                # Let's shift enough steps.
                steps = int((current_price - old_top) / grid_gap) + 1
                new_bottom = old_bottom + (grid_gap * steps)
                new_top = old_top + (grid_gap * steps)

        else:  # GEOMETRIC
            # Ratio = (Top/Bottom)^(1/N)
            ratio = (old_top / old_bottom) ** (1 / num_grids)
            new_bottom = old_bottom * ratio
            new_top = old_top * ratio

            if current_price > new_top:
                # Calculate needed steps
                # top * (ratio^steps) >= current
                # ratio^steps >= current/top
                # steps * ln(ratio) >= ln(current/top)
                import math

                steps = math.ceil(math.log(current_price / old_top) / math.log(ratio))
                multiplier = ratio**steps
                new_bottom = old_bottom * multiplier
                new_top = old_top * multiplier

        self.logger.info(f"   New Range: {new_bottom:.4f} - {new_top:.4f}")
        self._update_and_persist_config(new_bottom, new_top)
        self.initialize_grids_and_levels()

    def trail_grid_down(self, current_price: float):
        """
        Smart Trailing Logic: Shift the ENTIRE grid window DOWN.
        """
        self.logger.info(f"📉 TRAILING DOWN: Shifting grid window DOWN to cover {current_price}")

        old_bottom, old_top, num_grids, spacing_type = self._extract_grid_config()

        if spacing_type == SpacingType.ARITHMETIC:
            grid_gap = (old_top - old_bottom) / num_grids
            new_bottom = old_bottom - grid_gap
            new_top = old_top - grid_gap

            if current_price < new_bottom:
                steps = int((old_bottom - current_price) / grid_gap) + 1
                new_bottom = old_bottom - (grid_gap * steps)
                new_top = old_top - (grid_gap * steps)

        else:  # GEOMETRIC
            ratio = (old_top / old_bottom) ** (1 / num_grids)
            new_bottom = old_bottom / ratio
            new_top = old_top / ratio

            if current_price < new_bottom:
                import math

                # bottom / (ratio^steps) <= current
                # bottom/current <= ratio^steps
                steps = math.ceil(math.log(old_bottom / current_price) / math.log(ratio))
                divisor = ratio**steps
                new_bottom = old_bottom / divisor
                new_top = old_top / divisor

        self.logger.info(f"   New Range: {new_bottom:.4f} - {new_top:.4f}")
        self._update_and_persist_config(new_bottom, new_top)
        self.initialize_grids_and_levels()

    def _update_and_persist_config(self, new_bottom: float, new_top: float):
        """
        Updates the in-memory config and persists it to the DB.
        """
        # 1. Update ConfigManager state
        self.config_manager.config["grid_strategy"]["range"]["top"] = new_top
        self.config_manager.config["grid_strategy"]["range"]["bottom"] = new_bottom

        # 2. Persist to DB
        if hasattr(self, "bot_id") and self.bot_id:
            # Need to access BotDatabase.
            # GridManager doesn't natively have BotDatabase access in __init__,
            # but OrderManager does. Or we can instantiate it purely for storage.
            # Better: GridManager should perhaps rely on OrderManager?
            # Or just allow GridManager to initiate a DB connection for this persistence event.
            import json

            from core.storage.bot_database import BotDatabase

            db = BotDatabase()

            # serializing the FULL config might be heavy, but necessary.
            # We assume self.config_manager.config is the dict structure we want to save.
            config_json = json.dumps(self.config_manager.config)
            db.update_bot_status(self.bot_id, "RUNNING", config_json)
            self.logger.info("💾 Grid Configuration Persisted to DB.")

    def calculate_next_price_up(self, current_highest: float) -> float:
        """Calculates the next grid price ABOVE the current highest."""
        _, _, num_grids, spacing_type = self._extract_grid_config()
        # Note: num_grids in config is technically intervals or levels?
        # In _calculate... we generate num_grids + 1 points.
        # Spacing is derived from current range.

        # We need the current Grid Gap or Ratio
        sorted_grids = sorted(self.price_grids)
        if len(sorted_grids) < 2:
            return current_highest * 1.01  # Fallback

        if spacing_type == SpacingType.ARITHMETIC:
            gap = sorted_grids[-1] - sorted_grids[-2]
            return current_highest + gap
        else:  # GEOMETRIC
            # Be careful with floating point math.
            if sorted_grids[-2] == 0:
                ratio = 1.01
            else:
                ratio = sorted_grids[-1] / sorted_grids[-2]
            return current_highest * ratio

    def calculate_next_price_down(self, current_lowest: float) -> float:
        """Calculates the next grid price BELOW the current lowest."""
        _, _, num_grids, spacing_type = self._extract_grid_config()

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
