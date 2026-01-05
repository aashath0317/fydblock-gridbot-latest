import sys
import os
import unittest
from unittest.mock import MagicMock

# Path checks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.grid_management.grid_manager import GridManager
from core.grid_management.grid_level import GridLevel, GridCycleState
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType


class TestMaxOrders(unittest.TestCase):
    def setUp(self):
        # Mock Config
        self.mock_config = MagicMock()
        self.mock_config.get_investment_amount.return_value = 1000.0
        self.mock_config.get_bottom_range.return_value = 100.0
        self.mock_config.get_top_range.return_value = 104.0
        self.mock_config.get_num_grids.return_value = 4
        # 4 Grids => 5 Lines: 100, 101, 102, 103, 104.
        # Max active orders allowed: 4.

        self.mock_config.get_spacing_type.return_value = SpacingType.ARITHMETIC

        self.gm = GridManager(self.mock_config, StrategyType.SIMPLE_GRID)
        self.gm.initialize_grids_and_levels()

    def test_suppress_extra_order(self):
        print("\n--- Testing Extra Order Suppression ---")

        # 5 Lines: 100, 101, 102, 103, 104
        # Current Price: 102.0 (Exactly on a grid line)
        # Dead Zone Logic (Gap=1.0, Threshold=0.5):
        # Dead Zone: [101.5, 102.5]

        cur_price = 102.0

        # If we rely purely on thresholds:
        # 100 -> <= 101.5 -> BUY
        # 101 -> <= 101.5 -> BUY
        # 102 -> In Dead Zone -> Ignored
        # 103 -> >= 102.5 -> SELL
        # 104 -> >= 102.5 -> SELL
        # Total: 2 Buy + 2 Sell = 4 Orders. (Acceptable)

        # BUT, let's create a scenario where "Dead Zone" logic theoretically allows an extra one
        # OR simply inject a scenario where we pretend thresholds are skipped (as User implies happening).
        # Wait, I cannot "pretend" thresholds are skipped if I am calling `update_zones`.
        # I must choose a price that triggers too many.

        # Scenario: Price 102.0.
        # Let's say Dead Zone calculation is somehow allowing 102 to be included?
        # Or say price is 102.5 (Midpoint).
        # Gap = 1.0. Thresh = 0.5.
        # Dead Zone: [102.0, 103.0]
        # 100, 101 -> BUY
        # 102 -> Boundary? If <= 102.0 includes 102? YES.
        # 103 -> Boundary? If >= 103.0 includes 103? YES.
        # 104 -> SELL
        # Total: 100, 101, 102 (Buy) + 103, 104 (Sell).
        # Total = 3 Buy + 2 Sell = 5 Orders.
        # But Max Orders = 4.
        # So it SHOULD suppress one.

        # Let's try 102.5
        current_price = 102.5

        # This calls get_dead_zone_thresholds internally using the logic we added.
        # Gap = 103-102=1.0. T=0.5.
        # Limits: 102.0, 103.0

        self.gm.update_zones_based_on_price(current_price)

        buys = self.gm.sorted_buy_grids
        sells = self.gm.sorted_sell_grids
        total = len(buys) + len(sells)

        print(f"Buys: {buys}")
        print(f"Sells: {sells}")
        print(f"Total: {total}")

        # Assert Max Orders is respected
        max_allowed = 4

        # Before Fix: This might be 5.
        # After Fix: Should be 4.

        # Note: If logic is strictly correct, 102<=102.0 and 103>=103.0 might capture 5.

        if total > max_allowed:
            print(f"FAIL: Total ({total}) > Max ({max_allowed}). Extra order detected!")
        else:
            print(f"PASS: Total ({total}) <= Max ({max_allowed}).")

        self.assertLessEqual(total, max_allowed, f"Should not have more than {max_allowed} orders")

        # Verify WHICH one was suppressed (should be closest to price 102.5)
        # Closest grids are 102 and 103 (dist 0.5).
        # One of them should be gone.

        missing_102 = 102.0 not in buys
        missing_103 = 103.0 not in sells

        self.assertTrue(missing_102 or missing_103, "Should have suppressed one of the closest grids (102 or 103)")


if __name__ == "__main__":
    unittest.main()
