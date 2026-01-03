import sys
import os
import unittest
from unittest.mock import MagicMock

# Path checks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.grid_management.grid_manager import GridManager
from core.grid_management.grid_level import GridLevel, GridCycleState
from core.order_handling.order import OrderSide
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType


class TestResellFlow(unittest.TestCase):
    def setUp(self):
        # Mock Config
        self.mock_config = MagicMock()
        self.mock_config.get_investment_amount.return_value = 1000.0
        self.mock_config.get_bottom_range.return_value = 90.0
        self.mock_config.get_top_range.return_value = 110.0
        self.mock_config.get_num_grids.return_value = 10
        self.mock_config.get_spacing_type.return_value = SpacingType.ARITHMETIC

        self.gm = GridManager(self.mock_config, StrategyType.SIMPLE_GRID)

        # Setup 2 simple grids
        self.price_L1 = 100.0
        self.price_L2 = 101.0
        self.gm.price_grids = [self.price_L1, self.price_L2]
        self.gm.grid_levels = {
            self.price_L1: GridLevel(self.price_L1, GridCycleState.READY_TO_BUY),
            self.price_L2: GridLevel(self.price_L2, GridCycleState.READY_TO_SELL),
        }

    def test_resell_flow_state_transition(self):
        # L1 and L2
        L1 = self.gm.grid_levels[self.price_L1]
        L2 = self.gm.grid_levels[self.price_L2]

        # Scenario:
        # 1. L2 was holding a bag, and just Sold it.
        # User log: "Sell order completed at grid level 129.305. Transitioning to READY_TO_BUY."
        L2.state = GridCycleState.READY_TO_BUY
        print(f"L2 State after Sell: {L2.state}")

        # 2. L1 buys.
        # "Buy order completed at grid level 128.78778. Transitioning to READY_TO_SELL."
        L1.state = GridCycleState.READY_TO_SELL
        print(f"L1 State after Buy: {L1.state}")

        # 3. OrderManager checks if it can place SELL at L2 (using L1's bag)
        paired_sell = self.gm.get_paired_sell_level(L1)
        self.assertEqual(paired_sell, L2)

        can_place = self.gm.can_place_order(L2, OrderSide.SELL)
        print(f"Can place SELL at L2? {can_place}")

        # Expect TRUE now that we fixed it
        self.assertTrue(can_place, "Should be able to place SELL order at L2 even if it is READY_TO_BUY")


if __name__ == "__main__":
    unittest.main()
