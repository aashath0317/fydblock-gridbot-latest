import sys
import os
import unittest
import asyncio
import glob
from unittest.mock import MagicMock, AsyncMock

# Path checks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.grid_management.grid_manager import GridManager
from core.grid_management.grid_level import GridCycleState
from core.storage.bot_database import BotDatabase
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType

TEST_DB = "test_infinite_grid.db"


class TestInfiniteGridDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # 0. Clean up old DB
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

        # 1. Setup Mock Config
        self.mock_config = MagicMock()
        self.mock_config.get_investment_amount.return_value = 1000.0
        self.mock_config.config = {"bot_id": 999, "grid_strategy": {"range": {"top": 108.0, "bottom": 100.0}}}
        # 5 Grids: 100, 102, 104, 106, 108 (Gap = 2.0)
        self.mock_config.get_bottom_range.return_value = 100.0
        self.mock_config.get_top_range.return_value = 108.0
        self.mock_config.get_num_grids.return_value = 4
        self.mock_config.get_spacing_type.return_value = SpacingType.ARITHMETIC

        # 2. Init DB
        self.db = BotDatabase(TEST_DB)

        # 3. Init GridManager
        # We need to hack the DB path inject if GridManager internalizes it 'BotDatabase()'
        # But wait, GridManager does `self.db = BotDatabase() if self.bot_id else None`
        # `BotDatabase()` defaults to `bot_data.db`.
        # I need to Monkey Patch BotDatabase or inject the db instance.
        # Since I can't inject in __init__ easily (it creates it), I will assign it AFTER init.

        # Patch BotDatabase default path via class attribute if possible, or just overwrite `self.db`
        self.gm = GridManager(self.mock_config, StrategyType.SIMPLE_GRID)
        self.gm.db = self.db  # Injected test DB

        # Pre-seed internal state
        self.gm.initialize_grids_and_levels()
        # Should be [100.0, 102.0, 104.0, 106.0, 108.0]
        print(f"Initial Grids: {self.gm.price_grids}")

        # Populate DB with Initial Grids
        for p in self.gm.price_grids:
            self.db.add_grid_level(999, p, GridCycleState.READY_TO_BUY.value)  # Dummy status

    def tearDown(self):
        # Close DB connection if held? BotDatabase opens/closes per call.
        if os.path.exists(TEST_DB):
            try:
                os.remove(TEST_DB)
            except:
                pass

    async def test_trail_up(self):
        print("\n--- Testing Trail UP ---")
        # Current range: 100 - 108. Highest is 108. Gap is 2.0.
        # Trigger: current_price >= highest + gap = 108 + 2 = 110.

        current_price = 110.0

        # Mock Callback
        cancel_callback = AsyncMock()

        # ACTION
        await self.gm.trail_grid_up(current_price, cancel_callback)

        # VERIFICATION
        # 1. Old Lowest (100) should be REMOVED from DB and Memory
        self.assertNotIn(100.0, self.gm.price_grids)
        db_levels = self.db.get_grid_levels(999)
        self.assertNotIn(100.0, db_levels)

        # 2. New Highest (110) should be ADDED to DB and Memory
        self.assertIn(110.0, self.gm.price_grids)
        self.assertIn(110.0, db_levels)

        # 3. New Highest Status should be READY_TO_SELL
        self.assertEqual(db_levels[110.0], GridCycleState.READY_TO_SELL.value)

        # 4. Callback should have been called for 100.0
        # Check args passed to callback. It receives a GridLevel object.
        # We can check the price attribute of the arg.
        self.assertTrue(cancel_callback.called)
        args, _ = cancel_callback.call_args
        cancelled_grid_level = args[0]
        self.assertAlmostEqual(cancelled_grid_level.price, 100.0)

        print(f"New Grids: {self.gm.price_grids}")

    async def test_trail_down(self):
        print("\n--- Testing Trail DOWN ---")
        # Current range: 100 - 108. Lowest is 100. Gap is 2.0.
        # Trigger: current_price <= lowest - gap = 100 - 2 = 98.

        current_price = 98.0

        cancel_callback = AsyncMock()

        await self.gm.trail_grid_down(current_price, cancel_callback)

        # 1. Old Highest (108) removed
        self.assertNotIn(108.0, self.gm.price_grids)
        db_levels = self.db.get_grid_levels(999)
        self.assertNotIn(108.0, db_levels)

        # 2. New Lowest (98) added
        self.assertIn(98.0, self.gm.price_grids)
        self.assertIn(98.0, db_levels)

        # 3. New Lowest Status => READY_TO_BUY
        self.assertEqual(db_levels[98.0], GridCycleState.READY_TO_BUY.value)

        # 4. Callback called for 108.0
        self.assertTrue(cancel_callback.called)
        args, _ = cancel_callback.call_args
        self.assertAlmostEqual(args[0].price, 108.0)

        print(f"New Grids: {self.gm.price_grids}")


if __name__ == "__main__":
    unittest.main()
