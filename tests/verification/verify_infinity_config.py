import unittest
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config.config_validator import ConfigValidator
from core.grid_management.grid_manager import GridManager
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType
from config.config_manager import ConfigManager


# Mocking ConfigManager for GridManager tests
class MockConfigManager:
    def __init__(self, config):
        self.config = config

    def get_num_grids(self):
        return self.config["grid_strategy"].get("num_grids")

    def get_grid_settings(self):
        return self.config["grid_strategy"]

    def get_investment_amount(self):
        return self.config["grid_strategy"].get("investment", 1000.0)

    def get_amount_per_grid(self):
        return self.config["grid_strategy"].get("amount_per_grid", 0.0)

    def get_order_size_type(self):
        return self.config["grid_strategy"].get("order_size_type", "quote")

    def get_grid_gap(self):
        return self.config["grid_strategy"].get("grid_gap", 0.0)

    def get_spacing_type(self):
        s = self.config["grid_strategy"].get("spacing", "geometric")
        return SpacingType.from_string(s)

    def get_bottom_range(self):
        return self.config["grid_strategy"]["range"]["bottom"]

    def get_top_range(self):
        return self.config["grid_strategy"]["range"]["top"]


class TestInfinityGrid(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.ERROR)
        self.validator = ConfigValidator()

    def test_validation_restriction(self):
        """Test blocking Arithmetic + Base + Trailing Up"""
        config = {
            "grid_strategy": {
                "type": "simple_grid",
                "spacing": "arithmetic",
                "num_grids": 10,
                "range": {"top": 200, "bottom": 100},
                "order_size_type": "base",
                "amount_per_grid": 1.0,
                "trailing_up": True,
            },
            # Add minimal valid required fields
            "exchange": {"name": "binance", "trading_fee": 0.1, "trading_mode": "paper"},
            "pair": {"base_currency": "BTC", "quote_currency": "USDT"},
            "trading_settings": {
                "initial_balance": 1000,
                "period": {"start_date": "2023-01-01", "end_date": "2023-01-02"},
                "timeframe": "1m",
            },
            "risk_management": {
                "take_profit": {"enabled": False, "threshold": 0},
                "stop_loss": {"enabled": False, "threshold": 0},
            },
            "logging": {"log_level": "INFO", "log_to_file": False},
        }

        from config.exceptions import ConfigValidationError

        with self.assertRaises(ConfigValidationError) as cm:
            self.validator.validate(config)

        self.assertIn("grid_strategy.combination_forbidden", cm.exception.invalid_fields)
        print("\n✅ Validation Test Passed: Blocked Arithmetic+Base+TrailingUp")

    def test_math_geometric_next_price(self):
        """Test Geometric Spacing Calculation"""
        config = {
            "grid_strategy": {
                "spacing": "geometric",
                "grid_gap": 2.0,  # 2%
                "num_grids": 10,
                "range": {"top": 200, "bottom": 100},
                "order_size_type": "quote",
                "amount_per_grid": 10.0,
            }
        }
        cm = MockConfigManager(config)
        gm = GridManager(cm, StrategyType.SIMPLE_GRID)
        # We assume gm uses cm for everything. initialized?
        gm.price_grids = [100.0, 102.0]  # Dummy

        # Test UP: 100 * 1.02 = 102
        next_up = gm.calculate_next_price_up(100.0)
        self.assertAlmostEqual(next_up, 102.0)

        # Test DOWN: 100 * (1 - 0.02) = 98 (User specified formula)
        next_down = gm.calculate_next_price_down(100.0)
        self.assertAlmostEqual(next_down, 98.0)
        print("\n✅ Math Test Passed: Geometric Spacing (UP/DOWN)")

    def test_math_arithmetic_next_price(self):
        """Test Arithmetic Spacing Calculation"""
        config = {
            "grid_strategy": {
                "spacing": "arithmetic",
                "grid_gap": 10.0,  # Fixed 10
                "num_grids": 10,
                "range": {"top": 200, "bottom": 100},
                "order_size_type": "quote",
                "amount_per_grid": 10.0,
            }
        }
        cm = MockConfigManager(config)
        gm = GridManager(cm, StrategyType.SIMPLE_GRID)
        gm.price_grids = [100.0, 110.0]

        next_up = gm.calculate_next_price_up(100.0)
        self.assertAlmostEqual(next_up, 110.0)

        next_down = gm.calculate_next_price_down(100.0)
        self.assertAlmostEqual(next_down, 90.0)
        print("\n✅ Math Test Passed: Arithmetic Spacing (UP/DOWN)")

    def test_order_quantity_calculation(self):
        """Test Quote vs Base quantity"""
        # Case A: Fixed USDT (Quote)
        config_quote = {
            "grid_strategy": {
                "order_size_type": "quote",
                "amount_per_grid": 100.0,  # $100
                "spacing": "geometric",
            }
        }
        cm = MockConfigManager(config_quote)
        gm = GridManager(cm, StrategyType.SIMPLE_GRID)
        gm.uniform_order_quantity = 0  # Ensure it uses dynamic

        # At price $50, quantity should be 2
        qty = gm.get_order_size_for_grid_level(1000, 50.0)
        self.assertAlmostEqual(qty, 2.0)

        # Case B: Fixed Coin (Base)
        config_base = {
            "grid_strategy": {
                "order_size_type": "base",
                "amount_per_grid": 1.5,  # 1.5 coins
                "spacing": "geometric",
            }
        }
        cm2 = MockConfigManager(config_base)
        gm2 = GridManager(cm2, StrategyType.SIMPLE_GRID)

        qty2 = gm2.get_order_size_for_grid_level(1000, 50.0)
        self.assertAlmostEqual(qty2, 1.5)
        print("\n✅ Math Test Passed: Order Quantity (Quote vs Base)")


if __name__ == "__main__":
    unittest.main()
