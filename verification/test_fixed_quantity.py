import sys
import os
import logging
from unittest.mock import MagicMock

# Add project root to path
# We are currently in p:/Fydblock/gridbot/verification/
# We want access to 'config', 'core', 'strategies' which are in 'p:/Fydblock/gridbot'
# So we go up ONE level: '..'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.grid_management.grid_manager import GridManager
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType

# Configure Logging
logging.basicConfig(level=logging.INFO)


def test_fixed_quantity():
    print("\n--- Testing Fixed Quantity Logic ---\n")

    # Mock ConfigManager
    mock_config = MagicMock()
    mock_config.get_investment_amount.return_value = 1000.0  # 1000 USDT
    mock_config.get_bottom_range.return_value = 90.0
    mock_config.get_top_range.return_value = 110.0
    mock_config.get_num_grids.return_value = 5  # Total lines = 6
    mock_config.get_spacing_type.return_value = SpacingType.ARITHMETIC

    # Initialize GridManager
    gm = GridManager(mock_config, StrategyType.SIMPLE_GRID)
    gm.initialize_grids_and_levels()

    # Manual Calculation Check
    # Total Lines = 5 grids + 1 = 6 lines
    # Reserve = 1000 * 0.99 = 990
    # Central Price = (110+90)/2 = 100
    # Expected Quantity = 990 / 6 / 100 = 1.65

    expected_qty = (1000 * 0.99) / 6 / 100
    actual_qty = gm.uniform_order_quantity

    print(f"Investment: 1000")
    print(f"Reserve Factor: 0.99")
    print(f"Total Lines: 6")
    print(f"Central Price: 100")
    print(f"Expected Quantity: {expected_qty}")
    print(f"Actual Quantity:   {actual_qty}")

    if abs(expected_qty - actual_qty) < 1e-9:
        print("? Calculation MATCHES Formula.")
    else:
        print("? Calculation FAILED.")

    # Check Consistency across levels
    print("\nChecking Consistency across Grid Levels:")
    consistent = True
    for price in gm.price_grids:
        # Pass dummy total_balance of 5000 to see if it ignores it
        qty = gm.get_order_size_for_grid_level(5000.0, price)
        is_match = abs(qty - actual_qty) < 1e-9
        status = "?" if is_match else "?"
        if not is_match:
            consistent = False
        print(f"  Price {price}: Qty {qty} {status}")

    if consistent:
        print("\n? ALL LEVELS UNIFORM.")
    else:
        print("\n? INCONSISTENCY DETECTED.")


if __name__ == "__main__":
    test_fixed_quantity()
