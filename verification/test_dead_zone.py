import sys
import os
import logging
from unittest.mock import MagicMock

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.grid_management.grid_manager import GridManager
from core.grid_management.grid_level import GridLevel, GridCycleState
from strategies.strategy_type import StrategyType
from strategies.spacing_type import SpacingType

# Configure Logging
logging.basicConfig(level=logging.INFO)


def test_dead_zone():
    print("\n--- Testing Dynamic Dead Zone Logic ---\n")

    # Mock ConfigManager
    mock_config = MagicMock()
    # We don't really need these return values if we inject grids manually,
    # but good to have them just in case init is called tangentially.
    mock_config.get_investment_amount.return_value = 1000.0
    mock_config.get_bottom_range.return_value = 90.0
    mock_config.get_top_range.return_value = 110.0
    mock_config.get_num_grids.return_value = 10
    mock_config.get_spacing_type.return_value = SpacingType.ARITHMETIC

    # Initialize GridManager
    gm = GridManager(mock_config, StrategyType.SIMPLE_GRID)

    # ---------------------------------------------------------
    # Scenario: Gap is exactly 1.0
    # Current Price: 100.0
    # Threshold should be Gap / 2 = 0.5
    # Dead Zone: [99.5, 100.5]
    # ---------------------------------------------------------

    # Construct grids with exactly 1.0 spacing around 100
    # 98, 99, 100, 101, 102
    test_grids = [98.0, 99.0, 100.0, 101.0, 102.0]

    gm.price_grids = test_grids
    gm.grid_levels = {p: GridLevel(p, GridCycleState.READY_TO_BUY) for p in test_grids}

    current_price = 100.0
    print(f"Current Price: {current_price}")

    # Calculate Expected Thresholds
    # Gap is 1.0 (101-100 or 100-99)
    # Threshold = 1.0 / 2 = 0.5
    expected_buy_limit = 100.0 - 0.5  # 99.5
    expected_sell_limit = 100.0 + 0.5  # 100.5

    print(f"Expected Dead Zone: {expected_buy_limit} - {expected_sell_limit}")

    gm.update_zones_based_on_price(current_price)

    buy_threshold, sell_threshold = gm.get_dead_zone_thresholds(current_price)
    print(f"Calculated Dead Zone: {buy_threshold} - {sell_threshold}")

    # Check assertions
    if abs(buy_threshold - expected_buy_limit) > 0.0001:
        print(f"FAIL: Buy threshold mismatch. Got {buy_threshold}, expected {expected_buy_limit}")

    if abs(sell_threshold - expected_sell_limit) > 0.0001:
        print(f"FAIL: Sell threshold mismatch. Got {sell_threshold}, expected {expected_sell_limit}")

    # Verify Grid Alignments
    # 99.0 should be BUY selection (<= 99.5)
    # 100.0 should be DEAD ZONE ( > 99.5 AND < 100.5 )
    # 101.0 should be SELL selection (>= 100.5)

    print("\nBuy Grids (Expected [98.0, 99.0]):", gm.sorted_buy_grids)
    print("Sell Grids (Expected [101.0, 102.0]):", gm.sorted_sell_grids)

    failed = False
    if 99.0 not in gm.sorted_buy_grids:
        print("FAIL: 99.0 should be in Buy Grids")
        failed = True
    if 100.0 in gm.sorted_buy_grids or 100.0 in gm.sorted_sell_grids:
        print("FAIL: 100.0 should be excluded (Dead Zone)")
        failed = True
    if 101.0 not in gm.sorted_sell_grids:
        print("FAIL: 101.0 should be in Sell Grids")
        failed = True

    # ---------------------------------------------------------
    # Scenario 2: Smaller Gap (0.5)
    # Grids: 99.5, 100.0, 100.5
    # Current Price: 100.0
    # Threshold = 0.5 / 2 = 0.25
    # Dead Zone: [99.75, 100.25]
    # ---------------------------------------------------------
    print("\n--- Scenario 2: Gap 0.5 ---")
    test_grids_2 = [99.0, 99.5, 100.0, 100.5, 101.0]
    gm.price_grids = test_grids_2
    gm.grid_levels = {p: GridLevel(p, GridCycleState.READY_TO_BUY) for p in test_grids_2}

    gm.update_zones_based_on_price(current_price)

    b_thresh, s_thresh = gm.get_dead_zone_thresholds(current_price)
    print(f"Calculated Dead Zone (Gap 0.5): {b_thresh} - {s_thresh}")

    expected_b = 100.0 - 0.25  # 99.75
    expected_s = 100.0 + 0.25  # 100.25

    if abs(b_thresh - expected_b) > 0.0001:
        print(f"FAIL: Buy threshold mismatch. Got {b_thresh}, expected {expected_b}")
        failed = True

    # Verify 99.5 is BUY (<= 99.75)  - YES
    # Verify 100.0 is DEAD - YES
    # Verify 100.5 is SELL (>= 100.25) - YES

    if 99.5 not in gm.sorted_buy_grids:
        print("FAIL: 99.5 should be BUY (99.5 <= 99.75)")
        failed = True

    if not failed:
        print("\n? Dynamic Dead Zone Logic Verified Successfully.")
    else:
        print("\n? Verification Failed.")


if __name__ == "__main__":
    test_dead_zone()
