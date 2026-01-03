import sys
import os
import unittest
from unittest.mock import MagicMock

# Path checks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from core.order_handling.balance_tracker import BalanceTracker
from core.order_handling.fee_calculator import FeeCalculator
from core.order_handling.order import Order, OrderSide, OrderStatus
from config.trading_mode import TradingMode


class TestFeeStabilization(unittest.TestCase):
    def setUp(self):
        self.mock_event_bus = MagicMock()
        self.mock_fee_calculator = MagicMock()
        # Mock fee: 0.1%
        self.mock_fee_calculator.calculate_fee.side_effect = lambda amount: amount * 0.001

        self.balance_tracker = BalanceTracker(
            event_bus=self.mock_event_bus,
            fee_calculator=self.mock_fee_calculator,
            trading_mode=TradingMode.BACKTEST,
            base_currency="SOL",
            quote_currency="USDT",
        )
        import asyncio

        # Init balance: 1000 USDT, 0 SOL
        asyncio.run(self.balance_tracker.setup_balances(1000.0, 0.0))
        # Ensure reserve is 0 explicitly
        self.balance_tracker.operational_reserve = 0.0

    def test_profit_recycling_increases_reserve(self):
        print("\n--- Testing Profit Recycling ---")
        # Simulate Sell Order
        # Quantity 1 SOL @ 100 USDT
        # Fee = 100 * 0.001 = 0.1 USDT
        # Replacement Cost = 0.1 * 1.1 = 0.11 USDT

        # We need to give the bot some crypto first to sell (logic check)
        self.balance_tracker.crypto_balance = 1.0

        order = MagicMock(spec=Order)
        order.side = OrderSide.SELL
        order.filled = 1.0
        order.price = 100.0

        import asyncio

        asyncio.run(self.balance_tracker.update_balance_on_order_completion(order))

        print(f"Reserve: {self.balance_tracker.operational_reserve}")
        print(f"Balance: {self.balance_tracker.balance}")

        expected_reserve = 0.1 * 1.1  # 0.11
        self.assertAlmostEqual(self.balance_tracker.operational_reserve, expected_reserve)

        # Proceeds: 100 - 0.1 = 99.9
        # Net to Balance: 99.9 - 0.11 = 99.79
        # Initial Balance was 1000. So 1099.79
        self.assertAlmostEqual(self.balance_tracker.balance, 1000.0 + 99.79)

    def test_auto_healing_shortfall(self):
        print("\n--- Testing Auto-Healing ---")
        # Setup: Balance is slightly short for a buy order
        # Required: 100.0
        # Available: 99.9
        # Deficit: 0.1
        # Reserve: 0.2

        self.balance_tracker.balance = 99.9
        self.balance_tracker.operational_reserve = 0.2

        required = 100.0

        result = self.balance_tracker.attempt_fee_recovery(required)

        print(f"Recovery Result: {result}")
        print(f"New Balance: {self.balance_tracker.balance}")
        print(f"New Reserve: {self.balance_tracker.operational_reserve}")

        self.assertTrue(result)
        self.assertAlmostEqual(self.balance_tracker.balance, 100.0)
        self.assertAlmostEqual(self.balance_tracker.operational_reserve, 0.1)

    def test_auto_healing_fails_if_reserve_empty(self):
        print("\n--- Testing Auto-Healing Failure ---")
        self.balance_tracker.balance = 99.9
        self.balance_tracker.operational_reserve = 0.05  # Insufficient

        required = 100.0

        result = self.balance_tracker.attempt_fee_recovery(required)

        self.assertFalse(result)
        self.assertEqual(self.balance_tracker.balance, 99.9)  # Unchanged


if __name__ == "__main__":
    unittest.main()
