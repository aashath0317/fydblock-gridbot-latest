import asyncio
import logging
import sys
import os

# Add project root to path
# Add project root to path
# Add project root to path
# 1. Parent (gridbot directory): Allows 'from config import...' (used internally by bot modules)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# 2. Grandparent (Fydblock): Allows 'import gridbot'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

print(f"DEBUG: sys.path values: {sys.path[:3]}")

from gridbot.core.order_handling.balance_tracker import BalanceTracker
from gridbot.core.order_handling.order_manager import OrderManager
from gridbot.core.grid_management.grid_manager import GridManager
from gridbot.config.trading_mode import TradingMode


# Mock Classes
class MockExchange:
    async def get_balance(self):
        return {"free": {"USDT": 1000.0, "SOL": 0.0}, "total": {"USDT": 1000.0, "SOL": 0.0}}

    async def execute_market_order(self, side, pair, amount, price):
        print(f"MOCK EXCHANGE: Executing {side} {amount} @ {price}")
        from gridbot.core.order_handling.order import Order, OrderSide, OrderStatus, OrderType
        import pandas as pd

        return Order(
            identifier="mock_ord_1",
            status=OrderStatus.CLOSED,
            order_type=OrderType.MARKET,
            side=side,
            price=price,
            average=price,
            amount=amount,
            filled=amount,
            remaining=0.0,
            timestamp=int(pd.Timestamp.now().timestamp() * 1000),
            datetime=pd.Timestamp.now().isoformat(),
            last_trade_timestamp=None,
            symbol=pair,
            time_in_force="GTC",
        )


class MockStrategy:
    def __init__(self):
        self.exchange_service = MockExchange()
        self.cancel_order = lambda *args: None
        self.execute_limit_order = lambda *args: None
        self.execute_market_order = self.exchange_service.execute_market_order


async def test_rebalance():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("RebalanceTest")

    # 1. Setup
    # Create required mocks
    class MockEventBus:
        def subscribe(self, *args, **kwargs):
            pass

        async def publish(self, *args, **kwargs):
            pass

    mock_event_bus = MockEventBus()
    mock_fee_calc = type("MockFee", (), {"calculate_fee": lambda x: x * 0.001})()

    # Mock Notifier with async method
    class MockNotifier:
        async def async_send_notification(self, *args, **kwargs):
            return

    mock_notifier = MockNotifier()
    mock_val = type(
        "MockVal",
        (),
        {
            "adjust_and_validate_buy_quantity": lambda self, bal, amt, price: amt,
            "adjust_and_validate_sell_quantity": lambda self, bal, amt: amt,
            "validate_funds": lambda *args: True,
        },
    )()
    mock_book = type("MockBook", (), {"add_order": lambda *args: None})()

    tracker = BalanceTracker(
        event_bus=mock_event_bus,
        fee_calculator=mock_fee_calc,
        trading_mode=TradingMode.PAPER_TRADING,
        base_currency="SOL",
        quote_currency="USDT",
    )

    # Initialize with sufficient funds for test
    tracker.balance = 200.0  # Fiat (Enough to buy 1.0 SOL at 100.0)
    tracker.crypto_balance = 0.0  # Crypto
    tracker.investment_cap = 1000.0

    # Create a mock ConfigManager
    mock_config = type(
        "MockConfig",
        (),
        {
            "get_num_grids": lambda: 10,
            "get_bottom_range": lambda: 90.0,
            "get_top_range": lambda: 110.0,
            "get_investment_amount": lambda: 1000.0,
            "get_spacing_type": lambda: SpacingType.ARITHMETIC,
            "config": {"bot_id": 999},
        },
    )()

    from gridbot.strategies.strategy_type import StrategyType
    from gridbot.strategies.spacing_type import SpacingType

    grid_manager = GridManager(config_manager=mock_config, strategy_type=StrategyType.SIMPLE_GRID)
    # Sells need crypto. Buys need fiat.
    # Price = 100.
    current_price = 100.0

    # Manually populate grid manager with "requirements"
    # Say we have a sell grid at 105. We need crypto for it.
    grid_manager.grid_levels = {105: type("Level", (), {"price": 105})()}
    grid_manager.sorted_sell_grids = [105]
    grid_manager.sorted_buy_grids = []

    # Mock get_order_size to return 1.0 SOL needed
    grid_manager.get_order_size_for_grid_level = lambda val, p: 1.0
    grid_manager.get_dead_zone_thresholds = lambda p: (99, 101)  # Deadzone

    manager = OrderManager(
        grid_manager=grid_manager,
        order_validator=mock_val,
        balance_tracker=tracker,
        order_book=mock_book,
        event_bus=mock_event_bus,
        order_execution_strategy=MockStrategy(),
        notification_handler=mock_notifier,
        trading_mode=TradingMode.PAPER_TRADING,
        trading_pair="SOL/USDT",
        strategy_type="GRID",
    )

    # 2. Test Check Requirements
    req_fiat, req_crypto = manager.calculate_grid_requirements(current_price)
    logger.info(f"Requirements: Fiat={req_fiat}, Crypto={req_crypto}")

    # We expect 1.0 Crypto needed (for the sell grid at 105)
    # We have 0.0 Crypto.
    # Deficit should be detected.

    # 3. Test Execute Rebalance
    logger.info("Running ensure_funds_for_grid...")
    await manager.ensure_funds_for_grid(current_price)

    # 4. Verify
    if (
        tracker.crypto_balance > 0
    ):  # Note: Our mock 'execute_market_order' doesn't auto-update tracker unless event bus wired.
        # But verify_rebalance should check if execute_rebalance CALLED the execution strategy.
        # Check logs for "MOCK EXCHANGE: Executing..."
        pass


if __name__ == "__main__":
    asyncio.run(test_rebalance())
