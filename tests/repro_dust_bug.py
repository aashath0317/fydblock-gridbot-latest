
import pytest
from unittest.mock import AsyncMock, Mock
from core.order_handling.order_manager import OrderManager
from core.order_handling.order import OrderSide
from config.trading_mode import TradingMode
from strategies.strategy_type import StrategyType

@pytest.mark.asyncio
async def test_reconcile_grid_orders_places_dust_order():
    # Setup
    grid_manager = Mock()
    order_validator = Mock()
    balance_tracker = Mock()
    order_book = Mock()
    event_bus = Mock()
    order_execution_strategy = Mock()
    notification_handler = Mock()
    notification_handler.async_send_notification = AsyncMock()

    manager = OrderManager(
        grid_manager=grid_manager,
        order_validator=order_validator,
        balance_tracker=balance_tracker,
        order_book=order_book,
        event_bus=event_bus,
        order_execution_strategy=order_execution_strategy,
        notification_handler=notification_handler,
        trading_mode=TradingMode.LIVE,
        trading_pair="BTC/USD",
        strategy_type=StrategyType.HEDGED_GRID,
        bot_id=1
    )

    # Mock Exchange Service
    class MockExchangeService:
        async def fetch_open_orders(self, pair):
            return [] # No open orders on exchange
    
    order_execution_strategy.exchange_service = MockExchangeService()

    # Mock DB
    manager.db = Mock()
    manager.db.get_all_active_orders.return_value = {} # No active orders in DB

    # Mock Balances
    # "Dust" balance: $6 available
    balance_tracker.balance = 6.0 
    balance_tracker.crypto_balance = 0.0
    balance_tracker.get_total_balance_value.return_value = 1000.0
    
    # Mock Grid Manager
    # One buy grid at $50,000
    grid_manager.sorted_buy_grids = [50000.0]
    grid_manager.sorted_sell_grids = []
    grid_manager.grid_levels = {50000.0: Mock()}
    
    # Target order size: $50 (e.g. 0.001 BTC @ 50k)
    # 0.001 * 50000 = $50
    grid_manager.get_order_size_for_grid_level.return_value = 0.001

    # Mock Validator
    # Validator clamps to available balance if insufficient
    # 6.0 / 50000 = 0.00012
    def mock_validate_buy(balance, qty, price):
        required = qty * price
        if balance < required:
            # Emulate logic that returns what's possible (dust)
            return balance / price 
        return qty

    order_validator.adjust_and_validate_buy_quantity.side_effect = mock_validate_buy

    # Mock Execution
    async def mock_execute_limit(side, pair, qty, price):
        return Mock(identifier="order_1", amount=qty, price=price, status="OPEN")
    
    order_execution_strategy.execute_limit_order = AsyncMock(side_effect=mock_execute_limit)

    # Run Reconciliation
    # Current price 51000, so 50000 is a valid buy grid
    await manager.reconcile_grid_orders(current_price=51000.0)

    # Verification
    # Expected behavior currently (BUG): It places an order
    assert order_execution_strategy.execute_limit_order.called
    args = order_execution_strategy.execute_limit_order.call_args
    qty_placed = args[0][2]
    
    # Check if the placed quantity corresponds to $6 (dust) instead of $50
    value_placed = qty_placed * 50000
    print(f"Placed order value: {value_placed}")
    
    # If the bug exists, value_placed should be close to 6.0
    assert abs(value_placed - 6.0) < 0.01
