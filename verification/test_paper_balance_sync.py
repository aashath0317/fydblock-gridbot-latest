import asyncio
from unittest.mock import MagicMock, AsyncMock
from config.trading_mode import TradingMode
from core.order_handling.balance_tracker import BalanceTracker
from core.services.exchange_interface import ExchangeInterface


async def test_paper_trading_sync():
    print("Testing Paper Trading Balance Sync...")

    # Mock EventBus
    event_bus = MagicMock()

    # Mock FeeCalculator
    fee_calculator = MagicMock()

    # Mock DB
    db = MagicMock()

    # Initialize BalanceTracker with PAPER_TRADING mode
    tracker = BalanceTracker(
        event_bus=event_bus,
        fee_calculator=fee_calculator,
        trading_mode=TradingMode.PAPER_TRADING,
        base_currency="BTC",
        quote_currency="USDT",
        db=db,
        bot_id=1,
    )

    tracker.investment_cap = 10000.0

    # Mock ExchangeInterface
    exchange_service = AsyncMock(spec=ExchangeInterface)
    exchange_service.get_balance.return_value = {
        "free": {"USDT": 500.0, "BTC": 0.1},
        "total": {"USDT": 500.0, "BTC": 0.1},
    }

    # Run sync_balances
    await tracker.sync_balances(exchange_service, current_price=50000.0)

    # Verify that balances were updated
    if tracker.balance == 500.0 and tracker.crypto_balance == 0.1:
        print("✅ SUCCESS: Balances updated in PAPER_TRADING mode.")
        print(f"Fiat Balance: {tracker.balance}")
        print(f"Crypto Balance: {tracker.crypto_balance}")
    else:
        print("❌ FAILURE: Balances were NOT updated.")
        print(f"Fiat Balance: {tracker.balance}")
        print(f"Crypto Balance: {tracker.crypto_balance}")

    # Verify DB update was called
    if db.update_balances.called:
        print("✅ SUCCESS: DB update was called.")
    else:
        print("❌ FAILURE: DB update was NOT called.")


if __name__ == "__main__":
    asyncio.run(test_paper_trading_sync())
