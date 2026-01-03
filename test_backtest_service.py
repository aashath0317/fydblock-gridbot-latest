import sys
import os

sys.path.append(os.getcwd())

from core.services.backtest_exchange_service import BacktestExchangeService
from config.config_manager import ConfigManager


# Mock Config Manager to satisfy constructor
class MockConfigManager:
    def get_historical_data_file(self):
        return None

    def get_exchange_name(self):
        return "binance"

    # BacktestExchangeService calls self.config_manager.get_historical_data_file()


def test_instantiation():
    try:
        service = BacktestExchangeService(MockConfigManager())
        print("Successfully instantiated BacktestExchangeService")
        import asyncio

        orders = asyncio.run(service.fetch_open_orders("BTC/USDT"))
        print(f"fetch_open_orders returned: {orders}")
    except TypeError as e:
        print(f"Failed to instantiate: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_instantiation()
