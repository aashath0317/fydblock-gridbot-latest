import asyncio
import logging
import os
import sys
from unittest.mock import AsyncMock, MagicMock

# Add current directory to path
sys.path.append(os.getcwd())

# Configure logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_fix")


async def test_ownership_validation():
    try:
        from core.bot_management.event_bus import EventBus, Events
        from core.order_handling.order import OrderStatus
        from core.order_handling.order_status_tracker import OrderStatusTracker
    except ImportError as e:
        logger.error(f"Failed to import: {e}")
        return

    # Mock dependencies
    order_book = MagicMock()
    order_book.get_order.return_value = None  # Simulate orphan

    strategy = AsyncMock()
    event_bus = MagicMock()

    # --- TEST 1: Foreign Order (Should be IGNORED) ---
    logger.info("TEST 1: Testing Foreign Order Rejection...")
    tracker = OrderStatusTracker(
        order_book=order_book,
        order_execution_strategy=strategy,
        event_bus=event_bus,
        trading_pair="SOL/USDT",
        bot_id=123,  # We are Bot 123
    )

    foreign_order = MagicMock()
    foreign_order.identifier = "foreign_order_1"
    foreign_order.status = OrderStatus.CLOSED
    # Order belongs to Bot 999
    foreign_order.info = {"clientOrderId": "G999xForeign"}

    strategy.get_order.return_value = foreign_order

    # Process update
    await tracker._handle_order_status_change({"id": "foreign_order_1", "status": "closed"})

    if event_bus.publish_sync.call_count == 0:
        logger.info("✅ SUCCESS: Foreign order was NOT published.")
    else:
        logger.error(f"❌ FAILURE: Foreign order WAS published {event_bus.publish_sync.call_count} times.")

    # --- TEST 2: Own Order (Should be ACCEPTED) ---
    logger.info("TEST 2: Testing Own Order Acceptance...")

    # Reset mocks
    event_bus.reset_mock()
    tracker._processed_fills.clear()

    own_order = MagicMock()
    own_order.identifier = "own_order_1"
    own_order.status = OrderStatus.CLOSED
    # Order belongs to Bot 123
    own_order.info = {"clientOrderId": "G123xMyOrder"}

    strategy.get_order.return_value = own_order

    await tracker._handle_order_status_change({"id": "own_order_1", "status": "closed"})

    if event_bus.publish_sync.call_count == 1:
        logger.info("✅ SUCCESS: Own order WAS published.")
    else:
        logger.error(f"❌ FAILURE: Own order was NOT published (count={event_bus.publish_sync.call_count}).")


if __name__ == "__main__":
    asyncio.run(test_ownership_validation())
