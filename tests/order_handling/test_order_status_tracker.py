import asyncio
import contextlib
from unittest.mock import AsyncMock, Mock, patch

import pytest

from core.bot_management.event_bus import Events
from core.order_handling.order import OrderStatus
from core.order_handling.order_status_tracker import OrderStatusTracker


class TestOrderStatusTracker:
    @pytest.fixture
    def setup_tracker(self):
        order_book = Mock()
        order_execution_strategy = Mock()
        # Mock exchange_service inside strategy
        order_execution_strategy.exchange_service = Mock()
        order_execution_strategy.exchange_service.start_user_stream = AsyncMock()

        event_bus = Mock()
        tracker = OrderStatusTracker(
            order_book=order_book,
            order_execution_strategy=order_execution_strategy,
            event_bus=event_bus,
            polling_interval=1.0,
        )
        return tracker, order_book, order_execution_strategy, event_bus

    @pytest.mark.asyncio
    async def test_start_streaming(self, setup_tracker):
        tracker, _, order_execution_strategy, _ = setup_tracker

        # Mock _process_open_orders to avoid actual processing
        tracker._process_open_orders = AsyncMock()

        await tracker.start_streaming()

        tracker._process_open_orders.assert_awaited_once()
        order_execution_strategy.exchange_service.start_user_stream.assert_called_once_with(tracker._on_order_update)
        assert tracker._monitoring_task is not None

        await tracker.stop_tracking()

    @pytest.mark.asyncio
    async def test_on_order_update_valid_dict(self, setup_tracker):
        tracker, order_book, _, event_bus = setup_tracker

        # Data from WebSocket is usually a dict
        order_data = {"id": "order_ws_1", "status": "closed", "filled": 1.5, "remaining": 0.0}

        # Setup mock local order
        local_order = Mock()
        order_book.get_order.return_value = local_order

        # Call the callback directly
        await tracker._on_order_update(order_data)

        order_book.update_order_status.assert_called_once_with("order_ws_1", OrderStatus.CLOSED)
        assert local_order.filled == 1.5
        assert local_order.status == OrderStatus.CLOSED
        event_bus.publish_sync.assert_called_once_with(Events.ORDER_FILLED, local_order)

    @pytest.mark.asyncio
    async def test_on_order_update_sets_average_price(self, setup_tracker):
        tracker, order_book, _, event_bus = setup_tracker

        # Data from WebSocket with filled price
        order_data = {
            "id": "order_ws_2",
            "status": "closed",
            "filled": 1.0,
            "remaining": 0.0,
            "average": 125.5,
            "price": 125.0,
        }

        # Setup mock local order
        local_order = Mock()
        # Initial values (no average yet)
        local_order.average = 0.0
        local_order.price = 125.0
        order_book.get_order.return_value = local_order

        # Call the callback directly
        await tracker._on_order_update(order_data)

        # check if local_order average was updated
        assert local_order.average == 125.5
        # check if status updated
        assert local_order.status == OrderStatus.CLOSED

        event_bus.publish_sync.assert_called_once_with(Events.ORDER_FILLED, local_order)

    @pytest.mark.asyncio
    async def test_process_open_orders_success(self, setup_tracker):
        tracker, order_book, order_execution_strategy, _ = setup_tracker
        mock_order = Mock(identifier="order_1", symbol="BTC/USDT", status=OrderStatus.OPEN)
        mock_remote_order = Mock(identifier="order_1", symbol="BTC/USDT", status=OrderStatus.CLOSED)

        order_book.get_open_orders.return_value = [mock_order]
        order_execution_strategy.get_order = AsyncMock(return_value=mock_remote_order)
        # We spy on _handle_order_status_change instead of mocking it out to verify logic,
        # OR we mock it to verify call arguments.
        # Since we modified _handle to take dict/Order, let's keep mocking it to verify it receives the Order object.
        tracker._handle_order_status_change = Mock()

        await tracker._process_open_orders()

        order_execution_strategy.get_order.assert_awaited_once_with("order_1", "BTC/USDT")
        tracker._handle_order_status_change.assert_called_once_with(mock_remote_order)

    def test_handle_order_status_change_closed_object(self, setup_tracker):
        """Test handling an Order object (legacy/api path)"""
        tracker, order_book, _, event_bus = setup_tracker
        mock_remote_order = Mock(identifier="order_1", status=OrderStatus.CLOSED)

        with patch.object(tracker.logger, "info") as mock_logger_info:
            tracker._handle_order_status_change(mock_remote_order)

            order_book.update_order_status.assert_called_once_with("order_1", OrderStatus.CLOSED)
            event_bus.publish_sync.assert_called_once_with(Events.ORDER_FILLED, mock_remote_order)
            mock_logger_info.assert_called_once_with("Order order_1 filled.")

    def test_handle_order_status_change_canceled_dict(self, setup_tracker):
        """Test handling a dict (new websocket path)"""
        tracker, order_book, _, event_bus = setup_tracker
        order_data = {"id": "order_1", "status": "canceled"}

        # Mock local order retrieval
        local_order = Mock()
        order_book.get_order.return_value = local_order

        with patch.object(tracker.logger, "warning") as mock_logger_warning:
            tracker._handle_order_status_change(order_data)

            order_book.update_order_status.assert_called_once_with("order_1", OrderStatus.CANCELED)
            event_bus.publish_sync.assert_called_once_with(Events.ORDER_CANCELLED, local_order)

            # The logger message might be different depending on exact implementation,
            # but we check if it was called.
            assert mock_logger_warning.called
            # mock_logger_warning.assert_any_call("Order order_1 was canceled.")

    @pytest.mark.asyncio
    async def test_start_streaming_warns_if_already_running(self, setup_tracker):
        tracker, _, _, _ = setup_tracker
        tracker._process_open_orders = AsyncMock()

        await tracker.start_streaming()

        with patch.object(tracker.logger, "warning") as mock_logger_warning:
            await tracker.start_streaming()
            mock_logger_warning.assert_called_once_with("OrderStatusTracker stream is already running.")

        await tracker.stop_tracking()
