from ..grid_management.grid_level import GridLevel
from .order import Order, OrderSide, OrderStatus


class OrderBook:
    def __init__(self):
        self.buy_orders: list[Order] = []
        self.sell_orders: list[Order] = []
        self.non_grid_orders: list[Order] = []  # Orders that are not linked to any grid level
        # CHANGED: Map order.identifier (str) -> GridLevel instead of Order object -> GridLevel
        self.order_to_grid_map: dict[str, GridLevel] = {}

    def add_order(
        self,
        order: Order,
        grid_level: GridLevel | None = None,
    ) -> None:
        # FIX: Prevent duplicates
        if order.identifier in self.order_to_grid_map:
            # Already exists, just update the map if we have a new grid level (optional)
            if grid_level:
                self.order_to_grid_map[order.identifier] = grid_level
            return

        if order.side == OrderSide.BUY:
            self.buy_orders.append(order)
        else:
            self.sell_orders.append(order)

        if grid_level:
            # FIX: Use identifier as key for consistent lookup across different order instances
            self.order_to_grid_map[order.identifier] = grid_level
        else:
            self.non_grid_orders.append(order)

    def get_buy_orders_with_grid(self) -> list[tuple[Order, GridLevel | None]]:
        # FIX: Lookup by identifier
        return [(order, self.order_to_grid_map.get(order.identifier, None)) for order in self.buy_orders]

    def get_sell_orders_with_grid(self) -> list[tuple[Order, GridLevel | None]]:
        # FIX: Lookup by identifier
        return [(order, self.order_to_grid_map.get(order.identifier, None)) for order in self.sell_orders]

    def get_all_buy_orders(self) -> list[Order]:
        return self.buy_orders

    def get_all_sell_orders(self) -> list[Order]:
        return self.sell_orders

    def get_open_orders(self) -> list[Order]:
        return [order for order in self.buy_orders + self.sell_orders if order.is_open()]

    def get_completed_orders(self) -> list[Order]:
        return [order for order in self.buy_orders + self.sell_orders if order.is_filled()]

    def get_grid_level_for_order(self, order: Order) -> GridLevel | None:
        # FIX: Lookup by identifier so it works even if 'order' is a new instance
        return self.order_to_grid_map.get(order.identifier)

    def update_order_status(
        self,
        order_id: str,
        new_status: OrderStatus,
    ) -> None:
        for order in self.buy_orders + self.sell_orders:
            if order.identifier == order_id:
                order.status = new_status
                break

    def get_order(self, order_id: str) -> Order | None:
        """
        Retrieves an order by its identifier.
        """
        for order in self.buy_orders + self.sell_orders + self.non_grid_orders:
            if order.identifier == order_id:
                return order
        return None
