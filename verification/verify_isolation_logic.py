import logging
import uuid
import asyncio


# Mocking the context
class MockDB:
    def get_order(self, order_id):
        # Return True only for a specific legacy order ID
        if order_id == "legacy_db_order":
            return True
        return None


class MockOrderManager:
    def __init__(self, bot_id):
        self.bot_id = bot_id
        self.db = MockDB()
        self.logger = logging.getLogger("MockOrderManager")
        logging.basicConfig(level=logging.INFO)

    async def test_filtering(self, raw_orders):
        print(f"\n--- Testing Bot ID: {self.bot_id} ---")
        exchange_orders = []
        if self.bot_id:
            prefix = f"G_{self.bot_id}_"
            print(f"Prefix: {prefix}")
            for o in raw_orders:
                cid = o.get("clientOrderId") or ""
                print(f"Checking Order {o['id']} (CID: {cid})...", end="")

                # Logic copied from OrderManager
                if str(cid).startswith(prefix):
                    exchange_orders.append(o)
                    print(" MATCH (Prefix)")
                elif self.db.get_order(o["id"]):
                    exchange_orders.append(o)
                    print(" MATCH (DB Legacy)")
                else:
                    print(" IGNORED")
        else:
            exchange_orders = raw_orders
            print("Took ALL (No Bot ID)")

        return exchange_orders


async def main():
    # 1. Setup Orders
    # Bot 1 Orders
    o1 = {"id": "101", "clientOrderId": "G_1_abc123"}
    o2 = {"id": "102", "clientOrderId": "G_1_xyz789"}
    # Bot 2 Orders
    o3 = {"id": "201", "clientOrderId": "G_2_bob456"}
    # Legacy/Unknown Orders
    o4 = {"id": "999", "clientOrderId": None}
    o5 = {"id": "legacy_db_order", "clientOrderId": None}  # Should be picked up by DB check

    raw_orders = [o1, o2, o3, o4, o5]

    # 2. Test Bot 1
    bot1 = MockOrderManager(bot_id=1)
    filtered_1 = await bot1.test_filtering(raw_orders)

    # Assertions
    ids_1 = [o["id"] for o in filtered_1]
    assert "101" in ids_1
    assert "102" in ids_1
    assert "201" not in ids_1  # Bot 2 order
    assert "999" not in ids_1  # Random order
    assert "legacy_db_order" in ids_1  # DB tracked order
    print("✅ Bot 1 Filtering Passed")

    # 3. Test Bot 2
    bot2 = MockOrderManager(bot_id=2)
    filtered_2 = await bot2.test_filtering(raw_orders)

    ids_2 = [o["id"] for o in filtered_2]
    assert "201" in ids_2
    assert "101" not in ids_2
    assert "legacy_db_order" in ids_2  # Mock DB returns true for everyone in this simple mock, but logic holds
    print("✅ Bot 2 Filtering Passed")

    # 4. Verify Counting Logic (Simulation)
    print("\n--- Verifying Order Count Isolation ---")
    # Simulation: OrderManager.reconcile_grid_orders counting logic
    # It iterates over the FILTERED list.

    # Bot 1 Count
    count_1 = len(filtered_1)
    # Bot 2 Count
    count_2 = len(filtered_2)

    print(f"Bot 1 visible orders: {count_1}")
    print(f"Bot 2 visible orders: {count_2}")

    # In the bug scenario, they both saw (2 from bot1 + 1 from bot2 + 2 legacy = 5 total)
    # Now:
    # Bot 1 sees: 101, 102, legacy (3 total)
    # Bot 2 sees: 201, legacy (2 total)

    assert count_1 == 3
    assert count_2 == 2

    print(f"✅ Isolation Confirmed: Bot 1 count ({count_1}) != Bot 2 count ({count_2})")
    print("Each bot is judged ONLY by its own (and shared legacy) orders.")


if __name__ == "__main__":
    asyncio.run(main())
