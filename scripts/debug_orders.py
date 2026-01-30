import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from core.storage.bot_database import BotDatabase


async def debug_bot_orders(bot_id: int):
    print(f"🔍 Debugging Orders vs Grid Levels for Bot {bot_id}...")

    db = BotDatabase()
    await db.connect()

    try:
        # 1. Fetch Active Orders
        print("📂 Fetching Active Orders...")
        orders = await db.get_all_active_orders(bot_id)
        print(f"   Found {len(orders)} active orders.")

        # 2. Fetch Grid Levels
        print("📐 Fetching Grid Levels...")
        grid_levels = await db.get_grid_levels(bot_id)
        print(f"   Found {len(grid_levels)} grid levels.")

        # 3. Analyze Mismatches
        print("\n📊 Analysis:")

        # Group orders by price (bucketed)
        orders_by_price = []
        for oid, data in orders.items():
            orders_by_price.append({"id": oid, "price": data["price"], "side": data["side"]})

        # Check each Grid Level with Stock
        tolerance = 0.001
        mismatches = 0

        for price, level_data in grid_levels.items():
            stock = level_data["stock"]
            status = level_data["status"]

            # If we have stock, we expect a SELL order
            if stock > 0:
                print(f"\n🔹 Grid Level @ {price:.6f} has Stock: {stock:.6f}")

                # Search for matching order
                match = None
                closest_diff = float("inf")

                for o in orders_by_price:
                    diff = abs(o["price"] - price)
                    if diff < closest_diff:
                        closest_diff = diff

                    if diff < tolerance:
                        match = o
                        break

                if match:
                    print(f"   ✅ Matched with Order {match['id']} @ {match['price']:.6f} (Diff: {closest_diff:.6f})")
                else:
                    mismatches += 1
                    print(f"   ❌ NO MATCHING ORDER FOUND! (Closest Diff: {closest_diff:.6f})")
                    print(f"      This level thinks it has stock, but DB has no order within {tolerance} tolerance.")
                    print("      -> This causes OrderManager to try placing a DUPLICATE order.")

                    # Show closest neighbors
                    sorted_orders = sorted(orders_by_price, key=lambda x: abs(x["price"] - price))
                    print("      Closest candidates:")
                    for cand in sorted_orders[:3]:
                        print(f"      - {cand['price']:.6f} (Diff: {abs(cand['price'] - price):.6f}) [{cand['side']}]")

    finally:
        await db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", type=int, required=True)
    args = parser.parse_args()

    asyncio.run(debug_bot_orders(args.bot_id))
