import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from config.config_manager import ConfigManager
from config.config_validator import ConfigValidator
from core.grid_management.grid_manager import GridManager
from core.storage.bot_database import BotDatabase
from strategies.strategy_type import StrategyType


async def reconstruct_grid(bot_id: int):
    print(f"🛠️ Reconstructing Grid for Bot {bot_id}...")

    # 1. Connect to DB
    db = BotDatabase()
    await db.connect()

    try:
        # 2. Get Config from DB
        config_json = await db.get_bot_config(bot_id)
        if not config_json:
            print("❌ No config found in DB.")
            return

        # 3. Initialize Managers (In-Memory)
        import tempfile

        # Create a temp file for ConfigManager (it expects a file path)
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as tmp:
            tmp.write(config_json)
            tmp_path = tmp.name

        try:
            config_manager = ConfigManager(tmp_path, ConfigValidator())
            # Force Bot ID injection (config manager usually gets it from file, but here we inject)
            config_manager.config["bot_id"] = bot_id

            # Determine Strategy (assume Simple Grid for now or fetch from config)
            # Default to SIMPLE_GRID
            strategy_type = StrategyType.SIMPLE_GRID

            grid_manager = GridManager(config_manager, strategy_type, db)

            # 4. Generate Grid Structure (Memory Only)
            print("📐 Calculating Grid Levels from Config...")
            grid_manager.initialize_grids_and_levels()
            print(f"   Generated {len(grid_manager.grid_levels)} levels in memory.")

            # 5. Fetch Active Orders
            print("📂 Fetching Active Orders from DB...")
            active_orders = await db.get_all_active_orders(bot_id)
            print(f"   Found {len(active_orders)} active orders.")

            # 6. Map Orders to Grids and Persist
            print("💾 Persisting Grid Levels to DB and Mapping Orders...")

            # Clear existing (broken) levels first?
            # Ideally yes, but let's just UPSERT/Insert logic via add_grid_level loop

            matched_orders = 0

            tolerance = 0.001

            for price, level in grid_manager.grid_levels.items():
                # Check if this level matches an active order
                matched_order_id = None
                matched_side = None

                for oid, order_data in active_orders.items():
                    oprice = order_data["price"]
                    oside = order_data["side"]

                    if abs(oprice - price) < (price * tolerance):
                        matched_order_id = oid
                        matched_side = oside
                        break

                # Determine correct state/stock based on match
                if matched_order_id:
                    matched_orders += 1
                    level.stock_on_hand = 0.0  # Reset

                    if matched_side == "BUY":
                        level.state = "waiting_for_buy_fill"  # GridCycleState.WAITING_FOR_BUY_FILL
                    elif matched_side == "SELL":
                        level.state = "waiting_for_sell_fill"
                        # If we have a SELL order, it implies we successfully BOUGHT previously.
                        # Do we track stock?
                        # OrderManager usually holds stock only when "READY_TO_SELL" (Holding bag).
                        # If "WAITING_FOR_SELL_FILL", the stock is LOCKED in the order.
                        # So stock_on_hand should be 0 (because it's on the exchange).

                else:
                    # No active order at this price.
                    # Is it a Buy Zone or Sell Zone?
                    # initialization logic (initialize_grids_and_levels) set:
                    # < central => READY_TO_BUY
                    # >= central => READY_TO_SELL (Holding Bag?)

                    # If it's READY_TO_SELL, it means we SHOULD have stock on hand (Initial Position).
                    # BUT we don't know if we actually do.
                    # Safety: Mark as READY_TO_BUY? Or assume initial investment logic fulfilled?

                    # For a running bot, empty levels below price are READY_TO_BUY.
                    # Empty levels above price are... READY_TO_BUY? (If we sold everything)
                    # OR current price determines zone.

                    # Let's rely on updated zone logic if possible, or just persist the Init state.
                    # grid_manager.initialize_grids_and_levels() sets state based on current config range vs central price.
                    pass

                # PERSIST TO DB
                # Note: grid_level.state is an Enum in memory, need string for DB
                state_str = level.state.value if hasattr(level.state, "value") else str(level.state)

                # print(f"   Saving Level {price:.6f} | State: {state_str} | Stock: {level.stock_on_hand}")

                # Since we don't have a direct "upsert_all" method exposed in this context,
                # we iterate.
                # Use raw query for speed/certainty

                # Check existence
                await db.pool.execute(
                    """
                    INSERT INTO grid_levels (bot_id, price, status, stock_on_hand, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    ON CONFLICT (bot_id, price) DO UPDATE
                    SET status = $3, stock_on_hand = $4, updated_at = NOW()
                """,
                    bot_id,
                    price,
                    state_str,
                    level.stock_on_hand,
                )

            print(f"✅ Reconstructed {len(grid_manager.grid_levels)} levels.")
            print(f"Matched {matched_orders} orders to levels.")

        finally:
            os.remove(tmp_path)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", type=int, required=True)
    args = parser.parse_args()

    asyncio.run(reconstruct_grid(args.bot_id))
