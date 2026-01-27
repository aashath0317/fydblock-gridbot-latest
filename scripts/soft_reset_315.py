import asyncio
import os

import asyncpg
from dotenv import load_dotenv

# Load .env from the parent directory
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BOT_ID = 316


async def soft_reset(bot_id):
    print(f"🔄 Performing Soft Reset for Bot {bot_id} (PostgreSQL)...")

    # Construct DB URL from env
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")

    print(f"   ℹ️  Connecting to: {host}:{port} @ {dbname} (User: {user})")

    if not all([user, password, dbname]):
        print("❌ Missing DB credentials in .env")
        print(f"   Debug: User={user}, DB={dbname}, Host={host}")
        return

    # Pass credentials directly to avoid URL encoding issues with special chars in password (e.g. '@')
    try:
        conn = await asyncpg.connect(user=user, password=password, host=host, port=port, database=dbname)
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}")
        return

    try:
        # 1. Clear Active Orders
        res = await conn.fetchval("SELECT COUNT(*) FROM grid_orders WHERE bot_id = $1", bot_id)
        await conn.execute("DELETE FROM grid_orders WHERE bot_id = $1", bot_id)
        print(f"   - Cleared {res} active orders from DB.")

        # 2. Reset Balances to 0
        await conn.execute(
            "UPDATE bot_balances SET fiat_balance=0, crypto_balance=0, reserve_amount=0 WHERE bot_id = $1", bot_id
        )
        print("   - Reset tracked balances to 0.")

        # 3. Clear Grid Levels / Stock
        # Warning: This resets "Stock on Hand" tracking.
        # The bot will have to rebuild its understanding of inventory from the wallet balance.
        # This is what we want for a "Soft Reset" after corruption.
        await conn.execute("DELETE FROM grid_levels WHERE bot_id = $1", bot_id)
        print("   - Cleared grid level records (Stock/Status).")

        print("✅ Soft Reset Complete.")
        print("👉 Please RESTART the bot immediately to trigger a fresh sync.")

    except Exception as e:
        print(f"❌ Error during reset: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(soft_reset(BOT_ID))
