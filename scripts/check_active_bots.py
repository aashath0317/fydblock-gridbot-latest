import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


async def list_active_bots():
    print("🔍 Checking for RUNNING bots in DB...")

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")

    if not all([user, password, dbname]):
        print("❌ Missing DB credentials.")
        return

    try:
        conn = await asyncpg.connect(user=user, password=password, host=host, port=port, database=dbname)

        rows = await conn.fetch("SELECT bot_id, status FROM bots WHERE status = 'RUNNING' OR status = 'STARTING'")

        if not rows:
            print("✅ No bots found with status RUNNING/STARTING.")
        else:
            print(f"🔥 Found {len(rows)} active bots:")
            for row in rows:
                print(f"   - Bot ID: {row['bot_id']} (Status: {row['status']})")

        await conn.close()
    except Exception as e:
        print(f"❌ DB Check Failed: {e}")


if __name__ == "__main__":
    asyncio.run(list_active_bots())
