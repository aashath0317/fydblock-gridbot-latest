import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BOT_ID_TO_STOP = 315


async def stop_bot():
    print(f"🛑 Stopping Bot {BOT_ID_TO_STOP} in DB...")

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

        # Check current status
        status = await conn.fetchval("SELECT status FROM bots WHERE bot_id = $1", BOT_ID_TO_STOP)
        print(f"   Current Status: {status}")

        if status != "STOPPED":
            await conn.execute("UPDATE bots SET status = 'STOPPED' WHERE bot_id = $1", BOT_ID_TO_STOP)
            print(f"✅ Bot {BOT_ID_TO_STOP} marked as STOPPED.")
        else:
            print(f"ℹ️ Bot {BOT_ID_TO_STOP} is already STOPPED.")

        await conn.close()
    except Exception as e:
        print(f"❌ DB Update Failed: {e}")


if __name__ == "__main__":
    asyncio.run(stop_bot())
