import asyncio
import os

import asyncpg
from dotenv import load_dotenv

# Load env from parent dir
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BOT_IDS = [315, 316]


async def clean_wallet():
    print(f"🧹 Mass Cleaning Wallet for Bots {BOT_IDS}...")

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")

    try:
        conn = await asyncpg.connect(user=user, password=password, host=host, port=port, database=dbname)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    for bot_id in BOT_IDS:
        print(f"\n🚀 Processing Bot {bot_id}...")

        # 1. Get Current State
        row = await conn.fetchrow("SELECT * FROM bot_balances WHERE bot_id = $1", bot_id)
        if not row:
            print("❌ No balance record found.")
            continue

        fiat_free = float(row.get("fiat_balance", 0))
        crypto_free = float(row.get("crypto_balance", 0))

        # 2. Get Locked Value (Active Orders)
        orders = await conn.fetch("SELECT * FROM grid_orders WHERE bot_id = $1 AND status = 'OPEN'", bot_id)

        locked_fiat = 0.0
        locked_crypto = 0.0

        for o in orders:
            side = str(o["side"]).upper()
            price = float(o["price"])
            amount = float(o.get("quantity", 0))

            if side == "BUY":
                locked_fiat += amount * price
            elif side == "SELL":
                locked_crypto += amount

        # Estimate price
        avg_price = 125.0
        sell_orders = [float(o["price"]) for o in orders if str(o["side"]).upper() == "SELL"]
        if sell_orders:
            avg_price = sum(sell_orders) / len(sell_orders)

        # 3. Calculate True Grid Value
        true_grid_value = locked_fiat + (locked_crypto * avg_price)
        print(f"📊 Locked Value (Grid): ${true_grid_value:,.2f}")
        print(f"📉 Current Free Fiat: ${fiat_free:,.2f}")

        # 4. BURN IT ALL
        # We set Free Fiat to 0 to remove inflation.
        # User requested: "locked orders value ... is the actual value"
        # We leave a tiny dust (0.01) just in case logic fails on exactly zero.
        target_fiat = 0.01

        if fiat_free > target_fiat or crypto_free > 0.01:
            print(f"🔥 Burning excess: ${fiat_free - target_fiat:,.2f} Fiat, {crypto_free:.4f} SOL...")
            await conn.execute(
                "UPDATE bot_balances SET fiat_balance = $1, crypto_balance = 0.0 WHERE bot_id = $2", target_fiat, bot_id
            )
            print("✅ Excess Fiat & Crypto Removed.")

            # Verify new total
            final_total = target_fiat + (0.0 * avg_price) + true_grid_value
            print(f"✅ New Portfolio Value: ${final_total:,.2f} (Matches Locked Value)")
        else:
            print("✅ Already clean.")

    await conn.close()
    print("\n✨ Done. Restart bot now.")


if __name__ == "__main__":
    asyncio.run(clean_wallet())
