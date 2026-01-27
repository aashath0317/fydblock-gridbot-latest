import asyncio
import os

import asyncpg
from dotenv import load_dotenv

# Load env from parent dir
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BOT_ID = 316


async def audit_wallet():
    print(f"🔍 Auditing Wallet for Bot {BOT_ID}...")

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

    # 1. Get Wallet Balance
    # Note: Table might be 'bot_balances' or simply stored in 'bots' config?
    # Checking bot_database.py, there is no explicit 'bot_balances' table creation shown in lines 1-100.
    # But previous logs showed "Loaded PAPER Balances from DB".
    # Let's assume 'bot_balances' exists (maybe created elsewhere or I missed it).
    # If not, we might fail here too.
    # Wait, the previous run FAILED at 'active_orders'. It passed 'bot_balances'?
    # "Current Wallet (DB): Free Fiat: $6,132.94" -> So bot_balances EXISTS.

    row = await conn.fetchrow("SELECT * FROM bot_balances WHERE bot_id = $1", BOT_ID)
    if not row:
        print("❌ No balance record found in bot_balances.")
        return

    fiat_free = float(row.get("fiat_balance", 0))
    crypto_free = float(row.get("crypto_balance", 0))
    print("💰 Current Wallet (DB):")
    print(f"   - Free Fiat:   ${fiat_free:,.2f}")
    print(f"   - Free Crypto: {crypto_free:.4f} SOL")

    # 2. Get Active Orders (Table: grid_orders)
    # Filter for 'OPEN' status
    orders = await conn.fetch("SELECT * FROM grid_orders WHERE bot_id = $1 AND status = 'OPEN'", BOT_ID)

    locked_fiat = 0.0
    locked_crypto = 0.0
    buy_count = 0
    sell_count = 0

    print(f"\n📊 Active Grid Orders ({len(orders)}):")

    for o in orders:
        side = o["side"]  # 'buy' or 'sell' (case sensitive? usually caps in DB)
        price = float(o["price"])
        # Field name might be 'quantity' or 'amount'. DB def says 'quantity' (Line 93)
        amount = float(o.get("quantity", 0))

        if str(side).upper() == "BUY":
            # Buy Order locks Fiat
            val = amount * price
            locked_fiat += val
            buy_count += 1
        elif str(side).upper() == "SELL":
            # Sell Order locks Crypto
            locked_crypto += amount
            sell_count += 1

    print(f"   - Buys: {buy_count} (Locked: ${locked_fiat:,.2f})")
    print(f"   - Sells: {sell_count} (Locked: {locked_crypto:.4f} SOL)")

    # 3. Calculate True Value vs Current Total
    # Estimate SOL price from Sell Orders or hardcode
    avg_price = 125.0
    if sell_count > 0:
        prices = [float(o["price"]) for o in orders if str(o["side"]).upper() == "SELL"]
        if prices:
            avg_price = sum(prices) / len(prices)

    true_grid_value = locked_fiat + (locked_crypto * avg_price)

    # Portfolio = Free Cash + Locked Cash + (Free Crypto + Locked Crypto)*Price
    current_total_value = fiat_free + locked_fiat + ((crypto_free + locked_crypto) * avg_price)

    print(f"\n🧮 Valuation Summary (Est Price ~${avg_price:.2f}):")
    print(f"   A. Value Locked in Grids:  ${true_grid_value:,.2f}")
    print(f"   B. Free Cashed (Excess):   ${fiat_free:,.2f}")
    print(f"   C. Total Portfolio Value:  ${current_total_value:,.2f}")

    diff = current_total_value - 10000.0
    print(f"\n⚠️ Inflation check: ${current_total_value:,.2f} - $10,000 Init = ${diff:,.2f} Excess")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(audit_wallet())
