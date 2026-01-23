"""Quick check of bot orders."""

import sqlite3

conn = sqlite3.connect("bot_data.db")
c = conn.cursor()

# Check orders by side and status for both bots
for bot_id in [265, 266]:
    print(f"\n=== Bot {bot_id} ===")
    c.execute(
        """
        SELECT side, status, COUNT(*), ROUND(SUM(quantity * price), 2) 
        FROM grid_orders 
        WHERE bot_id = ? 
        GROUP BY side, status
    """,
        (bot_id,),
    )
    for row in c.fetchall():
        side, status, cnt, total = row
        print(f"  {side} {status}: {cnt} orders, ${total}")

    # Also get balances
    c.execute("SELECT fiat_balance, crypto_balance, reserve_amount FROM bot_balances WHERE bot_id = ?", (bot_id,))
    row = c.fetchone()
    if row:
        print(f"  DB Balances: fiat={row[0]}, crypto={row[1]}, reserve={row[2]}")

conn.close()
