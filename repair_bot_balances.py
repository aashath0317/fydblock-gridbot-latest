"""
Repair Script v2: Fix Corrupted Bot Balances

Correct calculation:
FREE USDT = Investment - Reserve - (Buy Filled USDT) + (Sell Filled USDT) - (Buy Open USDT)

Or simpler:
FREE USDT = Investment - Reserve - (Open Buy Orders) - (Value of SOL we currently hold)
Where "SOL we hold" = Open Sell orders (since each filled buy creates a sell order for the SOL)
"""

import argparse
import json
import sqlite3
from pathlib import Path


def get_connection(db_path: str = "bot_data.db"):
    return sqlite3.connect(db_path)


def get_all_bots(conn) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT bot_id, config_json, status 
        FROM bots 
        WHERE status = 'RUNNING' OR status = 'ACTIVE'
    """)
    rows = cursor.fetchall()

    bots = []
    for row in rows:
        bot_id, config_json, status = row
        config = json.loads(config_json) if config_json else {}
        bots.append({"bot_id": bot_id, "config": config, "status": status, "investment": config.get("investment", 0)})
    return bots


def get_order_stats(conn, bot_id: int) -> dict:
    """Get detailed order statistics."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT side, status, COUNT(*), ROUND(SUM(quantity * price), 2)
        FROM grid_orders 
        WHERE bot_id = ?
        GROUP BY side, status
    """,
        (bot_id,),
    )

    stats = {
        "buy_open": 0.0,
        "buy_filled": 0.0,
        "sell_open": 0.0,
        "sell_filled": 0.0,
        "buy_open_count": 0,
        "sell_open_count": 0,
    }

    for row in cursor.fetchall():
        side, status, count, total = row
        key = f"{side.lower()}_{status.lower()}"
        if key in stats:
            stats[key] = float(total) if total else 0.0
        if side.lower() == "buy" and status.upper() == "OPEN":
            stats["buy_open_count"] = count
        if side.lower() == "sell" and status.upper() == "OPEN":
            stats["sell_open_count"] = count

    return stats


def get_current_balances(conn, bot_id: int) -> dict | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT fiat_balance, crypto_balance, reserve_amount 
        FROM bot_balances 
        WHERE bot_id = ?
    """,
        (bot_id,),
    )
    row = cursor.fetchone()
    if row:
        return {
            "fiat_balance": float(row[0]) if row[0] else 0.0,
            "crypto_balance": float(row[1]) if row[1] else 0.0,
            "reserve_amount": float(row[2]) if row[2] else 0.0,
        }
    return None


def update_fiat_balance(conn, bot_id: int, new_fiat_balance: float, dry_run: bool = False):
    if dry_run:
        return

    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE bot_balances 
        SET fiat_balance = ? 
        WHERE bot_id = ?
    """,
        (new_fiat_balance, bot_id),
    )
    conn.commit()


def repair_bot(conn, bot: dict, dry_run: bool = False) -> dict:
    """
    Repair a single bot's balance using correct calculation.

    FREE USDT = Investment - Reserve - BuyFilled + SellFilled - BuyOpen

    This accounts for:
    - USDT spent on buys that filled (converted to SOL)
    - USDT recovered from sells that filled (SOL sold back)
    - USDT locked in pending buy orders
    """
    bot_id = bot["bot_id"]
    investment = bot["investment"]

    current = get_current_balances(conn, bot_id)
    if not current:
        return {"bot_id": bot_id, "status": "SKIPPED", "reason": "No balances found in DB"}

    stats = get_order_stats(conn, bot_id)
    reserve = current["reserve_amount"]

    # CORRECT CALCULATION:
    # Starting capital = Investment - Reserve
    # Minus USDT spent on filled buys
    # Plus USDT recovered from filled sells
    # Minus USDT locked in open buy orders
    # The result is FREE USDT available for trading

    starting_capital = investment - reserve
    usdt_spent_on_buys = stats["buy_filled"]
    usdt_from_sells = stats["sell_filled"]
    usdt_locked_in_buys = stats["buy_open"]

    correct_free = starting_capital - usdt_spent_on_buys + usdt_from_sells - usdt_locked_in_buys

    # For display: total quote holdings = free + locked
    locked_quote = usdt_locked_in_buys
    correct_total = correct_free + locked_quote

    diff = abs(current["fiat_balance"] - correct_free)

    result = {
        "bot_id": bot_id,
        "investment": investment,
        "reserve": reserve,
        "buy_filled": usdt_spent_on_buys,
        "sell_filled": usdt_from_sells,
        "buy_open": usdt_locked_in_buys,
        "sell_open": stats["sell_open"],
        "current_free": current["fiat_balance"],
        "correct_free": correct_free,
        "locked_quote": locked_quote,
        "current_total": current["fiat_balance"] + locked_quote,
        "correct_total": correct_total,
        "difference": diff,
    }

    if diff < 1.0:
        result["status"] = "OK"
        result["reason"] = "Balance is correct (diff < $1)"
    else:
        result["status"] = "REPAIRED" if not dry_run else "WOULD_REPAIR"
        result["reason"] = f"FREE balance off by ${diff:.2f}"
        update_fiat_balance(conn, bot_id, correct_free, dry_run)

    return result


def main():
    parser = argparse.ArgumentParser(description="Repair corrupted bot balances v2")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    parser.add_argument("--bot-id", type=int, help="Repair only a specific bot")
    parser.add_argument("--db", default="bot_data.db", help="Database path")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return

    print(f"{'🔍 DRY RUN - ' if args.dry_run else ''}Repairing bot balances (v2) in {db_path}")
    print("=" * 80)

    conn = get_connection(str(db_path))

    all_bots = get_all_bots(conn)
    if args.bot_id:
        all_bots = [b for b in all_bots if b["bot_id"] == args.bot_id]

    if not all_bots:
        print("No bots found to repair.")
        return

    print(f"Found {len(all_bots)} bot(s) to check.\n")

    for bot in all_bots:
        result = repair_bot(conn, bot, dry_run=args.dry_run)

        status_emoji = {
            "OK": "✅",
            "REPAIRED": "🔧",
            "WOULD_REPAIR": "🔧",
            "SKIPPED": "⏭️",
        }.get(result["status"], "❓")

        print(f"{status_emoji} Bot {result['bot_id']} - {result['status']}")
        print(f"   Investment: ${result.get('investment', 0):,.2f}, Reserve: ${result.get('reserve', 0):,.2f}")
        print(f"   Order Stats:")
        print(f"      Buy FILLED:  ${result.get('buy_filled', 0):,.2f} (USDT spent)")
        print(f"      Sell FILLED: ${result.get('sell_filled', 0):,.2f} (USDT recovered)")
        print(f"      Buy OPEN:    ${result.get('buy_open', 0):,.2f} (USDT locked)")
        print(f"      Sell OPEN:   ${result.get('sell_open', 0):,.2f} (SOL value locked)")
        print(f"   Current FREE:  ${result.get('current_free', 0):,.2f}")
        print(f"   Correct FREE:  ${result.get('correct_free', 0):,.2f}")
        print(f"   Locked (buys): ${result.get('locked_quote', 0):,.2f}")
        print(f"   Current Total: ${result.get('current_total', 0):,.2f}")
        print(f"   Correct Total: ${result.get('correct_total', 0):,.2f}")
        print(f"   Reason: {result.get('reason', 'N/A')}")
        print()

    conn.close()

    if args.dry_run:
        print("=" * 80)
        print("This was a DRY RUN. Run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
