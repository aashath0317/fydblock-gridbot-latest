import sqlite3
import json

DB_PATH = "p:\\Fydblock\\gridbot\\bot_data.db"
BOT_ID = 258
NEW_INVESTMENT = 1000.0


def patch_bot():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Get current config
        cursor.execute("SELECT config_json FROM bots WHERE bot_id = ?", (BOT_ID,))
        row = cursor.fetchone()
        if not row:
            print(f"Bot {BOT_ID} not found.")
            return

        conf_str = row[0]
        conf = json.loads(conf_str)

        # 2. Update Investment
        print(f"Old Config Investment: {conf.get('investment')}")
        conf["investment"] = NEW_INVESTMENT
        if "strategy" in conf:
            conf["strategy"]["investment"] = NEW_INVESTMENT
        if "grid_strategy" in conf:
            conf["grid_strategy"]["investment"] = NEW_INVESTMENT

        new_conf_str = json.dumps(conf)

        # 3. Save back
        cursor.execute("UPDATE bots SET config_json = ? WHERE bot_id = ?", (new_conf_str, BOT_ID))
        conn.commit()
        print(f"✅ Bot {BOT_ID} patched with Investment = {NEW_INVESTMENT}")

        # 4. ALSO Clear bot_balances to force clean setup_balances
        cursor.execute("DELETE FROM bot_balances WHERE bot_id = ?", (BOT_ID,))
        conn.commit()
        print(f"✅ Bot {BOT_ID} balances cleared to force regeneration.")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    patch_bot()
