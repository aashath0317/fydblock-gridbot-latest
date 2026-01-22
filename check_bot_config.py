import sqlite3
import json

DB_PATH = "p:\\Fydblock\\gridbot\\bot_data.db"
BOT_IDS = [258, 261]


def check_bots_config():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print(f"Checking 'bots' table for IDs: {BOT_IDS}")
        placeholders = ",".join("?" for _ in BOT_IDS)
        cursor.execute(f"SELECT bot_id, config_json FROM bots WHERE bot_id IN ({placeholders})", BOT_IDS)

        rows = cursor.fetchall()
        for row in rows:
            bid = row["bot_id"]
            conf_str = row["config_json"]
            try:
                conf = json.loads(conf_str)
                # Looking for investment in various places
                inv = (
                    conf.get("investment")
                    or conf.get("strategy", {}).get("investment")
                    or conf.get("grid_strategy", {}).get("investment")
                )
                print(f"[Bot {bid}] Investment in Config: {inv}")
                print(f"Full Config Snippet: {str(conf)[:100]}...")
            except Exception as e:
                print(f"[Bot {bid}] Failed to parse config: {e}")

        conn.close()
    except Exception as e:
        print(f"DB Error: {e}")


if __name__ == "__main__":
    check_bots_config()
