import requests
import json

BASE_URL = "http://127.0.0.1:8000"
BOT_IDS = [258, 261]  # IDs found in previous verification


def check_bot(bot_id):
    print(f"--- Checking Bot {bot_id} ---")
    try:
        url = f"{BASE_URL}/bot/{bot_id}/stats"
        print(f"GET {url}")
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            print(json.dumps(data, indent=2))

            holdings = data.get("holdings", {})
            fiat = holdings.get("quote", 0)
            print(f"[SUMMARY] Bot {bot_id} Fiat Holding: {fiat}")
        else:
            print(f"Error: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"Exception: {e}")


if __name__ == "__main__":
    for bid in BOT_IDS:
        check_bot(bid)
