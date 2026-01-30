import argparse
import asyncio
import logging
import os
import sys

# Load dotenv to ensure DB credentials are loaded in the Linux environment
try:
    from dotenv import load_dotenv
except ImportError:
    print("❌ python-dotenv not found. Install with: pip install python-dotenv")
    sys.exit(1)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config_manager import ConfigManager
from config.config_validator import ConfigValidator
from core.services.live_exchange_service import LiveExchangeService
from core.storage.bot_database import BotDatabase
from utils.config_name_generator import generate_config_name


async def audit_bot(bot_id: int, symbol: str, config_path: str, fix: bool = False):
    print(f"🔍 Starting Audit for Bot {bot_id} on {symbol}...")

    # 0. Load Environment Variables (Crucial for DB credentials)
    load_dotenv()

    # 1. Setup Config & Logging
    try:
        config_manager = ConfigManager(config_path, ConfigValidator())
        config_name = generate_config_name(config_manager)
        logging.basicConfig(level=logging.INFO)
    except Exception as e:
        print(f"❌ Config Error: {e}")
        return

    # 2. Setup Database
    db = BotDatabase()
    try:
        print("🔌 Connecting to Database (PostgreSQL)...")
        # Ensure we connect. BotDatabase.connect() creates the pool.
        await db.connect()

        # 2a. Attempt to load API keys and Exchange Info from Bot Config in DB
        print(f"📥 Fetching remote config for Bot {bot_id} from DB...")
        import json

        remote_config_str = await db.get_bot_config(bot_id)

        # Default to local config values
        db_exchange_name = None
        db_trading_mode = None

        if remote_config_str:
            try:
                remote_config = json.loads(remote_config_str)

                # Check for keys in credentials section (Standard) or exchange section (Legacy)
                creds = remote_config.get("credentials", {})
                exchange_conf = remote_config.get("exchange", {})

                db_api_key = creds.get("api_key") or exchange_conf.get("api_key")
                db_secret = (
                    creds.get("api_secret") or exchange_conf.get("api_secret") or exchange_conf.get("secret_key")
                )
                db_password = (
                    creds.get("password") or exchange_conf.get("password") or exchange_conf.get("api_password")
                )

                # Fetch Exchange Name and Mode from DB
                db_exchange_name = exchange_conf.get("name")
                db_trading_mode = exchange_conf.get("trading_mode")

                if db_api_key and db_secret:
                    print("   ✅ Found API Credentials in DB Config!")
                    os.environ["EXCHANGE_API_KEY"] = db_api_key
                    os.environ["EXCHANGE_SECRET_KEY"] = db_secret
                    if db_password:
                        os.environ["EXCHANGE_PASSWORD"] = db_password
                else:
                    print("   ⚠️  Bot config found in DB, but no API keys inside 'exchange' section.")
            except Exception as e:
                print(f"   ❌ Failed to parse DB config: {e}")
        else:
            print("   ⚠️  No config found for this bot in DB (New bot?).")

    except Exception as e:
        print(f"❌ Database Connection Failed: {e}")
        print("   Ensure you are running this in an environment with access to the Postgres DB.")
        print("   Required Env Vars: DB_CONNECTION_STRING or POSTGRES_USER/PASSWORD/HOST/DB")
        return

    # 3. Setup Exchange
    # Use DB values if available, otherwise fallback to local config
    local_mode_str = config_manager.get_trading_mode().value
    mode_str = db_trading_mode if db_trading_mode else local_mode_str

    # Normalize mode string to boolean for paper trading
    is_paper = mode_str == "paper_trading" or mode_str == "paper"

    exchange_id = db_exchange_name if db_exchange_name else config_manager.get_exchange_name()

    print(f"🔌 Connecting to Exchange: {exchange_id} (Paper: {is_paper})...")

    # Handle missing credentials by prompting user
    from core.services.exceptions import MissingEnvironmentVariableError

    # We need to manually override the exchange name in config_manager for the service to pick it up correctly
    if db_exchange_name:
        if "exchange" not in config_manager.config:
            config_manager.config["exchange"] = {}
        config_manager.config["exchange"]["name"] = db_exchange_name

    try:
        exchange_service = LiveExchangeService(config_manager=config_manager, is_paper_trading_activated=is_paper)
    except MissingEnvironmentVariableError:
        print("\n⚠️  API Credentials not found in environment.")
        print("   Please enter them manually for this session (input will be hidden):")
        import getpass

        api_key = getpass.getpass("   API Key: ").strip()
        secret = getpass.getpass("   Secret Key: ").strip()
        password = getpass.getpass("   API Password (optional, for OKX/KuCoin): ").strip()

        os.environ["EXCHANGE_API_KEY"] = api_key
        os.environ["EXCHANGE_SECRET_KEY"] = secret
        if password:
            os.environ["EXCHANGE_PASSWORD"] = password

        print("\n🔄 Retrying connection with provided credentials...")
        exchange_service = LiveExchangeService(config_manager=config_manager, is_paper_trading_activated=is_paper)
    try:
        # 4. Fetch DB Orders
        print("📂 Fetching Active Orders from DB...")
        db_orders = await db.get_all_active_orders(bot_id)
        db_order_ids = set(str(oid) for oid in db_orders.keys())
        print(f"   found {len(db_orders)} active orders in DB.")

        # 5. Fetch Exchange Orders
        print(f"🌐 Fetching Open Orders from Exchange for {symbol}...")
        exchange_orders = await exchange_service.refresh_open_orders(symbol)
        print(f"   found {len(exchange_orders)} open orders on Exchange.")

        # 6. Compare
        zombie_orders = []
        tracking_orders = []

        # Grid Bot Client ID Prefix
        prefix = f"G{bot_id}x"

        for order in exchange_orders:
            oid = str(order["id"])
            client_oid = str(order.get("clientOrderId", ""))

            # Check if it belongs to THIS bot instance
            if not client_oid.startswith(prefix):
                # Ignore foreign orders (manual or other bots)
                continue

            if oid not in db_order_ids:
                zombie_orders.append(order)
            else:
                tracking_orders.append(order)

        print("\n" + "=" * 60)
        print("📊 AUDIT RESULTS")
        print("=" * 60)
        print(f"✅ Synced Orders: {len(tracking_orders)}")
        print(f"🧟 Zombie Orders: {len(zombie_orders)}")

        if zombie_orders:
            print("\nWARNING: The following orders are open on exchange but UNTRACKED by the bot:")
            total_locked_base = 0.0
            total_locked_quote = 0.0

            for z in zombie_orders:
                side = z["side"].upper()
                amt = float(z["amount"])
                price = float(z["price"])

                print(f" - [{side}] {amt} @ {price} (ID: {z['id']})")

                if side == "SELL":
                    total_locked_base += amt
                elif side == "BUY":
                    total_locked_quote += amt * price

            print("\n💰 Total Funds Locked in Zombies:")
            print(f"   Base ({symbol.split('/')[0]}): {total_locked_base:.6f}")
            print(f"   Quote ({symbol.split('/')[1]}): {total_locked_quote:.2f}")

            if fix:
                print("\n" + "-" * 60)
                confirm = input("⚠️  Do you want to CANCEL these zombie orders to free up funds? (y/n): ")
                if confirm.lower() == "y":
                    print("🔪 Cancelling Zombie Orders...")
                    for z in zombie_orders:
                        oid = z["id"]
                        try:
                            await exchange_service.cancel_order(oid, symbol)
                            print(f"   ✅ Cancelled {oid}")
                            await asyncio.sleep(0.1)  # rate limit
                        except Exception as e:
                            print(f"   ❌ Failed to cancel {oid}: {e}")
                    print("Done.")
                else:
                    print("Skipping cancellation.")
        else:
            print("\n✅ System status GREEN. No zombie orders found for this bot.")

    except Exception as e:
        print(f"\n❌ ERROR during audit: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await exchange_service.close_connection()
        await db.close()


async def find_bots_by_email(email: str):
    """
    Connects to DB, fetches all bots, and returns a list of bot_ids
    where the config_json contains the matching email.
    """
    found_bot_ids = []

    # We need a temporary DB connection to search
    db = BotDatabase()
    try:
        await db.connect()
        print(f"🔎 Creating DB connection to search for user: {email}...")

        # Fetch all bots (ID + Config)
        # We use db.pool directly to fetch raw rows
        rows = await db.pool.fetch("SELECT bot_id, config_json FROM bots")

        print(f"   Scanning {len(rows)} bots in database...")

        import json

        for row in rows:
            bid = row["bot_id"]
            raw_conf = row["config_json"]

            if not raw_conf:
                continue

            try:
                conf = json.loads(raw_conf)
                # Check various common keys for user email
                # Adjust keys based on your actual config structure
                if len(found_bot_ids) == 0 and len(rows) > 0:
                    print(f"DEBUG: Config keys for first bot {bid}: {list(conf.keys())}")
                    # print(f"DEBUG: Full Config: {conf}") # Uncomment if needed

                conf_email = conf.get("user_email") or conf.get("email") or conf.get("user", {}).get("email")

                if conf_email and conf_email.strip().lower() == email.strip().lower():
                    found_bot_ids.append(bid)
            except Exception:
                continue

    except Exception as e:
        print(f"❌ Error searching by email: {e}")
    finally:
        await db.close()

    return found_bot_ids


async def main():
    # Load environment variables globally before any DB connection
    load_dotenv()

    parser = argparse.ArgumentParser(description="Audit Exchange Orders for Zombies")
    parser.add_argument("--bot-id", type=int, help="Specific Bot ID to audit")
    parser.add_argument("--email", type=str, help="Audit ALL bots belonging to this email (e.g. info9@email.com)")
    parser.add_argument("--symbol", type=str, required=True, help="Trading Pair (e.g. XRP/USDT)")
    parser.add_argument("--config", type=str, default="config/config.json", help="Path to bot config")
    parser.add_argument("--fix", action="store_true", help="Prompt to fix issues found")

    args = parser.parse_args()

    if args.email:
        print(f"👤 Searching for bots owned by: {args.email}")
        bot_ids = await find_bots_by_email(args.email)

        if not bot_ids:
            print(f"⚠️ No bots found for user {args.email}")
            return

        print(f"✅ Found {len(bot_ids)} bots for {args.email}: {bot_ids}")
        print("=" * 60)

        for i, bid in enumerate(bot_ids):
            print(f"\n[{i + 1}/{len(bot_ids)}] Processing Bot {bid}...")
            # We treat each audit as a separate run
            await audit_bot(bid, args.symbol, args.config, args.fix)

    elif args.bot_id:
        await audit_bot(args.bot_id, args.symbol, args.config, args.fix)
    else:
        print("❌ Error: You must provide either --bot-id OR --email")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
