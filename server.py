import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Any

from adapter.config_adapter import DictConfigManager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config.config_validator import ConfigValidator
from core.bot_management.event_bus import EventBus
from core.bot_management.grid_trading_bot import GridTradingBot
from core.bot_management.notification.notification_handler import NotificationHandler
from core.services.exchange_service_factory import ExchangeServiceFactory
from core.storage.bot_database import BotDatabase


# --- Logging Setup ---
class DuplicateLogFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.last_log = None

    def filter(self, record):
        current_log = (record.msg, record.args)
        if current_log == self.last_log:
            return False
        self.last_log = current_log
        return True


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FydEngine")

# Apply filter to uvicorn.error to suppress repetitive "Invalid HTTP request received"
logging.getLogger("uvicorn.error").addFilter(DuplicateLogFilter())

# Store active bots: { bot_id: { "bot": GridTradingBot, "task": asyncio.Task } }
active_instances: dict[int, dict[str, Any]] = {}


# --- Data Models ---
class StrategyConfig(BaseModel):
    upper_price: float
    lower_price: float
    grids: int
    spacing: str | None = "geometric"
    # Optional fallback for compatibility
    investment: float | None = None


class BotRequest(BaseModel):
    bot_id: int
    user_id: int
    exchange: str
    pair: str
    api_key: str
    api_secret: str
    passphrase: str | None = None
    mode: str = "live"
    strategy: StrategyConfig
    # Primary location for investment
    investment: float = 0.0


class BacktestRequest(BaseModel):
    exchange: str
    pair: str
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    capital: float
    upper_price: float = Field(..., alias="upperPrice")
    lower_price: float = Field(..., alias="lowerPrice")
    grid_size: int = Field(..., alias="gridSize")
    timeframe: str = "1h"


# --- Helper: Map Request to Bot Config ---
def create_config(exchange, pair, api_key, api_secret, passphrase, mode, strategy_settings, trading_settings):
    base, quote = pair.split("/")
    return {
        "exchange": {"name": exchange.lower(), "trading_fee": 0.001, "trading_mode": mode},
        "credentials": {"api_key": api_key, "api_secret": api_secret, "password": passphrase},
        "pair": {"base_currency": base, "quote_currency": quote},
        "trading_settings": trading_settings,
        "investment": trading_settings.get("initial_balance", 0.0),
        "grid_strategy": {
            "type": "simple_grid",
            "spacing": strategy_settings.get("spacing", "geometric"),
            "num_grids": strategy_settings["grids"],
            "range": {"top": strategy_settings["upper_price"], "bottom": strategy_settings["lower_price"]},
            "investment": trading_settings.get("initial_balance", 0.0),
        },
        "risk_management": {
            "take_profit": {"enabled": False, "threshold": 0.0},
            "stop_loss": {"enabled": False, "threshold": 0.0},
        },
        "logging": {"log_level": "INFO", "log_to_file": False},
    }


from core.health_monitor import HealthMonitor
from core.logging.db_logger import DBLoggingHandler

# --- Persistent State Manager ---
db = BotDatabase()

# --- Global Services ---
health_monitor = HealthMonitor(db)


# --- Lifecycle Management ---
import json


# --- Lifecycle Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 0. Setup Logging
    root_logger = logging.getLogger()
    db_handler = DBLoggingHandler(db=db)
    db_handler.setLevel(logging.INFO)
    root_logger.addHandler(db_handler)

    # 1. Startup: Recover Bots
    logger.info("?? Server Starting: Checking for Active Bots (Hot Boot)...")
    await health_monitor.start()
    await recover_active_bots()

    yield

    # 2. Shutdown: Graceful Stop
    logger.info("?? Server shutting down. Gracefully pausing active bots...")
    await health_monitor.stop()

    tasks = []
    for bot_id, instance in active_instances.items():
        bot = instance["bot"]
        logger.info(f"Pausing Bot {bot_id} (Orders remain active)...")
        # We DO NOT sell assets or cancel orders on server shutdown,
        # allowing for zero-downtime updates.
        # We just stop the loop and let the DB keep status='RUNNING'
        # so next boot picks it up.
        tasks.append(bot._stop(sell_assets=False, cancel_orders=False))

    if tasks:
        await asyncio.gather(*tasks)
    logger.info("All bots paused.")


async def recover_active_bots():
    """
    Scans DB for bots that were left RUNNING.
    Restarts them in Hot Boot mode (reconcile orders, don't cancel).
    """
    try:
        import sqlite3

        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT bot_id, config_json FROM bots WHERE status = 'RUNNING'")
        rows = cursor.fetchall()

        if rows:
            logger.info(f"🔥 Found {len(rows)} bots to recover.")
            for row in rows:
                bot_id = row[0]
                config_json = row[1]

                if not config_json:
                    logger.warning(
                        f"⚠️ Bot {bot_id} is marked RUNNING but config is missing. Waiting for manual restart."
                    )
                    continue

                try:
                    logger.info(f"♻️ Recovering Bot {bot_id}...")

                    config_dict = json.loads(config_json)
                    validator = ConfigValidator()
                    config_manager = DictConfigManager(config_dict, validator)

                    event_bus = EventBus()
                    notification_handler = NotificationHandler(event_bus, None, config_manager.get_trading_mode())

                    bot = GridTradingBot(
                        config_path="memory",
                        config_manager=config_manager,
                        notification_handler=notification_handler,
                        event_bus=event_bus,
                        no_plot=True,
                        bot_id=bot_id,
                    )

                    # Force Hot Boot
                    bot.strategy.use_hot_boot = True

                    task = asyncio.create_task(bot.run())
                    active_instances[bot_id] = {"bot": bot, "task": task, "event_bus": event_bus}
                    logger.info(f"✅ Bot {bot_id} successfully recovered and restarted!")

                except Exception as e:
                    logger.error(f"Failed to recover Bot {bot_id}: {e}")
                    db.update_bot_status(bot_id, "CRASHED")

        else:
            logger.info("✅ No active bots found.")
        conn.close()
    except Exception as e:
        logger.error(f"Failed to recover bots: {e}")


app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---


@app.get("/")
def health_check():
    return {"status": "online", "active_bots": len(active_instances)}


@app.post("/start")
async def start_bot(req: BotRequest):
    if req.bot_id in active_instances:
        raise HTTPException(status_code=400, detail="Bot already running")

    # --- 1. RESOLVE INVESTMENT AMOUNT ---
    final_investment = req.investment

    # Fallback: If root investment is 0, check inside strategy (backward compatibility)
    if final_investment == 0 and req.strategy.investment is not None:
        final_investment = req.strategy.investment

    logger.info(f"?? Starting Bot {req.bot_id} | Investment: {final_investment} USDT")

    # --- 2. Prepare Config ---
    trading_settings = {
        "initial_balance": final_investment,
        "investment": final_investment,
        "timeframe": "1m",
        "period": {"start_date": "2024-01-01T00:00:00Z", "end_date": "2070-01-01T00:00:00Z"},
        "historical_data_file": None,
    }

    strategy_settings = {
        "grids": req.strategy.grids,
        "upper_price": req.strategy.upper_price,
        "lower_price": req.strategy.lower_price,
        "spacing": req.strategy.spacing,
    }

    mode_str = "paper_trading" if req.mode == "paper" else "live"

    config_dict = create_config(
        req.exchange,
        req.pair,
        req.api_key,
        req.api_secret,
        req.passphrase,
        mode_str,
        strategy_settings,
        trading_settings,
    )

    try:
        # Check if this is a Hot Boot (was previously RUNNING)
        prev_status = db.get_bot_status(req.bot_id)
        is_hot_boot = prev_status == "RUNNING"

        if is_hot_boot:
            logger.info(f"🔥 Bot {req.bot_id} matched in DB as RUNNING. Initiating Hot Boot.")

        # Initialize Components
        validator = ConfigValidator()
        config_manager = DictConfigManager(config_dict, validator)

        event_bus = EventBus()
        notification_handler = NotificationHandler(event_bus, None, config_manager.get_trading_mode())

        bot = GridTradingBot(
            config_path="memory",
            config_manager=config_manager,
            notification_handler=notification_handler,
            event_bus=event_bus,
            no_plot=True,
            bot_id=req.bot_id,
        )

        # Update DB Status & Config immediately to Lock it and ensure persistence
        db.update_bot_status(req.bot_id, "RUNNING", config_json=json.dumps(config_dict))

        # Hack/Fix: Set a temporary attribute on the strategy instance.
        bot.strategy.use_hot_boot = is_hot_boot

        task = asyncio.create_task(bot.run())

        active_instances[req.bot_id] = {"bot": bot, "task": task, "event_bus": event_bus}

        return {"status": "started", "bot_id": req.bot_id, "hot_boot": is_hot_boot}

    except Exception as e:
        logger.error(f"Failed to start bot {req.bot_id}: {e}", exc_info=True)
        db.update_bot_status(req.bot_id, "CRASHED")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stop/{bot_id}")
async def stop_bot(bot_id: int):
    # Idempotency Check: If bot is not active, check if it exists in DB
    if bot_id not in active_instances:
        status = db.get_bot_status(bot_id)
        if status in ["STOPPED", "CRASHED", "DELETED"]:
            return {"status": "already_stopped", "message": f"Bot {bot_id} is already {status}"}

        # If DB says RUNNING but not in memory -> Zombie state
        # We force update to STOPPED to match reality
        db.update_bot_status(bot_id, "STOPPED")
        return {"status": "stopped", "message": "Bot was zombie (DB=RUNNING, Memory=None). Forced STOPPED."}

    instance = active_instances[bot_id]
    bot = instance["bot"]

    # 1. Update Status to STOPPING (Lock)
    db.update_bot_status(bot_id, "STOPPING")

    # 2. Trigger Strict Stop (No Liquidation by default on Stop)
    await bot._stop(sell_assets=False)

    try:
        await asyncio.wait_for(instance["task"], timeout=30.0)
    except TimeoutError:
        logger.warning(f"Bot {bot_id} stop timed out, forcing removal.")
    except Exception as e:
        logger.error(f"Error stopping bot {bot_id}: {e}")

    # 3. Finalize Status
    db.update_bot_status(bot_id, "STOPPED")

    del active_instances[bot_id]
    return {"status": "stopped"}


class DeleteBotRequest(BaseModel):
    api_key: str | None = None
    api_secret: str | None = None
    passphrase: str | None = None
    exchange: str | None = None
    pair: str | None = None
    mode: str = "live"


from config.trading_mode import TradingMode


@app.delete("/bot/{bot_id}")
async def delete_bot(bot_id: int, liquidate: bool = True, creds: DeleteBotRequest | None = None):
    """
    Deletes a bot.
    - If active: Stops it.
      - If liquidate=True (default), sells all assets first.
    - If inactive:
        - Requires 'creds' (DeleteBotRequest) body to liquidate assets.
        - Otherwise just marks DELETED.
    """
    logger.info(f"DELETE /bot/{bot_id}: Liquidate={liquidate}, Creds={creds}")

    logger.info(f"DELETE /bot/{bot_id}: Liquidate={liquidate}, Creds={creds}")

    if bot_id in active_instances:
        # Active Bot: Can Liquidate
        instance = active_instances[bot_id]
        bot = instance["bot"]

        db.update_bot_status(bot_id, "STOPPING")

        logger.info(f"🗑️ Deleting Bot {bot_id} (Active, Liquidate/Nuclear={liquidate})...")

        # If liquidate=True (Default on delete), we assume the user wants EVERYTHING gone for this bot.
        # We trigger the standard stop, but ALSO trigger the nuclear cleanup if requested.
        # However, `bot._stop` calls `cancel_all_open_orders`.
        # To access `force_nuclear_cleanup`, we need to call it explicitly on the order manager.

        await bot._stop(sell_assets=liquidate)

        if liquidate:
            logger.info(f"☢️ Triggering Nuclear Cleanup for Bot {bot_id}...")
            try:
                # Re-run nuclear cleanup just in case `_stop` missed something
                await bot.strategy.order_manager.force_nuclear_cleanup()
            except Exception as e:
                logger.error(f"Nuclear cleanup failed: {e}")

        try:
            await asyncio.wait_for(instance["task"], timeout=10.0)
        except Exception as e:
            logger.error(f"Error stopping/liquidating bot {bot_id}: {e}")

        del active_instances[bot_id]
        db.update_bot_status(bot_id, "DELETED")

        return {"status": "deleted", "liquidation_attempted": liquidate}

    else:
        # Inactive Bot
        status = db.get_bot_status(bot_id)
        if status == "DELETED":
            return {"status": "already_deleted"}

        db.update_bot_status(bot_id, "DELETED")

        liq_msg = "Skipped (No Creds)"

        # --- OFFLINE LIQUIDATION ---
        if liquidate and creds and creds.api_key and creds.exchange and creds.pair:
            try:
                logger.info(f"🔄 Offline Liquidation for Bot {bot_id}...")

                # 1. Create Minimal Exchange Service
                exchange_config = {
                    "exchange": {"name": creds.exchange, "trading_mode": creds.mode},
                    "credentials": {
                        "api_key": creds.api_key,
                        "api_secret": creds.api_secret,
                        "password": creds.passphrase,
                    },
                }

                class MockConfig:
                    def get_exchange_config(self):
                        return exchange_config["exchange"]

                    def get_credentials(self):
                        return exchange_config["credentials"]

                    # --- Attributes required by LiveExchangeService ---
                    def get_exchange_name(self):
                        return exchange_config["exchange"]["name"]

                    def get_api_key(self):
                        return exchange_config["credentials"]["api_key"]

                    def get_api_secret(self):
                        return exchange_config["credentials"]["api_secret"]

                    def get_api_password(self):
                        return exchange_config["credentials"]["password"]

                mode_enum = TradingMode.PAPER_TRADING if creds.mode == "paper" else TradingMode.LIVE
                service = ExchangeServiceFactory.create_exchange_service(MockConfig(), mode_enum)

                # 2. Get Balance
                # service.initialize() is not needed/does not exist (done in __init__)
                base, quote = creds.pair.split("/")
                balances = await service.get_balance()
                crypto_amount = balances.get(base, {}).get("free", 0.0)

                # 3. Sell if Amount > threshold
                if crypto_amount > 0.001:
                    logger.info(f"Selling {crypto_amount} {base} (Offline)...")
                    # place_order(pair, order_type, order_side, amount, price=None)
                    await service.place_order(creds.pair, "market", "sell", crypto_amount)
                    logger.info("✅ Offline Liquidation Successful.")
                    liq_msg = "Success"
                else:
                    logger.info(f"ℹ️ Offline Liquidation: No assets to sell ({crypto_amount} {base}).")
                    liq_msg = "No Assets"

                await service.close_connection()

            except Exception as e:
                logger.error(f"Offline Liquidation Failed: {e}", exc_info=True)
                liq_msg = f"Failed: {e!s}"

        return {"status": "deleted", "message": "Bot marked DELETED.", "offline_liquidation": liq_msg}


@app.get("/health/system")
async def health_system():
    # Return more detailed health info if available
    return {
        "status": "online",
        "active_bots": len(active_instances),
        "db_connected": True,  # Assumption since we are running
    }


@app.get("/logs/{bot_id}")
async def get_bot_logs(bot_id: int, limit: int = 50):
    logs = db.get_logs(bot_id, limit)
    return {"logs": logs}


@app.get("/allocations")
async def get_allocations(mode: str = "live"):
    """
    Returns the total funds currently allocated/reserved by ACTIVE bots.
    Used by the backend to calculate precise Available Balance.
    """
    try:
        # Normalize mode string to match TradingMode enum values
        target_mode = mode
        if mode == "paper":
            target_mode = "paper_trading"

        logger.info(f"🔍 /allocations called with mode='{mode}' -> target='{target_mode}'")
        logger.info(f"   Active Instances: {len(active_instances)}")

        allocations = {}

        for bot_id, instance in active_instances.items():
            bot = instance["bot"]
            b_mode = bot.trading_mode.value

            # Filter by mode
            if b_mode != target_mode:
                # logger.info(f"   Skipping Bot {bot_id} (Mode: {b_mode})")
                continue

            try:
                # Get Bot Balances (Free + Reserved)
                bals = bot.get_balances()
                # { "fiat": 100, "reserved_fiat": 50, "crypto": 0.1, "reserved_crypto": 0.2 }

                # Identify Currencies from pair
                pair = bot.trading_pair  # e.g. "SOL/USDT"
                base, quote = pair.split("/")

                # Fiat/Quote Breakdown
                q_idle = bals.get("fiat", 0.0)
                q_locked = bals.get("reserved_fiat", 0.0)
                q_total = q_idle + q_locked

                # Crypto/Base Breakdown
                b_idle = bals.get("crypto", 0.0)
                b_locked = bals.get("reserved_crypto", 0.0)
                b_total = b_idle + b_locked

                logger.info(
                    f"   Bot {bot_id} ({pair}): Quote total={q_total}(idle={q_idle}), Base total={b_total}(idle={b_idle})"
                )

                # Initialize Structure if needed
                if quote not in allocations:
                    allocations[quote] = {"total": 0.0, "idle": 0.0}
                if base not in allocations:
                    allocations[base] = {"total": 0.0, "idle": 0.0}

                # Aggregate Quote (e.g. USDT)
                allocations[quote]["total"] += q_total
                allocations[quote]["idle"] += q_idle

                # Aggregate Base (e.g. SOL)
                allocations[base]["total"] += b_total
                allocations[base]["idle"] += b_idle

            except Exception as e:
                logger.error(f"Error calculating allocation for bot {bot_id}: {e}")

        logger.info(f"   ✅ Final Allocations: {allocations}")
        return allocations

    except Exception as e:
        logger.error(f"Failed handling /allocations: {e}", exc_info=True)
        return {}


@app.post("/backtest")
async def run_backtest(req: BacktestRequest):
    try:
        trading_settings = {
            "initial_balance": req.capital,
            "investment": req.capital,
            "timeframe": req.timeframe,
            "period": {"start_date": req.start_date, "end_date": req.end_date},
            "historical_data_file": None,
        }

        strategy_settings = {
            "grids": req.grid_size,
            "upper_price": req.upper_price,
            "lower_price": req.lower_price,
            "spacing": "geometric",
        }

        config_dict = create_config(
            req.exchange, req.pair, "dummy_key", "dummy_secret", None, "backtest", strategy_settings, trading_settings
        )

        validator = ConfigValidator()
        config_manager = DictConfigManager(config_dict, validator)
        event_bus = EventBus()
        notification_handler = NotificationHandler(event_bus, None, config_manager.get_trading_mode())

        bot = GridTradingBot(
            config_path="memory",
            config_manager=config_manager,
            notification_handler=notification_handler,
            event_bus=event_bus,
            no_plot=True,
        )

        logger.info(f"Starting backtest for {req.pair}...")
        result = await bot.run()
        return result

    except Exception as e:
        logger.error(f"Backtest failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
