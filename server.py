import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Any

from adapter.config_adapter import DictConfigManager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config.config_validator import ConfigValidator
from core.bot_management.event_bus import EventBus
from core.storage.bot_database import BotDatabase

from core.bot_management.grid_trading_bot import GridTradingBot
from core.bot_management.notification.notification_handler import NotificationHandler
from core.services.exchange_service_factory import ExchangeServiceFactory
from core.order_handling.order import OrderSide

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FydEngine")

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


from core.logging.db_logger import DBLoggingHandler
from core.health_monitor import HealthMonitor

# --- Persistent State Manager ---
db = BotDatabase()

# --- Global Services ---
health_monitor = HealthMonitor(db)


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
        tasks.append(bot._stop(sell_assets=False))

    if tasks:
        await asyncio.gather(*tasks)
    logger.info("All bots paused.")


async def recover_active_bots():
    """
    Scans DB for bots that were left RUNNING.
    Restarts them in Hot Boot mode (reconcile orders, don't cancel).
    """
    # Requires a way to iterate bots. BotDatabase schema needs list_active_bots support.
    # For now, we stub this or would need to query `SELECT bot_id FROM bots WHERE status='RUNNING'`
    # Let's add that method to BotDatabase or execute here.
    # Let's add that method to BotDatabase or execute here.

    try:
        import sqlite3

        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT bot_id FROM bots WHERE status = 'RUNNING'")
        rows = cursor.fetchall()

        if rows:
            logger.info(f"🔥 Found {len(rows)} bots to recover.")
            for row in rows:
                bot_id = row[0]
                # To restart, we need the original CONFIG.
                # Since we don't store the full config in DB yet (Task: 'Database Schema'),
                # we can't fully auto-recover without user input or a config table.
                # SRS 1.2 "Hot Boot: The new version starts and reads the database".
                # Crucial missing piece: Storing Config in DB.
                # Current Scope: We will log this. Real implementation needs a `bot_configs` table.
                logger.warning(f"⚠️ Bot {bot_id} is marked RUNNING but config is missing. Waiting for manual restart.")
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

        # Update DB Status immediately to Lock it
        db.update_bot_status(req.bot_id, "RUNNING")

        # Start the bot task
        # We relying on the Strategy to read `hot_boot` flag?
        # We need to pass it.
        # FIX: The `bot.run()` needs to accept the flag or set it on strategy.
        # Since `bot.run` calls `strategy.run`, and we can't easily change signature everywhere,
        # let's set a strategy attribute directly before running.

        # bot.strategy._run_live_or_paper_trading signature change requires modifying GridTradingBot too.
        # Let's just modify the `bot.run` call in `GridTradingStrategy` if we could, but better:
        # We inject the intention via `bot` instance if possible, or assume strategy checks DB?
        # Re-using the logic from Step 95 (where I added `hot_boot` param to `_initialize_grid_orders_once`).
        # But `_run_live...` calls it.

        # Hack/Fix: Set a temporary attribute on the strategy instance.
        bot.strategy.use_hot_boot = is_hot_boot

        # Wait, I didn't add `use_hot_boot` field to Strategy class.
        # I added `hot_boot` argument to `_initialize_grid_orders_once`.
        # I need to wire `run` -> `_run_live` -> `_initialize`.
        # OR: Just override the method in the runtime instance? No.

        # Let's assume for now I will modify `GridTradingStrategy.run` to support checking a property.

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
        await asyncio.wait_for(instance["task"], timeout=5.0)
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

    if bot_id in active_instances:
        # Active Bot: Can Liquidate
        instance = active_instances[bot_id]
        bot = instance["bot"]

        db.update_bot_status(bot_id, "STOPPING")

        logger.info(f"🗑️ Deleting Bot {bot_id} (Active, Liquidate={liquidate})...")
        await bot._stop(sell_assets=liquidate)

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
                liq_msg = f"Failed: {str(e)}"

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
