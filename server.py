import asyncio
from contextlib import asynccontextmanager
from datetime import datetime  # Added import
import json
import logging
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from adapter.config_adapter import DictConfigManager
import ccxt.async_support as ccxt  # Use Async CCXT for public data
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config.config_validator import ConfigValidator
from core.bot_management.event_bus import EventBus
from core.bot_management.grid_trading_bot import GridTradingBot
from core.bot_management.notification.notification_handler import NotificationHandler
from core.services.exchange_service_factory import ExchangeServiceFactory
from core.storage.bot_database import BotDatabase
from utils.logging_config import setup_logging


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


# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# Use centralized logging setup to enable file logging (visible to Admin Panel)
setup_logging(logging.INFO, log_to_file=True, config_name="server")
logger = logging.getLogger("FydEngine")

# Apply filter to uvicorn.error to suppress repetitive "Invalid HTTP request received"
logging.getLogger("uvicorn.error").addFilter(DuplicateLogFilter())


class EndpointAccessFilter(logging.Filter):
    """
    Filter to suppress successful access logs for specific endpoints (e.g., /stats polling).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # Access logs usually have args like (client_addr, method, path, http_version, status_code)
            # But uvicorn.access format might vary. The message string often contains the info.
            # Typical msg: '%s - "%s %s HTTP/%s" %d'
            # We check the message string for the target endpoint.
            log_msg = record.getMessage()
            if "GET /bot/" in log_msg and "/stats" in log_msg and "200" in log_msg:
                return False
            return True
        except Exception:
            return True


# Apply filter to uvicorn.access to suppress spammy stats polling
logging.getLogger("uvicorn.access").addFilter(EndpointAccessFilter())

# Store active bots: { bot_id: { "bot": GridTradingBot, "task": asyncio.Task } }
active_instances: dict[int, dict[str, Any]] = {}


# --- Data Models ---
class StrategyConfig(BaseModel):
    upper_price: float
    lower_price: float
    grids: int
    spacing: str | None = "geometric"
    # Infinity Grid New Fields
    order_size_type: str = "quote"  # "quote" (Fixed USDT) or "base" (Fixed Coin)
    amount_per_grid: float = 0.0  # Optional: overrides default calculation if non-zero
    grid_gap: float = 0.0  # Optional: explicitly set gap size (%, e.g 1.0, or amount, e.g 20)
    trailing_up: bool = False  # Enable Infinity Grid Trailing Up
    trailing_down: bool = False  # Enable Infinity Grid Trailing Down
    # Initial Budget Allocation (from Frontend Projection)
    initial_base_balance_allocation: float = 0.0
    initial_quote_balance_allocation: float = 0.0
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
            # Infinity Grid
            "order_size_type": strategy_settings.get("order_size_type", "quote"),
            "amount_per_grid": strategy_settings.get("amount_per_grid", 0.0),
            "grid_gap": strategy_settings.get("grid_gap", 0.0),
            "trailing_up": strategy_settings.get("trailing_up", False),
        },
        "risk_management": {
            "take_profit": {"enabled": False, "threshold": 0.0},
            "stop_loss": {"enabled": False, "threshold": 0.0},
        },
        "logging": {"log_level": "INFO", "log_to_file": True},
    }


# --- Global Services ---
db = BotDatabase()


# --- Solvency Monitor ---
async def solvency_check_loop():
    """
    Background Task:
    Monitors global exchange balances vs total bot allocations.
    If a deficit is detected (User Withdraw request), it forces a proportional
    reduction (haircut) on all active bots to prevent 'Insufficient Funds' errors.
    """
    while True:
        try:
            await asyncio.sleep(60)  # Check every minute

            if not active_instances:
                continue

            # Group bots by Exchange Account
            # (Assuming single account for now per API limitations, but good to be extensible)
            # We use the FIRST active bot to fetch the Global Balance.
            first_bot_id = next(iter(active_instances))
            bot_instance = active_instances[first_bot_id]["bot"]

            # 1. Get Global Available Balance (Real World)
            # We need a fresh check.
            try:
                # Correction: Access exchange_service directly from the strategy
                ex_balances = await bot_instance.strategy.exchange_service.get_balance()
                # Assuming all bots trade USDT for now.
                # TODO: Handle multi-collateral.
                quote = "USDT"
                global_free = float(ex_balances.get("free", {}).get(quote, 0.0))
            except Exception as e:
                logger.warning(f"Solvency Check failed to fetch exchange balance: {e}")
                continue

            # 2. Sum Total Claims separately for LIVE and PAPER
            live_demand = 0.0
            live_reserve = 0.0
            live_bots = []

            paper_demand = 0.0
            # Paper bots are effectively self-funded, but we track them for consistency

            for b_id, item in active_instances.items():
                b = item["bot"]
                # FIX: Access quote_currency from BalanceTracker
                try:
                    b_quote = b.strategy.balance_tracker.quote_currency
                except AttributeError:
                    continue

                # Check mode
                is_paper = getattr(b, "mode", "live") == "paper_trading"

                if b_quote == quote:
                    b_bal = b.strategy.balance_tracker.balance
                    b_res = b.strategy.balance_tracker.operational_reserve

                    if is_paper:
                        paper_demand += b_bal + b_res
                    else:
                        live_demand += b_bal
                        live_reserve += b_res
                        live_bots.append(b)

            # 3. Perform Solvency Check for LIVE BOTS Only
            # (Paper bots run on virtual ledgers, so they don't share a real constraint)
            total_live_demand = live_demand + live_reserve

            if total_live_demand > (global_free + 1.0):
                shortfall = total_live_demand - global_free
                ratio = global_free / total_live_demand if total_live_demand > 0 else 0

                logger.warning(
                    f"🚨 SOLVENCY CRISIS (LIVE): Global Free {global_free:.2f} < Demand {total_live_demand:.2f}. "
                    f"Shortfall: {shortfall:.2f}. Applying Haircut Ratio: {ratio:.4f}"
                )

                for b in live_bots:
                    b.strategy.balance_tracker.adjust_capital_allocation(ratio)

            # else:
            #     logger.info(f"✅ Solvency Check Passed: Free {global_free:.2f} >= Demand {total_demand:.2f}")

        except Exception as e:
            logger.error(f"Error in Solvency Monitor: {e}")


# --- Lifecycle Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Server starting up...")

    # Connect to DB
    await db.connect()

    # Start Solvency Monitor
    asyncio.create_task(solvency_check_loop())

    # Resume active bots
    await recover_active_bots()

    yield

    # Shutdown
    logger.info("Server shutting down...")
    for bot_id, instance in active_instances.items():
        logger.info(f"Stopping bot {bot_id}...")
        try:
            bot = instance["bot"]
            # Graceful shutdown: Stop tasks but keep orders on exchange for Hot Boot
            await bot.stop(cancel_orders=False)
        except Exception as e:
            logger.error(f"Error stopping bot {bot_id}: {e}")

    await db.close()
    logger.info("All bots paused.")


async def recover_active_bots():
    """
    Scans DB for bots that were left RUNNING.
    Restarts them in Hot Boot mode (reconcile orders, don't cancel).
    """
    try:
        rows = await db.get_active_bots()

        if rows:
            logger.info(f"🔥 Found {len(rows)} bots to recover.")
            for row in rows:
                bot_id = row["bot_id"]
                config_json = row["config_json"]

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
                        db=db,
                    )

                    # Force Hot Boot
                    bot.strategy.use_hot_boot = True

                    task = asyncio.create_task(bot.run())
                    active_instances[bot_id] = {"bot": bot, "task": task, "event_bus": event_bus}
                    logger.info(f"✅ Bot {bot_id} successfully recovered and restarted!")

                except Exception as e:
                    logger.error(f"Failed to recover Bot {bot_id}: {e}")
                    await db.update_bot_status(bot_id, "CRASHED")

        else:
            logger.info("✅ No active bots found.")
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
        "order_size_type": req.strategy.order_size_type,
        "amount_per_grid": req.strategy.amount_per_grid,
        "grid_gap": req.strategy.grid_gap,
        "trailing_up": req.strategy.trailing_up,
        "trailing_down": req.strategy.trailing_down,
        "initial_base_balance_allocation": req.strategy.initial_base_balance_allocation,
        "initial_quote_balance_allocation": req.strategy.initial_quote_balance_allocation,
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
        # We also recover interrupted STARTING attempts
        prev_status = await db.get_bot_status(req.bot_id)
        is_hot_boot = prev_status in ["RUNNING", "STARTING"]

        if is_hot_boot:
            logger.info(f"🔥 Bot {req.bot_id} matched in DB as {prev_status}. Initiating Hot Boot.")

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
            db=db,
        )

        # Update DB Status to STARTING (Loading State)
        # We start as STARTING. The Bot itself will flip to RUNNING once orders are placed.
        # However, we preserve the config locking.
        await db.update_bot_status(req.bot_id, "STARTING", config_json=json.dumps(config_dict))

        # Hack/Fix: Set a temporary attribute on the strategy instance.
        bot.strategy.use_hot_boot = is_hot_boot

        task = asyncio.create_task(bot.run())

        active_instances[req.bot_id] = {"bot": bot, "task": task, "event_bus": event_bus}

        return {"status": "starting", "bot_id": req.bot_id, "hot_boot": is_hot_boot}

    except Exception as e:
        logger.error(f"Failed to start bot {req.bot_id}: {e}", exc_info=True)
        await db.update_bot_status(req.bot_id, "CRASHED")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stop/{bot_id}")
async def stop_bot(bot_id: int):
    # Idempotency Check: If bot is not active, check if it exists in DB
    if bot_id not in active_instances:
        status = await db.get_bot_status(bot_id)
        if status in ["STOPPED", "CRASHED", "DELETED"]:
            return {"status": "already_stopped", "message": f"Bot {bot_id} is already {status}"}

        # If DB says RUNNING/STARTING but not in memory -> Zombie state
        # We force update to STOPPED to match reality
        await db.update_bot_status(bot_id, "STOPPED")
        return {"status": "stopped", "message": f"Bot was zombie (DB={status}, Memory=None). Forced STOPPED."}

    instance = active_instances[bot_id]
    bot = instance["bot"]

    # 1. Update Status to STOPPING (Lock)
    await db.update_bot_status(bot_id, "STOPPING")

    # 2. Trigger Strict Stop (No Liquidation by default on Stop)
    await bot._stop(sell_assets=False)

    try:
        await asyncio.wait_for(instance["task"], timeout=30.0)
    except TimeoutError:
        logger.warning(f"Bot {bot_id} stop timed out, forcing removal.")
    except Exception as e:
        logger.error(f"Error stopping bot {bot_id}: {e}")

    # 3. Finalize Status
    await db.update_bot_status(bot_id, "STOPPED")

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

        await db.update_bot_status(bot_id, "STOPPING")

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
        await db.update_bot_status(bot_id, "DELETED")

        # Cleanup Grid Orders
        try:
            await db.clear_all_orders(bot_id)
            logger.info(f"✅ Cleaned up grid_orders for deleted Bot {bot_id}")
        except Exception as e:
            logger.error(f"Failed to cleanup orders for bot {bot_id}: {e}")

        return {"status": "deleted", "liquidation_attempted": liquidate}

    else:
        # Inactive Bot
        status = await db.get_bot_status(bot_id)
        if status == "DELETED":
            return {"status": "already_deleted"}

        await db.update_bot_status(bot_id, "DELETED")

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

        # Cleanup Grid Orders
        try:
            db.clear_all_orders(bot_id)
            logger.info(f"✅ Cleaned up grid_orders for deleted Bot {bot_id}")
        except Exception as e:
            logger.error(f"Failed to cleanup orders for bot {bot_id}: {e}")

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
    logs = await db.get_logs(bot_id, limit)
    return {"logs": logs}


@app.get("/bot/{bot_id}/stats")
async def get_bot_stats(bot_id: int):
    """
    Returns aggregated stats for a specific bot:
    - Holdings (Free + Locked)
    - Sparkline (Profit History)
    Used by the Node.js backend to display dashboards.
    """
    try:
        # 1. Initialize Response Structure
        stats = {
            "holdings": {
                "base": 0.0,
                "quote": 0.0,
                "free_base": 0.0,
                "free_quote": 0.0,
                "locked_base": 0.0,
                "locked_quote": 0.0,
                "reserve": 0.0,
            },
            "sparkline": [],
        }

        # 2. Get Investment amount from bot config
        config_json = await db.get_bot_config(bot_id)
        investment = 0.0
        if config_json:
            try:
                config = json.loads(config_json)
                investment = float(config.get("investment", 0))
            except:
                pass

        # 2b. Get Reserve from bot_balances
        bal_row = await db.get_balances(bot_id)
        reserve = 0.0
        if bal_row:
            stats["holdings"]["free_base"] = float(bal_row.get("crypto_balance") or 0)
            reserve = float(bal_row.get("reserve_amount") or 0)
            stats["holdings"]["reserve"] = reserve
            # Also set free_quote from DB as fallback/base
            stats["holdings"]["free_quote"] = float(bal_row.get("fiat_balance") or 0)

        # 3. Get Order Stats (FILLED and OPEN) for calculating correct FREE balance
        # Using direct pool query for aggregation
        try:
            # Formula: FREE = Investment - Reserve - BuyFilled + SellFilled - BuyOpen
            rows = await db.pool.fetch(
                """
                SELECT side, status, SUM(quantity * price) as total
                FROM grid_orders 
                WHERE bot_id = $1
                GROUP BY side, status
            """,
                bot_id,
            )

            buy_filled = 0.0
            sell_filled = 0.0
            buy_open_total = 0.0

            for row in rows:
                side = row["side"]
                status = row["status"]
                total = float(row["total"]) if row["total"] else 0.0

                if side.lower() == "buy" and status.upper() == "FILLED":
                    buy_filled = total
                elif side.lower() == "sell" and status.upper() == "FILLED":
                    sell_filled = total
                elif side.lower() == "buy" and status.upper() == "OPEN":
                    buy_open_total = total

            # Calculate correct FREE balance from order history
            # Note: This logic assumes 'investment' is the total started with.
            if investment > 0:
                correct_free = investment - reserve - buy_filled + sell_filled - buy_open_total
                stats["holdings"]["free_quote"] = max(0.0, correct_free)
        except Exception as e:
            logger.error(f"Stats Aggregation Failed: {e}")

        # 3b. Get Locked Funds (grid_orders) & Order Counts
        orders = await db.get_all_active_orders(bot_id)  # Returns dict: order_id -> {price, side, amount}

        buy_orders = 0
        sell_orders = 0
        grid_lines = []

        # We need raw list for stats, get_all_active_orders returns dict.
        # Let's use get_all_active_orders or just query again. Querying again matches original logic better for lists.
        # Actually, get_all_active_orders returns what we need.

        for order_id, info in orders.items():
            amt = float(info["amount"] or 0)
            prc = float(info["price"] or 0)
            side = info["side"]

            # Add to lines for chart
            grid_lines.append({"side": side, "price": prc, "qty": amt})

            if side == "sell":
                # Locked Base (Crypto)
                stats["holdings"]["locked_base"] += amt
                sell_orders += 1
            elif side == "buy":
                # Locked Quote (Fiat)
                stats["holdings"]["locked_quote"] += amt * prc
                buy_orders += 1

        # Add order counts and lines to stats
        stats["open_orders"] = {
            "buy": buy_orders,
            "sell": sell_orders,
            "total": buy_orders + sell_orders,
            "lines": grid_lines,
        }

        # 3.5 Get Execution History (Trades)
        trades = await db.get_trade_history(bot_id, limit=50)
        stats["recent_trades"] = []
        for t in trades:
            t_side = t["side"]
            t_qty = t["quantity"]
            t_price = t["price"]
            t_ts = t.get("timestamp") or t.get("executed_at")
            if isinstance(t_ts, datetime):
                t_ts = t_ts.isoformat()
            elif not t_ts:
                # Fallback to avoid Invalid Date
                t_ts = datetime.utcnow().isoformat() + "Z"

            t_fee = t.get("fee_amount")
            t_fee_curr = t.get("fee_currency")
            t_pnl = t.get("realized_pnl")
            t_pair = t.get("pair")

            # Fix UNKNOWN currency (Restored Logic)
            final_fee_curr = t_fee_curr
            if not final_fee_curr or final_fee_curr == "UNKNOWN":
                if t_pair and "/" in t_pair:
                    base, quote = t_pair.split("/")
                    final_fee_curr = base if t_side == "buy" else quote
                else:
                    final_fee_curr = "USDT"

            stats["recent_trades"].append(
                {
                    "side": t_side,
                    "qty": float(t_qty or 0),
                    "price": float(t_price or 0),
                    "timestamp": t_ts,
                    "fee": float(t_fee or 0),
                    "fee_currency": final_fee_curr,
                    "profit": float(t_pnl or 0),
                }
            )

        # 4. Calculate Totals
        stats["holdings"]["base"] = stats["holdings"]["free_base"] + stats["holdings"]["locked_base"]
        stats["holdings"]["quote"] = stats["holdings"]["free_quote"] + stats["holdings"]["locked_quote"]

        return stats

    except Exception as e:
        logger.error(f"Error fetching stats for bot {bot_id}: {e}", exc_info=True)
        # Return empty structure on error to prevent frontend crash
        return {
            "holdings": {
                "base": 0,
                "quote": 0,
                "free_base": 0,
                "free_quote": 0,
                "locked_base": 0,
                "locked_quote": 0,
            },
            "sparkline": [],
        }


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
            # Backtest doesn't use the persistence DB for balances in the same way,
            # but we can pass it if we want logs. For now, let's pass None or db.
            # Passing None to avoid polluting the prod DB with backtest data.
            db=None,
        )

        logger.info(f"Starting backtest for {req.pair}...")
        result = await bot.run()
        return result

    except Exception as e:
        logger.error(f"Backtest failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Global Exchange Cache
exchange_cache = {}


@app.get("/market/candles")
async def get_market_candles(
    symbol: str, exchange: str, timeframe: str = "1h", limit: int = 300, since: int | None = None
):
    """
    Fetches OHLCV candles via CCXT (Async).
    Used by the Frontend (via Node Proxy) to render charts.
    """
    try:
        clean_exchange = exchange.replace("_paper", "").lower()
        if not getattr(ccxt, clean_exchange, None):
            raise HTTPException(status_code=400, detail="Exchange not supported")

        # Reuse or Initialize Async Exchange
        if clean_exchange not in exchange_cache:
            exchange_class = getattr(ccxt, clean_exchange)
            exchange_cache[clean_exchange] = exchange_class({"enableRateLimit": True})

        exchange_instance = exchange_cache[clean_exchange]

        try:
            # Fetch Candles
            if exchange_instance.has["fetchOHLCV"]:
                # Pass 'since' if available
                candles = await exchange_instance.fetch_ohlcv(symbol, timeframe, limit=limit, since=since)

                # Format: [timestamp, open, high, low, close, volume]
                # Frontend expects: { time: usage, open, high, low, close }
                formatted = []
                for c in candles:
                    # c[0] is ms timestamp. Lightweight Charts prefers seconds.
                    formatted.append({"time": int(c[0] / 1000), "open": c[1], "high": c[2], "low": c[3], "close": c[4]})
                return formatted
            else:
                raise HTTPException(status_code=400, detail="Exchange does not support candles")
        except Exception as e:
            # If explicit error (like connection closed), maybe clear cache?
            # logger.warning(f"Exchange error, clearing cache for {clean_exchange}")
            # del exchange_cache[clean_exchange]
            raise e

    except Exception as e:
        logger.error(f"Candle fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import os

    import uvicorn

    # Get port from env or default to 8000
    # Note: Backend might expect a specific port.
    # checking Fydblock usage suggests 8000 is common.
    port = int(os.getenv("PORT", 8000))

    uvicorn.run(app, host="0.0.0.0", port=port, loop="asyncio")
