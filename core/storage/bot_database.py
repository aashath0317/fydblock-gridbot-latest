from datetime import datetime
import logging
import os

import asyncpg


class BotDatabase:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pool = None
        self.db_url = os.getenv("DB_CONNECTION_STRING")
        if not self.db_url:
            # Fallback to constructing from parts if full string not provided
            # Support both DB_* (from .env) and POSTGRES_* (standard) naming conventions
            self.user = (os.getenv("DB_USER") or os.getenv("POSTGRES_USER", "postgres")).strip()
            self.password = (os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "password")).strip()
            self.host = (os.getenv("DB_HOST") or os.getenv("POSTGRES_HOST", "localhost")).strip()
            self.port = (os.getenv("DB_PORT") or os.getenv("POSTGRES_PORT", "5432")).strip()
            self.dbname = (os.getenv("DB_NAME") or os.getenv("POSTGRES_DB", "gridbot_db")).strip()

            self.db_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    async def connect(self):
        """Initializes the connection pool and creates tables."""
        try:
            # DEBUG: Log the host and partial URL to debug connection issues
            self.logger.info(f"🔌 Attempting to connect to DB Host: {self.host}, Port: {self.port}, DB: {self.dbname}")

            self.pool = await asyncpg.create_pool(
                user=self.user, password=self.password, host=self.host, port=self.port, database=self.dbname
            )
            self.logger.info("✅ Connected to PostgreSQL Database")
            await self._init_db()
        except Exception as e:
            self.logger.critical(f"❌ Failed to connect to PostgreSQL: {e}")
            raise

    async def close(self):
        """Closes the connection pool."""
        if self.pool:
            await self.pool.close()
            self.logger.info("Database connection closed.")

    async def _init_db(self):
        """Initialize the database tables if they don't exist."""
        try:
            async with self.pool.acquire() as conn:
                # 1. Bots Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bots (
                        bot_id BIGINT PRIMARY KEY,
                        status TEXT DEFAULT 'STOPPED',
                        config_json TEXT,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                # 1b. Auto-Migration: Add missing columns if they don't exist (Backend compatibility)
                # Check for config_json
                try:
                    await conn.execute("ALTER TABLE bots ADD COLUMN IF NOT EXISTS config_json TEXT")
                except Exception as e:
                    self.logger.warning(f"Migration: optional config_json check: {e}")

                # Check for updated_at
                try:
                    await conn.execute(
                        "ALTER TABLE bots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"
                    )
                except Exception as e:
                    self.logger.warning(f"Migration: optional updated_at check: {e}")

                # 2. System Logs Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_logs (
                        id SERIAL PRIMARY KEY,
                        bot_id BIGINT,
                        severity TEXT,
                        message TEXT,
                        fix_action TEXT,
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                # 3. Grid Orders Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS grid_orders (
                        bot_id BIGINT,
                        order_id TEXT PRIMARY KEY,
                        price DOUBLE PRECISION,
                        side TEXT,
                        quantity DOUBLE PRECISION,
                        status TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                # 3b. Grid Levels Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS grid_levels (
                        bot_id BIGINT,
                        price DOUBLE PRECISION,
                        status TEXT,
                        stock_on_hand DOUBLE PRECISION DEFAULT 0.0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (bot_id, price)
                    )
                """)

                # 4. Trade History Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS trade_history (
                        id SERIAL PRIMARY KEY,
                        bot_id BIGINT,
                        order_id TEXT,
                        pair TEXT,
                        side TEXT,
                        price DOUBLE PRECISION,
                        quantity DOUBLE PRECISION,
                        fee_amount DOUBLE PRECISION,
                        fee_currency TEXT,
                        realized_pnl DOUBLE PRECISION,
                        executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                # 5. Bot Balances Table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_balances (
                        bot_id BIGINT PRIMARY KEY,
                        fiat_balance DOUBLE PRECISION,
                        crypto_balance DOUBLE PRECISION,
                        reserve_amount DOUBLE PRECISION DEFAULT 0.0,
                        updated_at TIMESTAMP WITH TIME ZONE
                    )
                """)

                self.logger.info("✅ Database tables verified/initialized")

        except Exception as e:
            self.logger.error(f"Failed to initialize DB schema: {e}")
            raise

    async def add_order(self, bot_id: int, order_id: str, price: float, side: str, quantity: float):
        """Saves a new active order to the DB."""
        try:
            await self.pool.execute(
                """
                INSERT INTO grid_orders (bot_id, order_id, price, side, quantity, status)
                VALUES ($1, $2, $3, $4, $5, 'OPEN')
            """,
                bot_id,
                order_id,
                price,
                side,
                quantity,
            )
            self.logger.info(f"💾 DB: Saved {side} order {order_id} at {price}")
        except Exception as e:
            self.logger.error(f"Failed to save order to DB: {e}")

    async def update_order_status(self, order_id: str, new_status: str):
        """Updates an order status (e.g. OPEN -> CLOSED)."""
        try:
            await self.pool.execute(
                """
                UPDATE grid_orders 
                SET status = $1, updated_at = NOW() 
                WHERE order_id = $2
            """,
                new_status,
                order_id,
            )
        except Exception as e:
            self.logger.error(f"Failed to update order status: {e}")

    async def get_active_order_at_price(self, bot_id: int, price: float, tolerance: float = 0.001):
        """Checks if we ALREADY have an open order at this price."""
        try:
            rows = await self.pool.fetch(
                """
                SELECT order_id, price, side FROM grid_orders 
                WHERE bot_id = $1 AND status = 'OPEN'
            """,
                bot_id,
            )

            for row in rows:
                db_price = row["price"]
                if abs(db_price - price) < tolerance:
                    return {"order_id": row["order_id"], "price": row["price"], "side": row["side"]}
            return None
        except Exception as e:
            self.logger.error(f"Failed to check active order: {e}")
            return None

    async def get_order(self, order_id: str):
        """Retrieves a specific order by its ID."""
        try:
            row = await self.pool.fetchrow("SELECT * FROM grid_orders WHERE order_id = $1", order_id)
            return dict(row) if row else None
        except Exception as e:
            self.logger.error(f"Failed to get order: {e}")
            return None

    async def get_all_active_orders(self, bot_id: int):
        """Returns map of active orders for initialization."""
        try:
            rows = await self.pool.fetch(
                """
                SELECT order_id, price, side, quantity FROM grid_orders 
                WHERE bot_id = $1 AND status = 'OPEN'
            """,
                bot_id,
            )

            # Return dict keyed by Order ID
            return {
                row["order_id"]: {"price": row["price"], "side": row["side"], "amount": row["quantity"]} for row in rows
            }
        except Exception as e:
            self.logger.error(f"Failed to get all active orders: {e}")
            return {}

    async def clear_all_orders(self, bot_id: int):
        """Deletes ALL open orders for a specific bot (Clean Start)."""
        try:
            await self.pool.execute("DELETE FROM grid_orders WHERE bot_id = $1", bot_id)
            self.logger.info(f"🧹 DB: Cleared all orders for Bot {bot_id}")
        except Exception as e:
            self.logger.error(f"Failed to clear DB orders: {e}")

    # --- Bot Status Persistence ---
    async def update_bot_status(self, bot_id: int, status: str, config_json: str | None = None):
        """Updates the persistent status of a bot."""
        try:
            if config_json:
                result = await self.pool.execute(
                    """
                    UPDATE bots 
                    SET status = $1, config_json = $2, updated_at = NOW()
                    WHERE bot_id = $3
                """,
                    status,
                    config_json,
                    bot_id,
                )
            else:
                result = await self.pool.execute(
                    """
                    UPDATE bots 
                    SET status = $1, updated_at = NOW()
                    WHERE bot_id = $2
                """,
                    status,
                    bot_id,
                )

            # Check if any row was actually updated
            # "UPDATE 1" means 1 row updated.
            if result == "UPDATE 0":
                self.logger.warning(
                    f"⚠️ DB: Could not update status for Bot {bot_id} (Not found in DB). Backend might have deleted it."
                )
            else:
                self.logger.info(f"🔄 DB: Bot {bot_id} status updated to {status}")
        except Exception as e:
            self.logger.error(f"Failed to update bot status: {e}")

    async def get_bot_status(self, bot_id: int) -> str:
        """Returns the persistent status of a bot (e.g. RUNNING, STOPPED)."""
        try:
            val = await self.pool.fetchval("SELECT status FROM bots WHERE bot_id = $1", bot_id)
            return val if val else "STOPPED"
        except Exception as e:
            self.logger.error(f"Failed to get bot status: {e}")
            return "STOPPED"

    async def get_bot_config(self, bot_id: int) -> str | None:
        """Returns the persistent config JSON of a bot."""
        try:
            val = await self.pool.fetchval("SELECT config_json FROM bots WHERE bot_id = $1", bot_id)
            return val
        except Exception as e:
            self.logger.error(f"Failed to get bot config: {e}")
            return None

    # --- Bot Balances Persistence ---
    async def update_balances(self, bot_id: int, fiat: float, crypto: float, reserve: float):
        """Upserts the bot's known balances."""
        try:
            await self.pool.execute(
                """
                INSERT INTO bot_balances (bot_id, fiat_balance, crypto_balance, reserve_amount, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT(bot_id) DO UPDATE SET
                    fiat_balance=EXCLUDED.fiat_balance,
                    crypto_balance=EXCLUDED.crypto_balance,
                    reserve_amount=EXCLUDED.reserve_amount,
                    updated_at=NOW()
            """,
                bot_id,
                fiat,
                crypto,
                reserve,
            )
        except Exception as e:
            self.logger.error(f"Failed to update balances: {e}")

    async def get_balances(self, bot_id: int):
        """Returns the last saved balances: (fiat, crypto, reserve) or None."""
        try:
            row = await self.pool.fetchrow("SELECT * FROM bot_balances WHERE bot_id = $1", bot_id)
            return dict(row) if row else None
        except Exception as e:
            self.logger.error(f"Failed to get balances: {e}")
            return None

    # --- System Logs Methods ---
    async def log_event(self, bot_id: int, severity: str, message: str, fix_action: str = None):
        """Inserts a structured log event."""
        try:
            await self.pool.execute(
                """
                INSERT INTO system_logs (bot_id, severity, message, fix_action)
                VALUES ($1, $2, $3, $4)
            """,
                bot_id,
                severity,
                message,
                fix_action,
            )
        except Exception as e:
            print(f"CRITICAL: Failed to write to DB Log: {e}")

    async def get_logs(self, bot_id: int, limit: int = 50):
        """Retrieves recent logs for a bot."""
        try:
            rows = await self.pool.fetch(
                """
                SELECT * FROM system_logs 
                WHERE bot_id = $1 
                ORDER BY timestamp DESC 
                LIMIT $2
            """,
                bot_id,
                limit,
            )

            # Convert timestamp to ISO format string for JSON serialization
            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("timestamp"), datetime):
                    d["timestamp"] = d["timestamp"].isoformat()
                results.append(d)
            return results
        except Exception as e:
            self.logger.error(f"Failed to get logs: {e}")
            return []

    # --- Trade History Methods ---
    async def add_trade_history(self, trade_data: dict):
        """Saves a finalized trade to history."""
        try:
            await self.pool.execute(
                """
                INSERT INTO trade_history (
                    bot_id, order_id, pair, side, price, quantity, 
                    fee_amount, fee_currency, realized_pnl
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                trade_data["bot_id"],
                trade_data["order_id"],
                trade_data["pair"],
                trade_data["side"],
                trade_data["price"],
                trade_data["quantity"],
                trade_data.get("fee_amount", 0.0),
                trade_data.get("fee_currency", "USDT"),
                trade_data.get("realized_pnl", 0.0),
            )
            self.logger.info(f"📜 DB: Recorded trade {trade_data['order_id']}")
        except Exception as e:
            self.logger.error(f"Failed to save trade history: {e}")

    async def get_trade_history(self, bot_id: int, limit: int = 50):
        """Retrieves verified trade history."""
        try:
            rows = await self.pool.fetch(
                """
                SELECT * FROM trade_history 
                WHERE bot_id = $1 
                ORDER BY executed_at DESC 
                LIMIT $2
            """,
                bot_id,
                limit,
            )

            results = []
            for row in rows:
                d = dict(row)
                # Map executed_at to timestamp for frontend compatibility
                # and ensure it is ISO formatted
                if isinstance(d.get("executed_at"), datetime):
                    iso_ts = d["executed_at"].isoformat()
                    d["executed_at"] = iso_ts
                    d["timestamp"] = iso_ts
                elif d.get("executed_at"):
                    # If already string (unlikely with asyncpg default but possible)
                    d["timestamp"] = str(d["executed_at"])

                results.append(d)
            return results
        except Exception as e:
            self.logger.error(f"Failed to get trade history: {e}")
            return []

    async def get_trade_aggregates(self, bot_id: int):
        """
        Returns the sum of all filled buy costs and sell proceeds from trade history.
        Used to reconstruct the 'Realized Balance' flow.
        """
        try:
            row = await self.pool.fetchrow(
                """
                SELECT 
                    SUM(CASE WHEN side = 'buy' THEN (quantity * price) ELSE 0 END) as total_buy_cost,
                    SUM(CASE WHEN side = 'sell' THEN (quantity * price) ELSE 0 END) as total_sell_proceeds
                FROM trade_history
                WHERE bot_id = $1
            """,
                bot_id,
            )
            return {
                "total_buy_cost": row["total_buy_cost"] or 0.0,
                "total_sell_proceeds": row["total_sell_proceeds"] or 0.0,
            }
        except Exception as e:
            self.logger.error(f"Failed to get trade aggregates: {e}")
            return {"total_buy_cost": 0.0, "total_sell_proceeds": 0.0}

    async def get_active_bots(self):
        """Retrieve all bots with status 'RUNNING'."""
        try:
            rows = await self.pool.fetch("""
                SELECT bot_id, status, config_json, updated_at 
                FROM bots WHERE status = 'RUNNING'
            """)
            return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Failed to get active bots: {e}")
            return []

    # --- Grid Level Persistence (Infinite Grid) ---
    async def add_grid_level(self, bot_id: int, price: float, status: str, stock_on_hand: float = 0.0):
        """Inserts a new grid level into the database."""
        try:
            await self.pool.execute(
                """
                INSERT INTO grid_levels (bot_id, price, status, stock_on_hand, created_at, updated_at)
                VALUES ($1, $2, $3, $4, NOW(), NOW())
                ON CONFLICT(bot_id, price) DO UPDATE SET
                    status=EXCLUDED.status,
                    stock_on_hand=EXCLUDED.stock_on_hand,
                    updated_at=NOW()
            """,
                bot_id,
                price,
                status,
                stock_on_hand,
            )
        except Exception as e:
            self.logger.error(f"Failed to add grid level {price}: {e}")

    async def update_grid_level_status(self, bot_id: int, price: float, new_status: str):
        """Updates the status of an existing grid level."""
        try:
            await self.pool.execute(
                """
                UPDATE grid_levels
                SET status = $1, updated_at = NOW()
                WHERE bot_id = $2 AND price = $3
            """,
                new_status,
                bot_id,
                price,
            )
        except Exception as e:
            self.logger.error(f"Failed to update grid level {price}: {e}")

    async def update_grid_stock(self, bot_id: int, price: float, stock_on_hand: float):
        """Updates the stock_on_hand for a grid level."""
        try:
            await self.pool.execute(
                """
                UPDATE grid_levels
                SET stock_on_hand = $1, updated_at = NOW()
                WHERE bot_id = $2 AND price = $3
            """,
                stock_on_hand,
                bot_id,
                price,
            )
            self.logger.info(f"💾 DB: Updated stock for {price} to {stock_on_hand:.6f}")
        except Exception as e:
            self.logger.error(f"Failed to update stock for {price}: {e}")

    async def delete_grid_level(self, bot_id: int, price: float):
        """Removes a grid level from the database."""
        try:
            await self.pool.execute("DELETE FROM grid_levels WHERE bot_id = $1 AND price = $2", bot_id, price)
            self.logger.info(f"💾 DB: Removed grid level {price}")
        except Exception as e:
            self.logger.error(f"Failed to delete grid level {price}: {e}")

    async def get_grid_levels(self, bot_id: int):
        """Retrieves all grid levels for this bot."""
        try:
            rows = await self.pool.fetch(
                "SELECT price, status, stock_on_hand FROM grid_levels WHERE bot_id = $1", bot_id
            )
            return {
                row["price"]: {
                    "status": row["status"],
                    "stock": row["stock_on_hand"] if row["stock_on_hand"] is not None else 0.0,
                }
                for row in rows
            }
        except Exception as e:
            self.logger.error(f"Failed to get grid levels: {e}")
            return {}
