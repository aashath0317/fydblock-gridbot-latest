import logging
import sqlite3


class BotDatabase:
    def __init__(self, db_path="bot_data.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init_db()

    def _init_db(self):
        """Initialize the database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 1. Bots Table (Persistence)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                bot_id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'STOPPED',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Grid Orders Table
        # We enforce a UNIQUE constraint on (bot_id, price, status)
        # so we physically cannot duplicate an open order at the same price.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS grid_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER,
                order_id TEXT,
                price REAL,
                side TEXT,
                quantity REAL,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_grid_orders_bot_price_status ON grid_orders (bot_id, price, status)"
        )

        # 2. System Logs Table (New)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER,
                severity TEXT,
                message TEXT,
                fix_action TEXT,
                read_status BOOLEAN DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_bot_timestamp ON system_logs (bot_id, timestamp)")

        # 3. Trade History Table (New)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER,
                order_id TEXT,
                pair TEXT,
                side TEXT,
                price REAL,
                quantity REAL,
                fee_amount REAL,
                fee_currency TEXT,
                realized_pnl REAL,
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_bot_time ON trade_history (bot_id, executed_at)")

        conn.commit()
        conn.close()

    def add_order(self, bot_id: int, order_id: str, price: float, side: str, quantity: float):
        """Saves a new active order to the DB."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO grid_orders (bot_id, order_id, price, side, quantity, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (bot_id, order_id, price, side, quantity, "OPEN"),
            )
            conn.commit()
            self.logger.info(f"💾 DB: Saved {side} order {order_id} at {price}")
        except Exception as e:
            self.logger.error(f"Failed to save order to DB: {e}")
        finally:
            conn.close()

    def update_order_status(self, order_id: str, new_status: str):
        """Updates an order status (e.g. OPEN -> CLOSED)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE grid_orders 
            SET status = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE order_id = ?
        """,
            (new_status, order_id),
        )
        conn.commit()
        conn.close()

    def get_active_order_at_price(self, bot_id: int, price: float, tolerance: float = 0.001):
        """
        Checks if we ALREADY have an open order at this price.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get all open orders for this bot
        cursor.execute('SELECT order_id, price, side FROM grid_orders WHERE bot_id = ? AND status = "OPEN"', (bot_id,))
        rows = cursor.fetchall()
        conn.close()

        # Check with tolerance (handling floating point math)
        for row in rows:
            db_order_id, db_price, db_side = row
            if abs(db_price - price) < tolerance:
                return {"order_id": db_order_id, "price": db_price, "side": db_side}

        return None

    def get_all_active_orders(self, bot_id: int):
        """Returns map of active orders for initialization."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT order_id, price, side, quantity FROM grid_orders WHERE bot_id = ? AND status = "OPEN"', (bot_id,)
        )
        rows = cursor.fetchall()
        conn.close()

        # Return dict keyed by Order ID
        return {row[0]: {"price": row[1], "side": row[2], "amount": row[3]} for row in rows}

    def clear_all_orders(self, bot_id: int):
        """Deletes ALL open orders for a specific bot (Clean Start)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM grid_orders WHERE bot_id = ?", (bot_id,))
            conn.commit()
            self.logger.info(f"🧹 DB: Cleared all orders for Bot {bot_id}")
        except Exception as e:
            self.logger.error(f"Failed to clear DB orders: {e}")
        finally:
            conn.close()

    # --- Bot Status Persistence ---
    def update_bot_status(self, bot_id: int, status: str):
        """Updates the persistent status of a bot."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # Upsert logic (Insert or Replace)
            cursor.execute(
                """
                INSERT INTO bots (bot_id, status, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(bot_id) DO UPDATE SET status=excluded.status, updated_at=CURRENT_TIMESTAMP
            """,
                (bot_id, status),
            )
            conn.commit()
            self.logger.info(f"🔄 DB: Bot {bot_id} status updated to {status}")
        except Exception as e:
            self.logger.error(f"Failed to update bot status: {e}")
        finally:
            conn.close()

    def get_bot_status(self, bot_id: int) -> str:
        """Returns the persistent status of a bot (e.g. RUNNING, STOPPED)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM bots WHERE bot_id = ?", (bot_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else "STOPPED"

    # --- System Logs Methods ---
    def log_event(self, bot_id: int, severity: str, message: str, fix_action: str = None):
        """Inserts a structured log event."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO system_logs (bot_id, severity, message, fix_action)
                VALUES (?, ?, ?, ?)
            """,
                (bot_id, severity, message, fix_action),
            )
            conn.commit()
        except Exception as e:
            # Fallback to standard logging if DB fails
            self.logger.error(f"Failed to write to DB Log: {e}")
        finally:
            conn.close()

    def get_logs(self, bot_id: int, limit: int = 50):
        """Retrieves recent logs for a bot."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM system_logs 
            WHERE bot_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """,
            (bot_id, limit),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows

    # --- Trade History Methods ---
    def add_trade_history(self, trade_data: dict):
        """
        Saves a finalized trade to history.
        Expected keys: bot_id, order_id, pair, side, price, quantity, fee_amount, fee_currency, realized_pnl
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO trade_history (
                    bot_id, order_id, pair, side, price, quantity, 
                    fee_amount, fee_currency, realized_pnl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    trade_data["bot_id"],
                    trade_data["order_id"],
                    trade_data["pair"],
                    trade_data["side"],
                    trade_data["price"],
                    trade_data["quantity"],
                    trade_data.get("fee_amount", 0.0),
                    trade_data.get("fee_currency", "USDT"),
                    trade_data.get("realized_pnl", 0.0),
                ),
            )
            conn.commit()
            self.logger.info(f"📜 DB: Recorded trade {trade_data['order_id']}")
        except Exception as e:
            self.logger.error(f"Failed to save trade history: {e}")
        finally:
            conn.close()

    def get_trade_history(self, bot_id: int, limit: int = 50):
        """Retrieves verified trade history."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM trade_history 
            WHERE bot_id = ? 
            ORDER BY executed_at DESC 
            LIMIT ?
        """,
            (bot_id, limit),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows
