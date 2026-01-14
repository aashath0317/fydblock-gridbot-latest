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
        # Added config_json column for restart recovery
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                bot_id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'STOPPED',
                config_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. System Logs Table (Logging)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER,
                severity TEXT,
                message TEXT,
                fix_action TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Schema Migration: Add config_json to existing table if missing
        cursor.execute("PRAGMA table_info(bots)")
        bot_columns = [info[1] for info in cursor.fetchall()]
        if "config_json" not in bot_columns:
            try:
                cursor.execute("ALTER TABLE bots ADD COLUMN config_json TEXT")
                self.logger.info("🔄 DB: Migrated bots table - Added 'config_json'")
            except Exception as e:
                self.logger.error(f"Failed to add config_json to bots: {e}")
                self.logger.error(f"Failed to add config_json to bots: {e}")

        # 3. Grid Orders Table (Active Orders)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS grid_orders (
                bot_id INTEGER,
                order_id TEXT PRIMARY KEY,
                price REAL,
                side TEXT,
                quantity REAL,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 5. Trade History Table (New)
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

        # 4. Bot Balances Table (New)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_balances (
                bot_id INTEGER PRIMARY KEY,
                fiat_balance REAL,
                crypto_balance REAL,
                reserve_amount REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Robust Migration for bot_balances
        cursor.execute("PRAGMA table_info(bot_balances)")
        balance_columns = [info[1] for info in cursor.fetchall()]

        if "reserve_amount" not in balance_columns:
            try:
                cursor.execute("ALTER TABLE bot_balances ADD COLUMN reserve_amount REAL DEFAULT 0.0")
                self.logger.info("🔄 DB: Migrated bot_balances table - Added 'reserve_amount'")
            except Exception as e:
                self.logger.error(f"Failed to add reserve_amount column: {e}")

        if "updated_at" not in balance_columns:
            try:
                # SQLite limitation: ALTER TABLE ADD COLUMN cannot have non-constant default like CURRENT_TIMESTAMP
                # We add it as NULLable (default NULL) since we explicitly set it on INSERT/UPDATE anyway.
                cursor.execute("ALTER TABLE bot_balances ADD COLUMN updated_at TIMESTAMP")
                self.logger.info("🔄 DB: Migrated bot_balances table - Added 'updated_at'")
            except Exception as e:
                self.logger.error(f"Failed to add updated_at column: {e}")

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
    def update_bot_status(self, bot_id: int, status: str, config_json: str | None = None):
        """
        Updates the persistent status of a bot.
        Optionally updates config_json (used on Start).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            if config_json:
                cursor.execute(
                    """
                    INSERT INTO bots (bot_id, status, config_json, updated_at) 
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(bot_id) DO UPDATE SET 
                        status=excluded.status, 
                        config_json=excluded.config_json,
                        updated_at=CURRENT_TIMESTAMP
                """,
                    (bot_id, status, config_json),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO bots (bot_id, status, updated_at) 
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(bot_id) DO UPDATE SET 
                        status=excluded.status, 
                        updated_at=CURRENT_TIMESTAMP
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

    def get_bot_config(self, bot_id: int) -> str | None:
        """Returns the persistent config JSON of a bot."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT config_json FROM bots WHERE bot_id = ?", (bot_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    # --- Bot Balances Persistence (New) ---
    def _repair_schema(self, missing_column: str = None):
        """Attempts to repair schema if columns are missing or PK is invalid."""
        self.logger.warning(f"🔧 DB: Attempting schema repair. Detected issue: {missing_column}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # 1. Check if bot_id is Primary Key
            cursor.execute("PRAGMA table_info(bot_balances)")
            columns = cursor.fetchall()
            # col format: (cid, name, type, notnull, dflt_value, pk)
            bot_id_pk = False
            for col in columns:
                if col[1] == "bot_id" and col[5] >= 1:  # pk > 0 means it is a PK
                    bot_id_pk = True
                    break

            if not bot_id_pk:
                self.logger.warning("️⚠️ DB: 'bot_balances' table missing Primary Key on bot_id. Recreating table...")
                cursor.execute("DROP TABLE IF EXISTS bot_balances")
                cursor.execute("""
                    CREATE TABLE bot_balances (
                        bot_id INTEGER PRIMARY KEY,
                        fiat_balance REAL,
                        crypto_balance REAL,
                        reserve_amount REAL DEFAULT 0.0,
                        updated_at TIMESTAMP
                    )
                """)
                self.logger.info("✅ DB: Recreated 'bot_balances' with correct schema.")
                conn.commit()
                return  # Table recreated, no need to add columns

            # 2. Check for missing columns (if table wasn't dropped)
            existing_col_names = [col[1] for col in columns]

            # Force add columns if they are suspected missing
            columns_to_check = [("reserve_amount", "REAL DEFAULT 0.0"), ("updated_at", "TIMESTAMP")]

            for col, type_def in columns_to_check:
                if col not in existing_col_names:
                    try:
                        cursor.execute(f"ALTER TABLE bot_balances ADD COLUMN {col} {type_def}")
                        self.logger.info(f"🔧 DB: Repaired schema - Added '{col}'")
                    except sqlite3.OperationalError:
                        pass
            conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to repair schema: {e}")
        finally:
            conn.close()

    def update_balances(self, bot_id: int, fiat: float, crypto: float, reserve: float):
        """Upserts the bot's known balances."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO bot_balances (bot_id, fiat_balance, crypto_balance, reserve_amount, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(bot_id) DO UPDATE SET
                    fiat_balance=excluded.fiat_balance,
                    crypto_balance=excluded.crypto_balance,
                    reserve_amount=excluded.reserve_amount,
                    updated_at=CURRENT_TIMESTAMP
            """,
                (bot_id, fiat, crypto, reserve),
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "no column" in err_msg or "on conflict" in err_msg or "constraint" in err_msg:
                self.logger.warning(f"⚠️ DB Schema Mismatch/Corruption detected: {e}. Triggering repair/recreation...")
                conn.close()  # Close current connection before repair
                self._repair_schema()
                # Use recursive call to retry once.
                return self.update_balances(bot_id, fiat, crypto, reserve)
            else:
                self.logger.error(f"Failed to update balances (OperationalError): {e}")
        except Exception as e:
            self.logger.error(f"Failed to update balances: {e}")
        finally:
            # Only close if not already closed/retried
            try:
                conn.close()
            except:
                pass

    def get_balances(self, bot_id: int):
        """Returns the last saved balances: (fiat, crypto, reserve) or None."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bot_balances WHERE bot_id = ?", (bot_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return dict(row)
        return None

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
            # Avoid using self.logger.error here as it might trigger recursion if this IS the db logger
            print(f"CRITICAL: Failed to write to DB Log: {e}")
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
