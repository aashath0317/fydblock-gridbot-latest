# Fydblock Spot Grid Trading Bot 🚀

A high-performance, asynchronous cryptocurrency trading bot specializing in **Spot Grid Trading**. Built with Python, FastAPI, and CCXT, it is designed for resilience, security, and automated grid management.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.95%2B-green)
![License](https://img.shields.io/badge/License-MIT-purple)

## 🌟 Key Features

* **⚡ Asynchronous Core**: Powered by `asyncio` and `FastAPI` for non-blocking I/O, allowing the bot to handle real-time WebSocket feeds for prices and order updates simultaneously.
* **🛡️ Resilient Order Management**:
    * **State Reconciliation**: Automatically detects and handles "vanished" orders (orders filled or canceled on the exchange but missed by the WebSocket) via `OrderManager.sync_orders`.
    * **Strict Isolation**: Prevents "phantom orders" by ensuring database state matches exchange state before placing new trades.
* **🔒 Enterprise-Grade Security**:
    * **AES Encryption**: API keys and secrets are encrypted at rest using `Fernet` (symmetric encryption) before being stored in the database.
    * **Environment Isolation**: Critical secrets (Encryption Keys, DB URLs) are managed via `.env` files.
* **🤖 Smart Auto-Tuner**:
    * **Reset Up**: Automatically resets the grid upwards if the price breaks the upper limit.
    * **Expand Down**: Intelligently lowers the bottom limit during market dips (with cooldowns) to catch lower prices without over-committing.
* **📊 Built-in Backtesting**: Includes a simulation engine to verify strategy logic against historical CSV data or dummy sine-wave data.

---

## 🛠️ Technology Stack

* **Framework**: FastAPI, Uvicorn
* **Database**: SQLite (Async/WAL mode) with SQLAlchemy
* **Exchange Integration**: CCXT Pro (WebSockets + REST)
* **Data Validation**: Pydantic

---

## 🚀 Getting Started

### Prerequisites

* Python 3.10 or higher
* Git

### Installation

1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/yourusername/fydblock-grid-bot.git](https://github.com/yourusername/fydblock-grid-bot.git)
    cd fydblock-grid-bot
    ```

2.  **Set up Virtual Environment**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration**
    Create a `.env` file in the root directory (use `config.py` as reference):
    ```env
    DB_URL=sqlite+aiosqlite:///grid_bot.db
    ENCRYPTION_KEY=YOUR_GENERATED_FERNET_KEY
    ```
    *Note: You can generate a key using `from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())`.*

---

## 🏃 Running the Bot

Start the API server:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000

```

* **API Documentation**: `http://localhost:8000/docs`
* **Health Check**: `http://localhost:8000/health_stats`

---

## 🕹️ Usage Guide

### 1. Start a New Grid Bot

Send a `POST` request to `/start_bot` with your configuration.

```bash
curl -X POST "http://localhost:8000/start_bot" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user_01",
           "pair": "BTC/USDT",
           "amount": 1000,
           "lower_limit": 50000,
           "upper_limit": 60000,
           "grid_count": 10,
           "mode": "AUTO",
           "api_key": "YOUR_BINANCE_API_KEY",
           "secret_key": "YOUR_BINANCE_SECRET_KEY"
         }'

```

### 2. Monitor Health

Check if the system is running smoothly and view API latency stats.

```bash
curl "http://localhost:8000/health_stats"

```

### 3. Stop a Bot

Gracefully stop a bot and cancel its active tasks.

```bash
curl -X POST "http://localhost:8000/stop_bot" \
     -H "Content-Type: application/json" \
     -d '{"bot_id": 1}'

```

---

## 🧠 How It Works

### The Grid Strategy

1. **Initialization**: The bot calculates grid levels and places `LIMIT` orders for the entire range.
2. **Execution**: When an order fills (e.g., a Buy), the bot immediately places a corresponding counter-order (Sell) at a higher price (profit step).
3. **Loop**: This repeats indefinitely, capturing profit from market volatility ("buying low and selling high" within the grid).

### Auto-Tuner Logic

The bot doesn't just sit idle when prices go out of range. The `AutoTuner` module monitors the price:

* **Price > Upper Limit**: Triggers `RESET_UP`. The bot cancels orders and shifts the entire grid up to center on the new price.
* **Price < Lower Limit**: Triggers `EXPAND_DOWN`. The bot extends the grid downwards to keep buying into the dip, respecting a configurable cooldown period.

---

## 🧪 Backtesting

You can verify the logic without real funds using the included backtester.

1. Place your historical data CSV in `backtest/data.csv`.
2. Run the verification script:
```bash
python verify_backtest.py

```



---

## 📂 Project Structure

```
├── backtest/           # Historical data loader & simulation engine
├── database/           # SQLAlchemy models & repositories
├── exchange/           # CCXT wrappers & Binance client
├── execution/          # OrderManager & BalanceManager
├── strategies/         # Grid math & Auto-Tuner logic
├── utils/              # Logger, Security, & Health checks
├── main.py             # FastAPI entry point & Lifecycle management
└── config.py           # Configuration loader

```

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
