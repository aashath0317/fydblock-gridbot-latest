# Fydblock Spot Grid Trading Bot 🚀

A high-performance, asynchronous cryptocurrency trading bot specializing in **Spot Grid Trading**. Built with Python (FastAPI) and CCXT, it features enterprise-grade security, real-time observability (Grafana/Loki), and a robust simulation engine.

## 🌟 Key Features

* **⚡ Asynchronous Core**: Powered by `asyncio` and `FastAPI` for non-blocking I/O, handling real-time WebSocket feeds and order updates simultaneously.
* **🛡️ Resilient Order Management**:
* **State Reconciliation**: Automatically detects and handles "vanished" orders via `OrderManager`.
* **Strict Isolation**: Prevents "phantom orders" by ensuring database state matches exchange state.


* **🔒 Enterprise Security**:
* **AES Encryption**: API keys/secrets are encrypted at rest using `Fernet` before storage.
* **Environment Isolation**: Critical secrets managed via `.env`.


* **🤖 Smart Auto-Tuner**:
* **Reset Up**: Automatically resets the grid upwards if the price breaks the upper limit.
* **Expand Down**: Intelligently lowers limits during market dips with configurable cooldowns.


* **👁️ Observability & Monitoring**:
* Full integration with **Grafana**, **Loki**, and **Promtail** for log aggregation and visualization.
* Health stats endpoints.


* **📊 Built-in Backtesting**: Simulation engine to verify strategies against historical data before going live.

---

## 🛠️ Technology Stack

* **Core**: Python 3.12+
* **Web Framework**: FastAPI, Uvicorn
* **Database**: SQLite (Async/WAL mode) with SQLAlchemy
* **Exchange**: CCXT (WebSockets + REST)
* **Monitoring**: Grafana, Loki, Promtail (via Docker)
* **Data Analysis**: Pandas, NumPy, Plotly

---

## 🚀 Getting Started

### Prerequisites

* Python 3.12 or higher
* Git
* Docker & Docker Compose (optional, for monitoring)

### Installation

1. **Clone the Repository**
```bash
git clone https://github.com/yourusername/fydblock-grid-bot.git
cd fydblock-grid-bot

```


2. **Set up Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

```


3. **Install Dependencies**
```bash
pip install -r requirements.txt

```


4. **Environment Configuration**
Create a `.env` file in the root directory:
```env
# Database Configuration
DB_URL=sqlite+aiosqlite:///grid_bot.db

# Security (Generate a key using cryptography.fernet)
ENCRYPTION_KEY=YOUR_GENERATED_FERNET_KEY

# Monitoring (Optional)
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin

# Notifications (Optional - Apprise)
APPRISE_NOTIFICATION_URLS=discord://webhook_id/webhook_token

```


*Tip: Generate a Fernet key with:*
```python
from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())

```



---

## 🏃 Running the Bot

### 1. Run the API Server (Live/Paper Trading)

The server handles API requests to start/stop bots and manage state.

```bash
uvicorn server:app --reload --host 0.0.0.0 --port 8000

```

* **Swagger Docs**: `http://localhost:8000/docs`
* **Health Check**: `http://localhost:8000/`

### 2. Run with Monitoring Stack (Docker)

To spin up Grafana, Loki, and Promtail for logs and dashboards:

```bash
docker-compose up -d

```

* **Grafana Dashboard**: `http://localhost:3000` (Default login: defined in `.env`)

### 3. Run Standalone (CLI)

To run a specific configuration file directly without the API server:

```bash
python main.py --config config/config.json

```

---

## 🕹️ API Usage Guide

The bot is primarily controlled via REST API.

### Start a Bot

**POST** `/start`

```json
{
  "bot_id": 1,
  "user_id": 101,
  "exchange": "binance",
  "pair": "BTC/USDT",
  "api_key": "YOUR_API_KEY",
  "api_secret": "YOUR_SECRET",
  "mode": "paper",
  "investment": 1000.0,
  "strategy": {
    "upper_price": 60000,
    "lower_price": 50000,
    "grids": 10,
    "spacing": "geometric"
  }
}

```

### Stop a Bot

**POST** `/stop/{bot_id}`

```bash
curl -X POST "http://localhost:8000/stop/1"

```

### Delete a Bot (Liquidate Assets)

**DELETE** `/bot/{bot_id}?liquidate=true`

* If the bot is running, it stops and sells assets.
* If the bot is offline, pass credentials in the body to perform "Offline Liquidation".

### Run Backtest

**POST** `/backtest`

```json
{
  "exchange": "binance",
  "pair": "SOL/USDT",
  "startDate": "2024-01-01T00:00:00Z",
  "endDate": "2024-02-01T00:00:00Z",
  "capital": 1000,
  "upperPrice": 150,
  "lowerPrice": 80,
  "gridSize": 20
}

```

---

## 📂 Project Structure

```text
├── backtest/            # Simulation engine & data loaders
├── config/              # Config validators & schema definitions
├── core/
│   ├── bot_management/  # Bot Controller, Event Bus, Health Checks
│   ├── grid_management/ # Logic for calculating grid levels
│   ├── order_handling/  # Order execution, fee calc, & reconciliation
│   ├── services/        # Exchange interfaces (CCXT wrapper)
│   └── storage/         # Database persistence layers
├── monitoring/          # Docker configs for Grafana/Loki/Promtail
├── strategies/          # Trading logic (Grid Strategy, Auto-Tuner)
├── server.py            # FastAPI Entry Point
├── main.py              # CLI Entry Point
└── docker-compose.yml   # Observability Stack

```

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.