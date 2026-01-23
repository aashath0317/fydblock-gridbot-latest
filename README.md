# Fydblock Spot Grid Trading Bot 🚀

A high-performance, asynchronous cryptocurrency trading bot specializing in **Spot Grid Trading**. Built with Python 3.12 (FastAPI) and CCXT, it features enterprise-grade security, real-time observability (Grafana/Loki), and a robust simulation engine designed for high-frequency order management and resilient bot operations.

---

## 🏗️ Enterprise Core Features

The Fydblock engine is designed for mission-critical trading with a focus on state integrity and financial safety.

### 🛡️ Smart Recovery (Hot Boot)
- **Zero-Downtime Recovery**: Automatically detects and resumes active bots after a system restart or crash.
- **State Reconstruction**: Rebuilds internal grid levels and order history directly from the SQLite database and exchange state without cancelling open orders.
- **Zombie Prevention**: Automatically cleans up and reconciles "vanished" or orphaned orders via a heartbeat watchdog.

### 💰 Financial Integrity & Solvency
- **Solvency Monitor**: A background auditor that ensures global exchange balances always match the total capital allocated across all active bots.
- **Haircut Mechanism**: Proactively adjusts bot allocations if a deficit is detected (e.g., due to external manual withdrawals) to prevent execution failures.
- **Operational Fee Reserve**: Automatically reserves **1% of initial capital** to stabilize fee execution and prevent "Dust Shortfall" errors.
- **Profit Banking**: Dynamically allocates **10% of realized profits** to the fee reserve (capped at 1% of investment) to ensure sustainability during long-running sessions.

### 🔒 Enterprise Security
- **At-Rest Encryption**: API keys and secrets are encrypted using `Fernet` (AES-128 in CBC mode) before persisting to the database.
- **Strict Isolation**: Every order is tagged with a unique `clientOrderId` (CID) prefix, ensuring zero interference between multiple bots running on the same trading pair.

---

## 🤖 Advanced Trading Strategies

Beyond simple grids, Fydblock implements sophisticated market-making logic.

### ♾️ Infinity Grid (Dynamic Trailing)
- **Trailing Up**: Automatically adjusts the grid range upwards during bullish breakouts, performing inventory re-buys to maintain market exposure.
- **Trailing Down**: Lowers the grid during bearish trends, liquidating excess inventory to protect capital.
- **Inventory Stabilization**: Implements precise coin tracking to handle exchange fees and "dust" accumulation during high-frequency flips.

### ⚡ Smart Auto-Tuner
- **Geometric vs. Arithmetic**: Supports both geometric (percentage-based) and arithmetic (fixed-step) spacing.
- **Breakout Management**: Automatically resets or expands the grid if the price moves out of the predefined range.
- **Dynamic Dead Zones**: Intelligently prevents order placement too close to the current market price to avoid "Instant Fill" slippage.

---

## 📊 Observability & Monitoring

Enterprise-grade visibility into your bot's health and performance.

- **Stack**: Fully Integrated **Grafana**, **Loki**, and **Promtail** (via Docker Compose).
- **Log Aggregation**: Real-time log streaming with dedicated dashboards for bot events, order fills, and system errors.
- **Performance Analytics**: 
    - Real-time PnL tracking and equity curves.
    - Historical trade history with precise fee and slippage breakdown.
    - REST API endpoints for real-time holdings, stats, and "Sparkline" data.

---

## 🛠️ Technology Stack

* **Core**: Python 3.12+ (Asyncio)
* **API Framework**: FastAPI & Uvicorn
* **Database**: SQLite (WAL Mode) with SQLAlchemy Async
* **Execution**: CCXT (Unified exchange interface with WebSocket support)
* **Security**: Cryptography (Fernet)
* **Data**: Pandas, NumPy (Performance analysis)

---

## 🚀 Getting Started

### Prerequisites
* Python 3.12+
* Docker & Docker Compose (for monitoring)

### Installation
1. **Clone & Setup**
   ```bash
   git clone https://github.com/yourusername/fydblock-grid-bot.git
   cd fydblock-grid-bot
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configuration**
   Create a `.env` file:
   ```env
   DB_URL=sqlite+aiosqlite:///bot_data.db
   ENCRYPTION_KEY=YOUR_FERNET_KEY # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   GRAFANA_ADMIN_PASSWORD=admin
   ```

---

## 🏃 Operation

### Run the Server
```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```
- **Interactive API**: [http://localhost:8000/docs](http://localhost:8000/docs)

### Deployment with Monitoring
```bash
docker-compose up -d
```
- **Grafana**: `http://localhost:3000`

---

## 🕹️ API Quick Start

### Start Infinity Grid
```json
// POST /start
{
  "bot_id": 1,
  "exchange": "binance",
  "pair": "SOL/USDT",
  "investment": 1000.0,
  "strategy": {
    "upper_price": 120.0,
    "lower_price": 80.0,
    "grids": 20,
    "trailing_up": true,
    "spacing": "geometric"
  }
}
```

---

## 📄 License
Distributed under the MIT License. See `LICENSE` for more information.