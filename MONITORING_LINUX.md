# Running Monitoring Stack Without Docker (PM2 & Linux)

Yes, you can absolutely run Grafana, Loki, and Promtail directly on your Linux server using PM2. This setup is often more lightweight than Docker.

## 1. Install Components

On your Linux server (Ubuntu/Debian), run:

```bash
# Install Grafana
sudo apt-get install -y apt-transport-https software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update && sudo apt-get install grafana

# Download Loki and Promtail Binaries
curl -O -L "https://github.com/grafana/loki/releases/download/v3.0.0/loki-linux-amd64.zip"
curl -O -L "https://github.com/grafana/loki/releases/download/v3.0.0/promtail-linux-amd64.zip"
unzip loki-linux-amd64.zip
unzip promtail-linux-amd64.zip
chmod +x loki-linux-amd64 promtail-linux-amd64
```

## 2. Prepare Config Files

You need to modify the config files in `monitoring/configs/` to use `localhost` instead of Docker container names.

### Update Loki Config (`monitoring/configs/loki/loki.yaml`)
Ensure internal paths like `/tmp/loki` exist or change them to your preferred data directory.

### Update Promtail Config (`monitoring/configs/promtail/promtail.yaml`)
Change the Loki client URL and the log path:

```yaml
clients:
  - url: "http://localhost:3100/loki/api/v1/push" # Change loki to localhost

scrape_configs:
  - job_name: bot_logs
    static_configs:
      - targets:
          - localhost
        labels:
          job: grid_trading_bot
          __path__: /absolute/path/to/your/gridbot/logs/**/*.log # Use absolute path
```

## 3. Run with PM2

Since Grafana usually runs as a system service, you can start it with `systemctl`, but if you want everything in PM2:

```bash
# Start Loki
pm2 start ./loki-linux-amd64 --name "loki" -- -config.file=./monitoring/configs/loki/loki.yaml

# Start Promtail
pm2 start ./promtail-linux-amd64 --name "promtail" -- -config.file=./monitoring/configs/promtail/promtail.yaml

# Start Grafana (if not using systemd)
pm2 start /usr/sbin/grafana-server --name "grafana" -- --config=/etc/grafana/grafana.ini --homepath=/usr/share/grafana
```

## 4. Setup Grafana Data Source

1. Login to Grafana ([http://your-server-ip:3000](http://your-server-ip:3000)).
2. Go to **Connections > Data Sources**.
3. Add **Loki**.
4. Set URL to `http://localhost:3100`.
5. Click **Save & Test**.

> [!TIP]
> You can also provision the data source automatically by copying `monitoring/configs/grafana/provisioning/datasources.yml` to `/etc/grafana/provisioning/datasources/` and changing the URL to `localhost`.
