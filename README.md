# Crypto Lake RS

A lightweight, self-hosted cryptocurrency data collector and local data lake written in Rust. Connects to exchange WebSocket feeds in real time, transforms trade events into OHLCV bars, and stores them as compressed Parquet files partitioned by exchange, symbol, and date. Includes a live web dashboard, REST API, intelligent backfill, and Python analysis tools.

---

## Features

### Data Collection
- **Three exchanges** simultaneously: Binance (13 symbols, 1-second bars), Coinbase (3 symbols, 60-second bars), Kraken (3 symbols, 60-second bars)
- **Per-exchange bar intervals** — configurable via `bar_interval_sec` in `config.yml`
- **Empty bar emission** — fills seconds/minutes with no trades so the timeline has no implicit gaps
- **Source tagging** — every bar records its origin: `live`, `empty`, `backfill_1s`, or `backfill_1m`
- **Configurable symbols** — add or remove any symbol per exchange in `config.yml`

### Backfill
- **Startup backfill** — on launch, scans parquet column statistics (file footer only, no row reads) to find the most recent bar per symbol, then fetches REST API klines to fill the gap up to the current time
- **Internal gap fill** — detects holes inside existing data (caused by mid-session crashes or outages) and fills them; runs at startup across all symbols before live collection begins
- **Reconnect-triggered backfill** — when a WebSocket reconnects after a gap, the collector triggers a targeted backfill for just that exchange, covering the downtime window
- **Daily scheduled backfill** — periodic sweep to catch any remaining gaps
- **Configurable limits** — `gap_threshold_secs`, `max_backfill_secs` (default 30 days), `timeout_secs`

### Storage
- **Apache Parquet** with Zstandard compression, partitioned by `exchange/symbol/year=Y/month=MM/day=DD/`
- **Schema**: `window_start, exchange, symbol, open, high, low, close, volume_base, volume_quote, trade_count, vwap, bid, ask, spread, source`
- **Flush interval**: 1 minute → one parquet file per flush per symbol
- **File naming**: `20260407T171400.parquet` (flush time UTC), `20260407T172700_backfill.parquet` for backfill files
- **Data retention** — configurable `--retention-days` flag to automatically purge old data

### Web Dashboard
- **Candlestick chart** powered by TradingView Lightweight Charts
- **Technical indicators**: Bollinger Bands, SMA (20/50/200), EMA (12/26), VWAP, RSI (separate pane)
- **Crosshair legend** — shows OHLCV + indicator values at cursor position
- **Multiple timeframes** — resample bars on the fly (1m, 5m, 15m, 1h, 4h, 1d)
- **Live WebSocket feed** — bars update in real time as trades arrive
- **Symbol switcher** — click any of the 19 collected symbols
- **System tab** — live health counters (messages, trades, bars, bytes, reconnects)
- **Feed indicator** — visual live/disconnected status

### REST & WebSocket API
| Endpoint | Description |
|---|---|
| `GET /api/v1/symbols` | List all configured exchange/symbol pairs |
| `GET /api/v1/bars/:symbol/latest?tf=5m&limit=500` | OHLCV bars for a symbol, resampled to `tf` |
| `GET /api/v1/health` | Health counters + disk usage |
| `GET /api/v1/analysis/summary` | Per-symbol bar count, completeness, and gap summary |
| `GET /api/v1/ws/stream` | WebSocket — real-time trade events |

### Windows Integration
- **System tray icon** with right-click menu (open dashboard, quit)
- **Windows autostart** — `--install-autostart` / `--remove-autostart` registers the app to launch at login
- **Console mode** — `--no-tray` runs without the tray icon (useful for server deployment)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Rust 2021 edition |
| Async runtime | Tokio |
| WebSocket client | tokio-tungstenite |
| HTTP/WS server | Axum + Tower (CORS middleware) |
| Data format | Apache Arrow + Parquet |
| Compression | Zstandard (zstd) |
| Serialisation | Serde (JSON + YAML) |
| Date/time | Chrono |
| Logging | Tracing (structured JSON) |
| CLI | Clap |
| System tray | tray-icon (Windows) |
| Dashboard charts | TradingView Lightweight Charts |
| Analysis | Python + DuckDB + pandas |

---

## Exchanges and Symbols

| Exchange | Symbols | Bar interval |
|---|---|---|
| Binance | ADAUSDT AVAXUSDT BNBUSDT BTCUSDT DOGEUSDT DOTUSDT ETHUSDT EURUSDT LINKUSDT LTCUSDT SOLUSDT SUIUSDT XRPUSDT | 1 second |
| Coinbase | BTC-USD ETH-USD SOL-USD | 60 seconds |
| Kraken | BTC/USD ETH/USD SOL/USD | 60 seconds |

---

## Build and Run

```bash
# Development build
cargo build

# Optimised production build (LTO, stripped, single codegen unit)
cargo build --release

# Run (Windows — launches with system tray)
./target/release/crypto-lake-rs.exe

# Run without system tray (headless / server mode)
./target/release/crypto-lake-rs.exe --no-tray

# Skip startup backfill
./target/release/crypto-lake-rs.exe --no-backfill

# Register Windows autostart
./target/release/crypto-lake-rs.exe --install-autostart

# Remove Windows autostart
./target/release/crypto-lake-rs.exe --remove-autostart
```

The dashboard is served at `http://localhost:8000` (port configurable in `config.yml`).

> **Note (Windows):** Stop the running app before `cargo build --release` — the locked exe cannot be replaced while it is running.

---

## Configuration

All settings live in `config.yml` at the repo root.

```yaml
general:
  timezone: "UTC"
  log_level: "INFO"
  base_path: "./data"

exchanges:
  - name: "binance"
    symbols: [BTCUSDT, ETHUSDT, ...]
    rest_url: "https://api.binance.com/api/v3"
    wss_url: "wss://stream.binance.com:9443/ws"

  - name: "coinbase"
    bar_interval_sec: 60
    symbols: [BTC-USD, ETH-USD, SOL-USD]
    ...

backfill:
  enabled: true
  gap_threshold_secs: 60
  max_backfill_secs: 2592000   # 30 days
  timeout_secs: 1800

archive:
  provider: "rclone"
  rclone_remote: "gdrive"
  rclone_dest: "crypto-lake"
  local_dest: ""               # optional: E:/crypto-lake-backup
  consolidate_days_older: 7
  schedule_time: "03:00"
```

---

## Data Storage Layout

```
data/
  parquet/
    binance/
      BTCUSDT/
        year=2026/
          month=04/
            day=07/
              20260407T171400.parquet
              20260407T172700_backfill.parquet
    coinbase/
      BTC-USD/
        ...
    kraken/
      BTC-USD/
        ...
  reports/
    health.json               # last health snapshot
    archive_log.jsonl         # sync history
```

---

## Python Analysis Tools

### Jupyter Notebook

```bash
pip install duckdb pandas matplotlib jupyter

# Launch notebook
jupyter notebook notebooks/01_data_audit.ipynb
```

The notebook (`notebooks/01_data_audit.ipynb`) has 8 sections:
1. Full summary across all exchanges and symbols
2. Per-day completeness table + bar chart
3. Gap analysis — list and duration of every gap
4. Bar interval verification
5. Source distribution (live / backfill / empty) pie chart
6. Price chart with volume (configurable resample)
7. Raw data explorer — query any symbol, date range, columns
8. Disk usage breakdown

### `notebooks/lake.py` — Python API

Import in any notebook:

```python
from lake import connect, query_symbol, completeness_report, find_gaps

con = connect()

# Query BTCUSDT for a date range
df = query_symbol(con, "binance", "BTCUSDT", "2026-04-01", "2026-04-10")

# Completeness report: bars, live%, backfill%, gaps per day
report = completeness_report(con, "binance", "BTCUSDT", "2026-04-01", "2026-04-10")

# Find all gaps longer than 5 minutes
gaps = find_gaps(con, "binance", "BTCUSDT", "2026-04-01", "2026-04-10", min_gap_sec=300)
```

### `tools/data_audit.py` — Standalone CLI Audit

```bash
python tools/data_audit.py > tools/audit_results.txt
```

Runs a full audit across all 19 symbols and all collected days. Reports per-day bar counts, completeness %, live/backfill/empty breakdown, gap detection, bar interval verification, source distribution, and disk usage. Avoids recursive glob timeouts by querying per-day directories.

### `tools/daily_check.py` and `tools/gap_analysis.py`

Lighter-weight standalone scripts for quick daily status checks and targeted gap inspection.

---

## Archive and Sync

Manage storage with `tools/archive.py` using [rclone](https://rclone.org) under the hood.

### First-time setup

```
tools\setup_rclone.bat
```

Downloads `rclone.exe` into `tools/` and walks through Google Drive authentication interactively (browser opens, sign in with your Google Workspace account, name the remote `gdrive`).

### Commands

```bash
# Verify rclone and remote are configured correctly
python tools/archive.py setup

# Show local vs Google Drive summary (sizes, file counts, last sync)
python tools/archive.py status

# Preview what would be uploaded
python tools/archive.py sync --dry-run

# Upload new/changed files to Google Drive (delta sync — only transfers differences)
python tools/archive.py sync

# Preview consolidation of old per-minute files into per-day files
python tools/archive.py consolidate --dry-run --days 7

# Consolidate partitions older than 7 days (1440 files → 1 per day, lossless)
python tools/archive.py consolidate --days 7

# Schedule daily automatic sync at 03:00 via Windows Task Scheduler
python tools/archive.py schedule

# Remove the scheduled task
python tools/archive.py schedule --remove
```

**Consolidation** merges the per-minute flush files into a single `consolidated.parquet` per day partition for older data. This reduces file count by 1440× per symbol-day, makes subsequent syncs faster, and speeds up DuckDB queries. The consolidated files are identical in schema to the originals — existing analysis tools work unchanged.

---

## Source Files

| Path | Description |
|---|---|
| `src/main.rs` | Entry point — CLI, tray, runtime setup |
| `src/config.rs` | Config structs and YAML loading |
| `src/collector/` | WebSocket collectors (Binance, Coinbase, Kraken) and JSONL writer |
| `src/transformer/` | Bar aggregator and Parquet writer |
| `src/backfill.rs` | Startup, internal gap, and reconnect backfill |
| `src/server.rs` | Axum HTTP/WebSocket server and API handlers |
| `src/health.rs` | Atomic health counters and JSON health file writer |
| `src/tray.rs` | Windows system tray (tray-icon) |
| `src/autostart.rs` | Windows registry autostart install/remove |
| `src/events.rs` | Shutdown and inter-task event handling |
| `src/cleanup.rs` | Resource cleanup on exit |
| `static/index.html` | Dashboard HTML |
| `static/js/dashboard.js` | Dashboard logic, charts, indicators, WebSocket |
| `static/css/dashboard.css` | Dashboard styles |
| `config.yml` | All runtime configuration |
| `notebooks/lake.py` | Python analysis API |
| `notebooks/01_data_audit.ipynb` | 8-section Jupyter data audit notebook |
| `tools/data_audit.py` | Standalone CLI full audit |
| `tools/daily_check.py` | Quick daily status check |
| `tools/gap_analysis.py` | Targeted gap inspection |
| `tools/archive.py` | Archive/sync CLI (setup, status, sync, consolidate, schedule) |
| `tools/setup_rclone.bat` | One-click rclone download and Google Drive config |
