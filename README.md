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
- **Non-blocking startup** — backfill runs as a background task; live collectors start immediately on launch
- **Trailing gap fill** — scans parquet column statistics (file footer only, no row reads) to find the most recent bar per symbol, then fetches REST API klines to fill the gap up to the current time
- **Internal gap fill** — detects holes inside existing data (caused by mid-session crashes or outages) and fills them automatically on startup
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

Base URL: `http://localhost:8000`

#### Core endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Live candlestick dashboard (TradingView Lightweight Charts) |
| `GET /api/v1/symbols` | Exchange → symbol list from config |
| `GET /api/v1/bars/:symbol/latest?tf=5m&limit=500` | OHLCV bars resampled to `tf`; newest-first; merges all exchanges |
| `GET /api/v1/health` | Live collector counters (messages, trades, bars, reconnects) |
| `GET /api/v1/analysis/summary` | Per-symbol parquet stats — bar counts, completeness, earliest/latest timestamp |
| `WS /api/v1/ws/stream?symbols=BTCUSDT,ETHUSDT` | Real-time trade events; omit `symbols` for all |

#### Agent endpoints

Designed for LLM trading agent consumption. All accept `?exchange=` to disambiguate symbols that exist on multiple exchanges (e.g. `BTC-USD` on both Coinbase and Kraken).

**`GET /api/v1/indicators/:symbol?tf=5m&limit=200&exchange=binance`**

Pre-computed technical indicators with signal labels. In-progress bar is dropped before computation; volume ratio uses only `source='live'` bars.

| Indicator | Signal values |
|---|---|
| `rsi` (14) | `oversold`, `neutral`, `overbought` |
| `macd` (12,26,9) | `direction`: `bullish_accelerating`, `bullish_weakening`, `bearish_weakening`, `bearish_accelerating` |
| `bollinger` (20,2) | `position` 0–1 float + `near_upper`, `upper_half`, `lower_half`, `near_lower` |
| `sma` 20/50/200 | `trend`: `bullish_aligned`, `bearish_aligned`, `mixed` |
| `ema` 12/26 | `signal`: `bullish`, `bullish_crossover`, `bearish`, `bearish_crossover` |
| `vwap` | `price_vs_vwap`: `above` / `below` |
| `volume` | `ratio` vs 20-bar avg + `elevated`, `normal`, `low` |

Also returns top-level `regime` label and `confidence` derived from signal-agreement ratio.

---

**`GET /api/v1/derivatives/:symbol?exchange=binance`**

Proxies Binance Futures REST locally — no external calls from the agent. Results cached 30 seconds.

| Field | Source |
|---|---|
| `funding.rate` + `rate_8h_annualised` | `/fapi/v1/fundingRate` |
| `funding.next_funding_time`, `funding.signal` | |
| `open_interest.value_contracts` | `/fapi/v1/openInterest` |
| `long_short_ratio.value` + `signal` | `/futures/data/globalLongShortAccountRatio` |
| `mark_price`, `change_24h_pct` | `/fapi/v1/ticker/24hr` |

Returns `null` fields for symbols with no perpetual listing (e.g. `EURUSDT`). Returns `404` for non-Binance symbols.

---

**`GET /api/v1/snapshot/:symbol?tf=5m&exchange=binance`**

Single round-trip — full context for the agent. Parquet read and Binance Futures fetch run in parallel.

| Section | Contents |
|---|---|
| `price` | `last`, `open_24h`, `change_24h_pct`, `high_24h`, `low_24h` |
| `volume` | `volume_24h_base`, `trade_count_24h`, `vwap_24h` |
| `indicators` | Full indicator block (same as `/indicators`) |
| `derivatives` | Full derivatives block; `null` for non-Binance symbols |
| `regime.label` | `bullish_momentum`, `bullish_bias`, `neutral_ranging`, `bearish_bias`, `bearish_momentum` |
| `regime.confidence` | `high` (≥6/7 signals agree), `medium` (4–5), `low` (close split) |
| `regime.basis` | Contributing signals e.g. `["price_above_vwap", "positive_funding"]` |
| `bars_sample` | Last 5 complete bars: `ts, open, high, low, close, volume, vwap` |

---

**`GET /api/v1/scan?exchange=binance&sort=momentum&limit=10`**

Scans all symbols for an exchange in parallel and ranks by criterion. Results cached 60 seconds.

| `sort` | Ranks by |
|---|---|
| `momentum` | RSI direction × MACD sign × price vs SMA20 |
| `volume` | 20-bar volume ratio — highest relative volume first |
| `volatility` | BB width / midpoint — widest bands first (breakout candidates) |
| `rsi_extreme` | Distance from RSI 50 either direction |

Returns `rank, symbol, score, price, change_24h_pct, rsi, macd_direction, volume_ratio, bb_position, regime` per result. `limit` capped at 20.

### Windows Integration
- **System tray icon** with right-click menu (open dashboard, quit)
- **Windows autostart** — `--install-autostart` / `--remove-autostart` registers the app to launch at login
- **Console mode** — `--no-tray` runs without the tray icon (useful for server deployment)
- **Background process architecture** — tray runs on the main thread (Windows requirement); all async work runs in a dedicated Tokio runtime on a second OS thread; the two communicate via shared `Arc<AtomicBool>` shutdown flag and `Arc<HealthCounters>`

### Monitoring — Betty Sentinel Integration
- **Built-in Betty agent** (`src/betty.rs`) — background tokio task that POSTs signed telemetry to a local [Betty Sentinel](https://github.com/tradewithmeai/betty-sentinel) instance every 60 seconds
- **Heartbeat** — proves the process is alive (`POST /ingest/heartbeat`)
- **Service state** — reports data freshness by scanning parquet file mtimes; sends `last_data_utc`, `status` (`ok`/`stale`/`unknown`), and live metrics (`bars_produced`, `ws_reconnects`, `uptime_seconds`, `last_write_age_seconds`)
- **HMAC-SHA256 signing** — every payload is signed with a shared secret; Betty verifies before accepting
- **Monotonic sequence numbers** — persisted to `data/reports/betty_seq.json` across restarts to satisfy Betty's replay guard
- **Graceful degradation** — if Betty is unreachable the agent logs a warning and retries next tick; never crashes the collector
- Configure via the `betty:` section in `config.yml`; set `enabled: false` to disable entirely

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
| Monitoring | Betty Sentinel (HMAC-SHA256, hmac + sha2) |

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

betty:
  enabled: true
  url: "http://localhost:8400"
  agent_id: "home-desktop"
  secret_hex: ""               # 32-byte hex — must match Betty's .env
  interval_sec: 60
  stale_threshold_sec: 300

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
    health.json               # last health snapshot (written every 60s)
    betty_seq.json            # Betty Sentinel sequence number (persisted across restarts)
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

## Betty Sentinel Monitoring

Betty Sentinel is a local monitoring server that sends Telegram alerts when service data goes stale. The built-in Betty agent (`src/betty.rs`) runs automatically alongside the collectors.

### Setup

1. Generate a shared secret:
   ```
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

2. Add to `config.yml`:
   ```yaml
   betty:
     enabled: true
     secret_hex: "<your hex string>"
   ```

3. Add the matching entry to Betty's `.env`:
   ```
   BETTY_AGENT_SECRET_HOME_DESKTOP=<same hex string>
   ```

4. Start Betty:
   ```
   uvicorn betty.api.app:app --host 0.0.0.0 --port 8400
   ```

The agent starts automatically with the app — no separate process needed. Betty will alert via Telegram if no parquet data has been written for more than 5 minutes (`stale_threshold_sec: 300`).

A standalone Python alternative is also available at `tools/betty_agent.py` for testing or running outside the main process.

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
| `src/betty.rs` | Betty Sentinel agent — signed heartbeat and service-state telemetry |
| `src/server.rs` | Axum HTTP/WebSocket server and API handlers |
| `src/indicators.rs` | Pure Rust indicator math — SMA, EMA, RSI, Bollinger Bands, MACD |
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
