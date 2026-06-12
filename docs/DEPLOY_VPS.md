# Deploying crypto-lake-rs on a Linux VPS

This runbook covers running the collector + dashboard/API as a 24/7 service on a
headless Linux box (tested: Ubuntu 24.04 LTS, CPU-only, 6 GB RAM). The rest of the
project docs assume Windows (Task Scheduler autostart, system-tray icon); this is
the Linux equivalent.

## 1. Build dependencies

Rust toolchain plus the OpenSSL dev headers (the `openssl-sys` crate needs them;
without these the build fails with "Could not find openssl via pkg-config"):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
sudo apt-get install -y pkg-config libssl-dev
```

On a small box, add swap before building — the arrow/parquet/tokio crates spike RAM:

```bash
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

Then build:

```bash
cd ~/crypto-lake-rs
~/.cargo/bin/cargo build --release -j 2     # -j 2 caps RAM on small boxes
```

## 2. Run flags (headless)

- `--no-tray` — **required** on a headless server; the default tries a system-tray icon.
- `--retention-days 0` — **disable** raw-file cleanup so historical data is preserved
  (default is 3, which purges old raw files).

```bash
./target/release/crypto-lake-rs --config config.yml --no-tray --retention-days 0
```

## 3. Config notes for a VPS

- **`betty.enabled: false`** unless the Betty Sentinel endpoint is actually reachable
  from the box (default points at `localhost:8400`). With it enabled and no target,
  heartbeats just fail harmlessly, but disabling is cleaner.
- `server.port: 8000` — the dashboard/API. The server binds `0.0.0.0:8000`, so it is
  **publicly reachable by default** — see §5 (firewall) and put it behind an
  authenticated reverse proxy before exposing it.
- Archive/consolidation (`tools/archive.py`) and gdrive sync via rclone are separate
  Python tools — schedule `consolidate` via cron to keep the per-minute live files
  collapsing to one-per-day (the Rust collector does not consolidate on its own).

## 4. systemd service

Install `deploy/cryptolake.service` (edit `User`/paths for your box):

```bash
sudo install -m644 deploy/cryptolake.service /etc/systemd/system/cryptolake.service
sudo systemctl daemon-reload
sudo systemctl enable --now cryptolake
journalctl -u cryptolake -f          # watch it connect to the exchanges
```

## 5. Firewall — do not leave :8000 open

The dashboard binds `0.0.0.0:8000` with no auth of its own. Lock the box down so it
is only reachable through an authenticated reverse proxy (nginx + HTTP basic auth +
TLS). Allow SSH **before** enabling ufw or you will lock yourself out:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'          # 80 + 443
sudo ufw --force enable              # :8000 now only reachable via localhost (nginx)
```

## 6. Seeding history on a fresh box

On an empty lake the startup backfill **skips** (it only fills gaps relative to
existing data — "no existing data, skipping"). To rebuild history from scratch, use
the one-shot deep backfill (Binance 1m klines), per symbol:

```bash
for s in BTCUSDT ETHUSDT SOLUSDT ... ; do
  ./target/release/crypto-lake-rs --deep-backfill --symbol "$s" --from 2020-01-01 --no-tray
done
```

It writes one backfill file per day and skips days already present, so it is
resumable. Note this is **1-minute** resolution; the live collector's native
**1-second** bars are only produced going forward (or migrated from another store).
