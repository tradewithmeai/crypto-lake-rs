"""
Daily Health Check for Crypto Lake

Runs a comprehensive check covering:
  - App process status
  - Live data flow (raw files, health counters)
  - Data quality (live vs empty bars, trade counts)
  - Gap analysis for the last 48 hours
  - Disk usage
  - Alerts for anything that needs attention

Output: markdown report saved to data/reports/daily_check.md
        and printed to stdout.
"""

import os
import json
import duckdb
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
RAW_DIR = DATA_DIR / "raw"
REPORT_DIR = DATA_DIR / "reports"
HEALTH_FILE = REPORT_DIR / "health.json"
REPORT_FILE = REPORT_DIR / "daily_check.md"

ALERT_THRESHOLD_GAP_MINS = 10       # Gap longer than this triggers alert
ALERT_THRESHOLD_LIVE_BAR_PCT = 0.1  # Below this % live bars triggers alert
ALERT_STALE_HEALTH_MINS = 5         # Health file older than this triggers alert
ALERT_STALE_RAW_MINS = 10           # Raw files older than this triggers alert
LOOKBACK_HOURS = 48                 # How far back to analyse bars


def now_utc():
    return datetime.now(timezone.utc)


# ── Process check ─────────────────────────────────────────────────────────────

def check_process():
    """Check if crypto-lake-rs.exe is running."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq crypto-lake-rs.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10
        )
        running = "crypto-lake-rs.exe" in result.stdout
        return running
    except Exception:
        return None  # Unknown


# ── Health file ───────────────────────────────────────────────────────────────

def read_health():
    if not HEALTH_FILE.exists():
        return None
    try:
        with open(HEALTH_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def health_age_mins(health):
    if not health:
        return None
    try:
        ts_str = health["ts_utc"].replace("Z", "+00:00")
        # Truncate sub-microsecond precision Python can't parse
        import re
        ts_str = re.sub(r'(\.\d{6})\d+', r'\1', ts_str)
        ts = datetime.fromisoformat(ts_str)
        return (now_utc() - ts).total_seconds() / 60
    except Exception:
        return None


# ── Raw file checks ───────────────────────────────────────────────────────────

def latest_raw_file_age_mins():
    """Return age in minutes of the most recently written raw file."""
    latest_mtime = None
    for root, _, files in os.walk(RAW_DIR):
        for f in files:
            if f.endswith(".jsonl.zst"):
                mtime = os.path.getmtime(os.path.join(root, f))
                if latest_mtime is None or mtime > latest_mtime:
                    latest_mtime = mtime
    if latest_mtime is None:
        return None
    age_secs = now_utc().timestamp() - latest_mtime
    return age_secs / 60


def count_raw_files_today():
    today = now_utc().strftime("%Y-%m-%d")
    count = 0
    for root, _, files in os.walk(RAW_DIR):
        if today in root:
            count += sum(1 for f in files if f.endswith(".jsonl.zst"))
    return count


# ── Parquet / bar analysis ────────────────────────────────────────────────────

def analyse_bars(con, lookback_hours=LOOKBACK_HOURS):
    """Analyse bars for the last N hours across all symbols."""
    since = now_utc() - timedelta(hours=lookback_hours)
    since_ts = f"timestamptz '{since.strftime('%Y-%m-%d %H:%M:%S')}+00'"

    results = {}
    alerts = []

    exchanges = sorted([
        d for d in PARQUET_DIR.iterdir() if d.is_dir()
    ]) if PARQUET_DIR.exists() else []

    for ex_dir in exchanges:
        exchange = ex_dir.name
        for sym_dir in sorted(ex_dir.iterdir()):
            if not sym_dir.is_dir():
                continue
            symbol = sym_dir.name
            pattern = str(sym_dir).replace("\\", "/") + "/**/*.parquet"

            try:
                row = con.execute(f"""
                    SELECT
                        count(*) as total_bars,
                        sum(CASE WHEN trade_count > 0 THEN 1 ELSE 0 END) as live_bars,
                        sum(CASE WHEN source = 'live' THEN 1 ELSE 0 END) as ws_live,
                        sum(CASE WHEN source = 'empty' THEN 1 ELSE 0 END) as empty_bars,
                        sum(CASE WHEN source LIKE 'backfill%' THEN 1 ELSE 0 END) as backfill_bars,
                        min(window_start) as first_bar,
                        max(window_start) as last_bar,
                        max(close) as max_price,
                        min(close) as min_price
                    FROM read_parquet('{pattern}', hive_partitioning=true)
                    WHERE window_start >= {since_ts}
                """).fetchone()
            except Exception as e:
                results[(exchange, symbol)] = {"error": str(e)}
                continue

            if not row or row[0] == 0:
                results[(exchange, symbol)] = {"error": "no data in window"}
                continue

            total, live, ws_live, empty, backfill, first, last, max_p, min_p = row
            live_pct = (live / total * 100) if total > 0 else 0
            ws_pct = (ws_live / total * 100) if total > 0 else 0

            results[(exchange, symbol)] = {
                "total_bars": total,
                "live_bars": live,
                "ws_live": ws_live,
                "empty_bars": empty,
                "backfill_bars": backfill,
                "live_pct": live_pct,
                "ws_pct": ws_pct,
                "first_bar": first,
                "last_bar": last,
                "max_price": max_p,
                "min_price": min_p,
            }

            if ws_live < ALERT_THRESHOLD_LIVE_BAR_PCT * total / 100:
                alerts.append(
                    f"LOW LIVE DATA: {exchange}/{symbol} — only {ws_live} WebSocket live bars "
                    f"({ws_pct:.3f}%) in last {lookback_hours}h"
                )

    return results, alerts


def find_gaps_recent(con, lookback_hours=LOOKBACK_HOURS):
    """Find gaps in the last N hours."""
    since = now_utc() - timedelta(hours=lookback_hours)
    since_ts = since.timestamp()
    gap_threshold = 60  # seconds

    all_gaps = []
    exchanges = sorted([
        d for d in PARQUET_DIR.iterdir() if d.is_dir()
    ]) if PARQUET_DIR.exists() else []

    for ex_dir in exchanges:
        exchange = ex_dir.name
        for sym_dir in sorted(ex_dir.iterdir()):
            if not sym_dir.is_dir():
                continue
            symbol = sym_dir.name
            pattern = str(sym_dir).replace("\\", "/") + "/**/*.parquet"

            try:
                rows = con.execute(f"""
                    SELECT epoch_us(window_start) / 1000000 as ts_sec
                    FROM read_parquet('{pattern}', hive_partitioning=true)
                    WHERE window_start >= timestamptz '{since.strftime('%Y-%m-%d %H:%M:%S')}+00'
                    ORDER BY ts_sec
                """).fetchall()
            except Exception:
                continue

            timestamps = [r[0] for r in rows]
            for i in range(1, len(timestamps)):
                diff = timestamps[i] - timestamps[i - 1]
                if diff > gap_threshold:
                    gap_start = datetime.fromtimestamp(timestamps[i - 1], tz=timezone.utc)
                    gap_end = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
                    all_gaps.append({
                        "exchange": exchange,
                        "symbol": symbol,
                        "start": gap_start,
                        "end": gap_end,
                        "duration_secs": int(diff),
                    })

    return all_gaps


def last_bar_age_mins(bar_results):
    """How long since the most recent bar was produced (any symbol)."""
    latest = None
    for v in bar_results.values():
        if "last_bar" in v and v["last_bar"] is not None:
            if latest is None or v["last_bar"] > latest:
                latest = v["last_bar"]
    if latest is None:
        return None
    # last_bar is a datetime from duckdb
    if hasattr(latest, "tzinfo") and latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    return (now_utc() - latest).total_seconds() / 60


# ── Disk usage ────────────────────────────────────────────────────────────────

def dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except Exception:
                pass
    return total / (1024 * 1024)


def count_parquet_files():
    count = 0
    for root, _, files in os.walk(PARQUET_DIR):
        count += sum(1 for f in files if f.endswith(".parquet"))
    return count


# ── Formatting helpers ────────────────────────────────────────────────────────

def fmt_dur(secs):
    if secs >= 3600:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h {m}m"
    if secs >= 60:
        return f"{secs // 60}m {secs % 60}s"
    return f"{secs}s"


def status_icon(ok):
    return "OK" if ok else "WARN"


# ── Report generation ─────────────────────────────────────────────────────────

def generate_report(process_running, health, bar_results, gaps, alerts):
    lines = []
    now = now_utc()
    h_age = health_age_mins(health)
    raw_age = latest_raw_file_age_mins()

    lines.append(f"# Crypto Lake — Daily Health Check")
    lines.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}\n")

    # ── Status Summary ──────────────────────────────────────────────────────
    lines.append("## Status Summary\n")

    proc_status = "RUNNING" if process_running else ("UNKNOWN" if process_running is None else "STOPPED")
    lines.append(f"- **Process**: {proc_status}")

    if health:
        uptime_h = health.get("counters", {}).get("uptime_seconds", 0) // 3600 if health else 0
        # Derive uptime from health timestamp
        uptime_secs = health.get("collector", {}).get("uptime_seconds", 0)
        h_age_disp = f"{h_age:.1f}" if h_age is not None else "?"
        lines.append(f"- **Health file age**: {h_age_disp} min ago  [{status_icon(h_age is not None and h_age < ALERT_STALE_HEALTH_MINS)}]")
        lines.append(f"- **App uptime**: {fmt_dur(uptime_secs)}")

        ctrs = health.get("counters", {})
        lines.append(f"- **Messages received**: {ctrs.get('messages_received', 0):,}")
        lines.append(f"- **Trades received**: {ctrs.get('trades_received', 0):,}")
        lines.append(f"- **Bars produced**: {ctrs.get('bars_produced', 0):,}")
        lines.append(f"- **Raw lines written**: {ctrs.get('raw_lines_written', 0):,}")
        lines.append(f"- **WS disconnects**: {ctrs.get('ws_disconnects', 0)}")
        lines.append(f"- **WS reconnects**: {ctrs.get('ws_reconnects', 0)}")
    else:
        lines.append(f"- **Health file**: NOT FOUND")

    if raw_age is not None:
        lines.append(f"- **Last raw file**: {raw_age:.1f} min ago  [{status_icon(raw_age < ALERT_STALE_RAW_MINS)}]")
        lines.append(f"- **Raw files today**: {count_raw_files_today()}")
    else:
        lines.append(f"- **Last raw file**: NO RAW FILES FOUND  [WARN]")
    lines.append("")

    # ── Alerts ──────────────────────────────────────────────────────────────
    if not process_running:
        alerts.insert(0, "CRITICAL: crypto-lake-rs.exe is NOT running")
    if h_age is not None and h_age > ALERT_STALE_HEALTH_MINS:
        alerts.insert(0, f"WARN: Health file is {h_age:.0f} minutes old (app may be frozen)")
    if raw_age is not None and raw_age > ALERT_STALE_RAW_MINS:
        alerts.insert(0, f"WARN: Last raw file is {raw_age:.0f} minutes old (data not flowing)")
    if raw_age is None:
        alerts.insert(0, "WARN: No raw files found — data may not be writing to disk")

    if alerts:
        lines.append("## Alerts\n")
        for a in alerts:
            lines.append(f"- **{a}**")
        lines.append("")
    else:
        lines.append("## Alerts\n")
        lines.append("- None — all checks passed.")
        lines.append("")

    # ── Bar quality (last 48h) ───────────────────────────────────────────────
    lines.append(f"## Bar Quality (last {LOOKBACK_HOURS}h)\n")
    lines.append("| Exchange | Symbol | Total Bars | With Trades | WS Live | Empty | Live% | Last Bar |")
    lines.append("|----------|--------|-----------|-------------|---------|-------|-------|----------|")
    for (exchange, symbol), v in sorted(bar_results.items()):
        if "error" in v:
            lines.append(f"| {exchange} | {symbol} | *{v['error']}* | | | | | |")
        else:
            last_str = v["last_bar"].strftime("%m-%d %H:%M") if v["last_bar"] else "n/a"
            lines.append(
                f"| {exchange} | {symbol} | {v['total_bars']:,} | {v['live_bars']:,} "
                f"| {v['ws_live']:,} | {v['empty_bars']:,} | {v['live_pct']:.1f}% | {last_str} |"
            )
    lines.append("")

    # ── Recent gaps ──────────────────────────────────────────────────────────
    lines.append(f"## Gaps (last {LOOKBACK_HOURS}h)\n")
    if not gaps:
        lines.append("No gaps detected in this period.")
    else:
        # Group correlated gaps
        sorted_gaps = sorted(gaps, key=lambda g: g["start"])
        sig_gaps = [g for g in sorted_gaps if g["duration_secs"] >= ALERT_THRESHOLD_GAP_MINS * 60]
        minor_gaps = [g for g in sorted_gaps if g["duration_secs"] < ALERT_THRESHOLD_GAP_MINS * 60]

        if sig_gaps:
            lines.append(f"### Significant Gaps (> {ALERT_THRESHOLD_GAP_MINS} min)\n")
            lines.append("| Exchange | Symbol | Start (UTC) | End (UTC) | Duration |")
            lines.append("|----------|--------|-------------|-----------|----------|")
            for g in sig_gaps:
                lines.append(
                    f"| {g['exchange']} | {g['symbol']} "
                    f"| {g['start'].strftime('%m-%d %H:%M:%S')} "
                    f"| {g['end'].strftime('%m-%d %H:%M:%S')} "
                    f"| {fmt_dur(g['duration_secs'])} |"
                )
            lines.append("")

        lines.append(f"- **Total gaps**: {len(gaps)}")
        lines.append(f"- **Significant (>{ALERT_THRESHOLD_GAP_MINS}m)**: {len(sig_gaps)}")
        lines.append(f"- **Minor (<{ALERT_THRESHOLD_GAP_MINS}m)**: {len(minor_gaps)}")
        if gaps:
            longest = max(gaps, key=lambda g: g["duration_secs"])
            lines.append(f"- **Longest gap**: {fmt_dur(longest['duration_secs'])} "
                         f"at {longest['start'].strftime('%m-%d %H:%M')} UTC "
                         f"({longest['exchange']}/{longest['symbol']})")
    lines.append("")

    # ── Disk usage ───────────────────────────────────────────────────────────
    lines.append("## Disk Usage\n")
    raw_mb = dir_size_mb(RAW_DIR)
    parquet_mb = dir_size_mb(PARQUET_DIR)
    total_mb = raw_mb + parquet_mb
    parquet_count = count_parquet_files()
    lines.append(f"| Directory | Size |")
    lines.append(f"|-----------|------|")
    lines.append(f"| Raw JSONL | {raw_mb:.1f} MB |")
    lines.append(f"| Parquet   | {parquet_mb:.1f} MB |")
    lines.append(f"| **Total** | **{total_mb:.1f} MB** |")
    lines.append(f"\n- **Parquet files**: {parquet_count:,}")
    lines.append("")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Running daily health check...")

    process_running = check_process()
    health = read_health()
    alerts = []

    con = duckdb.connect()
    print("Analysing bars (last 48h)...")
    bar_results, bar_alerts = analyse_bars(con)
    alerts.extend(bar_alerts)

    print("Scanning for gaps...")
    gaps = find_gaps_recent(con)

    # Alert on significant gaps
    for g in gaps:
        if g["duration_secs"] >= ALERT_THRESHOLD_GAP_MINS * 60:
            alerts.append(
                f"GAP: {g['exchange']}/{g['symbol']} — {fmt_dur(g['duration_secs'])} gap "
                f"at {g['start'].strftime('%Y-%m-%d %H:%M')} UTC"
            )

    report = generate_report(process_running, health, bar_results, gaps, alerts)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)
    print(f"\nReport saved to: {REPORT_FILE}")

    # Exit with non-zero code if there are alerts (useful for scheduled tasks)
    if any("CRITICAL" in a or "WARN" in a for a in alerts):
        sys.exit(1)


if __name__ == "__main__":
    main()
