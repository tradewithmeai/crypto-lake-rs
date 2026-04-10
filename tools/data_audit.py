#!/usr/bin/env python3
"""
Data audit: verify completeness, bar intervals, gaps, and source distribution.
Uses DuckDB with per-day targeted queries to avoid recursive glob timeouts.
"""

import os
from pathlib import Path
from datetime import date, timedelta
from collections import Counter
import duckdb

DATA_ROOT = Path(__file__).parent.parent / "data" / "parquet"
# Determine actual start date from directory structure
START_DATE = date(2026, 3, 21)
END_DATE   = date(2026, 4, 10)

EXCHANGES = {
    "binance":  sorted(["ADAUSDT","AVAXUSDT","BNBUSDT","BTCUSDT","DOGEUSDT",
                         "DOTUSDT","ETHUSDT","EURUSDT","LINKUSDT","LTCUSDT",
                         "SOLUSDT","SUIUSDT","XRPUSDT"]),
    "coinbase": sorted(["BTC-USD","ETH-USD","SOL-USD"]),
    "kraken":   sorted(["BTC-USD","ETH-USD","SOL-USD"]),
}

BINANCE_BARS_PER_DAY  = 86400   # 1 per second
MINUTE_BARS_PER_DAY   = 1440    # 1 per minute (Coinbase/Kraken)

TODAY = END_DATE

def day_path(exchange, symbol, d):
    return DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"

def all_dates():
    d = START_DATE
    while d <= END_DATE:
        yield d
        d += timedelta(days=1)

def get_day_stats(con, exchange, symbol, d):
    dp = day_path(exchange, symbol, d)
    if not dp.exists():
        return None
    files = list(dp.glob("*.parquet"))
    if not files:
        return None
    glob = str(dp).replace("\\", "/") + "/*.parquet"
    try:
        row = con.execute(f"""
            SELECT
                COUNT(*)                                          AS bar_count,
                MIN(epoch(window_start))                          AS first_ts,
                MAX(epoch(window_start))                          AS last_ts,
                SUM(CASE WHEN source='live'         THEN 1 END)   AS live_bars,
                SUM(CASE WHEN source LIKE 'backfill%' THEN 1 END) AS bf_bars,
                SUM(CASE WHEN source='empty'        THEN 1 END)   AS empty_bars,
                SUM(trade_count)                                  AS total_trades,
                COUNT(DISTINCT source)                            AS source_types
            FROM read_parquet('{glob}')
        """).fetchone()
        return row
    except Exception as e:
        return f"ERROR: {e}"

def detect_gaps(con, exchange, symbol, d, interval_sec):
    dp = day_path(exchange, symbol, d)
    if not dp.exists():
        return []
    files = list(dp.glob("*.parquet"))
    if not files:
        return []
    glob = str(dp).replace("\\", "/") + "/*.parquet"
    try:
        rows = con.execute(f"""
            SELECT epoch(window_start) AS ts
            FROM read_parquet('{glob}')
            ORDER BY ts
        """).fetchall()
        gaps = []
        for i in range(1, len(rows)):
            diff = rows[i][0] - rows[i-1][0]
            if diff > interval_sec * 2:
                gaps.append((rows[i-1][0], rows[i][0], int(diff)))
        return gaps
    except Exception as e:
        return [f"ERROR: {e}"]

def fmt_ts(ts):
    from datetime import datetime, timezone
    if ts is None:
        return "N/A"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%H:%M:%S")

def fmt_dur(secs):
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        return f"{secs//60}m{secs%60:02d}s"
    else:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h{m:02d}m"

def main():
    con = duckdb.connect()

    print("=" * 90)
    print(f"DATA AUDIT — {START_DATE} to {END_DATE}  ({(END_DATE - START_DATE).days + 1} days)")
    print("=" * 90)

    all_missing_days = []
    summary_rows = []
    total_gap_count = 0

    for exchange, symbols in EXCHANGES.items():
        interval_sec  = 1 if exchange == "binance" else 60
        expected_bars = BINANCE_BARS_PER_DAY if exchange == "binance" else MINUTE_BARS_PER_DAY

        print(f"\n{'='*90}")
        print(f"EXCHANGE: {exchange.upper()}   interval={interval_sec}s   expected ~{expected_bars:,} bars/full day")
        print(f"{'='*90}")

        for symbol in symbols:
            print(f"\n  {symbol}")
            print(f"  {'Date':<12} {'Bars':>8} {'First':>9} {'Last':>9} {'Live%':>6} {'BF%':>6} {'Empt%':>6} {'Gaps':>5}  Notes")
            print(f"  {'-'*12} {'-'*8} {'-'*9} {'-'*9} {'-'*6} {'-'*6} {'-'*6} {'-'*5}  {'-'*30}")

            sym_bars = 0
            sym_gaps = 0
            sym_days = 0
            sym_missing = 0

            for d in all_dates():
                is_today = (d == TODAY)
                stats = get_day_stats(con, exchange, symbol, d)

                if stats is None:
                    sym_missing += 1
                    if not is_today:
                        all_missing_days.append((exchange, symbol, d))
                    note = "partial (today)" if is_today else "MISSING"
                    print(f"  {str(d):<12} {'—':>8} {'—':>9} {'—':>9} {'—':>6} {'—':>6} {'—':>6} {'—':>5}  {note}")
                    continue
                if isinstance(stats, str):
                    print(f"  {str(d):<12} {'ERR':>8} {'—':>9} {'—':>9} {'—':>6} {'—':>6} {'—':>6} {'—':>5}  {stats[:60]}")
                    continue

                bar_count, first_ts, last_ts, live, bf, empty, trades, src_types = stats
                sym_bars += bar_count
                sym_days += 1

                live  = live  or 0
                bf    = bf    or 0
                empty = empty or 0
                live_pct  = (live  / bar_count * 100) if bar_count else 0
                bf_pct    = (bf    / bar_count * 100) if bar_count else 0
                empty_pct = (empty / bar_count * 100) if bar_count else 0

                # Gap detection (skip today — partial)
                gaps = [] if is_today else detect_gaps(con, exchange, symbol, d, interval_sec)
                sym_gaps += len(gaps)
                total_gap_count += len(gaps)

                notes = []
                completeness = bar_count / expected_bars * 100
                if is_today:
                    notes.append("partial")
                elif completeness < 80:
                    notes.append(f"INCOMPLETE {completeness:.0f}%")
                for g in gaps[:2]:
                    if isinstance(g, str):
                        notes.append(g)
                    else:
                        notes.append(f"gap {fmt_dur(g[2])} @{fmt_ts(g[0])}")

                print(f"  {str(d):<12} {bar_count:>8,} {fmt_ts(first_ts):>9} {fmt_ts(last_ts):>9} "
                      f"{live_pct:>5.1f}% {bf_pct:>5.1f}% {empty_pct:>5.1f}% {len(gaps):>5}  "
                      f"{'; '.join(notes) if notes else 'OK'}")

            summary_rows.append({
                "exchange": exchange, "symbol": symbol,
                "days_present": sym_days, "days_missing": sym_missing,
                "total_bars": sym_bars, "total_gaps": sym_gaps,
            })

    # ── Bar interval spot-check ─────────────────────────────────────────────
    print(f"\n{'='*90}")
    print("BAR INTERVAL VERIFICATION  (50 consecutive bars from 2026-04-09)")
    print(f"{'='*90}")
    check_date = date(2026, 4, 9)
    for exchange, expected_iv in [("binance", 1), ("coinbase", 60), ("kraken", 60)]:
        symbol = list(EXCHANGES[exchange])[0]
        dp = day_path(exchange, symbol, check_date)
        if not dp.exists():
            print(f"  {exchange}/{symbol}: no data for {check_date}")
            continue
        glob = str(dp).replace("\\", "/") + "/*.parquet"
        try:
            rows = con.execute(f"""
                SELECT epoch(window_start) AS ts
                FROM read_parquet('{glob}')
                ORDER BY ts
                LIMIT 51
            """).fetchall()
            if len(rows) < 2:
                print(f"  {exchange}/{symbol}: too few rows")
                continue
            diffs = [rows[i][0] - rows[i-1][0] for i in range(1, len(rows))]
            c = Counter(diffs)
            top = c.most_common(3)
            mode = top[0][0]
            status = "OK" if mode == expected_iv else f"MISMATCH — expected {expected_iv}s"
            print(f"  {exchange:<10} {symbol:<10}  mode={mode}s ({top[0][1]} of {len(diffs)})  "
                  f"top intervals: {top}  => {status}")
        except Exception as e:
            print(f"  {exchange}/{symbol}: ERROR — {e}")

    # ── Source distribution summary ──────────────────────────────────────────
    print(f"\n{'='*90}")
    print("SOURCE DISTRIBUTION SUMMARY — BTCUSDT (all days combined)")
    print(f"{'='*90}")
    for exchange, symbol in [("binance","BTCUSDT"), ("coinbase","BTC-USD"), ("kraken","BTC-USD")]:
        sym_path = DATA_ROOT / exchange / symbol
        if not sym_path.exists():
            continue
        glob = str(sym_path).replace("\\", "/") + "/**/*.parquet"
        try:
            rows = con.execute(f"""
                SELECT source, COUNT(*) as n
                FROM read_parquet('{glob}', hive_partitioning=true)
                GROUP BY source
                ORDER BY n DESC
            """).fetchall()
            total = sum(r[1] for r in rows)
            parts = [f"{r[0]}: {r[1]:,} ({r[1]/total*100:.1f}%)" for r in rows]
            print(f"  {exchange}/{symbol}: {' | '.join(parts)}")
        except Exception as e:
            print(f"  {exchange}/{symbol}: ERROR — {e}")

    # ── Summary table ───────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"SUMMARY TABLE")
    print(f"{'='*90}")
    print(f"  {'Exchange':<10} {'Symbol':<12} {'Days':>5} {'Missing':>8} {'Total Bars':>14} {'Gaps':>6}")
    print(f"  {'-'*10} {'-'*12} {'-'*5} {'-'*8} {'-'*14} {'-'*6}")
    for r in summary_rows:
        print(f"  {r['exchange']:<10} {r['symbol']:<12} {r['days_present']:>5} "
              f"{r['days_missing']:>8} {r['total_bars']:>14,} {r['total_gaps']:>6}")

    if all_missing_days:
        print(f"\n  Missing days ({len(all_missing_days)}):")
        for exchange, symbol, d in all_missing_days:
            print(f"    {exchange}/{symbol}: {d}")
    else:
        print(f"\n  No missing days across all symbols!")

    # ── Disk usage ──────────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print("DISK USAGE")
    print(f"{'='*90}")
    parquet_bytes = sum(f.stat().st_size for f in DATA_ROOT.rglob("*.parquet"))
    jsonl_root = DATA_ROOT.parent / "jsonl"
    jsonl_bytes = sum(f.stat().st_size for f in jsonl_root.rglob("*") if f.is_file()) if jsonl_root.exists() else 0
    print(f"  Parquet total: {parquet_bytes / 1024**3:.3f} GB  ({parquet_bytes / 1024**2:.1f} MB)")
    if jsonl_bytes:
        print(f"  JSONL   total: {jsonl_bytes  / 1024**3:.3f} GB  ({jsonl_bytes  / 1024**2:.1f} MB)")
    print(f"  Grand total:   {(parquet_bytes + jsonl_bytes) / 1024**3:.3f} GB")

    print(f"\n{'='*90}")
    print("AUDIT COMPLETE")
    print(f"{'='*90}")

if __name__ == "__main__":
    main()
