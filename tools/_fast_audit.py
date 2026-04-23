#!/usr/bin/env python3
"""
Fast incremental audit — Apr 11–Apr 22 (new days since last full audit).
Uses DuckDB window functions for gap detection (no Python-side row fetching).
"""
import sys
import duckdb
from pathlib import Path
from datetime import date, timedelta

DATA_ROOT = Path(__file__).parent.parent / "data" / "parquet"
START_DATE = date(2026, 4, 11)
END_DATE   = date(2026, 4, 22)

EXCHANGES = {
    "binance":  (1,  sorted(["ADAUSDT","AVAXUSDT","BNBUSDT","BTCUSDT","DOGEUSDT",
                              "DOTUSDT","ETHUSDT","EURUSDT","LINKUSDT","LTCUSDT",
                              "SOLUSDT","SUIUSDT","XRPUSDT"])),
    "coinbase": (60, sorted(["BTC-USD","ETH-USD","SOL-USD"])),
    "kraken":   (60, sorted(["BTC-USD","ETH-USD","SOL-USD"])),
}

EXPECTED = {1: 86400, 60: 86400}  # both use 1s bars from WS

def day_glob(exchange, symbol, d):
    dp = DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
    return str(dp).replace("\\", "/") + "/*.parquet" if dp.exists() else None

def all_dates():
    d = START_DATE
    while d <= END_DATE:
        yield d
        d += timedelta(days=1)

W = 90
con = duckdb.connect()
con.execute("SET memory_limit='2GB'")
con.execute("SET threads=2")

print("=" * W)
print(f"INCREMENTAL AUDIT — {START_DATE} to {END_DATE}  ({(END_DATE-START_DATE).days+1} days)")
print("=" * W)

summary_rows = []
total_gaps = 0

for exchange, (interval_sec, symbols) in EXCHANGES.items():
    exp = EXPECTED[interval_sec]
    print()
    print("=" * W)
    print(f"EXCHANGE: {exchange.upper()}   interval=1s   expected ~{exp:,} bars/full day")
    print("=" * W)

    for symbol in symbols:
        print(f"\n  {symbol}")
        print(f"  {'Date':12} {'Bars':>8} {'First':>9} {'Last':>9} {'Live%':>6} {'Empt%':>6} {'Gaps':>5}  Notes")
        print(f"  {'-'*12} {'-'*8} {'-'*9} {'-'*9} {'-'*6} {'-'*6} {'-'*5}  -----")

        sym_gaps = 0
        missing = 0
        for d in all_dates():
            g = day_glob(exchange, symbol, d)
            if not g:
                print(f"  {str(d):12} {'MISSING':>8}")
                missing += 1
                continue

            try:
                # Bar counts + source breakdown
                row = con.execute(f"""
                    SELECT
                        COUNT(*)                                                   AS bars,
                        MIN(epoch(window_start))                                   AS first_ts,
                        MAX(epoch(window_start))                                   AS last_ts,
                        100.0 * SUM(CASE WHEN source='live'  THEN 1 ELSE 0 END) / COUNT(*) AS live_pct,
                        100.0 * SUM(CASE WHEN source='empty' THEN 1 ELSE 0 END) / COUNT(*) AS empt_pct
                    FROM read_parquet('{g}')
                """).fetchone()

                bars, first_ts, last_ts, live_pct, empt_pct = row
                from datetime import datetime, timezone
                def ts(t):
                    if t is None: return "N/A"
                    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%H:%M:%S")

                # Gap detection via window function — no Python row fetch
                gap_rows = con.execute(f"""
                    WITH ordered AS (
                        SELECT epoch(window_start) AS t
                        FROM read_parquet('{g}')
                        ORDER BY t
                    ),
                    diffs AS (
                        SELECT t, t - LAG(t) OVER (ORDER BY t) AS diff
                        FROM ordered
                    )
                    SELECT t - diff AS gap_start, t AS gap_end, CAST(diff AS INTEGER) AS gap_secs
                    FROM diffs
                    WHERE diff > {interval_sec * 2}
                    ORDER BY gap_start
                """).fetchall()

                n_gaps = len(gap_rows)
                sym_gaps += n_gaps
                total_gaps += n_gaps

                is_partial = d == END_DATE or (bars < exp * 0.95 and d < END_DATE and last_ts and
                    datetime.fromtimestamp(last_ts, tz=timezone.utc).hour < 23)

                notes = []
                if is_partial and d == END_DATE:
                    notes.append("partial")
                for gs, ge, gsecs in gap_rows[:3]:
                    h = gsecs // 3600; m = (gsecs%3600)//60; s = gsecs%60
                    dur = f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"
                    notes.append(f"gap {dur} @{ts(gs)}")
                if n_gaps > 3:
                    notes.append(f"+{n_gaps-3} more gaps")
                note_str = "; ".join(notes) if notes else "OK"

                print(f"  {str(d):12} {bars:>8,} {ts(first_ts):>9} {ts(last_ts):>9} "
                      f"{live_pct:>5.1f}% {empt_pct:>5.1f}% {n_gaps:>5}  {note_str}")

            except Exception as e:
                print(f"  {str(d):12} ERROR: {e}")

        summary_rows.append((exchange, symbol, (END_DATE-START_DATE).days+1, missing, sym_gaps))

print()
print("=" * W)
print("SUMMARY")
print("=" * W)
print(f"  {'Exchange':10} {'Symbol':14} {'Days':>5} {'Missing':>8} {'Gaps':>6}")
print(f"  {'-'*10} {'-'*14} {'-'*5} {'-'*8} {'-'*6}")
for exc, sym, days, miss, gaps in summary_rows:
    print(f"  {exc:10} {sym:14} {days:>5} {miss:>8} {gaps:>6}")

print()
miss_total = sum(r[3] for r in summary_rows)
if miss_total == 0:
    print("  No missing days.")
else:
    print(f"  WARNING: {miss_total} missing day-partitions found.")
print(f"  Total gaps detected: {total_gaps}")
print()
print("=" * W)
print("AUDIT COMPLETE")
print("=" * W)
