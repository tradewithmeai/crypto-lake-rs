#!/usr/bin/env python3
"""Quick OHLCV sanity check and recent bar count verification."""
import duckdb
from pathlib import Path
from datetime import date

DATA_ROOT = Path(__file__).parent.parent / "data" / "parquet"
con = duckdb.connect()

CHECK_DATE = date(2026, 4, 22)

EXCHANGES = {
    "binance":  ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"],
    "coinbase": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "kraken":   ["BTC-USD", "ETH-USD", "SOL-USD"],
}

print(f"=== OHLCV SANITY CHECK — {CHECK_DATE} ===")
print(f"  {'exchange':10} {'symbol':12} {'bars':>8}  {'open range':>24}  {'min_vol':>10}  status")
print(f"  {'-'*10} {'-'*12} {'-'*8}  {'-'*24}  {'-'*10}  ------")

all_ok = True
for exchange, symbols in EXCHANGES.items():
    for symbol in symbols:
        dp = DATA_ROOT / exchange / symbol / f"year={CHECK_DATE.year}" / f"month={CHECK_DATE.month:02d}" / f"day={CHECK_DATE.day:02d}"
        if not dp.exists():
            print(f"  {'MISSING':10} {exchange}/{symbol}")
            all_ok = False
            continue
        glob = str(dp).replace("\\", "/") + "/*.parquet"
        try:
            row = con.execute(f"""
                SELECT
                    COUNT(*)                                                          AS total,
                    SUM(CASE WHEN high < low THEN 1 ELSE 0 END)                      AS high_lt_low,
                    SUM(CASE WHEN high < open  AND source='live' THEN 1 ELSE 0 END)  AS high_lt_open,
                    SUM(CASE WHEN high < close AND source='live' THEN 1 ELSE 0 END)  AS high_lt_close,
                    SUM(CASE WHEN low  > open  AND source='live' THEN 1 ELSE 0 END)  AS low_gt_open,
                    SUM(CASE WHEN low  > close AND source='live' THEN 1 ELSE 0 END)  AS low_gt_close,
                    SUM(CASE WHEN volume_base < 0 THEN 1 ELSE 0 END)                 AS neg_vol,
                    SUM(CASE WHEN open <= 0 AND source='live' THEN 1 ELSE 0 END)     AS zero_open,
                    MIN(open)        FILTER (WHERE source='live')                     AS min_open,
                    MAX(open)        FILTER (WHERE source='live')                     AS max_open,
                    MIN(volume_base) FILTER (WHERE source='live')                     AS min_vol
                FROM read_parquet('{glob}')
            """).fetchone()
            issues = []
            if row[1]: issues.append(f"high<low:{row[1]}")
            if row[2]: issues.append(f"high<open:{row[2]}")
            if row[3]: issues.append(f"high<close:{row[3]}")
            if row[4]: issues.append(f"low>open:{row[4]}")
            if row[5]: issues.append(f"low>close:{row[5]}")
            if row[6]: issues.append(f"neg_vol:{row[6]}")
            if row[7]: issues.append(f"zero_open:{row[7]}")
            if issues:
                all_ok = False
            status = "OK" if not issues else "FAIL: " + ", ".join(issues)
            pr = f"[{row[8]:.2f} – {row[9]:.2f}]" if row[8] else "N/A"
            vmin = f"{row[10]:.6f}" if row[10] is not None else "N/A"
            print(f"  {exchange:10} {symbol:12} {row[0]:>8,}  {pr:>24}  {vmin:>10}  {status}")
        except Exception as e:
            print(f"  ERROR {exchange}/{symbol}: {e}")
            all_ok = False

print()
print("=== RECENT BAR COUNTS — binance/BTCUSDT ===")
for d in [date(2026,4,19), date(2026,4,20), date(2026,4,21), date(2026,4,22)]:
    dp = DATA_ROOT / "binance" / "BTCUSDT" / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
    if not dp.exists():
        print(f"  {d}: MISSING")
        continue
    glob = str(dp).replace("\\", "/") + "/*.parquet"
    row = con.execute(f"""
        SELECT COUNT(*),
               SUM(CASE WHEN source='live'  THEN 1 ELSE 0 END),
               SUM(CASE WHEN source='empty' THEN 1 ELSE 0 END)
        FROM read_parquet('{glob}')
    """).fetchone()
    live_pct = 100*row[1]/row[0] if row[0] else 0
    print(f"  {d}: {row[0]:,} bars  live={row[1]:,} ({live_pct:.1f}%)  empty={row[2]:,}")

print()
print("=== SCHEMA COLUMNS (binance/BTCUSDT sample) ===")
dp = DATA_ROOT / "binance" / "BTCUSDT" / "year=2026" / "month=04" / "day=22"
glob = str(dp).replace("\\", "/") + "/*.parquet"
cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}') LIMIT 1").fetchall()
for c in cols:
    print(f"  {c[0]:25} {c[1]}")

print()
print("RESULT:", "ALL OK" if all_ok else "ISSUES FOUND — see above")
