"""
lake.py — shared utilities for crypto-lake-rs Jupyter analysis.

Usage in a notebook:
    import lake
    con = lake.connect()
    df = lake.query_symbol(con, "binance", "BTCUSDT", "2026-04-01", "2026-04-10")
"""

from __future__ import annotations
import sys
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from typing import Optional
import duckdb
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
_HERE       = Path(__file__).parent
REPO_ROOT   = _HERE.parent
DATA_ROOT   = REPO_ROOT / "data" / "parquet"

# ── Exchange metadata ─────────────────────────────────────────────────────────
EXCHANGE_META = {
    "binance":  {"interval_sec": 1,  "symbols": ["BTCUSDT","ETHUSDT","SOLUSDT","SUIUSDT",
                                                   "ADAUSDT","BNBUSDT","XRPUSDT","DOGEUSDT",
                                                   "AVAXUSDT","LINKUSDT","LTCUSDT","DOTUSDT","EURUSDT"]},
    "coinbase": {"interval_sec": 60, "symbols": ["BTC-USD","ETH-USD","SOL-USD"]},
    "kraken":   {"interval_sec": 60, "symbols": ["BTC-USD","ETH-USD","SOL-USD"]},
}

# ── DuckDB connection ─────────────────────────────────────────────────────────
def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB in-process connection. thread_safety=1 (multi-reader fine)."""
    con = duckdb.connect()
    return con


# ── Path helpers ──────────────────────────────────────────────────────────────
def sym_root(exchange: str, symbol: str) -> Path:
    return DATA_ROOT / exchange / symbol


def day_glob(exchange: str, symbol: str, d: date) -> str:
    """Return the parquet glob pattern for a single day."""
    p = DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
    return str(p).replace("\\", "/") + "/*.parquet"


def date_range_glob(exchange: str, symbol: str) -> str:
    """Return a hive-partitioned glob for the whole symbol tree."""
    p = DATA_ROOT / exchange / symbol
    return str(p).replace("\\", "/") + "/**/*.parquet"


# ── Core query ────────────────────────────────────────────────────────────────
def query_symbol(
    con: duckdb.DuckDBPyConnection,
    exchange: str,
    symbol: str,
    start: str | date | None = None,
    end:   str | date | None = None,
    cols: str = "*",
) -> pd.DataFrame:
    """
    Load bars for an exchange/symbol between start and end (inclusive, UTC dates).
    Returns a pandas DataFrame sorted by window_start.

    start/end: "YYYY-MM-DD" string or date object. None = no filter.
    """
    # Build per-day path list to avoid full recursive scan when dates given
    if start is not None and end is not None:
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)
        paths = []
        d = start
        while d <= end:
            p = DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
            if p.exists() and list(p.glob("*.parquet")):
                paths.append(str(p).replace("\\", "/") + "/*.parquet")
            d += timedelta(days=1)
        if not paths:
            return pd.DataFrame()
        glob_list = "[" + ",".join(f"'{p}'" for p in paths) + "]"
        sql = f"SELECT {cols} FROM read_parquet({glob_list}) ORDER BY window_start"
    else:
        sym_p = DATA_ROOT / exchange / symbol
        if not sym_p.exists():
            return pd.DataFrame()
        glob = str(sym_p).replace("\\", "/") + "/**/*.parquet"
        sql = f"SELECT {cols} FROM read_parquet('{glob}', hive_partitioning=true) ORDER BY window_start"

    df = con.execute(sql).df()
    if not df.empty and "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        df = df.set_index("window_start")
    return df


# ── Completeness check ────────────────────────────────────────────────────────
def completeness_report(
    con: duckdb.DuckDBPyConnection,
    exchange: str,
    symbol: str,
    start: str | date,
    end: str | date,
) -> pd.DataFrame:
    """
    Return a per-day summary DataFrame:
      date | bar_count | live | backfill | empty | gaps | first | last | completeness_pct
    """
    if isinstance(start, str):
        start = date.fromisoformat(start)
    if isinstance(end, str):
        end = date.fromisoformat(end)

    interval_sec = EXCHANGE_META[exchange]["interval_sec"]
    expected = 86400 // interval_sec  # bars per full day

    rows = []
    d = start
    while d <= end:
        dp = DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
        if not dp.exists() or not list(dp.glob("*.parquet")):
            rows.append({"date": d, "bar_count": 0, "live": 0, "backfill": 0,
                          "empty": 0, "gaps": None, "first": None, "last": None,
                          "completeness_pct": 0.0, "status": "MISSING"})
            d += timedelta(days=1)
            continue
        glob = str(dp).replace("\\", "/") + "/*.parquet"
        r = con.execute(f"""
            SELECT
                COUNT(*)                                           AS bar_count,
                MIN(epoch(window_start))                           AS first_ts,
                MAX(epoch(window_start))                           AS last_ts,
                COALESCE(SUM(CASE WHEN source='live'          THEN 1 END),0) AS live,
                COALESCE(SUM(CASE WHEN source LIKE 'backfill%' THEN 1 END),0) AS backfill,
                COALESCE(SUM(CASE WHEN source='empty'          THEN 1 END),0) AS empty
            FROM read_parquet('{glob}')
        """).fetchone()
        bar_count, first_ts, last_ts, live, bf, empty = r

        # Gap count
        ts_rows = con.execute(f"""
            SELECT epoch(window_start) FROM read_parquet('{glob}') ORDER BY 1
        """).fetchall()
        gaps = sum(1 for i in range(1, len(ts_rows))
                   if (ts_rows[i][0] - ts_rows[i-1][0]) > interval_sec * 2)

        def _fmt(ts):
            if ts is None:
                return None
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")

        is_today = d == date.today()
        pct = bar_count / expected * 100
        rows.append({
            "date": d,
            "bar_count": bar_count,
            "live": live,
            "backfill": bf,
            "empty": empty,
            "gaps": gaps,
            "first": _fmt(first_ts),
            "last": _fmt(last_ts),
            "completeness_pct": round(pct, 1),
            "status": "partial" if is_today else ("OK" if pct >= 90 else "LOW"),
        })
        d += timedelta(days=1)

    return pd.DataFrame(rows).set_index("date")


# ── Gap analysis ──────────────────────────────────────────────────────────────
def find_gaps(
    con: duckdb.DuckDBPyConnection,
    exchange: str,
    symbol: str,
    start: str | date,
    end: str | date,
    min_gap_sec: int | None = None,
) -> pd.DataFrame:
    """
    Return all time gaps in the data larger than 2× the expected interval
    (or min_gap_sec if specified).
    Columns: gap_start | gap_end | duration_sec | duration_human
    """
    if isinstance(start, str):
        start = date.fromisoformat(start)
    if isinstance(end, str):
        end = date.fromisoformat(end)

    interval_sec = EXCHANGE_META[exchange]["interval_sec"]
    threshold    = min_gap_sec if min_gap_sec is not None else interval_sec * 2

    paths = []
    d = start
    while d <= end:
        p = DATA_ROOT / exchange / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"
        if p.exists() and list(p.glob("*.parquet")):
            paths.append(str(p).replace("\\", "/") + "/*.parquet")
        d += timedelta(days=1)

    if not paths:
        return pd.DataFrame()

    glob_list = "[" + ",".join(f"'{p}'" for p in paths) + "]"
    ts_rows = con.execute(f"""
        SELECT epoch(window_start) FROM read_parquet({glob_list}) ORDER BY 1
    """).fetchall()

    gap_rows = []
    for i in range(1, len(ts_rows)):
        diff = int(ts_rows[i][0] - ts_rows[i-1][0])
        if diff > threshold:
            t0 = datetime.fromtimestamp(ts_rows[i-1][0], tz=timezone.utc)
            t1 = datetime.fromtimestamp(ts_rows[i][0],   tz=timezone.utc)
            gap_rows.append({
                "gap_start": t0, "gap_end": t1,
                "duration_sec": diff,
                "duration_human": _fmt_dur(diff),
            })

    return pd.DataFrame(gap_rows)


def _fmt_dur(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        return f"{secs//60}m {secs%60:02d}s"
    elif secs < 86400:
        h, rem = divmod(secs, 3600)
        m = rem // 60
        return f"{h}h {m:02d}m"
    else:
        d, rem = divmod(secs, 86400)
        h = rem // 3600
        return f"{d}d {h:02d}h"


# ── Source distribution ───────────────────────────────────────────────────────
def source_distribution(
    con: duckdb.DuckDBPyConnection,
    exchange: str,
    symbol: str,
    start: str | date | None = None,
    end:   str | date | None = None,
) -> pd.DataFrame:
    """Return bar counts by source type."""
    df = query_symbol(con, exchange, symbol, start, end, cols="source")
    if df.empty:
        return pd.DataFrame()
    return df["source"].value_counts().rename_axis("source").reset_index(name="count")


# ── Convenience display ───────────────────────────────────────────────────────
def print_completeness(df: pd.DataFrame, title: str = "") -> None:
    if title:
        print(f"\n{'='*70}\n{title}\n{'='*70}")
    print(df.to_string())


# ── Quick summary for all symbols ────────────────────────────────────────────
def full_audit_summary(
    con: duckdb.DuckDBPyConnection,
    start: str | date,
    end: str | date,
) -> pd.DataFrame:
    """
    One-row-per-symbol summary across all exchanges:
      exchange | symbol | days_present | total_bars | total_gaps | live_pct | bf_pct
    """
    if isinstance(start, str): start = date.fromisoformat(start)
    if isinstance(end,   str): end   = date.fromisoformat(end)

    rows = []
    for exchange, meta in EXCHANGE_META.items():
        for symbol in meta["symbols"]:
            df = completeness_report(con, exchange, symbol, start, end)
            total_bars = int(df["bar_count"].sum())
            total_gaps = int(df["gaps"].fillna(0).sum())
            days_pres  = int((df["bar_count"] > 0).sum())
            live_total = int(df["live"].sum())
            bf_total   = int(df["backfill"].sum())
            live_pct   = round(live_total / total_bars * 100, 1) if total_bars else 0
            bf_pct     = round(bf_total   / total_bars * 100, 1) if total_bars else 0
            rows.append({
                "exchange": exchange, "symbol": symbol,
                "days_present": days_pres, "total_bars": total_bars,
                "total_gaps": total_gaps, "live_pct": live_pct, "backfill_pct": bf_pct,
            })

    return pd.DataFrame(rows)
