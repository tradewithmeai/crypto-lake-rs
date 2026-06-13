"""Non-destructive consolidate-and-stage for the VPS migration (parallel-shardable).

Builds a staging mirror (D:/lake_ship) of LOCAL-UNIQUE lake data, consolidated
one-file-per-day (SELECT * ORDER BY window_start -> zstd parquet). Source lake is
never modified. Resumable (skips days already staged). Corruption-tolerant.

Run sharded across cores with --only:
    python _stage_ship.py --only coinbase/BTC-USD
    python _stage_ship.py --only binance            # whole exchange (prefix match)

Rules:
  - binance: only LIVE days (>50 files = the 1s history). 1m-backfill days skipped
    (VPS already holds identical 1m data; shipping would duplicate rows).
  - coinbase/kraken: all days (history unique to this machine).
  - today's partition skipped everywhere (partial here, complete on VPS).
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb

SRC = Path("D:/Documents/11Projects/crypto-lake-rs/data/parquet")
DST = Path("D:/lake_ship")
TODAY = date.today()
LIVE_THRESHOLD = 50

ap = argparse.ArgumentParser()
ap.add_argument("--only", default="", help="comma-list of exchange or exchange/symbol prefixes")
args = ap.parse_args()
prefixes = [p.strip() for p in args.only.split(",") if p.strip()]

con = duckdb.connect()
con.execute("SET threads TO 2")

staged = skipped_backfill = 0
corrupt: list[str] = []
problems: list[str] = []


def valid_parquet(path: Path) -> bool:
    try:
        with open(path, "rb") as fh:
            fh.seek(-4, 2)
            return fh.read(4) == b"PAR1"
    except OSError:
        return False


def day_date(day_dir: Path) -> date:
    y = int(day_dir.parent.parent.name.split("=")[1])
    m = int(day_dir.parent.name.split("=")[1])
    d = int(day_dir.name.split("=")[1])
    return date(y, m, d)


def sql_list(paths: list[Path]) -> str:
    return "[" + ",".join("'" + str(p).replace("\\", "/") + "'" for p in paths) + "]"


def out_rows(path: Path) -> int:
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{str(path).replace(chr(92), '/')}')"
    ).fetchone()[0]


for exchange_dir in sorted(SRC.iterdir()):
    if not exchange_dir.is_dir():
        continue
    exchange = exchange_dir.name
    for symbol_dir in sorted(exchange_dir.iterdir()):
        if not symbol_dir.is_dir():
            continue
        stream = f"{exchange}/{symbol_dir.name}"
        if prefixes and not any(stream.startswith(pfx) for pfx in prefixes):
            continue
        for day_dir in sorted(symbol_dir.glob("year=*/month=*/day=*")):
            if not day_dir.is_dir() or day_date(day_dir) >= TODAY:
                continue
            # cheap skip FIRST — a path-exists check, no directory glob — so
            # re-runs skim already-staged days instantly instead of globbing
            # thousands of files just to discard them.
            rel = day_dir.relative_to(SRC)
            out_file = DST / rel / "consolidated.parquet"
            if out_file.exists():
                staged += 1
                continue

            files = list(day_dir.glob("*.parquet"))
            if not files:
                continue
            if exchange == "binance" and len(files) <= LIVE_THRESHOLD:
                skipped_backfill += 1
                continue
            out_file.parent.mkdir(parents=True, exist_ok=True)

            good = [f for f in files if valid_parquet(f)]
            if len(good) != len(files):
                corrupt.append(f"{rel}: {len(files) - len(good)} footer-corrupt excluded")
            if not good:
                problems.append(f"{rel}: no valid files")
                continue

            tmp = out_file.with_suffix(".tmp")
            # clear any orphan temps from a prior killed/reset run so the COPY
            # can't collide with them
            tmp.unlink(missing_ok=True)
            (out_file.parent / "tmp_consolidated.tmp").unlink(missing_ok=True)

            def merge(paths: list[Path]) -> None:
                con.execute(
                    f"COPY (SELECT * FROM read_parquet({sql_list(paths)}) ORDER BY window_start) "
                    f"TO '{str(tmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION 'zstd')"
                )

            def fully_readable(f: Path) -> bool:
                # force a full data-page decode of EVERY column (tz-safe: epoch()
                # avoids a Python pytz conversion). count()/LIMIT 1 read only
                # metadata and miss corrupt data pages, so they are NOT enough.
                try:
                    con.execute(
                        "SELECT count(*), sum(open+high+low+close+volume_base), "
                        f"max(epoch(window_start)) FROM read_parquet('{str(f).replace(chr(92), '/')}')"
                    ).fetchone()
                    return True
                except Exception:
                    return False

            try:
                merge(good)
            except Exception:
                readable = [f for f in good if fully_readable(f)]
                dropped = len(good) - len(readable)
                corrupt.append(f"{rel}: {dropped} corrupt-data-page file(s) excluded ({dropped} min lost)")
                if not readable:
                    problems.append(f"{rel}: no readable files")
                    continue
                try:
                    merge(readable)
                except Exception as exc:  # noqa: BLE001
                    problems.append(f"{rel}: merge failed even after exclusion: {exc}")
                    continue

            if out_rows(tmp) == 0:
                problems.append(f"{rel}: 0 rows after merge — skipped")
                tmp.unlink(missing_ok=True)
                continue
            tmp.rename(out_file)
            staged += 1
            if staged % 50 == 0:
                print(f"  [{args.only or 'all'}] staged {staged}...", flush=True)

tag = args.only or "all"
print(f"\n[{tag}] DONE staged={staged} skipped_redundant_binance_backfill={skipped_backfill}")
if corrupt:
    print(f"[{tag}] corrupt files excluded (source untouched): {len(corrupt)} day(s)")
    for c in corrupt[:20]:
        print("  ", c)
if problems:
    print(f"[{tag}] PROBLEM DAYS: {len(problems)}")
    for p in problems[:20]:
        print("  ", p)
print(f"[{tag}] STAGE_OK")
