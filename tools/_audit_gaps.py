"""Audit: for each source day-dir in an exchange, is it staged? If not, why?

Classifies every un-staged day as one of:
  TODAY          - intentionally excluded (partial locally, complete on VPS)
  NO_FILES       - source day-dir exists but is empty (real gap in source)
  ALL_CORRUPT    - has files but none pass the PAR1 footer check (real loss)
  HAS_VALID      - has readable files but wasn't staged (BUG — should not happen)
Prints counts + the offending days so nothing is hand-waved.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

SRC = Path("D:/Documents/11Projects/crypto-lake-rs/data/parquet")
DST = Path("D:/lake_ship")
TODAY = date.today()
exchanges = sys.argv[1:] or ["coinbase", "kraken"]


def par1(p: Path) -> bool:
    try:
        with open(p, "rb") as fh:
            fh.seek(-4, 2)
            return fh.read(4) == b"PAR1"
    except OSError:
        return False


def dd(day_dir: Path) -> date:
    return date(
        int(day_dir.parent.parent.name.split("=")[1]),
        int(day_dir.parent.name.split("=")[1]),
        int(day_dir.name.split("=")[1]),
    )


for ex in exchanges:
    exdir = SRC / ex
    if not exdir.is_dir():
        continue
    for sym in sorted(exdir.iterdir()):
        if not sym.is_dir():
            continue
        src_days = sorted(sym.glob("year=*/month=*/day=*"))
        staged = total = 0
        buckets = {"TODAY": [], "NO_FILES": [], "ALL_CORRUPT": [], "HAS_VALID": []}
        for day_dir in src_days:
            total += 1
            rel = day_dir.relative_to(SRC)
            if (DST / rel / "consolidated.parquet").exists():
                staged += 1
                continue
            if dd(day_dir) >= TODAY:
                buckets["TODAY"].append(day_dir.name)
                continue
            files = list(day_dir.glob("*.parquet"))
            if not files:
                buckets["NO_FILES"].append(rel)
            elif not any(par1(f) for f in files):
                buckets["ALL_CORRUPT"].append(f"{rel} ({len(files)} files)")
            else:
                buckets["HAS_VALID"].append(f"{rel} ({len(files)} files)")
        miss = {k: v for k, v in buckets.items() if v}
        flag = "  <-- INVESTIGATE" if buckets["HAS_VALID"] else ""
        print(f"{ex}/{sym.name}: {staged}/{total} staged{flag}")
        for k, v in miss.items():
            print(f"    {k} ({len(v)}): {v}")
