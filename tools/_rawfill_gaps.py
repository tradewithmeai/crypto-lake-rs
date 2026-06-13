"""Finish the migration: for Coinbase/Kraken days that did NOT consolidate
(1-2 corrupt minute-files broke the merge), RAW-COPY the original files into the
staging tree. Plain byte copy, no decode — corrupt files copied as-is; the VPS
lake_adapter quarantines them on read.

Scope: Coinbase + Kraken only.
  - binance gap days are intentionally NOT raw-filled: a 1440-file raw day would
    be skipped by backfill_only readers (the dashboard), and binance already has
    clean 1m data on the VPS; the 1s version stays in the frozen local archive.
  - today excluded; days already consolidated are skipped.
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

SRC = Path("D:/Documents/11Projects/crypto-lake-rs/data/parquet")
DST = Path("D:/lake_ship")
TODAY = date.today()
EXCHANGES = ["coinbase", "kraken"]


def dd(day_dir: Path) -> date:
    return date(
        int(day_dir.parent.parent.name.split("=")[1]),
        int(day_dir.parent.name.split("=")[1]),
        int(day_dir.name.split("=")[1]),
    )


filled = copied = 0
report = []
for ex in EXCHANGES:
    ex_dir = SRC / ex
    if not ex_dir.is_dir():
        continue
    for sym_dir in sorted(ex_dir.iterdir()):
        if not sym_dir.is_dir():
            continue
        for day_dir in sorted(sym_dir.glob("year=*/month=*/day=*")):
            if dd(day_dir) >= TODAY:
                continue
            rel = day_dir.relative_to(SRC)
            out_dir = DST / rel
            if (out_dir / "consolidated.parquet").exists():
                continue  # already consolidated cleanly
            files = list(day_dir.glob("*.parquet"))
            if not files:
                continue
            out_dir.mkdir(parents=True, exist_ok=True)
            for t in out_dir.glob("*.tmp"):  # clear orphan temps
                t.unlink()
            for f in files:
                shutil.copy2(f, out_dir / f.name)
                copied += 1
            filled += 1
            report.append(f"{rel}: raw-copied {len(files)} files")

print(f"raw-filled {filled} coinbase/kraken gap days, {copied} files copied")
for r in report:
    print(" ", r)
print("RAWFILL_DONE")
