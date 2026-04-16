#!/usr/bin/env python3
"""
archive.py — Data archive management for crypto-lake.

Commands:
  setup       Check rclone installation and remote configuration
  status      Show local vs Google Drive summary
  sync        Upload new/changed files to Google Drive (or local dest)
  consolidate Merge per-minute parquet files into per-day files
  schedule    Register/remove a Windows Task Scheduler daily sync task

Usage:
  python tools/archive.py <command> [options]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Repo root and config ────────────────────────────────────────────────────

REPO_ROOT  = Path(__file__).parent.parent
DATA_ROOT  = REPO_ROOT / "data" / "parquet"
LOG_FILE   = REPO_ROOT / "data" / "reports" / "archive_log.jsonl"
CONFIG_YML = REPO_ROOT / "config.yml"


def load_config():
    """Load config.yml; return archive section with defaults."""
    defaults = {
        "provider":               "rclone",
        "rclone_remote":          "gdrive",
        "rclone_dest":            "crypto-lake",
        "local_dest":             "",
        "sync_on_startup":        False,
        "consolidate_days_older": 7,
        "schedule_time":          "03:00",
    }
    try:
        import yaml
        with open(CONFIG_YML) as f:
            cfg = yaml.safe_load(f)
        return {**defaults, **(cfg.get("archive", {}) or {})}
    except Exception:
        return defaults


# ── rclone helpers ──────────────────────────────────────────────────────────

def rclone_exe():
    """Return path to rclone executable (tools/ first, then PATH)."""
    local = REPO_ROOT / "tools" / "rclone.exe"
    if local.exists():
        return str(local)
    found = shutil.which("rclone")
    if found:
        return found
    return None


def run_rclone(args, capture=False, check=False, **kwargs):
    """Run rclone with the given args. Returns CompletedProcess."""
    exe = rclone_exe()
    if exe is None:
        print("ERROR: rclone not found. Run tools\\setup_rclone.bat first.")
        sys.exit(1)
    cmd = [exe] + args
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    return subprocess.run(cmd, **kwargs)


def remote_path(cfg):
    """Return the rclone remote path string, e.g. 'gdrive:crypto-lake/parquet'."""
    return f"{cfg['rclone_remote']}:{cfg['rclone_dest']}/parquet"


# ── setup ───────────────────────────────────────────────────────────────────

def cmd_setup(cfg):
    print()
    print("  Crypto Lake — Archive Setup Check")
    print("  " + "=" * 40)

    # 1. rclone binary
    exe = rclone_exe()
    if exe:
        result = run_rclone(["version"], capture=True)
        ver = result.stdout.splitlines()[0] if result.stdout else "unknown"
        print(f"\n  [OK] rclone found: {exe}")
        print(f"       {ver}")
    else:
        print("\n  [!!] rclone NOT found.")
        print("       Run tools\\setup_rclone.bat to download and configure it.")
        return

    # 2. Remote configured?
    remote = cfg["rclone_remote"]
    result = run_rclone(["listremotes"], capture=True)
    remotes = [r.strip().rstrip(":") for r in result.stdout.splitlines()]
    if remote in remotes:
        print(f"\n  [OK] Remote '{remote}' is configured in rclone.")
    else:
        print(f"\n  [!!] Remote '{remote}' NOT found in rclone config.")
        print(f"       Available remotes: {remotes or '(none)'}")
        print(f"       Run tools\\setup_rclone.bat to add it.")
        return

    # 3. Can we reach the destination?
    dest = f"{remote}:{cfg['rclone_dest']}"
    print(f"\n  Checking access to {dest}...")
    result = run_rclone(["lsd", dest], capture=True)
    if result.returncode == 0:
        print(f"  [OK] Google Drive folder '{cfg['rclone_dest']}' is accessible.")
    else:
        # Folder may not exist yet — try creating it
        result2 = run_rclone(["mkdir", dest], capture=True)
        if result2.returncode == 0:
            print(f"  [OK] Created Google Drive folder '{cfg['rclone_dest']}'.")
        else:
            print(f"  [!!] Cannot access {dest}.")
            print(f"       {result.stderr.strip()}")
            return

    # 4. Local data root
    if DATA_ROOT.exists():
        parquet_files = list(DATA_ROOT.rglob("*.parquet"))
        print(f"\n  [OK] Local data: {len(parquet_files):,} parquet files in {DATA_ROOT}")
    else:
        print(f"\n  [!!] No local data found at {DATA_ROOT}")

    print()
    print("  Setup OK — ready to sync.")
    print("  Run: python tools/archive.py status")
    print()


# ── status ──────────────────────────────────────────────────────────────────

def _count_local():
    """Return (total_bytes, file_count, last_mtime) for local parquet files."""
    total = 0
    count = 0
    last_mt = 0.0
    for f in DATA_ROOT.rglob("*.parquet"):
        st = f.stat()
        total += st.st_size
        count += 1
        if st.st_mtime > last_mt:
            last_mt = st.st_mtime
    return total, count, last_mt


def _last_sync():
    """Return last sync timestamp from archive_log.jsonl, or None."""
    if not LOG_FILE.exists():
        return None
    try:
        last_line = None
        with open(LOG_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    last_line = line
        if last_line:
            rec = json.loads(last_line)
            return datetime.fromisoformat(rec["timestamp"])
    except Exception:
        pass
    return None


def _fmt_bytes(n):
    if n >= 1024**3:
        return f"{n/1024**3:.2f} GB"
    return f"{n/1024**2:.1f} MB"


def _fmt_ago(dt):
    if dt is None:
        return "never"
    delta = datetime.now(timezone.utc) - dt
    secs = int(delta.total_seconds())
    if secs < 120:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs//60}m ago"
    if secs < 86400:
        return f"{secs//3600}h ago"
    return f"{secs//86400}d ago"


def cmd_status(cfg):
    print()
    print("  Checking local data...")
    local_bytes, local_count, last_mt = _count_local()
    last_write_dt = datetime.fromtimestamp(last_mt, tz=timezone.utc) if last_mt else None
    last_sync_dt = _last_sync()

    print(f"\n  Local data:    {_fmt_bytes(local_bytes):<10}  ({local_count:,} files)  "
          f"last write: {_fmt_ago(last_write_dt)}")

    # Drive stats (may take a while)
    exe = rclone_exe()
    if exe is None:
        print("  Google Drive:  rclone not installed — run tools\\setup_rclone.bat")
        print()
        return

    remote = cfg["rclone_remote"]
    result = run_rclone(["listremotes"], capture=True)
    remotes = [r.strip().rstrip(":") for r in result.stdout.splitlines()]
    if remote not in remotes:
        print(f"  Google Drive:  remote '{remote}' not configured — run tools\\setup_rclone.bat")
        print(f"  Last sync:     {_fmt_ago(last_sync_dt)}")
        print()
        return

    rpath = remote_path(cfg)
    print(f"  Checking Google Drive ({rpath})...")

    result = run_rclone(["size", rpath, "--json"], capture=True)
    if result.returncode == 0 and result.stdout.strip():
        try:
            data = json.loads(result.stdout)
            drive_bytes = data.get("bytes", 0)
            drive_count = data.get("count", 0)
        except Exception:
            drive_bytes = drive_count = 0
    else:
        drive_bytes = drive_count = -1

    if drive_bytes >= 0:
        delta_bytes = local_bytes - drive_bytes
        delta_count = local_count - drive_count
        print(f"  Google Drive:  {_fmt_bytes(drive_bytes):<10}  ({drive_count:,} files)  "
              f"last sync:  {_fmt_ago(last_sync_dt)}")
        if delta_count > 0:
            print(f"  Delta:         {_fmt_bytes(delta_bytes):<10}  ({delta_count:,} files)  not yet synced")
        else:
            print(f"  Delta:         none — Drive is up to date")
    else:
        print(f"  Google Drive:  unable to query ({result.stderr.strip()[:80]})")
        print(f"  Last sync:     {_fmt_ago(last_sync_dt)}")

    # Drive quota
    result2 = run_rclone(["about", f"{remote}:", "--json"], capture=True)
    if result2.returncode == 0 and result2.stdout.strip():
        try:
            about = json.loads(result2.stdout)
            used  = about.get("used", 0)
            total = about.get("total", 0)
            if total:
                print(f"  Drive quota:   {_fmt_bytes(used)} of {_fmt_bytes(total)} "
                      f"({used/total*100:.1f}% used)")
        except Exception:
            pass

    print()


# ── sync ────────────────────────────────────────────────────────────────────

def _log_sync(cfg, dry_run, returncode):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "command":   "sync",
        "dry_run":   dry_run,
        "dest":      remote_path(cfg) if cfg["provider"] == "rclone" else cfg.get("local_dest", ""),
        "returncode": returncode,
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def cmd_sync(cfg, dry_run=False):
    src = str(DATA_ROOT).replace("\\", "/")
    rpath = remote_path(cfg)

    args = ["sync", src, rpath, "--progress"]
    if dry_run:
        args.append("--dry-run")
        print(f"\n  DRY RUN — would sync {src}")
        print(f"         to {rpath}")
        print()
    else:
        print(f"\n  Syncing {src}")
        print(f"       to {rpath}")
        print()

    result = run_rclone(args)

    if not dry_run:
        _log_sync(cfg, dry_run, result.returncode)

    if result.returncode == 0:
        if not dry_run:
            print("\n  Sync complete. Log written to data/reports/archive_log.jsonl")
    else:
        print(f"\n  Sync exited with code {result.returncode}.")
    print()


# ── consolidate ─────────────────────────────────────────────────────────────

def _find_consolidatable(days_older):
    """
    Yield (exchange, symbol, day_path) for partitions that:
    - are older than `days_older` days
    - have >1 parquet file
    - do not already have a single consolidated.parquet
    """
    cutoff = date.today() - timedelta(days=days_older)
    for exchange_dir in sorted(DATA_ROOT.iterdir()):
        if not exchange_dir.is_dir():
            continue
        for symbol_dir in sorted(exchange_dir.iterdir()):
            if not symbol_dir.is_dir():
                continue
            for year_dir in sorted(symbol_dir.iterdir()):
                if not year_dir.is_dir() or not year_dir.name.startswith("year="):
                    continue
                year = int(year_dir.name[5:])
                for month_dir in sorted(year_dir.iterdir()):
                    if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                        continue
                    month = int(month_dir.name[6:])
                    for day_dir in sorted(month_dir.iterdir()):
                        if not day_dir.is_dir() or not day_dir.name.startswith("day="):
                            continue
                        day = int(day_dir.name[4:])
                        try:
                            partition_date = date(year, month, day)
                        except ValueError:
                            continue
                        if partition_date >= cutoff:
                            continue
                        files = list(day_dir.glob("*.parquet"))
                        if len(files) <= 1:
                            continue  # already consolidated or empty
                        yield exchange_dir.name, symbol_dir.name, day_dir


def cmd_consolidate(cfg, dry_run=False, days_override=None):
    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb not installed. Run: pip install duckdb")
        sys.exit(1)

    days_older = days_override if days_override is not None else cfg["consolidate_days_older"]
    cutoff = date.today() - timedelta(days=days_older)

    print(f"\n  Consolidating partitions older than {days_older} days (before {cutoff})")
    if dry_run:
        print("  DRY RUN — no files will be changed")
    print()

    targets = list(_find_consolidatable(days_older))
    if not targets:
        print("  Nothing to consolidate.")
        print()
        return

    total_before = 0
    total_after  = 0
    bytes_before = 0
    bytes_after  = 0
    processed    = 0

    con = duckdb.connect()

    for exchange, symbol, day_dir in targets:
        files = list(day_dir.glob("*.parquet"))
        n = len(files)
        size_before = sum(f.stat().st_size for f in files)

        out_file = day_dir / "consolidated.parquet"
        glob     = str(day_dir).replace("\\", "/") + "/*.parquet"

        if dry_run:
            print(f"  Would consolidate {exchange}/{symbol}/{day_dir.parent.parent.name}/"
                  f"{day_dir.parent.name}/{day_dir.name}  ({n} files, {_fmt_bytes(size_before)})")
            total_before += n
            total_after  += 1
            bytes_before += size_before
            continue

        try:
            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{glob}')
                    ORDER BY window_start
                ) TO '{str(out_file).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION 'zstd')
            """)
            size_after = out_file.stat().st_size

            # Delete originals (not the consolidated file we just wrote)
            for f in files:
                if f.name != "consolidated.parquet":
                    f.unlink()

            total_before += n
            total_after  += 1
            bytes_before += size_before
            bytes_after  += size_after
            processed    += 1
            print(f"  {exchange}/{symbol}/{day_dir.parent.parent.name}/"
                  f"{day_dir.parent.name}/{day_dir.name}  "
                  f"{n} -> 1 file  ({_fmt_bytes(size_before)} -> {_fmt_bytes(size_after)})")
        except Exception as e:
            print(f"  ERROR in {day_dir}: {e}")

    con.close()

    print()
    if dry_run:
        print(f"  Would consolidate {len(targets)} partitions: "
              f"{total_before:,} files -> {total_after:,} files  "
              f"(estimated {_fmt_bytes(bytes_before)} input)")
    else:
        saved = bytes_before - bytes_after
        print(f"  Consolidated {processed} partitions: "
              f"{total_before:,} -> {total_after:,} files  "
              f"({_fmt_bytes(bytes_before)} -> {_fmt_bytes(bytes_after)}, "
              f"saved {_fmt_bytes(saved)})")
    print()


# ── schedule ────────────────────────────────────────────────────────────────

TASK_NAME = "CryptoLakeArchiveSync"


def cmd_schedule(cfg, remove=False):
    python_exe = sys.executable
    script     = str(REPO_ROOT / "tools" / "archive.py").replace("/", "\\")
    time_str   = cfg.get("schedule_time", "03:00")

    if remove:
        print(f"\n  Removing scheduled task '{TASK_NAME}'...")
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  [OK] Task removed.")
        else:
            print(f"  [!!] {result.stderr.strip() or result.stdout.strip()}")
        print()
        return

    # Parse HH:MM
    parts = time_str.split(":")
    hour, minute = (int(parts[0]), int(parts[1])) if len(parts) == 2 else (3, 0)
    time_fmt = f"{hour:02d}:{minute:02d}"

    cmd_str = f'"{python_exe}" "{script}" sync'

    print(f"\n  Scheduling daily sync at {time_fmt} via Windows Task Scheduler...")
    result = subprocess.run([
        "schtasks", "/Create", "/F",
        "/TN", TASK_NAME,
        "/TR", cmd_str,
        "/SC", "DAILY",
        "/ST", time_fmt,
        "/RL", "HIGHEST",
    ], capture_output=True, text=True)

    if result.returncode == 0:
        print(f"  [OK] Task '{TASK_NAME}' created — runs daily at {time_fmt}.")
        print(f"       Command: {cmd_str}")
        print(f"\n  To remove: python tools/archive.py schedule --remove")
    else:
        print(f"  [!!] Failed to create task:")
        print(f"       {result.stderr.strip() or result.stdout.strip()}")
    print()


# ── CLI entry point ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="archive.py",
        description="Crypto Lake data archive manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", metavar="command")

    sub.add_parser("setup",  help="Verify rclone and remote configuration")
    sub.add_parser("status", help="Show local vs Google Drive summary")

    p_sync = sub.add_parser("sync", help="Upload new/changed files to Google Drive")
    p_sync.add_argument("--dry-run", action="store_true", help="Show what would be uploaded")

    p_cons = sub.add_parser("consolidate", help="Merge per-minute parquet files into per-day")
    p_cons.add_argument("--dry-run", action="store_true", help="Show what would be changed")
    p_cons.add_argument("--days",    type=int, default=None,
                        help="Consolidate partitions older than N days (default: from config)")

    p_sched = sub.add_parser("schedule", help="Register/remove Windows Task Scheduler daily sync")
    p_sched.add_argument("--remove", action="store_true", help="Remove the scheduled task")

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        sys.exit(0)

    cfg = load_config()

    if args.command == "setup":
        cmd_setup(cfg)
    elif args.command == "status":
        cmd_status(cfg)
    elif args.command == "sync":
        cmd_sync(cfg, dry_run=args.dry_run)
    elif args.command == "consolidate":
        cmd_consolidate(cfg, dry_run=args.dry_run, days_override=args.days)
    elif args.command == "schedule":
        cmd_schedule(cfg, remove=args.remove)


if __name__ == "__main__":
    main()
