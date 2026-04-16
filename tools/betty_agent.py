#!/usr/bin/env python3
"""
betty_agent.py — Standalone Betty Sentinel agent for Crypto Lake.

Runs as a daemon alongside the Rust app.  Every `interval_sec` seconds it:
  1. Reads data freshness from the most recently modified parquet file
  2. Reads live metrics from data/reports/health.json
  3. POSTs a signed heartbeat to Betty's /ingest/heartbeat
  4. POSTs a signed service-state to Betty's /ingest/service-state

Usage:
  python tools/betty_agent.py [--config config.yml]

Stop cleanly with Ctrl-C or SIGTERM.  The current iteration completes first.
"""

import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install httpx")
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT    = Path(__file__).parent.parent
DATA_ROOT    = REPO_ROOT / "data" / "parquet"
HEALTH_FILE  = REPO_ROOT / "data" / "reports" / "health.json"
SEQ_FILE     = REPO_ROOT / "data" / "reports" / "betty_seq.json"

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [betty] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("betty_agent")


# ── Signing ──────────────────────────────────────────────────────────────────

def _json_default(obj):
    if isinstance(obj, datetime):
        utc = obj.astimezone(timezone.utc)
        return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond:06d}Z"
    raise TypeError(f"Not serialisable: {type(obj)}")


def _canonical(payload: dict) -> bytes:
    body = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"),
                      default=_json_default).encode("utf-8")


def compute_signature(payload: dict, secret_bytes: bytes) -> str:
    return hmac.new(secret_bytes, _canonical(payload), hashlib.sha256).hexdigest()


# ── Data freshness ───────────────────────────────────────────────────────────

def _latest_parquet_mtime() -> float | None:
    """Return mtime (epoch seconds) of the most recently modified parquet file."""
    latest = None
    try:
        for f in DATA_ROOT.rglob("*.parquet"):
            mt = f.stat().st_mtime
            if latest is None or mt > latest:
                latest = mt
    except Exception:
        pass
    return latest


def _read_health_json() -> dict:
    """Read data/reports/health.json; return {} on any error."""
    try:
        with open(HEALTH_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def measure_freshness(stale_threshold_sec: int):
    """
    Returns (status, last_data_utc_str | None, metrics dict).
    status: one of "ok", "stale", "error", "unknown"
    """
    mtime = _latest_parquet_mtime()
    health = _read_health_json()

    now = time.time()
    metrics = {}

    if mtime is None:
        # No parquet files at all — data path empty or app never ran
        status        = "unknown"
        last_data_str = None
    else:
        age_sec = now - mtime
        last_data_dt  = datetime.fromtimestamp(mtime, tz=timezone.utc)
        last_data_str = last_data_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{last_data_dt.microsecond:06d}Z"
        metrics["last_write_age_seconds"] = round(age_sec, 1)

        if age_sec > stale_threshold_sec:
            status = "stale"
        else:
            status = "ok"

    # Enrich from health.json if available
    counters = health.get("counters", {})
    collector = health.get("collector", {})

    if "symbols_count" in collector:
        metrics["symbols_active"] = collector["symbols_count"]
    if "bars_produced" in counters:
        metrics["bars_produced"] = counters["bars_produced"]
    if "uptime_seconds" in collector:
        metrics["uptime_seconds"] = collector["uptime_seconds"]
    if "ws_reconnects" in counters:
        metrics["ws_reconnects"] = counters["ws_reconnects"]

    # If health.json exists but is very old, downgrade to stale regardless
    if health and status == "ok":
        health_ts_str = health.get("ts_utc")
        if health_ts_str:
            try:
                health_ts = datetime.fromisoformat(health_ts_str.replace("Z", "+00:00"))
                health_age = now - health_ts.timestamp()
                if health_age > stale_threshold_sec * 2:
                    status = "stale"
            except Exception:
                pass

    return status, last_data_str, metrics


# ── BettyAgent ───────────────────────────────────────────────────────────────

class BettyAgent:
    def __init__(self, config: dict):
        self.betty_url   = config["url"].rstrip("/")
        self.agent_id    = config["agent_id"]
        self.secret      = bytes.fromhex(config["secret_hex"])
        self.host_id     = socket.gethostname()
        self._client     = httpx.Client(timeout=10.0)

    def close(self):
        self._client.close()

    # ── Sequence numbers ──────────────────────────────────────────────────────

    def _next_sequence(self) -> int:
        """
        Return next sequence number.  Persists to SEQ_FILE across restarts.
        Falls back to int(time.time()) if the file is unreadable.
        """
        SEQ_FILE.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(SEQ_FILE) as f:
                data = json.load(f)
            seq = int(data.get("seq", 0)) + 1
        except Exception:
            seq = int(time.time())

        try:
            with open(SEQ_FILE, "w") as f:
                json.dump({"seq": seq}, f)
        except Exception as e:
            log.warning("Could not persist sequence number: %s", e)

        return seq

    # ── Signing ───────────────────────────────────────────────────────────────

    def _sign(self, payload: dict) -> dict:
        sig = compute_signature(payload, self.secret)
        return {**payload, "signature": sig}

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def send_heartbeat(self) -> bool:
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"

        payload = self._sign({
            "event_type":       "agent_heartbeat",
            "schema_version":   "1.0",
            "agent_id":         self.agent_id,
            "host_id":          self.host_id,
            "environment":      "production",
            "bridge_version":   "1.0.0",
            "ts_utc":           ts,
            "sequence_number":  self._next_sequence(),
            "services_summary": {},
            "system_summary":   {},
        })

        return self._post("/ingest/heartbeat", payload)

    # ── Service state ─────────────────────────────────────────────────────────

    def send_service_state(self, last_data_utc: str | None, status: str, metrics: dict) -> bool:
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"

        payload: dict = {
            "event_type":       "service_state",
            "schema_version":   "1.0",
            "agent_id":         self.agent_id,
            "service_name":     "crypto-lake",
            "status":           status,
            "metrics_summary":  metrics,
            "ts_utc":           ts,
            "sequence_number":  self._next_sequence(),
        }

        if last_data_utc is not None:
            payload["last_data_utc"] = last_data_utc

        payload = self._sign(payload)
        return self._post("/ingest/service-state", payload)

    # ── HTTP helper ───────────────────────────────────────────────────────────

    def _post(self, path: str, payload: dict) -> bool:
        url = self.betty_url + path
        try:
            resp = self._client.post(url, json=payload)
            if resp.status_code == 202:
                return True
            log.warning("Betty %s returned %d: %s", path, resp.status_code, resp.text[:200])
            return False
        except Exception as e:
            log.warning("Betty %s unreachable: %s", path, e)
            return False


# ── Main loop ────────────────────────────────────────────────────────────────

def run_loop(cfg: dict):
    interval        = int(cfg.get("interval_sec", 60))
    stale_threshold = int(cfg.get("stale_threshold_sec", 300))

    agent = BettyAgent(cfg)
    log.info("Betty agent started — agent_id=%s  betty=%s  interval=%ds",
             cfg["agent_id"], cfg["url"], interval)

    stop = threading.Event()

    def _handle_signal(signum, frame):
        log.info("Signal %s received — stopping after current iteration", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    # SIGINT (Ctrl-C) raises KeyboardInterrupt, handled in the try/except below

    try:
        while not stop.is_set():
            tick_start = time.monotonic()

            # Measure data freshness
            status, last_data_utc, metrics = measure_freshness(stale_threshold)

            # Send heartbeat
            hb_ok = agent.send_heartbeat()

            # Send service state
            ss_ok = agent.send_service_state(last_data_utc, status, metrics)

            log.info(
                "tick  status=%-8s  last_data=%s  age=%.0fs  heartbeat=%s  service_state=%s",
                status,
                last_data_utc or "unknown",
                metrics.get("last_write_age_seconds", -1),
                "ok" if hb_ok else "FAIL",
                "ok" if ss_ok else "FAIL",
            )

            # Sleep for the remainder of the interval, waking on stop signal
            elapsed = time.monotonic() - tick_start
            stop.wait(timeout=max(0.0, interval - elapsed))

    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — stopping cleanly")
    finally:
        agent.close()
        log.info("Betty agent stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Betty Sentinel agent for Crypto Lake")
    parser.add_argument("--config", default=str(REPO_ROOT / "config.yml"),
                        help="Path to config.yml (default: repo root config.yml)")
    args = parser.parse_args()

    try:
        with open(args.config) as f:
            full_cfg = yaml.safe_load(f)
    except FileNotFoundError:
        log.error("config.yml not found at %s", args.config)
        sys.exit(1)

    betty_cfg = full_cfg.get("betty") or {}

    if not betty_cfg.get("enabled", False):
        log.error("betty.enabled is false in config.yml — nothing to do")
        log.error("Set betty.enabled: true and fill in secret_hex to start the agent")
        sys.exit(1)

    secret = betty_cfg.get("secret_hex", "")
    if not secret:
        log.error("betty.secret_hex is empty in config.yml — cannot sign payloads")
        log.error("Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\"")
        sys.exit(1)

    run_loop(betty_cfg)


if __name__ == "__main__":
    main()
