//! Betty Sentinel agent — sends signed heartbeat and service-state telemetry
//! to the local Betty HTTP API every `interval_sec` seconds.
//!
//! Runs as a background tokio task.  Stops cleanly when the shared `shutdown`
//! flag is set.  Never panics; all errors are logged and skipped.

use crate::config::Betty as BettyCfg;
use crate::health::HealthCounters;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

// ── Hex decode (no extra dep) ─────────────────────────────────────────────────

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

// ── Timestamp formatting ──────────────────────────────────────────────────────

fn format_ts(dt: &DateTime<Utc>) -> String {
    format!(
        "{}.{:06}Z",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        dt.timestamp_subsec_micros()
    )
}

// ── Canonical JSON + signing ──────────────────────────────────────────────────
//
// Matches the Python canonical form exactly:
//   json.dumps(body, sort_keys=True, separators=(",", ":"))
//
// We recursively sort all Object keys before serialising so that nested
// objects (e.g. metrics_summary) are also sorted.

fn sort_recursive(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            // Collect into BTreeMap (sorted by key), then back to serde_json::Map
            let sorted: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(k, v)| (k.clone(), sort_recursive(v)))
                .collect();
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sort_recursive).collect())
        }
        _ => v.clone(),
    }
}

fn canonical_bytes(payload: &serde_json::Value) -> Vec<u8> {
    // Top-level: exclude "signature" key, sort remaining keys, recurse into values
    if let serde_json::Value::Object(map) = payload {
        let sorted: serde_json::Map<String, serde_json::Value> = map
            .iter()
            .filter(|(k, _)| k.as_str() != "signature")
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(k, v)| (k.clone(), sort_recursive(v)))
            .collect();
        return serde_json::to_string(&serde_json::Value::Object(sorted))
            .unwrap_or_default()
            .into_bytes();
    }
    serde_json::to_string(&sort_recursive(payload))
        .unwrap_or_default()
        .into_bytes()
}

fn compute_signature(payload: &serde_json::Value, secret: &[u8]) -> String {
    let msg = canonical_bytes(payload);
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(&msg);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn with_signature(mut payload: serde_json::Value, secret: &[u8]) -> serde_json::Value {
    let sig = compute_signature(&payload, secret);
    if let serde_json::Value::Object(ref mut map) = payload {
        map.insert("signature".into(), serde_json::json!(sig));
    }
    payload
}

// ── Sequence numbers ──────────────────────────────────────────────────────────
//
// Persisted to data/reports/betty_seq.json.
// Uses max(last+1, current_unix_time) so restarts never produce a lower number.

fn next_sequence_sync(seq_path: &std::path::Path) -> u64 {
    let last = std::fs::read_to_string(seq_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v["seq"].as_u64())
        .unwrap_or(0);

    let time_based = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1);

    // Take whichever is larger to guarantee monotonic increase across restarts
    let next = (last + 1).max(time_based);

    if let Some(parent) = seq_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(seq_path, format!("{{\"seq\":{}}}", next));
    next
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

async fn post_to_betty(client: &reqwest::Client, url: &str, payload: &serde_json::Value) -> bool {
    match client.post(url).json(payload).send().await {
        Ok(resp) if resp.status().as_u16() == 202 => true,
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let preview: String = body.chars().take(200).collect();
            warn!("Betty {}: HTTP {} — {}", url, status, preview);
            false
        }
        Err(e) => {
            warn!("Betty {}: unreachable — {}", url, e);
            false
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn spawn_betty_task(
    cfg: BettyCfg,
    data_path: PathBuf,
    counters: Arc<HealthCounters>,
    shutdown: Arc<AtomicBool>,
) {
    if !cfg.enabled {
        info!("Betty agent: disabled in config");
        return;
    }
    if cfg.secret_hex.is_empty() {
        warn!("Betty agent: secret_hex is empty — not starting");
        return;
    }
    let secret = match decode_hex(&cfg.secret_hex) {
        Some(b) => b,
        None => {
            warn!("Betty agent: secret_hex is not valid hex — not starting");
            return;
        }
    };

    // Supervisor: if run_betty_loop ever panics or exits unexpectedly,
    // respawn it with backoff so monitoring never silently dies.
    tokio::spawn(async move {
        let mut backoff_sec: u64 = 2;
        loop {
            if shutdown.load(Ordering::SeqCst) {
                info!("Betty supervisor: shutting down");
                return;
            }

            let cfg_c      = cfg.clone();
            let dp_c       = data_path.clone();
            let counters_c = counters.clone();
            let shutdown_c = shutdown.clone();
            let secret_c   = secret.clone();

            let handle = tokio::spawn(async move {
                run_betty_loop(cfg_c, dp_c, counters_c, shutdown_c, secret_c).await;
            });

            match handle.await {
                Ok(()) => {
                    if shutdown.load(Ordering::SeqCst) {
                        info!("Betty supervisor: inner task exited on shutdown");
                        return;
                    }
                    warn!(
                        "Betty supervisor: inner task exited unexpectedly — restarting in {}s",
                        backoff_sec
                    );
                }
                Err(e) if e.is_panic() => {
                    warn!(
                        "Betty supervisor: inner task panicked — restarting in {}s ({:?})",
                        backoff_sec, e
                    );
                }
                Err(e) => {
                    warn!(
                        "Betty supervisor: inner task join error — restarting in {}s ({:?})",
                        backoff_sec, e
                    );
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(backoff_sec)).await;
            backoff_sec = (backoff_sec * 2).min(60);
        }
    });
}

async fn run_betty_loop(
    cfg: BettyCfg,
    data_path: PathBuf,
    counters: Arc<HealthCounters>,
    shutdown: Arc<AtomicBool>,
    secret: Vec<u8>,
) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("Betty agent: failed to build HTTP client: {}", e);
            return;
        }
    };

    let seq_path    = data_path.join("reports").join("betty_seq.json");
    let hostname    = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let hb_url = format!("{}/ingest/heartbeat",     cfg.url.trim_end_matches('/'));
    let ss_url = format!("{}/ingest/service-state", cfg.url.trim_end_matches('/'));
    let start  = Instant::now();

    info!(
        "Betty agent: started — agent_id={} url={} interval={}s stale_threshold={}s",
        cfg.agent_id, cfg.url, cfg.interval_sec, cfg.stale_threshold_sec
    );

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(cfg.interval_sec));

    // Freshness tracked in-memory from bars_produced counter — no filesystem scan.
    let mut last_bars: u64 = counters.bars_produced.load(Ordering::Relaxed);
    let mut last_change_instant: Instant = Instant::now();
    let mut last_change_utc: DateTime<Utc> = Utc::now();

    loop {
        ticker.tick().await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Betty agent: shutting down");
            return;
        }

        // ── Read counters ─────────────────────────────────────────────────────
        let bars       = counters.bars_produced.load(Ordering::Relaxed);
        let reconnects = counters.ws_reconnects.load(Ordering::Relaxed);
        let uptime     = start.elapsed().as_secs();

        // ── Measure data freshness (from counter, not filesystem) ─────────────
        if bars > last_bars {
            last_bars = bars;
            last_change_instant = Instant::now();
            last_change_utc = Utc::now();
        }
        let age_secs = last_change_instant.elapsed().as_secs_f64();
        let (status, last_data_utc): (&str, Option<String>) = if bars == 0 && uptime < 120 {
            ("unknown", None)
        } else if age_secs > cfg.stale_threshold_sec as f64 {
            ("stale", Some(format_ts(&last_change_utc)))
        } else {
            ("ok", Some(format_ts(&last_change_utc)))
        };

        // ── Sequence numbers (blocking file I/O) ──────────────────────────────
        let sp1 = seq_path.clone();
        let hb_seq = tokio::task::spawn_blocking(move || next_sequence_sync(&sp1))
            .await
            .unwrap_or_else(|_| std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs()).unwrap_or(1));

        let sp2 = seq_path.clone();
        let ss_seq = tokio::task::spawn_blocking(move || next_sequence_sync(&sp2))
            .await
            .unwrap_or_else(|_| std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs()).unwrap_or(2));

        // ── Build heartbeat ───────────────────────────────────────────────────
        let now_utc = Utc::now();
        let hb = with_signature(serde_json::json!({
            "event_type":      "agent_heartbeat",
            "schema_version":  "1.0",
            "agent_id":        cfg.agent_id,
            "host_id":         hostname,
            "environment":     "production",
            "bridge_version":  "1.0.0",
            "ts_utc":          format_ts(&now_utc),
            "sequence_number": hb_seq,
            "services_summary": {},
            "system_summary":  {},
        }), &secret);

        let hb_ok = post_to_betty(&client, &hb_url, &hb).await;

        // ── Build service-state ───────────────────────────────────────────────
        let now_utc2 = Utc::now();

        let mut metrics = serde_json::json!({
            "bars_produced":  bars,
            "ws_reconnects":  reconnects,
            "uptime_seconds": uptime,
        });
        if age_secs >= 0.0 {
            metrics["last_write_age_seconds"] =
                serde_json::json!((age_secs * 10.0).round() / 10.0);
        }

        let mut ss_payload = serde_json::json!({
            "event_type":      "service_state",
            "schema_version":  "1.0",
            "agent_id":        cfg.agent_id,
            "service_name":    "crypto-lake",
            "status":          status,
            "metrics_summary": metrics,
            "ts_utc":          format_ts(&now_utc2),
            "sequence_number": ss_seq,
        });

        if let Some(ref last) = last_data_utc {
            ss_payload["last_data_utc"] = serde_json::json!(last);
        }

        let ss = with_signature(ss_payload, &secret);
        let ss_ok = post_to_betty(&client, &ss_url, &ss).await;

        info!(
            "Betty: status={} age={:.0}s bars={} uptime={}s hb={} ss={}",
            status,
            age_secs,
            bars,
            uptime,
            if hb_ok { "ok" } else { "FAIL" },
            if ss_ok { "ok" } else { "FAIL" },
        );
    }
}
