use chrono::Utc;
use serde::Serialize;
use std::path::Path;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::warn;

/// A structured connection event, written to `_events/connections_{date}.jsonl`.
#[derive(Debug, Serialize)]
pub struct ConnectionEvent {
    pub event: String,
    pub exchange: String,
    pub ts: String,
    #[serde(default)]
    pub reason: String,
    pub gap_seconds: f64,
}

/// Append a connection event to the daily events file.
///
/// Path: `{raw_root}/_events/connections_{date}.jsonl`
pub async fn log_connection_event(
    raw_root: &Path,
    exchange: &str,
    event: &str,
    reason: &str,
    gap_seconds: f64,
) {
    let events_dir = raw_root.join("_events");
    if let Err(e) = fs::create_dir_all(&events_dir).await {
        warn!("Failed to create events dir: {}", e);
        return;
    }

    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    let path = events_dir.join(format!("connections_{}.jsonl", date_str));

    let record = ConnectionEvent {
        event: event.to_string(),
        exchange: exchange.to_string(),
        ts: Utc::now().to_rfc3339(),
        reason: reason.to_string(),
        gap_seconds: (gap_seconds * 10.0).round() / 10.0,
    };

    let line = match serde_json::to_string(&record) {
        Ok(s) => s + "\n",
        Err(e) => {
            warn!("Failed to serialize connection event: {}", e);
            return;
        }
    };

    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await;

    match result {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()).await {
                warn!("Failed to write connection event: {}", e);
            }
        }
        Err(e) => warn!("Failed to open events file {:?}: {}", path, e),
    }
}
