use chrono::Utc;
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

/// Shared counters for health reporting, updated atomically from collector tasks.
#[derive(Debug, Default)]
pub struct HealthCounters {
    pub messages_received: AtomicU64,
    pub trades_received: AtomicU64,
    pub bars_produced: AtomicU64,
    pub raw_lines_written: AtomicU64,
    pub ws_disconnects: AtomicU64,
    pub ws_reconnects: AtomicU64,
    /// Total bytes received from WebSocket streams (network usage).
    pub bytes_received: AtomicU64,
    /// Total bytes written to disk (after compression).
    pub bytes_written: AtomicU64,
}

/// JSON health payload compatible with the Python health report format.
#[derive(Debug, Serialize)]
struct HealthPayload {
    ts_utc: String,
    mode: String,
    collector: CollectorHealth,
    counters: Counters,
    disk: DiskHealth,
}

#[derive(Debug, Serialize)]
struct CollectorHealth {
    status: String,
    uptime_seconds: u64,
    exchanges: Vec<String>,
    symbols_count: usize,
}

#[derive(Debug, Serialize)]
struct Counters {
    messages_received: u64,
    trades_received: u64,
    bars_produced: u64,
    raw_lines_written: u64,
    ws_disconnects: u64,
    ws_reconnects: u64,
}

#[derive(Debug, Serialize)]
struct DiskHealth {
    data_path: String,
    raw_files_today: u64,
    parquet_files_total: u64,
}

/// Spawn a periodic health writer task.
///
/// Writes `{data_path}/reports/health.json` every `interval_secs`.
pub fn spawn_health_writer(
    data_path: std::path::PathBuf,
    counters: Arc<HealthCounters>,
    exchanges: Vec<String>,
    symbols_count: usize,
    interval_secs: u64,
) {
    let start_time = std::time::Instant::now();

    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            let payload = build_payload(
                &data_path,
                &counters,
                &exchanges,
                symbols_count,
                start_time.elapsed().as_secs(),
            )
            .await;

            if let Err(e) = write_health_file(&data_path, &payload).await {
                warn!("Failed to write health file: {}", e);
            }
        }
    });
}

async fn build_payload(
    data_path: &Path,
    counters: &HealthCounters,
    exchanges: &[String],
    symbols_count: usize,
    uptime_secs: u64,
) -> HealthPayload {
    let today = Utc::now().format("%Y-%m-%d").to_string();

    // Count raw files for today
    let raw_today = count_raw_files_today(data_path, &today).await;
    let parquet_total = count_parquet_files(data_path).await;

    HealthPayload {
        ts_utc: Utc::now().to_rfc3339(),
        mode: "PRODUCTION".to_string(),
        collector: CollectorHealth {
            status: "running".to_string(),
            uptime_seconds: uptime_secs,
            exchanges: exchanges.to_vec(),
            symbols_count,
        },
        counters: Counters {
            messages_received: counters.messages_received.load(Ordering::Relaxed),
            trades_received: counters.trades_received.load(Ordering::Relaxed),
            bars_produced: counters.bars_produced.load(Ordering::Relaxed),
            raw_lines_written: counters.raw_lines_written.load(Ordering::Relaxed),
            ws_disconnects: counters.ws_disconnects.load(Ordering::Relaxed),
            ws_reconnects: counters.ws_reconnects.load(Ordering::Relaxed),
        },
        disk: DiskHealth {
            data_path: data_path.to_string_lossy().to_string(),
            raw_files_today: raw_today,
            parquet_files_total: parquet_total,
        },
    }
}

async fn write_health_file(
    data_path: &Path,
    payload: &HealthPayload,
) -> std::io::Result<()> {
    let reports_dir = data_path.join("reports");
    fs::create_dir_all(&reports_dir).await?;

    let json_path = reports_dir.join("health.json");
    let json = serde_json::to_string_pretty(payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut file = fs::File::create(&json_path).await?;
    file.write_all(json.as_bytes()).await?;

    info!(
        "Health: msgs={} trades={} bars={} raw_today={} uptime={}s",
        payload.counters.messages_received,
        payload.counters.trades_received,
        payload.counters.bars_produced,
        payload.disk.raw_files_today,
        payload.collector.uptime_seconds,
    );

    Ok(())
}

/// Count JSONL files in raw/{exchange}/{symbol}/{today}/ directories.
async fn count_raw_files_today(data_path: &Path, today: &str) -> u64 {
    let raw_dir = data_path.join("raw");
    let mut count = 0u64;

    let mut exchanges = match fs::read_dir(&raw_dir).await {
        Ok(r) => r,
        Err(_) => return 0,
    };

    while let Ok(Some(ex)) = exchanges.next_entry().await {
        if !ex.path().is_dir() {
            continue;
        }
        let mut symbols = match fs::read_dir(ex.path()).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        while let Ok(Some(sym)) = symbols.next_entry().await {
            let today_dir = sym.path().join(today);
            if today_dir.is_dir() {
                let mut files = match fs::read_dir(&today_dir).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                while let Ok(Some(f)) = files.next_entry().await {
                    if f.path()
                        .extension()
                        .map_or(false, |e| e == "jsonl")
                    {
                        count += 1;
                    }
                }
            }
        }
    }

    count
}

/// Count total Parquet files under data_path/parquet/.
async fn count_parquet_files(data_path: &Path) -> u64 {
    let parquet_dir = data_path.join("parquet");
    count_files_recursive(&parquet_dir, "parquet").await
}

async fn count_files_recursive(dir: &Path, extension: &str) -> u64 {
    let mut count = 0u64;
    let mut entries = match fs::read_dir(dir).await {
        Ok(r) => r,
        Err(_) => return 0,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            count += Box::pin(count_files_recursive(&path, extension)).await;
        } else if path.extension().map_or(false, |e| e == extension) {
            count += 1;
        }
    }
    count
}
