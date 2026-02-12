use crate::health::HealthCounters;
use chrono::Utc;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};
use zstd::stream::Encoder;

/// A raw message to be written to JSONL.
#[derive(Debug)]
pub struct RawMessage {
    pub exchange: String,
    pub symbol: String,
    pub payload: String, // Already-serialized JSON line
}

/// Rotating JSONL writer that batches writes per file and rotates every `interval_sec`.
///
/// File layout: `{base_path}/raw/{exchange}/{symbol}/{date}/{timestamp}.jsonl`
struct RotatingWriter {
    base_path: PathBuf,
    interval_sec: u64,
    /// Buffered lines per (exchange, symbol) -> Vec<String>
    buffers: HashMap<(String, String), Vec<String>>,
    /// Current rotation timestamp (start of current window)
    current_window: i64,
    counters: Arc<HealthCounters>,
}

impl RotatingWriter {
    fn new(base_path: PathBuf, interval_sec: u64, counters: Arc<HealthCounters>) -> Self {
        let now = Utc::now().timestamp();
        let window = now - (now % interval_sec as i64);
        Self {
            base_path,
            interval_sec,
            buffers: HashMap::new(),
            current_window: window,
            counters,
        }
    }

    /// Add a line to the buffer.
    fn push(&mut self, msg: RawMessage) {
        let key = (msg.exchange, msg.symbol);
        self.buffers.entry(key).or_default().push(msg.payload);
    }

    /// Check if the current rotation window has elapsed and flush if needed.
    async fn maybe_rotate(&mut self) -> std::io::Result<()> {
        let now = Utc::now().timestamp();
        let window = now - (now % self.interval_sec as i64);

        if window > self.current_window {
            self.flush().await?;
            self.current_window = window;
        }
        Ok(())
    }

    /// Flush all buffered lines to disk with zstd compression and clear buffers.
    async fn flush(&mut self) -> std::io::Result<()> {
        if self.buffers.is_empty() {
            return Ok(());
        }

        let ts_str = Utc::now().format("%Y-%m-%dT%H_%M_%S").to_string();
        let date_str = Utc::now().format("%Y-%m-%d").to_string();
        let mut total_lines = 0usize;

        for ((exchange, symbol), lines) in self.buffers.drain() {
            if lines.is_empty() {
                continue;
            }

            let dir = self
                .base_path
                .join("raw")
                .join(&exchange)
                .join(&symbol)
                .join(&date_str);
            fs::create_dir_all(&dir).await?;

            // Write compressed .jsonl.zst file
            let file_path = dir.join(format!("{}.jsonl.zst", ts_str));
            let data = lines.join("\n") + "\n";

            // Use blocking file I/O for zstd compression (spawn_blocking for async context)
            let file_path_clone = file_path.clone();
            let line_count = lines.len();
            let compressed_size = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
                let file = std::fs::File::create(&file_path_clone)?;
                // Compression level 3 is a good balance of speed and compression
                let mut encoder = Encoder::new(file, 3)?;
                encoder.write_all(data.as_bytes())?;
                encoder.finish()?;
                let meta = std::fs::metadata(&file_path_clone)?;
                Ok(meta.len())
            })
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))??;

            self.counters.bytes_written.fetch_add(compressed_size, Ordering::Relaxed);
            total_lines += line_count;

            debug!(
                "Wrote {} lines to {:?} (zstd compressed)",
                line_count,
                file_path.file_name().unwrap_or_default()
            );
        }

        if total_lines > 0 {
            self.counters
                .raw_lines_written
                .fetch_add(total_lines as u64, Ordering::Relaxed);
            info!("Flushed {} raw lines to disk (zstd)", total_lines);
        }
        Ok(())
    }
}

/// Spawn the writer task that consumes from a channel and writes to disk.
///
/// Returns a sender for raw messages and a oneshot sender for shutdown signalling.
pub fn spawn_writer(
    base_path: PathBuf,
    interval_sec: u64,
    counters: Arc<HealthCounters>,
) -> (mpsc::UnboundedSender<RawMessage>, oneshot::Sender<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<RawMessage>();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut writer = RotatingWriter::new(base_path, interval_sec, counters);
        let mut rotate_interval =
            tokio::time::interval(std::time::Duration::from_secs(interval_sec));

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    writer.push(msg);
                }
                _ = rotate_interval.tick() => {
                    if let Err(e) = writer.maybe_rotate().await {
                        warn!("Rotation error: {}", e);
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Writer: shutdown signal received, flushing...");
                    // Drain remaining messages
                    while let Ok(msg) = rx.try_recv() {
                        writer.push(msg);
                    }
                    if let Err(e) = writer.flush().await {
                        warn!("Writer: final flush error: {}", e);
                    }
                    info!("Writer: final flush complete");
                    return;
                }
            }
        }
    });

    (tx, shutdown_tx)
}
