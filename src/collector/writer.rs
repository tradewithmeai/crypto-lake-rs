use chrono::Utc;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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
pub struct RotatingWriter {
    base_path: PathBuf,
    interval_sec: u64,
    /// Buffered lines per (exchange, symbol) → Vec<String>
    buffers: HashMap<(String, String), Vec<String>>,
    /// Current rotation timestamp (start of current window)
    current_window: i64,
}

impl RotatingWriter {
    pub fn new(base_path: PathBuf, interval_sec: u64) -> Self {
        let now = Utc::now().timestamp();
        let window = now - (now % interval_sec as i64);
        Self {
            base_path,
            interval_sec,
            buffers: HashMap::new(),
            current_window: window,
        }
    }

    /// Add a line to the buffer.
    pub fn push(&mut self, msg: RawMessage) {
        let key = (msg.exchange, msg.symbol);
        self.buffers.entry(key).or_default().push(msg.payload);
    }

    /// Check if the current rotation window has elapsed and flush if needed.
    pub async fn maybe_rotate(&mut self) -> std::io::Result<()> {
        let now = Utc::now().timestamp();
        let window = now - (now % self.interval_sec as i64);

        if window > self.current_window {
            self.flush().await?;
            self.current_window = window;
        }
        Ok(())
    }

    /// Flush all buffered lines to disk and clear buffers.
    pub async fn flush(&mut self) -> std::io::Result<()> {
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

            let file_path = dir.join(format!("{}.jsonl", ts_str));
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await?;

            let data = lines.join("\n") + "\n";
            file.write_all(data.as_bytes()).await?;
            total_lines += lines.len();

            debug!(
                "Wrote {} lines to {:?}",
                lines.len(),
                file_path.file_name().unwrap_or_default()
            );
        }

        if total_lines > 0 {
            info!("Flushed {} raw lines to disk", total_lines);
        }
        Ok(())
    }
}

/// Spawn the writer task that consumes from a channel and writes to disk.
///
/// Returns a sender that collectors use to submit raw messages.
pub fn spawn_writer(
    base_path: PathBuf,
    interval_sec: u64,
) -> mpsc::UnboundedSender<RawMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel::<RawMessage>();

    tokio::spawn(async move {
        let mut writer = RotatingWriter::new(base_path, interval_sec);
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
            }
        }
    });

    tx
}
