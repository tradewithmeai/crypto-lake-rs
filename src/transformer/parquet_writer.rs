use crate::transformer::aggregator::Bar1s;
use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tracing::{error, info};

/// Schema matching the Python Parquet output exactly.
pub fn bars_schema() -> Schema {
    Schema::new(vec![
        Field::new("window_start", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume_base", DataType::Float64, false),
        Field::new("volume_quote", DataType::Float64, false),
        Field::new("trade_count", DataType::UInt64, false),
        Field::new("vwap", DataType::Float64, false),
        Field::new("bid", DataType::Float64, true),
        Field::new("ask", DataType::Float64, true),
        Field::new("spread", DataType::Float64, true),
        Field::new("source", DataType::Utf8, false),
    ])
}

/// Spawn the Parquet writer task.
///
/// Buffers completed 1-second bars in memory and flushes to Parquet files
/// on the configured schedule (default: every hour).
///
/// Returns a oneshot sender for signalling shutdown (triggers final flush).
pub fn spawn_parquet_writer(
    mut bar_rx: mpsc::UnboundedReceiver<Bar1s>,
    base_path: PathBuf,
    flush_interval_minutes: u64,
    compression: String,
) -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut buffer: Vec<Bar1s> = Vec::with_capacity(100_000);
        let mut flush_timer =
            time::interval(Duration::from_secs(flush_interval_minutes * 60));

        // Skip the immediate first tick
        flush_timer.tick().await;

        loop {
            tokio::select! {
                Some(bar) = bar_rx.recv() => {
                    buffer.push(bar);
                }
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() {
                        let bars = std::mem::replace(&mut buffer, Vec::with_capacity(100_000));
                        if let Err(e) = flush_to_parquet(&bars, &base_path, &compression).await {
                            error!("Parquet flush failed: {}", e);
                            buffer.extend(bars);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Parquet writer: shutdown signal, flushing {} buffered bars...", buffer.len());
                    // Drain any remaining bars from channel
                    while let Ok(bar) = bar_rx.try_recv() {
                        buffer.push(bar);
                    }
                    if !buffer.is_empty() {
                        if let Err(e) = flush_to_parquet(&buffer, &base_path, &compression).await {
                            error!("Parquet final flush failed: {}", e);
                        }
                    }
                    info!("Parquet writer: shutdown complete");
                    return;
                }
            }
        }
    });

    shutdown_tx
}

/// Write a batch of bars to partitioned Parquet files.
///
/// Partition scheme: `{base_path}/parquet/{exchange}/{symbol}/year={Y}/month={M}/day={D}/{timestamp}.parquet`
async fn flush_to_parquet(
    bars: &[Bar1s],
    base_path: &Path,
    compression: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bars.is_empty() {
        return Ok(());
    }

    info!("Flushing {} bars to Parquet", bars.len());

    // Group bars by (exchange, symbol, date)
    let mut groups: std::collections::HashMap<(String, String, String), Vec<&Bar1s>> =
        std::collections::HashMap::new();

    for bar in bars {
        let dt = DateTime::from_timestamp(bar.ts, 0)
            .unwrap_or_default()
            .naive_utc();
        let date_key = format!(
            "year={}/month={:02}/day={:02}",
            dt.year(),
            dt.month(),
            dt.day()
        );
        let key = (bar.exchange.clone(), bar.symbol.clone(), date_key);
        groups.entry(key).or_default().push(bar);
    }

    let comp = match compression.to_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "zstd" => Compression::ZSTD(Default::default()),
        _ => Compression::SNAPPY,
    };

    for ((exchange, symbol, date_partition), group) in &groups {
        let dir = base_path
            .join("parquet")
            .join(&exchange)
            .join(&symbol)
            .join(date_partition);
        tokio::fs::create_dir_all(&dir).await?;

        let ts_str = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let file_path = dir.join(format!("{}.parquet", ts_str));

        write_parquet_file(&file_path, group, comp)?;

        info!(
            "Wrote {} bars to {:?}",
            group.len(),
            file_path.file_name().unwrap_or_default()
        );
    }

    Ok(())
}

/// Write a single Parquet file from a slice of bars (blocking I/O, called from async context).
pub fn write_parquet_file(
    path: &Path,
    bars: &[&Bar1s],
    compression: Compression,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schema = Arc::new(bars_schema());

    let window_start: Vec<i64> = bars.iter().map(|b| b.ts * 1_000_000).collect();
    let exchange: Vec<&str> = bars.iter().map(|b| b.exchange.as_str()).collect();
    let symbol: Vec<&str> = bars.iter().map(|b| b.symbol.as_str()).collect();
    let open: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let low: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let volume_base: Vec<f64> = bars.iter().map(|b| b.volume_base).collect();
    let volume_quote: Vec<f64> = bars.iter().map(|b| b.volume_quote).collect();
    let trade_count: Vec<u64> = bars.iter().map(|b| b.trade_count).collect();
    let vwap: Vec<f64> = bars.iter().map(|b| b.vwap).collect();
    let bid: Vec<f64> = bars.iter().map(|b| b.bid).collect();
    let ask: Vec<f64> = bars.iter().map(|b| b.ask).collect();
    let spread: Vec<f64> = bars.iter().map(|b| b.spread).collect();
    let source: Vec<&str> = bars.iter().map(|b| b.source.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(window_start).with_timezone("UTC")),
            Arc::new(StringArray::from(exchange)),
            Arc::new(StringArray::from(symbol)),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(volume_base)),
            Arc::new(Float64Array::from(volume_quote)),
            Arc::new(UInt64Array::from(trade_count)),
            Arc::new(Float64Array::from(vwap)),
            Arc::new(Float64Array::from(bid)),
            Arc::new(Float64Array::from(ask)),
            Arc::new(Float64Array::from(spread)),
            Arc::new(StringArray::from(source)),
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Write a batch of Bar1s to partitioned Parquet files (used by backfill).
///
/// Groups bars by (exchange, symbol, date) and writes each group to its own file.
pub async fn write_bars(
    bars: &[Bar1s],
    base_path: &Path,
    compression: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bars.is_empty() {
        return Ok(());
    }

    // Group bars by (exchange, symbol, date)
    let mut groups: std::collections::HashMap<(String, String, String), Vec<&Bar1s>> =
        std::collections::HashMap::new();

    for bar in bars {
        let dt = DateTime::from_timestamp(bar.ts, 0)
            .unwrap_or_default()
            .naive_utc();
        let date_key = format!(
            "year={}/month={:02}/day={:02}",
            dt.year(),
            dt.month(),
            dt.day()
        );
        let key = (bar.exchange.clone(), bar.symbol.clone(), date_key);
        groups.entry(key).or_default().push(bar);
    }

    let comp = match compression.to_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "zstd" => Compression::ZSTD(Default::default()),
        _ => Compression::SNAPPY,
    };

    for ((exchange, symbol, date_partition), group) in &groups {
        let dir = base_path
            .join("parquet")
            .join(exchange)
            .join(symbol)
            .join(date_partition);
        tokio::fs::create_dir_all(&dir).await?;

        let ts_str = chrono::Utc::now().format("%Y%m%dT%H%M%S_backfill").to_string();
        let file_path = dir.join(format!("{}.parquet", ts_str));

        write_parquet_file(&file_path, group, comp)?;

        info!(
            "Backfill: wrote {} bars to {:?}",
            group.len(),
            file_path.file_name().unwrap_or_default()
        );
    }

    Ok(())
}
