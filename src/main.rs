mod cleanup;
mod collector;
mod config;
mod events;
mod health;
mod transformer;

use clap::Parser;
use config::Config;
use health::HealthCounters;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "crypto-lake-rs", about = "Lightweight crypto data collector")]
struct Cli {
    /// Path to config.yml
    #[arg(short, long, default_value = "config.yml")]
    config: PathBuf,

    /// Override base data path
    #[arg(long)]
    data_path: Option<PathBuf>,

    /// Raw file retention in days (0 = no cleanup)
    #[arg(long, default_value = "3")]
    retention_days: i64,

    /// Health report interval in seconds
    #[arg(long, default_value = "60")]
    health_interval: u64,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Load config
    let cfg = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading config: {}", e);
            std::process::exit(1);
        }
    };

    // Init tracing
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.general.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let data_path = cli.data_path.unwrap_or_else(|| cfg.data_path());
    info!("Data path: {:?}", data_path);
    info!(
        "Exchanges: {:?}",
        cfg.exchanges.iter().map(|e| &e.name).collect::<Vec<_>>()
    );

    // Shared health counters
    let counters = Arc::new(HealthCounters::default());

    // Spawn the rotating JSONL writer (returns sender + shutdown handle)
    let (writer_tx, writer_shutdown) = collector::writer::spawn_writer(
        data_path.clone(),
        cfg.collector.write_interval_sec,
        counters.clone(),
    );

    // Spawn the trade aggregator (trades -> 1s bars)
    let (trade_tx, trade_rx) = tokio::sync::mpsc::unbounded_channel();
    let bar_rx = transformer::aggregator::spawn_aggregator(trade_rx, counters.clone());

    // Spawn the Parquet writer (1s bars -> Parquet files, returns shutdown handle)
    let parquet_shutdown = transformer::parquet_writer::spawn_parquet_writer(
        bar_rx,
        data_path.clone(),
        cfg.transformer.schedule_minutes,
        cfg.transformer.parquet_compression.clone(),
    );

    // Spawn cleanup task
    let retention = cli.retention_days;
    if retention > 0 {
        let cleanup_path = data_path.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                interval.tick().await;
                cleanup::cleanup_raw_files(&cleanup_path, retention).await;
            }
        });
        info!("Cleanup: removing raw files older than {} days", retention);
    }

    // Collect exchange names and total symbol count for health
    let exchange_names: Vec<String> = cfg.exchanges.iter().map(|e| e.name.clone()).collect();
    let total_symbols: usize = cfg.exchanges.iter().map(|e| e.symbols.len()).sum();

    // Spawn health writer
    health::spawn_health_writer(
        data_path.clone(),
        counters.clone(),
        exchange_names,
        total_symbols,
        cli.health_interval,
    );

    // Spawn exchange collectors
    // Binance (primary)
    if let Some(binance_cfg) = cfg.exchange("binance").cloned() {
        let wtx = writer_tx.clone();
        let ttx = trade_tx.clone();
        let dp = data_path.clone();
        let rb = cfg.collector.reconnect_backoff;
        let mrb = cfg.collector.max_reconnect_backoff;
        let rj = cfg.collector.reconnect_jitter;
        let ctrs = counters.clone();

        let symbols_count = binance_cfg.symbols.len();
        info!("[binance] Starting collector for {} symbols", symbols_count);

        tokio::spawn(async move {
            collector::binance::run(binance_cfg, wtx, ttx, dp, rb, mrb, rj, ctrs).await;
        });
    }

    // TODO: Phase 4 - Add Coinbase and Kraken collectors here.

    info!("Collector running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutdown signal received, flushing buffers...");
        }
        Err(e) => {
            error!("Error waiting for signal: {}", e);
        }
    }

    // Graceful shutdown: flush writer and parquet buffers
    drop(writer_tx);
    drop(trade_tx);

    // Signal writer to flush and wait
    if let Err(e) = writer_shutdown.send(()) {
        warn!("Writer already stopped: {:?}", e);
    }
    // Give writer a moment to flush
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Signal parquet writer to flush and wait
    if let Err(e) = parquet_shutdown.send(()) {
        warn!("Parquet writer already stopped: {:?}", e);
    }
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    info!("Shutdown complete.");
}
