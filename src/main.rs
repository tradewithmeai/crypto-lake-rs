mod cleanup;
mod collector;
mod config;
mod events;
mod transformer;

use clap::Parser;
use config::Config;
use std::path::PathBuf;
use tracing::{error, info};
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

    // Spawn the rotating JSONL writer
    let writer_tx = collector::writer::spawn_writer(
        data_path.clone(),
        cfg.collector.write_interval_sec,
    );

    // Spawn the trade aggregator (trades -> 1s bars)
    let (trade_tx, trade_rx) = tokio::sync::mpsc::unbounded_channel();
    let bar_rx = transformer::aggregator::spawn_aggregator(trade_rx);

    // Spawn the Parquet writer (1s bars -> Parquet files)
    transformer::parquet_writer::spawn_parquet_writer(
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

    // Spawn exchange collectors
    let mut handles = Vec::new();

    // Binance (primary)
    if let Some(binance_cfg) = cfg.exchange("binance").cloned() {
        let wtx = writer_tx.clone();
        let ttx = trade_tx.clone();
        let dp = data_path.clone();
        let rb = cfg.collector.reconnect_backoff;
        let mrb = cfg.collector.max_reconnect_backoff;
        let rj = cfg.collector.reconnect_jitter;

        let symbols_count = binance_cfg.symbols.len();
        info!("[binance] Starting collector for {} symbols", symbols_count);

        let handle = tokio::spawn(async move {
            collector::binance::run(binance_cfg, wtx, ttx, dp, rb, mrb, rj).await;
        });
        handles.push(handle);
    }

    // TODO: Phase 4 - Add Coinbase and Kraken collectors here.

    info!("Collector running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutdown signal received, flushing...");
        }
        Err(e) => {
            error!("Error waiting for signal: {}", e);
        }
    }

    info!("Shutdown complete.");
}
