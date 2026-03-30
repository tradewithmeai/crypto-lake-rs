mod backfill;
mod cleanup;
mod collector;
mod config;
mod events;
mod health;
mod server;
mod transformer;
#[cfg(windows)]
mod autostart;
#[cfg(windows)]
mod tray;

use clap::Parser;
use config::Config;
use health::HealthCounters;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tracing::{info, warn};
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

    /// Disable system tray icon (console-only mode)
    #[arg(long)]
    no_tray: bool,

    /// Skip startup backfill
    #[arg(long)]
    no_backfill: bool,

    /// Install auto-start on Windows boot and exit
    #[arg(long)]
    install_autostart: bool,

    /// Remove auto-start from Windows boot and exit
    #[arg(long)]
    remove_autostart: bool,
}

fn main() {
    // Install panic hook to write crashes to a log file for post-mortem analysis
    std::panic::set_hook(Box::new(|info| {
        let timestamp = chrono::Utc::now().to_rfc3339();
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_string()
        };
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown location".to_string());
        let backtrace = std::backtrace::Backtrace::force_capture();

        let crash_msg = format!(
            "[{}] PANIC in thread '{}'\n  Location: {}\n  Message: {}\n  Backtrace:\n{}\n\n",
            timestamp, thread_name, location, payload, backtrace
        );

        // Try to write to crash.log next to the executable
        let crash_path = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("crash.log")))
            .unwrap_or_else(|| std::path::PathBuf::from("crash.log"));

        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&crash_path)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(crash_msg.as_bytes())
            });

        // Also print to stderr
        eprintln!("{}", crash_msg);
    }));

    let cli = Cli::parse();

    // Handle autostart commands early (no config needed)
    #[cfg(windows)]
    {
        if cli.install_autostart {
            match autostart::install_autostart() {
                Ok(()) => {
                    eprintln!("Auto-start installed successfully.");
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("Failed to install auto-start: {}", e);
                    std::process::exit(1);
                }
            }
        }
        if cli.remove_autostart {
            match autostart::remove_autostart() {
                Ok(()) => {
                    eprintln!("Auto-start removed successfully.");
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("Failed to remove auto-start: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    // Load config - try specified path first, then fall back to exe directory
    let config_path = if cli.config.exists() {
        cli.config.clone()
    } else if let Ok(exe_dir) = std::env::current_exe().map(|p| p.parent().unwrap().to_path_buf()) {
        let beside_exe = exe_dir.join(&cli.config);
        if beside_exe.exists() {
            beside_exe
        } else {
            cli.config.clone()
        }
    } else {
        cli.config.clone()
    };
    let cfg = match Config::load(&config_path) {
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

    // Extract values before any moves
    let data_path = cli.data_path.unwrap_or_else(|| cfg.data_path());
    let retention_days = cli.retention_days;
    let health_interval = cli.health_interval;
    let counters = Arc::new(HealthCounters::default());
    let shutdown = Arc::new(AtomicBool::new(false));
    let exchange_names: Vec<String> = cfg.exchanges.iter().map(|e| e.name.clone()).collect();

    // On Windows, use system tray mode by default
    #[cfg(windows)]
    {
        if !cli.no_tray {
            // Detach from console so no window appears on double-click launch
            extern "system" {
                fn FreeConsole() -> i32;
            }
            unsafe { FreeConsole(); }

            let server_port = cfg.server.port;
            let c = counters.clone();
            let s = shutdown.clone();
            let dp = data_path.clone();
            let en = exchange_names.clone();

            let no_backfill = cli.no_backfill;
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create tokio runtime");
                rt.block_on(run_collector(cfg, dp, retention_days, health_interval, en, c, s, no_backfill));
            });

            // Tray blocks the main thread until Quit
            tray::run(counters, exchange_names, shutdown, server_port);
            let _ = handle.join();
            return;
        }
    }

    // Console mode
    info!("Starting in console mode");
    let no_backfill = cli.no_backfill;
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(run_collector(
        cfg,
        data_path,
        retention_days,
        health_interval,
        exchange_names,
        counters,
        shutdown,
        no_backfill,
    ));
}

async fn run_collector(
    cfg: Config,
    data_path: PathBuf,
    retention_days: i64,
    health_interval: u64,
    exchange_names: Vec<String>,
    counters: Arc<HealthCounters>,
    shutdown: Arc<AtomicBool>,
    no_backfill: bool,
) {
    info!("Data path: {:?}", data_path);
    info!(
        "Exchanges: {:?}",
        cfg.exchanges.iter().map(|e| &e.name).collect::<Vec<_>>()
    );

    // Shared health counters
    let counters = counters;

    // Spawn the rotating JSONL writer (returns sender + shutdown handle)
    let (writer_tx, writer_shutdown) = collector::writer::spawn_writer(
        data_path.clone(),
        cfg.collector.write_interval_sec,
        counters.clone(),
    );

    // Broadcast channel for real-time WebSocket streaming
    let (broadcast_tx, _) = broadcast::channel::<collector::TradeEvent>(4096);

    // Build per-exchange bar interval map (default 1s; Coinbase/Kraken use 60s)
    let bar_intervals: HashMap<String, u64> = cfg.exchanges.iter()
        .map(|e| (e.name.clone(), e.bar_interval_sec))
        .collect();

    // Spawn the trade aggregator (trades -> OHLCV bars at per-exchange interval)
    let (trade_tx, trade_rx) = tokio::sync::mpsc::unbounded_channel();
    let bar_rx = transformer::aggregator::spawn_aggregator(trade_rx, counters.clone(), bar_intervals);

    // Backfill trigger channel: collectors send exchange name on reconnect after a gap
    let (backfill_tx, mut backfill_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    // Spawn the Parquet writer (1s bars -> Parquet files, returns shutdown handle)
    let parquet_shutdown = transformer::parquet_writer::spawn_parquet_writer(
        bar_rx,
        data_path.clone(),
        cfg.transformer.schedule_minutes,
        cfg.transformer.parquet_compression.clone(),
    );

    // Spawn cleanup task
    let retention = retention_days;
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
    let total_symbols: usize = cfg.exchanges.iter().map(|e| e.symbols.len()).sum();

    // Spawn health writer
    health::spawn_health_writer(
        data_path.clone(),
        counters.clone(),
        exchange_names,
        total_symbols,
        health_interval,
    );

    // Spawn dashboard server
    {
        let server_cfg = cfg.clone();
        let server_broadcast = broadcast_tx.clone();
        let server_counters = counters.clone();
        let server_data_path = data_path.clone();
        tokio::spawn(async move {
            server::start_server(server_cfg, server_broadcast, server_counters, server_data_path).await;
        });
    }

    // Run startup backfill (before starting live collectors)
    if !no_backfill && cfg.backfill.enabled {
        info!("Running startup backfill...");
        backfill::run(
            &cfg.exchanges,
            &data_path,
            &cfg.backfill,
            &cfg.transformer.parquet_compression,
        )
        .await;
    } else if no_backfill {
        info!("Backfill: skipped (--no-backfill flag)");
    }

    // Shared mutex to prevent concurrent backfill runs
    let backfill_lock = Arc::new(Mutex::new(()));

    // Reconnect-triggered backfill runner.
    // Collectors send their exchange name when they reconnect after a gap.
    // A 5-second debounce collects all reconnecting exchanges (they usually
    // reconnect within seconds of each other), then runs a single backfill pass.
    if cfg.backfill.enabled {
        let exchanges_bf = cfg.exchanges.clone();
        let backfill_cfg_rf = cfg.backfill.clone();
        let data_path_rf = data_path.clone();
        let compression_rf = cfg.transformer.parquet_compression.clone();
        let lock_rf = backfill_lock.clone();

        tokio::spawn(async move {
            let mut pending: HashSet<String> = HashSet::new();
            let mut debounce = tokio::time::interval(std::time::Duration::from_secs(5));
            debounce.tick().await; // skip immediate first tick

            loop {
                tokio::select! {
                    Some(name) = backfill_rx.recv() => {
                        pending.insert(name);
                    }
                    _ = debounce.tick() => {
                        if !pending.is_empty() {
                            let names: Vec<String> = pending.drain().collect();
                            let _guard = lock_rf.lock().await;
                            info!("Reconnect-triggered backfill for: {:?}", names);
                            backfill::run_for_exchanges(
                                &names, &exchanges_bf, &data_path_rf,
                                &backfill_cfg_rf, &compression_rf,
                            ).await;
                        }
                    }
                }
            }
        });
    }

    // Daily scheduled backfill — runs every 24 hours to catch any gaps.
    if cfg.backfill.enabled {
        let exchanges_daily = cfg.exchanges.clone();
        let backfill_cfg_daily = cfg.backfill.clone();
        let data_path_daily = data_path.clone();
        let compression_daily = cfg.transformer.parquet_compression.clone();
        let lock_daily = backfill_lock.clone();

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(24 * 3600));
            timer.tick().await; // skip first tick (startup backfill already ran)
            loop {
                timer.tick().await;
                let _guard = lock_daily.lock().await;
                info!("Daily scheduled backfill starting...");
                backfill::run(
                    &exchanges_daily, &data_path_daily,
                    &backfill_cfg_daily, &compression_daily,
                ).await;
            }
        });
    }

    // Spawn exchange collectors
    let gap_threshold = cfg.backfill.gap_threshold_secs;

    // Binance (primary — 1-second bars)
    if let Some(binance_cfg) = cfg.exchange("binance").cloned() {
        let wtx = writer_tx.clone();
        let ttx = trade_tx.clone();
        let btx = broadcast_tx.clone();
        let dp = data_path.clone();
        let rb = cfg.collector.reconnect_backoff;
        let mrb = cfg.collector.max_reconnect_backoff;
        let rj = cfg.collector.reconnect_jitter;
        let ctrs = counters.clone();
        let bftx = backfill_tx.clone();

        let symbols_count = binance_cfg.symbols.len();
        info!("[binance] Starting collector for {} symbols", symbols_count);

        tokio::spawn(async move {
            collector::binance::run(binance_cfg, wtx, ttx, btx, dp, rb, mrb, rj, ctrs, bftx, gap_threshold).await;
        });
    }

    // Coinbase (60-second bars)
    if let Some(coinbase_cfg) = cfg.exchange("coinbase").cloned() {
        let wtx = writer_tx.clone();
        let ttx = trade_tx.clone();
        let btx = broadcast_tx.clone();
        let dp = data_path.clone();
        let rb = cfg.collector.reconnect_backoff;
        let mrb = cfg.collector.max_reconnect_backoff;
        let rj = cfg.collector.reconnect_jitter;
        let ctrs = counters.clone();
        let bftx = backfill_tx.clone();

        let symbols_count = coinbase_cfg.symbols.len();
        info!("[coinbase] Starting collector for {} symbols", symbols_count);

        tokio::spawn(async move {
            collector::coinbase::run(coinbase_cfg, wtx, ttx, btx, dp, rb, mrb, rj, ctrs, bftx, gap_threshold).await;
        });
    }

    // Kraken (60-second bars)
    if let Some(kraken_cfg) = cfg.exchange("kraken").cloned() {
        let wtx = writer_tx.clone();
        let ttx = trade_tx.clone();
        let btx = broadcast_tx.clone();
        let dp = data_path.clone();
        let rb = cfg.collector.reconnect_backoff;
        let mrb = cfg.collector.max_reconnect_backoff;
        let rj = cfg.collector.reconnect_jitter;
        let ctrs = counters.clone();
        let bftx = backfill_tx.clone();

        let symbols_count = kraken_cfg.symbols.len();
        info!("[kraken] Starting collector for {} symbols", symbols_count);

        tokio::spawn(async move {
            collector::kraken::run(kraken_cfg, wtx, ttx, btx, dp, rb, mrb, rj, ctrs, bftx, gap_threshold).await;
        });
    }

    info!("Collector running. Press Ctrl+C to stop.");

    // Wait for shutdown signal (either Ctrl+C or tray quit)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, shutting down...");
        }
        _ = wait_for_shutdown(&shutdown) => {
            info!("Shutdown signal received, flushing buffers...");
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

async fn wait_for_shutdown(flag: &AtomicBool) {
    loop {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
