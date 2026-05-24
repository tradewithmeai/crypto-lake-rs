use crate::collector::TradeEvent;
use crate::config::Config;
use crate::health::HealthCounters;
use crate::indicators;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, Mutex};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    config: Config,
    broadcast_tx: broadcast::Sender<TradeEvent>,
    counters: Arc<HealthCounters>,
    data_path: PathBuf,
    static_dir: PathBuf,
    /// derivatives cache: symbol → (snapshot JSON, fetched_at)
    deriv_cache: Arc<Mutex<HashMap<String, (serde_json::Value, Instant)>>>,
    /// scan cache: exchange → (result JSON, fetched_at)
    scan_cache: Arc<Mutex<HashMap<String, (serde_json::Value, Instant)>>>,
}

pub async fn start_server(
    config: Config,
    broadcast_tx: broadcast::Sender<TradeEvent>,
    counters: Arc<HealthCounters>,
    data_path: PathBuf,
) {
    let port = config.server.port;

    // Resolve static dir relative to exe if needed
    let static_dir = {
        let p = PathBuf::from(&config.server.static_dir);
        if p.is_absolute() {
            p
        } else if let Ok(exe_dir) = std::env::current_exe().map(|e| e.parent().unwrap().to_path_buf()) {
            let beside_exe = exe_dir.join(&p);
            if beside_exe.exists() {
                beside_exe
            } else {
                std::env::current_dir().unwrap_or_default().join(&p)
            }
        } else {
            std::env::current_dir().unwrap_or_default().join(&p)
        }
    };

    info!("Static dir: {:?}", static_dir);

    let state = AppState {
        config,
        broadcast_tx,
        counters,
        data_path,
        static_dir: static_dir.clone(),
        deriv_cache: Arc::new(Mutex::new(HashMap::new())),
        scan_cache: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/v1/auth/me", get(auth_me))
        .route("/api/v1/symbols", get(symbols_handler))
        .route("/api/v1/bars/:symbol/latest", get(bars_handler))
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/analysis/summary", get(analysis_summary_handler))
        .route("/api/v1/indicators/:symbol", get(indicators_handler))
        .route("/api/v1/derivatives/:symbol", get(derivatives_handler))
        .route("/api/v1/snapshot/:symbol", get(snapshot_handler))
        .route("/api/v1/scan", get(scan_handler))
        .route("/api/v1/ws/stream", get(ws_handler))
        .nest_service("/static", ServeDir::new(&static_dir))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("Dashboard server listening on http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind server address");
    axum::serve(listener, app).await.unwrap_or_else(|e| {
        warn!("Server error: {}", e);
    });
}

// GET / -> serve index.html
async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let index_path = state.static_dir.join("index.html");
    match tokio::fs::read_to_string(&index_path).await {
        Ok(html) => Html(html).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "index.html not found").into_response(),
    }
}

// GET /api/v1/auth/me -> bypass auth
async fn auth_me() -> Json<serde_json::Value> {
    Json(serde_json::json!({"username": "local"}))
}

// GET /api/v1/symbols
async fn symbols_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut exchanges: HashMap<String, Vec<String>> = HashMap::new();
    for ex in &state.config.exchanges {
        exchanges.insert(ex.name.clone(), ex.symbols.clone());
    }
    Json(serde_json::json!({"exchanges": exchanges}))
}

// GET /api/v1/health
async fn health_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let c = &state.counters;
    Json(serde_json::json!({
        "messages_received": c.messages_received.load(Ordering::Relaxed),
        "trades_received": c.trades_received.load(Ordering::Relaxed),
        "bars_produced": c.bars_produced.load(Ordering::Relaxed),
        "ws_disconnects": c.ws_disconnects.load(Ordering::Relaxed),
        "ws_reconnects": c.ws_reconnects.load(Ordering::Relaxed),
    }))
}

// ── Bars endpoint ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BarsQuery {
    #[serde(default = "default_tf")]
    tf: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_tf() -> String { "5m".into() }
fn default_limit() -> usize { 500 }

#[derive(Serialize)]
struct BarRow {
    ts: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_base: f64,
    trade_count: u64,
    vwap: f64,
}

// GET /api/v1/bars/{symbol}/latest?tf=5m&limit=500
async fn bars_handler(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(params): Query<BarsQuery>,
) -> Json<serde_json::Value> {
    let tf_seconds: i64 = match params.tf.as_str() {
        "1s" => 1,
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        _ => 300,
    };

    // Find parquet files for this symbol across all exchanges
    let parquet_base = state.data_path.join("parquet");
    let mut all_bars: Vec<BarRow> = Vec::new();

    if let Ok(mut exchanges) = tokio::fs::read_dir(&parquet_base).await {
        while let Ok(Some(ex_entry)) = exchanges.next_entry().await {
            let sym_dir = ex_entry.path().join(&symbol);
            if !sym_dir.is_dir() {
                continue;
            }
            // Read all parquet files recursively
            if let Ok(bars) = read_parquet_bars(&sym_dir, tf_seconds, params.limit).await {
                all_bars.extend(bars);
            }
        }
    }

    // Sort by timestamp descending (newest first) and limit
    all_bars.sort_by(|a, b| b.ts.cmp(&a.ts));
    all_bars.truncate(params.limit);

    Json(serde_json::json!({"data": all_bars}))
}

/// Read parquet files from a symbol directory and aggregate into bars.
async fn read_parquet_bars(
    sym_dir: &std::path::Path,
    tf_seconds: i64,
    limit: usize,
) -> Result<Vec<BarRow>, Box<dyn std::error::Error + Send + Sync>> {
    // Calculate how many minute-files we need.
    // Each file covers ~60 seconds. For limit bars at tf_seconds each:
    //   needed_minutes = limit * tf_seconds / 60
    // Cap at 3000 files (~50 hours of 1m files, ~2 days of 1s files).
    let needed_files = ((limit as i64 * tf_seconds / 60) as usize + limit / 4 + 5).min(3000);

    let mut parquet_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files(sym_dir, &mut parquet_files).await;

    // Sort descending (newest first), keep only what we need
    parquet_files.sort_by(|a, b| b.cmp(a));
    parquet_files.truncate(needed_files);

    if parquet_files.is_empty() {
        return Ok(Vec::new());
    }

    // Read all files in parallel via the blocking thread pool
    type RawRow = (i64, f64, f64, f64, f64, f64, u64, f64);
    let handles: Vec<_> = parquet_files
        .into_iter()
        .map(|path| {
            tokio::task::spawn_blocking(move || -> Vec<RawRow> {
                use arrow::array::TimestampMicrosecondArray;
                use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

                let file = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(_) => return Vec::new(),
                };
                let reader = match ParquetRecordBatchReaderBuilder::try_new(file).and_then(|b| b.build()) {
                    Ok(r) => r,
                    Err(_) => return Vec::new(),
                };
                let mut rows = Vec::new();
                for batch in reader.flatten() {
                    let ts_col  = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
                    let open_col  = batch.column(3).as_any().downcast_ref::<arrow::array::Float64Array>();
                    let high_col  = batch.column(4).as_any().downcast_ref::<arrow::array::Float64Array>();
                    let low_col   = batch.column(5).as_any().downcast_ref::<arrow::array::Float64Array>();
                    let close_col = batch.column(6).as_any().downcast_ref::<arrow::array::Float64Array>();
                    let vol_col   = batch.column(7).as_any().downcast_ref::<arrow::array::Float64Array>();
                    let count_col = batch.column(9).as_any().downcast_ref::<arrow::array::UInt64Array>();
                    let vwap_col  = batch.column(10).as_any().downcast_ref::<arrow::array::Float64Array>();

                    if let (Some(ts), Some(o), Some(h), Some(l), Some(c), Some(v), Some(cnt), Some(vw)) =
                        (ts_col, open_col, high_col, low_col, close_col, vol_col, count_col, vwap_col)
                    {
                        for i in 0..batch.num_rows() {
                            let ts_sec = ts.value(i) / 1_000_000;
                            rows.push((ts_sec, o.value(i), h.value(i), l.value(i), c.value(i), v.value(i), cnt.value(i), vw.value(i)));
                        }
                    }
                }
                rows
            })
        })
        .collect();

    // (ts_sec, open, high, low, close, volume, trade_count, vwap)
    let mut raw_bars: Vec<RawRow> = Vec::new();
    for handle in handles {
        if let Ok(rows) = handle.await {
            raw_bars.extend(rows);
        }
    }

    if raw_bars.is_empty() {
        return Ok(Vec::new());
    }

    // Aggregate raw 1s bars into the requested timeframe.
    // Map value: (open, high, low, close, vol, cnt, vwap_notional, vol_for_vwap)
    let mut agg: HashMap<i64, (f64, f64, f64, f64, f64, u64, f64, f64)> = HashMap::new();
    for (ts, o, h, l, c, v, cnt, vwap) in &raw_bars {
        let bucket = (*ts / tf_seconds) * tf_seconds;
        agg.entry(bucket)
            .and_modify(|(_, ah, al, ac, av, acnt, avn, avv)| {
                *ah = ah.max(*h);
                *al = al.min(*l);
                *ac = *c; // latest close wins
                *av += v;
                *acnt += cnt;
                *avn += vwap * v; // accumulate vwap numerator
                *avv += v;        // accumulate vwap denominator
            })
            .or_insert((*o, *h, *l, *c, *v, *cnt, vwap * v, *v));
    }

    let mut bars: Vec<BarRow> = agg
        .into_iter()
        .map(|(ts, (open, high, low, close, vol, cnt, vwap_n, vwap_v))| {
            let vwap = if vwap_v > 0.0 { vwap_n / vwap_v } else { close };
            let dt = chrono::DateTime::from_timestamp(ts, 0)
                .unwrap_or_default()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string();
            BarRow {
                ts: dt,
                open,
                high,
                low,
                close,
                volume_base: vol,
                trade_count: cnt,
                vwap,
            }
        })
        .collect();

    bars.sort_by(|a, b| b.ts.cmp(&a.ts));
    bars.truncate(limit);

    Ok(bars)
}

/// Recursively collect .parquet files from a directory.
async fn collect_parquet_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(r) => r,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            Box::pin(collect_parquet_files(&path, out)).await;
        } else if path.extension().map_or(false, |e| e == "parquet") {
            out.push(path);
        }
    }
}

// ── Analysis endpoint ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SymbolSummary {
    exchange: String,
    symbol: String,
    total_bars: u64,
    live_bars: u64,
    empty_bars: u64,
    total_trades: u64,
    live_pct: f64,
    earliest_ts: String,
    latest_ts: String,
    last_close: f64,
    data_hours: f64,
}

// GET /api/v1/analysis/summary
async fn analysis_summary_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let parquet_base = state.data_path.join("parquet");
    let mut summaries: Vec<SymbolSummary> = Vec::new();

    if let Ok(mut exchanges) = tokio::fs::read_dir(&parquet_base).await {
        while let Ok(Some(ex_entry)) = exchanges.next_entry().await {
            if !ex_entry.path().is_dir() { continue; }
            let exchange_name = ex_entry.file_name().to_string_lossy().to_string();

            if let Ok(mut symbols) = tokio::fs::read_dir(ex_entry.path()).await {
                while let Ok(Some(sym_entry)) = symbols.next_entry().await {
                    if !sym_entry.path().is_dir() { continue; }
                    let symbol_name = sym_entry.file_name().to_string_lossy().to_string();

                    let mut parquet_files: Vec<PathBuf> = Vec::new();
                    collect_parquet_files(&sym_entry.path(), &mut parquet_files).await;

                    if parquet_files.is_empty() { continue; }

                    if let Ok(summary) = compute_symbol_summary(&exchange_name, &symbol_name, &parquet_files).await {
                        summaries.push(summary);
                    }
                }
            }
        }
    }

    summaries.sort_by(|a, b| a.exchange.cmp(&b.exchange).then(a.symbol.cmp(&b.symbol)));
    Json(serde_json::json!({ "symbols": summaries }))
}

async fn compute_symbol_summary(
    exchange: &str,
    symbol: &str,
    files: &[PathBuf],
) -> Result<SymbolSummary, Box<dyn std::error::Error + Send + Sync>> {
    let files = files.to_vec();
    let exchange = exchange.to_string();
    let symbol = symbol.to_string();

    let result = tokio::task::spawn_blocking(move || -> Result<SymbolSummary, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut total_bars: u64 = 0;
        let mut live_bars: u64 = 0;
        let mut empty_bars: u64 = 0;
        let mut total_trades: u64 = 0;
        let mut earliest_us: Option<i64> = None;
        let mut latest_us: Option<i64> = None;
        let mut last_close: f64 = 0.0;

        for file_path in &files {
            let file = match std::fs::File::open(file_path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let reader = match ParquetRecordBatchReaderBuilder::try_new(file).and_then(|b| b.build()) {
                Ok(r) => r,
                Err(_) => continue,
            };

            for batch in reader {
                let batch = match batch { Ok(b) => b, Err(_) => continue };
                let n = batch.num_rows();
                // Schema: col 0=window_start, 6=close, 9=trade_count, 14=source
                let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
                let close_col = batch.column(6).as_any().downcast_ref::<Float64Array>();
                let count_col = batch.column(9).as_any().downcast_ref::<UInt64Array>();
                let source_col = batch.column(14).as_any().downcast_ref::<StringArray>();

                if let (Some(ts), Some(close), Some(cnt), Some(src)) =
                    (ts_col, close_col, count_col, source_col)
                {
                    for i in 0..n {
                        let ts_us = ts.value(i);
                        let count = cnt.value(i);
                        let src_val = src.value(i);
                        let close_val = close.value(i);

                        total_bars += 1;
                        total_trades += count;
                        if src_val == "live" { live_bars += 1; }
                        if count == 0 { empty_bars += 1; }

                        match earliest_us {
                            None => earliest_us = Some(ts_us),
                            Some(e) if ts_us < e => earliest_us = Some(ts_us),
                            _ => {}
                        }
                        match latest_us {
                            None => { latest_us = Some(ts_us); last_close = close_val; }
                            Some(l) if ts_us > l => { latest_us = Some(ts_us); last_close = close_val; }
                            _ => {}
                        }
                    }
                }
            }
        }

        let live_pct = if total_bars > 0 { live_bars as f64 / total_bars as f64 * 100.0 } else { 0.0 };

        let fmt_ts = |us: i64| {
            chrono::DateTime::from_timestamp(us / 1_000_000, 0)
                .unwrap_or_default()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        };

        let earliest_ts = earliest_us.map(fmt_ts).unwrap_or_default();
        let latest_ts = latest_us.map(fmt_ts).unwrap_or_default();

        let data_hours = match (earliest_us, latest_us) {
            (Some(e), Some(l)) => (l - e) as f64 / 1_000_000.0 / 3600.0,
            _ => 0.0,
        };

        Ok(SymbolSummary {
            exchange, symbol, total_bars, live_bars, empty_bars, total_trades,
            live_pct, earliest_ts, latest_ts, last_close, data_hours,
        })
    }).await??;

    Ok(result)
}

// ── WebSocket handler ───────────────────────────────────────────────────────

#[derive(Deserialize)]
#[allow(dead_code)]
struct WsQuery {
    #[serde(default)]
    symbols: String,
    #[serde(default)]
    types: String,
    #[serde(default)]
    token: String,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<WsQuery>,
) -> impl IntoResponse {
    let symbols: Vec<String> = if params.symbols.is_empty() {
        Vec::new()
    } else {
        params.symbols.split(',').map(|s| s.to_string()).collect()
    };

    let broadcast_rx = state.broadcast_tx.subscribe();

    ws.on_upgrade(move |socket| handle_ws(socket, broadcast_rx, symbols))
}

async fn handle_ws(
    mut socket: WebSocket,
    mut broadcast_rx: broadcast::Receiver<TradeEvent>,
    symbols: Vec<String>,
) {
    info!("WebSocket client connected, filter: {:?}", symbols);

    loop {
        tokio::select! {
            result = broadcast_rx.recv() => {
                match result {
                    Ok(trade) => {
                        // Filter by symbol if specified
                        if !symbols.is_empty() && !symbols.contains(&trade.symbol) {
                            continue;
                        }

                        let msg = serde_json::json!({
                            "exchange": trade.exchange,
                            "symbol": trade.symbol,
                            "price": trade.price,
                            "qty": trade.qty,
                            "ts_event": trade.timestamp_ms,
                            "ts_recv": chrono::Utc::now().timestamp_millis(),
                            "side": if trade.is_buyer_maker { "sell" } else { "buy" },
                            "stream": "trade",
                        });

                        if let Err(_) = socket.send(Message::Text(msg.to_string().into())).await {
                            break; // Client disconnected
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client lagged, skipped {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            // Check for client messages (ping/pong/close)
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    _ => {}
                }
            }
        }
    }

    info!("WebSocket client disconnected");
}

// ── Shared query params ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct AgentQuery {
    #[serde(default = "default_tf")]
    tf: String,
    #[serde(default = "default_limit")]
    limit: usize,
    /// Optional exchange filter to resolve symbol ambiguity.
    exchange: Option<String>,
}

// ── Indicator computation ────────────────────────────────────────────────────

/// A bar with only what indicator math needs.
struct CompactBar {
    ts: i64,   // unix seconds (bucket start)
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_base: f64,
    trade_count: u64,
    vwap: f64,
    is_live: bool,
}

/// Read bars for a single exchange/symbol dir and return them oldest-first,
/// dropping any in-progress (incomplete) last bar.
/// `limit` controls how many *complete* bars are returned.
async fn read_bars_for_indicators(
    sym_dir: &std::path::Path,
    tf_seconds: i64,
    limit: usize,
) -> Vec<CompactBar> {
    // We need limit+1 to detect and drop the in-progress bar.
    let needed = limit + 1;
    let needed_files = ((needed as i64 * tf_seconds / 60) as usize + needed / 4 + 10).min(3000);

    let mut parquet_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files(sym_dir, &mut parquet_files).await;
    parquet_files.sort_by(|a, b| b.cmp(a));
    parquet_files.truncate(needed_files);

    if parquet_files.is_empty() {
        return Vec::new();
    }

    // (ts_sec, o, h, l, c, vol, cnt, vwap, is_live)
    type RawRow = (i64, f64, f64, f64, f64, f64, u64, f64, bool);
    let handles: Vec<_> = parquet_files
        .into_iter()
        .map(|path| {
            tokio::task::spawn_blocking(move || -> Vec<RawRow> {
                use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
                use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

                let file = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(_) => return Vec::new(),
                };
                let reader = match ParquetRecordBatchReaderBuilder::try_new(file)
                    .and_then(|b| b.build())
                {
                    Ok(r) => r,
                    Err(_) => return Vec::new(),
                };
                let mut rows = Vec::new();
                for batch in reader.flatten() {
                    let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
                    let open_col = batch.column(3).as_any().downcast_ref::<Float64Array>();
                    let high_col = batch.column(4).as_any().downcast_ref::<Float64Array>();
                    let low_col = batch.column(5).as_any().downcast_ref::<Float64Array>();
                    let close_col = batch.column(6).as_any().downcast_ref::<Float64Array>();
                    let vol_col = batch.column(7).as_any().downcast_ref::<Float64Array>();
                    let count_col = batch.column(9).as_any().downcast_ref::<UInt64Array>();
                    let vwap_col = batch.column(10).as_any().downcast_ref::<Float64Array>();
                    let src_col = batch.column(14).as_any().downcast_ref::<StringArray>();

                    if let (Some(ts), Some(o), Some(h), Some(l), Some(c), Some(v), Some(cnt), Some(vw), Some(src)) =
                        (ts_col, open_col, high_col, low_col, close_col, vol_col, count_col, vwap_col, src_col)
                    {
                        for i in 0..batch.num_rows() {
                            rows.push((
                                ts.value(i) / 1_000_000,
                                o.value(i), h.value(i), l.value(i), c.value(i),
                                v.value(i), cnt.value(i), vw.value(i),
                                src.value(i) == "live",
                            ));
                        }
                    }
                }
                rows
            })
        })
        .collect();

    let mut raw: Vec<RawRow> = Vec::new();
    for h in handles {
        if let Ok(rows) = h.await {
            raw.extend(rows);
        }
    }
    if raw.is_empty() {
        return Vec::new();
    }

    // Aggregate into tf-second buckets
    let mut agg: HashMap<i64, (f64, f64, f64, f64, f64, u64, f64, f64, bool)> = HashMap::new();
    for (ts, o, h, l, c, v, cnt, vwap, is_live) in &raw {
        let bucket = (*ts / tf_seconds) * tf_seconds;
        agg.entry(bucket)
            .and_modify(|(_, ah, al, ac, av, acnt, avn, avv, alive)| {
                *ah = ah.max(*h);
                *al = al.min(*l);
                *ac = *c;
                *av += v;
                *acnt += cnt;
                *avn += vwap * v;
                *avv += v;
                if *is_live { *alive = true; }
            })
            .or_insert((*o, *h, *l, *c, *v, *cnt, vwap * v, *v, *is_live));
    }

    let mut bars: Vec<CompactBar> = agg
        .into_iter()
        .map(|(ts, (open, high, low, close, vol, cnt, vwap_n, vwap_v, is_live))| {
            let vwap = if vwap_v > 0.0 { vwap_n / vwap_v } else { close };
            CompactBar { ts, open, high, low, close, volume_base: vol, trade_count: cnt, vwap, is_live }
        })
        .collect();

    // Oldest-first for indicator math
    bars.sort_by_key(|b| b.ts);

    // Drop the last bar — it may be in-progress at query time
    if bars.len() > 1 {
        bars.pop();
    }

    // Keep only the last `limit` complete bars
    let len = bars.len();
    if len > limit {
        bars.drain(0..(len - limit));
    }
    bars
}

/// Resolve (exchange_name, sym_dir) for a given symbol, optionally filtered by exchange.
fn resolve_sym_dir(
    state: &AppState,
    symbol: &str,
    exchange_filter: Option<&str>,
) -> Option<(String, PathBuf)> {
    let parquet_base = state.data_path.join("parquet");
    for ex in &state.config.exchanges {
        if let Some(ef) = exchange_filter {
            if ex.name != ef {
                continue;
            }
        }
        if ex.symbols.contains(&symbol.to_string()) {
            let dir = parquet_base.join(&ex.name).join(symbol);
            if dir.is_dir() {
                return Some((ex.name.clone(), dir));
            }
        }
    }
    None
}

/// Compute all indicators from an ordered (oldest-first) bar slice.
/// Returns a JSON-ready value plus signal counters for regime classification.
fn compute_indicator_json(
    bars: &[CompactBar],
    price: f64,
) -> (serde_json::Value, u8, u8) {
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let n = closes.len();

    // ── RSI ──
    let rsi_series = indicators::calc_rsi(&closes, 14);
    let rsi_val = rsi_series.last().copied().flatten().unwrap_or(50.0);
    let (rsi_signal, rsi_note) = if rsi_val < 30.0 {
        ("oversold", "approaching support")
    } else if rsi_val > 70.0 {
        ("overbought", "approaching resistance")
    } else if rsi_val > 60.0 {
        ("neutral", "approaching overbought")
    } else if rsi_val < 40.0 {
        ("neutral", "approaching oversold")
    } else {
        ("neutral", "mid-range")
    };

    // ── MACD ──
    let (macd_line, signal_line, histogram) = indicators::calc_macd(&closes);
    let macd_val = macd_line.last().copied().flatten().unwrap_or(0.0);
    let sig_val = signal_line.last().copied().flatten().unwrap_or(0.0);
    let hist_val = histogram.last().copied().flatten().unwrap_or(0.0);
    let prev_hist = if n >= 2 { histogram.get(n - 2).copied().flatten().unwrap_or(0.0) } else { 0.0 };
    let macd_direction = if hist_val > 0.0 && hist_val >= prev_hist {
        "bullish_accelerating"
    } else if hist_val > 0.0 && hist_val < prev_hist {
        "bullish_weakening"
    } else if hist_val < 0.0 && hist_val.abs() <= prev_hist.abs() {
        "bearish_weakening"
    } else if hist_val < 0.0 {
        "bearish_accelerating"
    } else {
        "neutral"
    };
    let macd_crossover = if macd_val > sig_val && macd_val - sig_val < (macd_val.abs() * 0.01 + 0.001) {
        "bullish_crossover"
    } else if macd_val < sig_val && sig_val - macd_val < (sig_val.abs() * 0.01 + 0.001) {
        "bearish_crossover"
    } else {
        "none"
    };

    // ── Bollinger Bands ──
    let (bb_upper, bb_middle, bb_lower) = indicators::calc_bb(&closes, 20, 2.0);
    let bb_u = bb_upper.last().copied().flatten().unwrap_or(price);
    let bb_m = bb_middle.last().copied().flatten().unwrap_or(price);
    let bb_l = bb_lower.last().copied().flatten().unwrap_or(price);
    let bb_pos = if bb_u > bb_l { (price - bb_l) / (bb_u - bb_l) } else { 0.5 };
    let bb_signal = if bb_pos > 0.8 { "near_upper" } else if bb_pos < 0.2 { "near_lower" } else if bb_pos >= 0.5 { "upper_half" } else { "lower_half" };

    // ── SMAs ──
    let sma20_s = indicators::calc_sma(&closes, 20);
    let sma50_s = indicators::calc_sma(&closes, 50);
    let sma200_s = indicators::calc_sma(&closes, 200);
    let sma20 = sma20_s.last().copied().flatten().unwrap_or(price);
    let sma50 = sma50_s.last().copied().flatten().unwrap_or(price);
    let sma200 = sma200_s.last().copied().flatten().unwrap_or(price);
    let sma_trend = if price > sma20 && sma20 > sma50 && sma50 > sma200 {
        "bullish_aligned"
    } else if price < sma20 && sma20 < sma50 && sma50 < sma200 {
        "bearish_aligned"
    } else {
        "mixed"
    };

    // ── EMAs ──
    let ema12_s = indicators::calc_ema(&closes, 12);
    let ema26_s = indicators::calc_ema(&closes, 26);
    let ema12 = ema12_s.last().copied().flatten().unwrap_or(price);
    let ema26 = ema26_s.last().copied().flatten().unwrap_or(price);
    let prev_ema12 = if n >= 2 { ema12_s.get(n - 2).copied().flatten() } else { None };
    let prev_ema26 = if n >= 2 { ema26_s.get(n - 2).copied().flatten() } else { None };
    let ema_signal = if ema12 > ema26 {
        if prev_ema12.map_or(true, |p| p <= prev_ema26.unwrap_or(f64::MAX)) {
            "bullish_crossover"
        } else {
            "bullish"
        }
    } else if prev_ema12.map_or(true, |p| p >= prev_ema26.unwrap_or(0.0)) {
        "bearish_crossover"
    } else {
        "bearish"
    };

    // ── VWAP ──
    let vwap_val = bars.last().map(|b| b.vwap).unwrap_or(price);
    let vwap_signal = if price > vwap_val { "bullish" } else { "bearish" };
    let price_vs_vwap = if price > vwap_val { "above" } else { "below" };

    // ── Volume ──
    let live_vols: Vec<f64> = bars.iter().filter(|b| b.is_live).map(|b| b.volume_base).collect();
    let current_vol = bars.last().map(|b| b.volume_base).unwrap_or(0.0);
    let avg_20_vol = if live_vols.len() >= 20 {
        live_vols[live_vols.len() - 20..].iter().sum::<f64>() / 20.0
    } else if !live_vols.is_empty() {
        live_vols.iter().sum::<f64>() / live_vols.len() as f64
    } else {
        1.0
    };
    let vol_ratio = if avg_20_vol > 0.0 { current_vol / avg_20_vol } else { 1.0 };
    let vol_signal = if vol_ratio > 1.5 { "elevated" } else if vol_ratio < 0.5 { "low" } else { "normal" };

    // ── Regime signal counting ──
    let mut bullish: u8 = 0;
    let mut bearish: u8 = 0;
    // RSI: >50 bullish, <50 bearish
    if rsi_val > 50.0 { bullish += 1; } else { bearish += 1; }
    // MACD: positive histogram bullish
    if hist_val > 0.0 { bullish += 1; } else { bearish += 1; }
    // BB: >0.5 bullish
    if bb_pos > 0.5 { bullish += 1; } else { bearish += 1; }
    // EMA crossover/direction
    if ema12 > ema26 { bullish += 1; } else { bearish += 1; }
    // Price vs VWAP
    if price > vwap_val { bullish += 1; } else { bearish += 1; }
    // SMA trend
    if sma_trend == "bullish_aligned" { bullish += 1; } else if sma_trend == "bearish_aligned" { bearish += 1; }

    let total_signals: u8 = bullish + bearish;

    let indicators_json = serde_json::json!({
        "rsi": {
            "value": round2(rsi_val),
            "signal": rsi_signal,
            "note": rsi_note
        },
        "macd": {
            "value": round4(macd_val),
            "signal_line": round4(sig_val),
            "histogram": round4(hist_val),
            "direction": macd_direction,
            "crossover": macd_crossover
        },
        "bollinger": {
            "upper": round2(bb_u),
            "middle": round2(bb_m),
            "lower": round2(bb_l),
            "position": (bb_pos * 1000.0).round() / 1000.0,
            "signal": bb_signal
        },
        "sma": {
            "sma20": round2(sma20),
            "sma50": round2(sma50),
            "sma200": round2(sma200),
            "trend": sma_trend
        },
        "ema": {
            "ema12": round2(ema12),
            "ema26": round2(ema26),
            "signal": ema_signal
        },
        "vwap": {
            "value": round2(vwap_val),
            "price_vs_vwap": price_vs_vwap,
            "signal": vwap_signal
        },
        "volume": {
            "current_bar": round4(current_vol),
            "avg_20": round4(avg_20_vol),
            "ratio": (vol_ratio * 100.0).round() / 100.0,
            "signal": vol_signal
        }
    });

    (indicators_json, bullish, total_signals)
}

fn round2(v: f64) -> f64 { (v * 100.0).round() / 100.0 }
fn round4(v: f64) -> f64 { (v * 10000.0).round() / 10000.0 }

fn regime_label(bullish: u8, total: u8) -> (&'static str, &'static str) {
    if total == 0 { return ("neutral_ranging", "low"); }
    let bearish = total - bullish;
    let confidence = if bullish >= total - 1 || bearish >= total - 1 {
        "high"
    } else if bullish >= (total as f32 * 0.6) as u8 || bearish >= (total as f32 * 0.6) as u8 {
        "medium"
    } else {
        "low"
    };
    let label = if bullish >= total - 1 {
        "bullish_momentum"
    } else if bullish > bearish + 1 {
        "bullish_bias"
    } else if bearish > bullish + 1 {
        "bearish_bias"
    } else if bearish >= total - 1 {
        "bearish_momentum"
    } else {
        "neutral_ranging"
    };
    (label, confidence)
}

// ── GET /api/v1/indicators/:symbol ──────────────────────────────────────────

async fn indicators_handler(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(params): Query<AgentQuery>,
) -> impl IntoResponse {
    let tf_seconds: i64 = parse_tf(&params.tf);
    let limit = params.limit.max(200);

    let (exchange, sym_dir) = match resolve_sym_dir(&state, &symbol, params.exchange.as_deref()) {
        Some(v) => v,
        None => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "symbol not found"}))).into_response();
        }
    };

    let bars = read_bars_for_indicators(&sym_dir, tf_seconds, limit).await;
    if bars.is_empty() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "no data"}))).into_response();
    }

    let price = bars.last().map(|b| b.close).unwrap_or(0.0);
    let last_ts = bars.last().map(|b| {
        chrono::DateTime::from_timestamp(b.ts, 0)
            .unwrap_or_default()
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string()
    }).unwrap_or_default();

    let (ind_json, bullish, total) = compute_indicator_json(&bars, price);
    let (regime, confidence) = regime_label(bullish, total);

    Json(serde_json::json!({
        "symbol": symbol,
        "exchange": exchange,
        "tf": params.tf,
        "ts": last_ts,
        "bar_complete": true,
        "price": round2(price),
        "regime": regime,
        "confidence": confidence,
        "indicators": ind_json
    })).into_response()
}

// ── GET /api/v1/derivatives/:symbol ─────────────────────────────────────────

#[derive(Deserialize)]
struct ExchangeQuery {
    exchange: Option<String>,
}

async fn derivatives_handler(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(params): Query<ExchangeQuery>,
) -> impl IntoResponse {
    // Only Binance spot symbols map to Binance Futures perpetuals.
    let exchange = params.exchange.as_deref().unwrap_or("binance");
    if exchange != "binance" {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "derivatives only available for binance symbols"
        }))).into_response();
    }
    // Verify symbol exists in config
    let known = state.config.exchanges.iter()
        .find(|e| e.name == "binance")
        .map(|e| e.symbols.contains(&symbol))
        .unwrap_or(false);
    if !known {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "symbol not found in binance exchange config"
        }))).into_response();
    }

    // Check cache (30s TTL)
    {
        let cache = state.deriv_cache.lock().await;
        if let Some((cached, fetched_at)) = cache.get(&symbol) {
            if fetched_at.elapsed().as_secs() < 30 {
                return Json(cached.clone()).into_response();
            }
        }
    }

    let snap = fetch_derivatives(&symbol).await;
    {
        let mut cache = state.deriv_cache.lock().await;
        cache.insert(symbol.clone(), (snap.clone(), Instant::now()));
    }
    Json(snap).into_response()
}

async fn fetch_derivatives(symbol: &str) -> serde_json::Value {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let sym = symbol.to_string();
    let c1 = client.clone();
    let c2 = client.clone();
    let c3 = client.clone();
    let c4 = client.clone();
    let s1 = sym.clone();
    let s2 = sym.clone();
    let s3 = sym.clone();
    let s4 = sym.clone();

    let (funding_res, oi_res, lsr_res, ticker_res) = tokio::join!(
        async move { c1.get(format!("https://fapi.binance.com/fapi/v1/fundingRate?symbol={}&limit=1", s1)).send().await },
        async move { c2.get(format!("https://fapi.binance.com/fapi/v1/openInterest?symbol={}", s2)).send().await },
        async move { c3.get(format!("https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={}&period=5m&limit=1", s3)).send().await },
        async move { c4.get(format!("https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={}", s4)).send().await }
    );

    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // ── Funding rate ──
    let funding_json = match funding_res {
        Ok(r) if r.status().is_success() => r.json::<serde_json::Value>().await.ok(),
        _ => None,
    };
    let funding_block = if let Some(arr) = funding_json.as_ref().and_then(|v| v.as_array()).filter(|a| !a.is_empty()) {
        let entry = &arr[0];
        let rate: f64 = entry["fundingRate"].as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let next_ms = entry["fundingTime"].as_i64().unwrap_or(0);
        let next_ts = chrono::DateTime::from_timestamp_millis(next_ms)
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_default();
        let rate_annualised = rate * 3.0 * 365.0;
        let signal = if rate > 0.0001 { "bullish" } else if rate < -0.0001 { "bearish" } else { "neutral" };
        let note = if rate > 0.0 { "positive funding: longs paying shorts" } else { "negative funding: shorts paying longs" };
        serde_json::json!({
            "rate": rate,
            "rate_8h_annualised": round4(rate_annualised),
            "next_funding_time": next_ts,
            "signal": signal,
            "note": note
        })
    } else {
        serde_json::Value::Null
    };

    // ── Open interest ──
    let oi_json = match oi_res {
        Ok(r) if r.status().is_success() => r.json::<serde_json::Value>().await.ok(),
        _ => None,
    };
    let oi_block = if let Some(v) = oi_json {
        let oi: f64 = v["openInterest"].as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        serde_json::json!({ "value_contracts": round2(oi), "signal": "unknown" })
    } else {
        serde_json::Value::Null
    };

    // ── Long/short ratio ──
    let lsr_json = match lsr_res {
        Ok(r) if r.status().is_success() => r.json::<serde_json::Value>().await.ok(),
        _ => None,
    };
    let lsr_block = if let Some(arr) = lsr_json.as_ref().and_then(|v| v.as_array()).filter(|a| !a.is_empty()) {
        let entry = &arr[0];
        let ratio: f64 = entry["longShortRatio"].as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1.0);
        let signal = if ratio > 1.1 { "longs_dominant" } else if ratio < 0.9 { "shorts_dominant" } else { "balanced" };
        serde_json::json!({ "value": round4(ratio), "signal": signal })
    } else {
        serde_json::Value::Null
    };

    // ── Ticker ──
    let ticker_json = match ticker_res {
        Ok(r) if r.status().is_success() => r.json::<serde_json::Value>().await.ok(),
        _ => None,
    };
    let (mark_price, change_24h_pct) = if let Some(ref v) = ticker_json {
        let mp: f64 = v["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let pct: f64 = v["priceChangePercent"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        (mp, pct)
    } else {
        (0.0, 0.0)
    };

    serde_json::json!({
        "symbol": symbol,
        "ts": now,
        "funding": funding_block,
        "open_interest": oi_block,
        "long_short_ratio": lsr_block,
        "mark_price": round2(mark_price),
        "change_24h_pct": round4(change_24h_pct)
    })
}

// ── GET /api/v1/snapshot/:symbol ─────────────────────────────────────────────

async fn snapshot_handler(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(params): Query<AgentQuery>,
) -> impl IntoResponse {
    let tf_seconds: i64 = parse_tf(&params.tf);
    let limit = params.limit.max(200);

    let (exchange, sym_dir) = match resolve_sym_dir(&state, &symbol, params.exchange.as_deref()) {
        Some(v) => v,
        None => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "symbol not found"}))).into_response();
        }
    };

    // Fire indicator read + (maybe) derivatives fetch in parallel
    let is_binance = exchange == "binance";
    let sym_for_deriv = symbol.clone();
    let state_for_deriv = state.clone();

    let (bars, deriv_result) = tokio::join!(
        read_bars_for_indicators(&sym_dir, tf_seconds, limit),
        async move {
            if is_binance {
                // Check cache first
                let cached = {
                    let cache = state_for_deriv.deriv_cache.lock().await;
                    cache.get(&sym_for_deriv).and_then(|(v, t)| {
                        if t.elapsed().as_secs() < 30 { Some(v.clone()) } else { None }
                    })
                };
                if let Some(v) = cached { return Some(v); }
                let snap = fetch_derivatives(&sym_for_deriv).await;
                let mut cache = state_for_deriv.deriv_cache.lock().await;
                cache.insert(sym_for_deriv.clone(), (snap.clone(), Instant::now()));
                Some(snap)
            } else {
                None
            }
        }
    );

    if bars.is_empty() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "no data"}))).into_response();
    }

    let price = bars.last().map(|b| b.close).unwrap_or(0.0);
    let last_bar = bars.last().unwrap();
    let last_ts = chrono::DateTime::from_timestamp(last_bar.ts, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let (ind_json, mut bullish, mut total) = compute_indicator_json(&bars, price);

    // Add derivatives signals to regime if available
    let mut regime_basis: Vec<String> = vec![
        if price > bars.last().map(|b| b.vwap).unwrap_or(price) { "price_above_vwap".into() } else { "price_below_vwap".into() }
    ];
    if let Some(ref deriv) = deriv_result {
        if let Some(rate) = deriv["funding"]["rate"].as_f64() {
            if rate > 0.0 {
                bullish += 1;
                regime_basis.push("positive_funding".into());
            } else {
                total += 1;
                regime_basis.push("negative_funding".into());
            }
            total += 1;
        }
    }

    let (regime_lbl, confidence) = regime_label(bullish, total);

    // 24h stats (from last 1440 1m bars = 24 bars in 1h tf, etc.)
    let bars_24h = (86400i64 / tf_seconds) as usize;
    let start_24h = if bars.len() > bars_24h { &bars[bars.len() - bars_24h..] } else { &bars[..] };
    let open_24h = start_24h.first().map(|b| b.open).unwrap_or(price);
    let high_24h = start_24h.iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
    let low_24h = start_24h.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
    let live_bars = bars.iter().filter(|b| b.is_live);
    let vol_24h: f64 = start_24h.iter().filter(|b| b.is_live).map(|b| b.volume_base).sum();
    let trades_24h: u64 = start_24h.iter().map(|b| b.trade_count).sum();
    let vwap_24h_n: f64 = start_24h.iter().filter(|b| b.is_live).map(|b| b.vwap * b.volume_base).sum();
    let vwap_24h_d: f64 = start_24h.iter().filter(|b| b.is_live).map(|b| b.volume_base).sum();
    let vwap_24h = if vwap_24h_d > 0.0 { vwap_24h_n / vwap_24h_d } else { price };
    let change_24h_pct = if open_24h > 0.0 { (price - open_24h) / open_24h * 100.0 } else { 0.0 };

    // bars_sample: last 5 complete bars
    let sample_start = if bars.len() > 5 { bars.len() - 5 } else { 0 };
    let bars_sample: Vec<serde_json::Value> = bars[sample_start..].iter().map(|b| {
        serde_json::json!({
            "ts": chrono::DateTime::from_timestamp(b.ts, 0).unwrap_or_default().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "open": round2(b.open),
            "high": round2(b.high),
            "low": round2(b.low),
            "close": round2(b.close),
            "volume": round4(b.volume_base),
            "vwap": round2(b.vwap)
        })
    }).collect();

    // Suppress unused variable warning
    let _ = live_bars;

    Json(serde_json::json!({
        "symbol": symbol,
        "exchange": exchange,
        "tf": params.tf,
        "ts": last_ts,
        "generated_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "bar_complete": true,
        "price": {
            "last": round2(price),
            "open_24h": round2(open_24h),
            "change_24h_pct": round4(change_24h_pct),
            "high_24h": round2(high_24h),
            "low_24h": round2(low_24h)
        },
        "volume": {
            "volume_24h_base": round4(vol_24h),
            "trade_count_24h": trades_24h,
            "vwap_24h": round2(vwap_24h)
        },
        "indicators": ind_json,
        "derivatives": deriv_result,
        "regime": {
            "label": regime_lbl,
            "basis": regime_basis,
            "confidence": confidence
        },
        "bars_sample": bars_sample
    })).into_response()
}

// ── GET /api/v1/scan ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ScanQuery {
    #[serde(default = "default_exchange")]
    exchange: String,
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default = "default_scan_limit")]
    limit: usize,
}
fn default_exchange() -> String { "binance".into() }
fn default_sort() -> String { "momentum".into() }
fn default_scan_limit() -> usize { 10 }

async fn scan_handler(
    State(state): State<AppState>,
    Query(params): Query<ScanQuery>,
) -> impl IntoResponse {
    let limit = params.limit.min(20).max(1);

    // Check cache (60s TTL)
    let cache_key = format!("{}:{}", params.exchange, params.sort);
    {
        let cache = state.scan_cache.lock().await;
        if let Some((cached, fetched_at)) = cache.get(&cache_key) {
            if fetched_at.elapsed().as_secs() < 60 {
                return Json(cached.clone()).into_response();
            }
        }
    }

    // Find all symbols for the requested exchange
    let symbols: Vec<String> = state.config.exchanges.iter()
        .find(|e| e.name == params.exchange)
        .map(|e| e.symbols.clone())
        .unwrap_or_default();

    if symbols.is_empty() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "exchange not found"}))).into_response();
    }

    let parquet_base = state.data_path.join("parquet");
    let exchange_name = params.exchange.clone();

    // Compute indicators for all symbols in parallel (100 bar cap for speed)
    let mut handles = Vec::new();
    for sym in &symbols {
        let sym_dir = parquet_base.join(&exchange_name).join(sym);
        if !sym_dir.is_dir() { continue; }
        let sym_clone = sym.clone();
        let dir_clone = sym_dir.clone();
        handles.push(async move {
            let bars = read_bars_for_indicators(&dir_clone, 300, 100).await; // 5m, 100 bars
            (sym_clone, bars)
        });
    }

    let results = futures_util::future::join_all(handles).await;

    let mut scan_rows: Vec<serde_json::Value> = Vec::new();
    for (sym, bars) in results {
        if bars.len() < 14 { continue; }
        let price = bars.last().map(|b| b.close).unwrap_or(0.0);
        let (ind_json, bullish, total) = compute_indicator_json(&bars, price);
        let (regime, _confidence) = regime_label(bullish, total);

        let rsi = ind_json["rsi"]["value"].as_f64().unwrap_or(50.0);
        let hist = ind_json["macd"]["histogram"].as_f64().unwrap_or(0.0);
        let bb_pos = ind_json["bollinger"]["position"].as_f64().unwrap_or(0.5);
        let vol_ratio = ind_json["volume"]["ratio"].as_f64().unwrap_or(1.0);
        let bb_upper = ind_json["bollinger"]["upper"].as_f64().unwrap_or(price);
        let bb_lower = ind_json["bollinger"]["lower"].as_f64().unwrap_or(price);
        let bb_middle = ind_json["bollinger"]["middle"].as_f64().unwrap_or(price);
        let sma20 = ind_json["sma"]["sma20"].as_f64().unwrap_or(price);
        let macd_dir = ind_json["macd"]["direction"].as_str().unwrap_or("neutral");

        // 24h price change (using available bars)
        let start_24h = if bars.len() > 288 { &bars[bars.len()-288..] } else { &bars[..] }; // 288 5m = 24h
        let open_24h = start_24h.first().map(|b| b.open).unwrap_or(price);
        let change_24h_pct = if open_24h > 0.0 { (price - open_24h) / open_24h * 100.0 } else { 0.0 };

        let score: f64 = match params.sort.as_str() {
            "momentum" => {
                let rsi_component = (rsi - 50.0) / 50.0;
                let macd_sign = if hist > 0.0 { 1.0 } else { -1.0 };
                let sma_component = if price > sma20 { 1.0 } else { -1.0 };
                (rsi_component + macd_sign + sma_component) / 3.0
            }
            "volume" => vol_ratio,
            "volatility" => if bb_middle > 0.0 { (bb_upper - bb_lower) / bb_middle } else { 0.0 },
            "rsi_extreme" => (rsi - 50.0).abs() / 50.0,
            _ => 0.0,
        };

        scan_rows.push(serde_json::json!({
            "symbol": sym,
            "score": round4(score),
            "price": round2(price),
            "change_24h_pct": round4(change_24h_pct),
            "rsi": round2(rsi),
            "macd_direction": macd_dir,
            "volume_ratio": round4(vol_ratio),
            "bb_position": (bb_pos * 1000.0).round() / 1000.0,
            "regime": regime
        }));
    }

    // Sort descending by score
    scan_rows.sort_by(|a, b| {
        let sa = a["score"].as_f64().unwrap_or(0.0);
        let sb = b["score"].as_f64().unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });
    scan_rows.truncate(limit);

    // Add rank
    for (i, row) in scan_rows.iter_mut().enumerate() {
        if let Some(obj) = row.as_object_mut() {
            obj.insert("rank".into(), serde_json::json!(i + 1));
        }
    }

    let result = serde_json::json!({
        "exchange": params.exchange,
        "sort": params.sort,
        "ts": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "results": scan_rows
    });

    {
        let mut cache = state.scan_cache.lock().await;
        cache.insert(cache_key, (result.clone(), Instant::now()));
    }

    Json(result).into_response()
}

fn parse_tf(tf: &str) -> i64 {
    match tf {
        "1s" => 1,
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        _ => 300,
    }
}
