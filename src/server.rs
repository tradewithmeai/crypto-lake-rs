use crate::collector::TradeEvent;
use crate::config::Config;
use crate::health::HealthCounters;
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
use tokio::sync::broadcast;
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
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/v1/auth/me", get(auth_me))
        .route("/api/v1/symbols", get(symbols_handler))
        .route("/api/v1/bars/:symbol/latest", get(bars_handler))
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/analysis/summary", get(analysis_summary_handler))
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
    use arrow::array::TimestampMicrosecondArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut parquet_files: Vec<PathBuf> = Vec::new();
    collect_parquet_files(sym_dir, &mut parquet_files).await;

    // Sort by filename descending (newest first) and take only recent files
    parquet_files.sort_by(|a, b| b.cmp(a));
    parquet_files.truncate(10); // Only read the 10 most recent files

    // (ts_sec, open, high, low, close, volume, trade_count, vwap)
    let mut raw_bars: Vec<(i64, f64, f64, f64, f64, f64, u64, f64)> = Vec::new();

    for file_path in &parquet_files {
        let path = file_path.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<(i64, f64, f64, f64, f64, f64, u64, f64)>, Box<dyn std::error::Error + Send + Sync>> {
            let file = std::fs::File::open(&path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                .build()?;

            let mut rows = Vec::new();
            for batch in reader {
                let batch = batch?;
                let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
                let open_col = batch.column(3).as_any().downcast_ref::<arrow::array::Float64Array>();
                let high_col = batch.column(4).as_any().downcast_ref::<arrow::array::Float64Array>();
                let low_col = batch.column(5).as_any().downcast_ref::<arrow::array::Float64Array>();
                let close_col = batch.column(6).as_any().downcast_ref::<arrow::array::Float64Array>();
                let vol_col = batch.column(7).as_any().downcast_ref::<arrow::array::Float64Array>();
                let count_col = batch.column(9).as_any().downcast_ref::<arrow::array::UInt64Array>();
                let vwap_col = batch.column(10).as_any().downcast_ref::<arrow::array::Float64Array>();

                if let (Some(ts), Some(o), Some(h), Some(l), Some(c), Some(v), Some(cnt), Some(vw)) =
                    (ts_col, open_col, high_col, low_col, close_col, vol_col, count_col, vwap_col)
                {
                    for i in 0..batch.num_rows() {
                        let ts_us = ts.value(i);
                        let ts_sec = ts_us / 1_000_000;
                        rows.push((ts_sec, o.value(i), h.value(i), l.value(i), c.value(i), v.value(i), cnt.value(i), vw.value(i)));
                    }
                }
            }
            Ok(rows)
        }).await??;

        raw_bars.extend(result);
    }

    if raw_bars.is_empty() {
        return Ok(Vec::new());
    }

    // Aggregate 1s bars into requested timeframe.
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
