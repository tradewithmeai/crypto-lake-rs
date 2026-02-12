use super::TradeEvent;
use crate::collector::writer::RawMessage;
use crate::config::Exchange;
use crate::events;
use crate::health::HealthCounters;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

/// Spawn a Binance WebSocket collector task.
///
/// Connects to the combined stream for all symbols, parses trade and bookTicker
/// messages, and forwards raw JSON lines to the writer channel.
pub async fn run(
    exchange_cfg: Exchange,
    writer_tx: mpsc::UnboundedSender<RawMessage>,
    trade_tx: mpsc::UnboundedSender<TradeEvent>,
    broadcast_tx: broadcast::Sender<TradeEvent>,
    data_path: PathBuf,
    reconnect_backoff: u64,
    max_reconnect_backoff: u64,
    reconnect_jitter: f64,
    counters: Arc<HealthCounters>,
) {
    let exchange_name = exchange_cfg.name.clone();
    let mut backoff = reconnect_backoff;
    let mut last_disconnect: Option<Instant> = None;

    loop {
        let url = build_stream_url(&exchange_cfg);
        info!("[{}] Connecting to {}", exchange_name, url);

        match connect_async(&url).await {
            Ok((ws_stream, _response)) => {
                info!("[{}] Connected", exchange_name);
                backoff = reconnect_backoff; // Reset backoff on success

                // Log reconnect event with gap duration
                if let Some(disc_time) = last_disconnect.take() {
                    counters.ws_reconnects.fetch_add(1, Ordering::Relaxed);
                    let gap = disc_time.elapsed().as_secs_f64();
                    events::log_connection_event(
                        &data_path.join("raw"),
                        &exchange_name,
                        "reconnect",
                        "",
                        gap,
                    )
                    .await;
                } else {
                    events::log_connection_event(
                        &data_path.join("raw"),
                        &exchange_name,
                        "connect",
                        "",
                        0.0,
                    )
                    .await;
                }

                let (mut ws_write, mut ws_read) = ws_stream.split();

                // Read messages until disconnect
                while let Some(msg_result) = ws_read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            counters.messages_received.fetch_add(1, Ordering::Relaxed);
                            counters.bytes_received.fetch_add(text.len() as u64, Ordering::Relaxed);
                            handle_message(
                                &exchange_name,
                                &text,
                                &writer_tx,
                                &trade_tx,
                                &broadcast_tx,
                                &counters,
                            );
                        }
                        Ok(Message::Ping(data)) => {
                            if let Err(e) = ws_write.send(Message::Pong(data)).await {
                                warn!("[{}] Pong send error: {}", exchange_name, e);
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => {
                            info!("[{}] Server sent close frame", exchange_name);
                            break;
                        }
                        Err(e) => {
                            warn!("[{}] WebSocket error: {}", exchange_name, e);
                            break;
                        }
                        _ => {}
                    }
                }

                // Disconnected
                counters.ws_disconnects.fetch_add(1, Ordering::Relaxed);
                last_disconnect = Some(Instant::now());
                events::log_connection_event(
                    &data_path.join("raw"),
                    &exchange_name,
                    "disconnect",
                    "stream ended",
                    0.0,
                )
                .await;
            }
            Err(e) => {
                error!("[{}] Connection failed: {}", exchange_name, e);
                if last_disconnect.is_none() {
                    last_disconnect = Some(Instant::now());
                }
                events::log_connection_event(
                    &data_path.join("raw"),
                    &exchange_name,
                    "disconnect",
                    &format!("connection failed: {}", e),
                    0.0,
                )
                .await;
            }
        }

        // Exponential backoff with jitter
        let jitter = 1.0 + (rand_jitter() * reconnect_jitter);
        let wait = Duration::from_secs_f64(backoff as f64 * jitter);
        warn!(
            "[{}] Reconnecting in {:.1}s (backoff={}s)",
            exchange_name,
            wait.as_secs_f64(),
            backoff
        );
        sleep(wait).await;
        backoff = (backoff * 2).min(max_reconnect_backoff);
    }
}

/// Build the Binance combined stream URL for all symbols.
fn build_stream_url(cfg: &Exchange) -> String {
    // Combined stream: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker/...
    let mut streams = Vec::new();
    for symbol in &cfg.symbols {
        let s = symbol.to_lowercase();
        streams.push(format!("{}@trade", s));
        streams.push(format!("{}@bookTicker", s));
    }
    let base = cfg.wss_url.trim_end_matches('/');
    // Use /stream?streams= for combined stream endpoint
    let base = base.replace("/ws", "/stream");
    format!("{}?streams={}", base, streams.join("/"))
}

/// Parse a Binance combined stream message and forward to writer + aggregator.
fn handle_message(
    exchange: &str,
    text: &str,
    writer_tx: &mpsc::UnboundedSender<RawMessage>,
    trade_tx: &mpsc::UnboundedSender<TradeEvent>,
    broadcast_tx: &broadcast::Sender<TradeEvent>,
    counters: &Arc<HealthCounters>,
) {
    // Combined stream wraps messages as: { "stream": "...", "data": {...} }
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    let stream = parsed.get("stream").and_then(|s| s.as_str()).unwrap_or("");
    let data = match parsed.get("data") {
        Some(d) => d,
        None => return,
    };

    // Determine symbol from stream name (e.g., "btcusdt@trade" → "BTCUSDT")
    let symbol = stream
        .split('@')
        .next()
        .unwrap_or("")
        .to_uppercase();

    if symbol.is_empty() {
        return;
    }

    // Skip bookTicker from raw writes to save storage (still feeds aggregator below)
    // Only write trades to raw JSONL files
    if stream.ends_with("@trade") {
        // Build the raw JSONL line (matching Python format)
        let recv_ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        let mut raw = data.clone();
        if let Some(obj) = raw.as_object_mut() {
            obj.insert("_recv_ts".to_string(), Value::String(recv_ts));
            obj.insert("_exchange".to_string(), Value::String(exchange.to_string()));
        }

        let payload = serde_json::to_string(&raw).unwrap_or_default();

        // Send to writer
        let _ = writer_tx.send(RawMessage {
            exchange: exchange.to_string(),
            symbol: symbol.clone(),
            payload,
        });
    }

    // If it's a trade, send to aggregator
    if stream.ends_with("@trade") {
        counters.trades_received.fetch_add(1, Ordering::Relaxed);
        if let (Some(price_str), Some(qty_str), Some(buyer_maker)) = (
            data.get("p").and_then(|v| v.as_str()),
            data.get("q").and_then(|v| v.as_str()),
            data.get("m").and_then(|v| v.as_bool()),
        ) {
            let ts_ms = data.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
            if let (Ok(price), Ok(qty)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                let trade = TradeEvent {
                    exchange: exchange.to_string(),
                    symbol,
                    price,
                    qty,
                    timestamp_ms: ts_ms,
                    is_buyer_maker: buyer_maker,
                };
                let _ = broadcast_tx.send(trade.clone());
                let _ = trade_tx.send(trade);
            }
        }
    }
}

/// Simple pseudo-random jitter in [0.0, 1.0) using timestamp nanos.
fn rand_jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos as f64) / 1_000_000_000.0
}
