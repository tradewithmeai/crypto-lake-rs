use super::TradeEvent;
use crate::collector::writer::RawMessage;
use crate::config::Exchange;
use crate::events;
use crate::health::HealthCounters;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

/// Spawn a Kraken v2 WebSocket collector task.
///
/// Subscribes to `trade` and `ticker` channels via separate messages.
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
        let url = &exchange_cfg.wss_url;
        info!("[{}] Connecting to {}", exchange_name, url);

        match connect_async(url).await {
            Ok((ws_stream, _response)) => {
                info!("[{}] Connected", exchange_name);
                backoff = reconnect_backoff;

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

                // Kraken v2: send separate subscribe messages for trade and ticker
                let trade_sub = json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "trade",
                        "symbol": exchange_cfg.symbols,
                    },
                });
                let ticker_sub = json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "ticker",
                        "symbol": exchange_cfg.symbols,
                    },
                });

                for (sub, name) in [(trade_sub, "trade"), (ticker_sub, "ticker")] {
                    if let Err(e) = ws_write
                        .send(Message::Text(sub.to_string().into()))
                        .await
                    {
                        warn!("[{}] Subscribe send error for {}: {}", exchange_name, name, e);
                        break;
                    }
                    info!("[{}] Subscribed to {} channel", exchange_name, name);
                }

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

/// Parse a Kraken v2 message and forward to writer + aggregator.
///
/// Kraken v2 messages have: `{"channel": "trade", "type": "update", "data": [...]}`
fn handle_message(
    exchange: &str,
    text: &str,
    writer_tx: &mpsc::UnboundedSender<RawMessage>,
    trade_tx: &mpsc::UnboundedSender<TradeEvent>,
    broadcast_tx: &broadcast::Sender<TradeEvent>,
    counters: &Arc<HealthCounters>,
) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    let channel = parsed.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let msg_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");

    // Skip system/subscription messages
    if matches!(channel, "status" | "heartbeat" | "")
        || matches!(msg_type, "subscribe" | "unsubscribe" | "error")
    {
        return;
    }

    let data = match parsed.get("data").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return,
    };

    let recv_ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();

    if channel == "trade" {
        for trade in data {
            let symbol = trade.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            if symbol.is_empty() {
                continue;
            }
            // Sanitize symbol for filesystem (BTC/USD -> BTC-USD)
            let safe_symbol = symbol.replace('/', "-");

            // Write raw JSONL
            let mut raw = trade.clone();
            if let Some(obj) = raw.as_object_mut() {
                obj.insert("_recv_ts".to_string(), Value::String(recv_ts.clone()));
                obj.insert("_exchange".to_string(), Value::String(exchange.to_string()));
                obj.insert("_channel".to_string(), Value::String("trade".to_string()));
            }
            let payload = serde_json::to_string(&raw).unwrap_or_default();
            let _ = writer_tx.send(RawMessage {
                exchange: exchange.to_string(),
                symbol: safe_symbol.clone(),
                payload,
            });

            counters.trades_received.fetch_add(1, Ordering::Relaxed);

            // Parse trade fields
            let price = trade
                .get("price")
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let qty = trade
                .get("qty")
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let ts_ms = trade
                .get("timestamp")
                .and_then(|v| v.as_str())
                .and_then(|s| parse_iso_to_epoch_ms(s))
                .unwrap_or(Utc::now().timestamp_millis());
            let side = trade.get("side").and_then(|v| v.as_str()).unwrap_or("unknown");

            if let (Some(p), Some(q)) = (price, qty) {
                let trade = TradeEvent {
                    exchange: exchange.to_string(),
                    symbol: safe_symbol,
                    price: p,
                    qty: q,
                    timestamp_ms: ts_ms,
                    is_buyer_maker: side == "sell",
                };
                let _ = broadcast_tx.send(trade.clone());
                let _ = trade_tx.send(trade);
            }
        }
    }
    // Skip ticker channel from raw writes to save storage
    // (bookTicker data still feeds the aggregator for bid/ask in Parquet bars)
}

fn parse_iso_to_epoch_ms(s: &str) -> Option<i64> {
    // Kraken uses ISO 8601: "2026-02-04T22:30:00.123456Z"
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn rand_jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos as f64) / 1_000_000_000.0
}
