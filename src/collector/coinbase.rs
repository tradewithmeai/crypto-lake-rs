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

/// Spawn a Coinbase WebSocket collector task.
///
/// Subscribes to `matches` (trades) and `ticker` (best bid/ask) channels.
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

                // Send subscribe message
                let sub_msg = json!({
                    "type": "subscribe",
                    "product_ids": exchange_cfg.symbols,
                    "channels": ["ticker", "matches"],
                });
                if let Err(e) = ws_write
                    .send(Message::Text(sub_msg.to_string().into()))
                    .await
                {
                    warn!("[{}] Subscribe send error: {}", exchange_name, e);
                    continue;
                }
                info!("[{}] Subscribed to {} symbols", exchange_name, exchange_cfg.symbols.len());

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

/// Parse a Coinbase message and forward to writer + aggregator.
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

    let msg_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");

    // Skip non-data messages
    if matches!(msg_type, "subscriptions" | "heartbeat" | "error" | "") {
        return;
    }

    let symbol = parsed
        .get("product_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if symbol.is_empty() {
        return;
    }

    // Skip ticker from raw writes to save storage, only write trades
    // Parse trades (match/last_match)
    if matches!(msg_type, "match" | "last_match") {
        // Write raw JSONL for trades only
        let recv_ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        let mut raw = parsed.clone();
        if let Some(obj) = raw.as_object_mut() {
            obj.insert("_recv_ts".to_string(), Value::String(recv_ts));
            obj.insert("_exchange".to_string(), Value::String(exchange.to_string()));
        }
        let payload = serde_json::to_string(&raw).unwrap_or_default();
        let _ = writer_tx.send(RawMessage {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            payload,
        });

        counters.trades_received.fetch_add(1, Ordering::Relaxed);

        let price = parsed
            .get("price")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let qty = parsed
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let ts_ms = parsed
            .get("time")
            .and_then(|v| v.as_str())
            .and_then(|s| parse_iso_to_epoch_ms(s))
            .unwrap_or(Utc::now().timestamp_millis());
        let side = parsed
            .get("side")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if let (Some(p), Some(q)) = (price, qty) {
            let trade = TradeEvent {
                exchange: exchange.to_string(),
                symbol: symbol.to_string(),
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

/// Parse ISO 8601 timestamp to epoch milliseconds.
fn parse_iso_to_epoch_ms(s: &str) -> Option<i64> {
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
