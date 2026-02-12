pub mod binance;
pub mod coinbase;
pub mod kraken;
pub mod writer;

use serde::Serialize;

/// A parsed trade for the aggregator and WebSocket broadcast.
#[derive(Debug, Clone, Serialize)]
pub struct TradeEvent {
    pub exchange: String,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub timestamp_ms: i64,
    #[serde(skip)]
    pub is_buyer_maker: bool,
}
