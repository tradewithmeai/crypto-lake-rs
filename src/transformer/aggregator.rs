use crate::collector::binance::TradeEvent;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::info;

/// A completed 1-second OHLCV bar.
#[derive(Debug, Clone)]
pub struct Bar1s {
    pub exchange: String,
    pub symbol: String,
    /// Truncated to the second (Unix timestamp).
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume_base: f64,
    pub volume_quote: f64,
    pub trade_count: u64,
    pub vwap: f64,
    pub bid: f64,
    pub ask: f64,
    pub spread: f64,
}

/// Accumulator for a single 1-second bar.
#[derive(Debug)]
struct BarAccumulator {
    exchange: String,
    symbol: String,
    ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_base: f64,
    volume_quote: f64,
    trade_count: u64,
    cost_sum: f64, // price * qty sum for VWAP
}

impl BarAccumulator {
    fn new(trade: &TradeEvent, ts: i64) -> Self {
        let cost = trade.price * trade.qty;
        Self {
            exchange: trade.exchange.clone(),
            symbol: trade.symbol.clone(),
            ts,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume_base: trade.qty,
            volume_quote: cost,
            trade_count: 1,
            cost_sum: cost,
        }
    }

    fn update(&mut self, trade: &TradeEvent) {
        let cost = trade.price * trade.qty;
        self.high = self.high.max(trade.price);
        self.low = self.low.min(trade.price);
        self.close = trade.price;
        self.volume_base += trade.qty;
        self.volume_quote += cost;
        self.trade_count += 1;
        self.cost_sum += cost;
    }

    fn finalize(&self) -> Bar1s {
        let vwap = if self.volume_base > 0.0 {
            self.cost_sum / self.volume_base
        } else {
            self.close
        };
        Bar1s {
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            ts: self.ts,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume_base: self.volume_base,
            volume_quote: self.volume_quote,
            trade_count: self.trade_count,
            vwap,
            bid: 0.0,   // Updated from bookTicker stream separately
            ask: 0.0,
            spread: 0.0,
        }
    }
}

/// Key for the accumulator map.
type BarKey = (String, String, i64); // (exchange, symbol, second_ts)

/// Spawn the aggregator task.
///
/// Consumes trade events, accumulates into 1-second bars, and sends
/// completed bars to the parquet writer via the returned receiver.
pub fn spawn_aggregator(
    mut trade_rx: mpsc::UnboundedReceiver<TradeEvent>,
) -> mpsc::UnboundedReceiver<Bar1s> {
    let (bar_tx, bar_rx) = mpsc::unbounded_channel::<Bar1s>();

    tokio::spawn(async move {
        // Active accumulators: (exchange, symbol, second_ts) → accumulator
        let mut accumulators: HashMap<BarKey, BarAccumulator> = HashMap::new();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(2));
        let mut total_bars = 0u64;

        loop {
            tokio::select! {
                Some(trade) = trade_rx.recv() => {
                    let second_ts = trade.timestamp_ms / 1000;
                    let key = (trade.exchange.clone(), trade.symbol.clone(), second_ts);

                    accumulators
                        .entry(key)
                        .and_modify(|acc| acc.update(&trade))
                        .or_insert_with(|| BarAccumulator::new(&trade, second_ts));
                }
                _ = flush_interval.tick() => {
                    // Flush bars that are at least 2 seconds old
                    let cutoff = chrono::Utc::now().timestamp() - 2;
                    let stale_keys: Vec<BarKey> = accumulators
                        .keys()
                        .filter(|(_, _, ts)| *ts < cutoff)
                        .cloned()
                        .collect();

                    for key in stale_keys {
                        if let Some(acc) = accumulators.remove(&key) {
                            let bar = acc.finalize();
                            total_bars += 1;
                            if total_bars % 10000 == 0 {
                                info!("Aggregator: {} total bars produced", total_bars);
                            }
                            let _ = bar_tx.send(bar);
                        }
                    }
                }
            }
        }
    });

    bar_rx
}
