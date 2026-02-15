use crate::collector::TradeEvent;
use crate::health::HealthCounters;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
            bid: 0.0,
            ask: 0.0,
            spread: 0.0,
        }
    }
}

/// Key for the accumulator map.
type BarKey = (String, String, i64); // (exchange, symbol, second_ts)

/// Symbol key for tracking state per (exchange, symbol).
type SymbolKey = (String, String);

/// Spawn the aggregator task.
///
/// Consumes trade events, accumulates into 1-second bars, and sends
/// completed bars to the parquet writer via the returned receiver.
///
/// Emits a bar for every second the collector is running per symbol.
/// Seconds with no trades get `trade_count=0` with the last known
/// close price carried forward, so gaps in parquet data always mean
/// the collector was genuinely offline.
pub fn spawn_aggregator(
    mut trade_rx: mpsc::UnboundedReceiver<TradeEvent>,
    counters: Arc<HealthCounters>,
) -> mpsc::UnboundedReceiver<Bar1s> {
    let (bar_tx, bar_rx) = mpsc::unbounded_channel::<Bar1s>();

    tokio::spawn(async move {
        let mut accumulators: HashMap<BarKey, BarAccumulator> = HashMap::new();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(2));
        let mut total_bars = 0u64;

        // Track last emitted second and last close price per symbol
        let mut last_emitted: HashMap<SymbolKey, i64> = HashMap::new();
        let mut last_close: HashMap<SymbolKey, f64> = HashMap::new();

        loop {
            tokio::select! {
                Some(trade) = trade_rx.recv() => {
                    let second_ts = trade.timestamp_ms / 1000;
                    let sym_key = (trade.exchange.clone(), trade.symbol.clone());

                    // Initialize tracking on first trade for this symbol
                    last_emitted.entry(sym_key.clone()).or_insert(second_ts - 1);
                    last_close.entry(sym_key).or_insert(trade.price);

                    let key = (trade.exchange.clone(), trade.symbol.clone(), second_ts);
                    accumulators
                        .entry(key)
                        .and_modify(|acc| acc.update(&trade))
                        .or_insert_with(|| BarAccumulator::new(&trade, second_ts));
                }
                _ = flush_interval.tick() => {
                    let cutoff = chrono::Utc::now().timestamp() - 2;
                    let mut flushed = 0u64;

                    // For each tracked symbol, emit bars for every second
                    // from last_emitted+1 through cutoff
                    let symbols: Vec<SymbolKey> = last_emitted.keys().cloned().collect();

                    for sym_key in &symbols {
                        let last = *last_emitted.get(sym_key).unwrap();
                        let price = *last_close.get(sym_key).unwrap_or(&0.0);

                        for ts in (last + 1)..=cutoff {
                            let bar_key = (sym_key.0.clone(), sym_key.1.clone(), ts);

                            let bar = if let Some(acc) = accumulators.remove(&bar_key) {
                                // Real bar with trades
                                let b = acc.finalize();
                                last_close.insert(sym_key.clone(), b.close);
                                b
                            } else {
                                // Empty bar — no trades this second
                                Bar1s {
                                    exchange: sym_key.0.clone(),
                                    symbol: sym_key.1.clone(),
                                    ts,
                                    open: price,
                                    high: price,
                                    low: price,
                                    close: price,
                                    volume_base: 0.0,
                                    volume_quote: 0.0,
                                    trade_count: 0,
                                    vwap: price,
                                    bid: 0.0,
                                    ask: 0.0,
                                    spread: 0.0,
                                }
                            };

                            total_bars += 1;
                            flushed += 1;
                            if total_bars % 100_000 == 0 {
                                info!("Aggregator: {} total bars produced", total_bars);
                            }
                            let _ = bar_tx.send(bar);
                        }

                        last_emitted.insert(sym_key.clone(), cutoff);
                    }

                    if flushed > 0 {
                        counters.bars_produced.fetch_add(flushed, Ordering::Relaxed);
                    }
                }
            }
        }
    });

    bar_rx
}
