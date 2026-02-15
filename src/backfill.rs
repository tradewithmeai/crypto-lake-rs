use crate::config::{Backfill, Exchange};
use crate::transformer::aggregator::Bar1s;
use crate::transformer::parquet_writer;
use arrow::array::Array;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Run startup backfill for all configured exchanges.
///
/// For each exchange/symbol pair, scans existing parquet data to find the most
/// recent timestamp, then fetches klines from the exchange REST API to fill
/// the gap up to the current time.
pub async fn run(
    exchanges: &[Exchange],
    data_path: &Path,
    backfill_cfg: &Backfill,
    compression: &str,
) {
    if !backfill_cfg.enabled {
        info!("Backfill: disabled in config");
        return;
    }

    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(backfill_cfg.timeout_secs),
        run_inner(exchanges, data_path, backfill_cfg, compression),
    );

    match timeout.await {
        Ok(()) => info!("Backfill: complete"),
        Err(_) => warn!("Backfill: timed out after {}s, proceeding without full backfill", backfill_cfg.timeout_secs),
    }
}

async fn run_inner(
    exchanges: &[Exchange],
    data_path: &Path,
    backfill_cfg: &Backfill,
    compression: &str,
) {
    let now = Utc::now().timestamp();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();

    for exchange in exchanges {
        let exchange_name = &exchange.name;
        let rest_url = &exchange.rest_url;

        if rest_url.is_empty() {
            info!("Backfill: [{}] no REST URL configured, skipping", exchange_name);
            continue;
        }

        for symbol in &exchange.symbols {
            // Determine the filesystem-safe symbol (matches collector logic)
            let safe_symbol = match exchange_name.as_str() {
                "kraken" => symbol.replace('/', "-"),
                _ => symbol.clone(),
            };

            let last_ts = scan_latest_timestamp(data_path, exchange_name, &safe_symbol).await;

            let gap_secs = match last_ts {
                Some(ts) => now - ts,
                None => {
                    info!("Backfill: [{}] {} — no existing data, skipping", exchange_name, symbol);
                    continue;
                }
            };

            if gap_secs < backfill_cfg.gap_threshold_secs as i64 {
                info!(
                    "Backfill: [{}] {} — gap {}s < threshold {}s, skipping",
                    exchange_name, symbol, gap_secs, backfill_cfg.gap_threshold_secs
                );
                continue;
            }

            let start_ts = last_ts.unwrap();
            let max_end = start_ts + backfill_cfg.max_backfill_secs as i64;
            let end_ts = now.min(max_end);

            info!(
                "Backfill: [{}] {} — gap {}s, fetching from {} to {}",
                exchange_name, symbol, gap_secs, start_ts, end_ts
            );

            let bars = match exchange_name.as_str() {
                "binance" => fetch_binance(&client, rest_url, symbol, start_ts, end_ts).await,
                "coinbase" => fetch_coinbase(&client, rest_url, symbol, start_ts, end_ts).await,
                "kraken" => fetch_kraken(&client, rest_url, symbol, start_ts, end_ts).await,
                other => {
                    warn!("Backfill: unknown exchange '{}', skipping", other);
                    continue;
                }
            };

            match bars {
                Ok(bars) if !bars.is_empty() => {
                    info!(
                        "Backfill: [{}] {} — fetched {} bars",
                        exchange_name, symbol, bars.len()
                    );
                    if let Err(e) = parquet_writer::write_bars(&bars, data_path, compression).await {
                        warn!("Backfill: [{}] {} — write error: {}", exchange_name, symbol, e);
                    }
                }
                Ok(_) => {
                    info!("Backfill: [{}] {} — no bars returned", exchange_name, symbol);
                }
                Err(e) => {
                    warn!("Backfill: [{}] {} — fetch error: {}", exchange_name, symbol, e);
                }
            }

            // Rate limiting between requests
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// Scan parquet files to find the most recent `window_start` timestamp for an exchange/symbol.
async fn scan_latest_timestamp(
    data_path: &Path,
    exchange: &str,
    symbol: &str,
) -> Option<i64> {
    let parquet_dir = data_path.join("parquet").join(exchange).join(symbol);

    if !parquet_dir.exists() {
        return None;
    }

    // Walk year/month/day directories in reverse to find most recent data
    let mut latest_ts: Option<i64> = None;

    // Collect and sort partition dirs (year=YYYY/month=MM/day=DD)
    let mut partition_dirs = Vec::new();
    collect_leaf_dirs(&parquet_dir, &mut partition_dirs);
    partition_dirs.sort();
    partition_dirs.reverse();

    for dir in partition_dirs.iter().take(3) {
        // Read parquet files in this directory
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }

            if let Some(ts) = read_max_timestamp(&path) {
                latest_ts = Some(latest_ts.map_or(ts, |prev: i64| prev.max(ts)));
            }
        }
    }

    latest_ts
}

/// Recursively collect leaf directories (directories containing no subdirectories).
fn collect_leaf_dirs(dir: &Path, results: &mut Vec<PathBuf>) {
    let read_dir = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut has_subdirs = false;
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            has_subdirs = true;
            collect_leaf_dirs(&path, results);
        }
    }

    if !has_subdirs {
        results.push(dir.to_path_buf());
    }
}

/// Read the maximum `window_start` timestamp from a parquet file.
/// Returns epoch seconds.
fn read_max_timestamp(path: &Path) -> Option<i64> {
    let file = std::fs::File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let reader = builder.build().ok()?;

    let mut max_ts: Option<i64> = None;

    for batch in reader {
        let batch = match batch {
            Ok(b) => b,
            Err(_) => continue,
        };

        let ts_col = batch
            .column_by_name("window_start")?
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?;

        for i in 0..ts_col.len() {
            if !ts_col.is_null(i) {
                // Convert microseconds to seconds
                let ts_secs = ts_col.value(i) / 1_000_000;
                max_ts = Some(max_ts.map_or(ts_secs, |prev| prev.max(ts_secs)));
            }
        }
    }

    max_ts
}

// ── Exchange REST fetchers ──────────────────────────────────────────────────

/// Fetch klines from Binance REST API (1-second bars, 1000 per request).
///
/// `GET /api/v3/klines?symbol=X&interval=1s&startTime=X&endTime=X&limit=1000`
async fn fetch_binance(
    client: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<Bar1s>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_bars = Vec::new();
    let mut cursor = start_ts * 1000; // Binance uses milliseconds
    let end_ms = end_ts * 1000;

    while cursor < end_ms {
        let url = format!(
            "{}/klines?symbol={}&interval=1s&startTime={}&endTime={}&limit=1000",
            rest_url, symbol, cursor, end_ms
        );

        let resp: Value = client.get(&url).send().await?.json().await?;

        let klines = match resp.as_array() {
            Some(arr) if !arr.is_empty() => arr,
            _ => break,
        };

        let mut last_open_time = cursor;
        for kline in klines {
            let arr = match kline.as_array() {
                Some(a) if a.len() >= 11 => a,
                _ => continue,
            };

            let open_time_ms = arr[0].as_i64().unwrap_or(0);
            let open = parse_f64(&arr[1]);
            let high = parse_f64(&arr[2]);
            let low = parse_f64(&arr[3]);
            let close = parse_f64(&arr[4]);
            let volume = parse_f64(&arr[5]);
            let quote_volume = parse_f64(&arr[7]);
            let trade_count = arr[8].as_u64().unwrap_or(0);

            let vwap = if volume > 0.0 {
                quote_volume / volume
            } else {
                close
            };

            all_bars.push(Bar1s {
                exchange: "binance".to_string(),
                symbol: symbol.to_string(),
                ts: open_time_ms / 1000,
                open,
                high,
                low,
                close,
                volume_base: volume,
                volume_quote: quote_volume,
                trade_count,
                vwap,
                bid: 0.0,
                ask: 0.0,
                spread: 0.0,
            });

            last_open_time = open_time_ms;
        }

        // Move cursor past the last kline
        cursor = last_open_time + 1000; // +1 second in ms

        // Rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(all_bars)
}

/// Fetch candles from Coinbase REST API (1-minute bars, 300 per request).
///
/// `GET /products/{id}/candles?granularity=60&start=X&end=X`
async fn fetch_coinbase(
    client: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<Bar1s>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_bars = Vec::new();
    let mut cursor = start_ts;
    let granularity = 60; // 1 minute

    while cursor < end_ts {
        // Coinbase returns max 300 candles per request
        let batch_end = (cursor + 300 * granularity).min(end_ts);

        let url = format!(
            "{}/products/{}/candles?granularity={}&start={}&end={}",
            rest_url, symbol, granularity,
            DateTime::from_timestamp(cursor, 0).unwrap_or_default().to_rfc3339(),
            DateTime::from_timestamp(batch_end, 0).unwrap_or_default().to_rfc3339(),
        );

        let resp: Value = client.get(&url).send().await?.json().await?;

        let candles = match resp.as_array() {
            Some(arr) if !arr.is_empty() => arr,
            _ => break,
        };

        // Coinbase returns [timestamp, low, high, open, close, volume]
        for candle in candles {
            let arr = match candle.as_array() {
                Some(a) if a.len() >= 6 => a,
                _ => continue,
            };

            let ts = arr[0].as_i64().unwrap_or(0);
            let low = parse_f64(&arr[1]);
            let high = parse_f64(&arr[2]);
            let open = parse_f64(&arr[3]);
            let close = parse_f64(&arr[4]);
            let volume = parse_f64(&arr[5]);
            let quote_volume = volume * ((high + low) / 2.0); // approximate
            let vwap = if volume > 0.0 { quote_volume / volume } else { close };

            all_bars.push(Bar1s {
                exchange: "coinbase".to_string(),
                symbol: symbol.to_string(),
                ts,
                open,
                high,
                low,
                close,
                volume_base: volume,
                volume_quote: quote_volume,
                trade_count: 0,
                vwap,
                bid: 0.0,
                ask: 0.0,
                spread: 0.0,
            });
        }

        cursor = batch_end;

        // Rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Coinbase returns candles in reverse chronological order
    all_bars.sort_by_key(|b| b.ts);
    all_bars.dedup_by_key(|b| b.ts);

    Ok(all_bars)
}

/// Fetch OHLC from Kraken REST API (1-minute bars, 720 per request).
///
/// `GET /0/public/OHLC?pair=X&interval=1&since=X`
async fn fetch_kraken(
    client: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<Bar1s>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_bars = Vec::new();
    let mut cursor = start_ts;

    // Kraken uses pair format like "XBTUSD" for REST, but config has "BTC/USD"
    // The REST API also accepts "BTC/USD" format
    let pair = symbol;
    // Kraken symbol for filesystem
    let safe_symbol = symbol.replace('/', "-");

    while cursor < end_ts {
        let url = format!(
            "{}/0/public/OHLC?pair={}&interval=1&since={}",
            rest_url, pair, cursor
        );

        let resp: Value = client.get(&url).send().await?.json().await?;

        // Check for errors
        if let Some(errors) = resp.get("error").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                warn!("Backfill: Kraken API error: {:?}", errors);
                break;
            }
        }

        let result = match resp.get("result") {
            Some(r) => r,
            None => break,
        };

        // Find the data key (it's the pair name, which varies)
        let mut found_data = false;
        for (key, value) in result.as_object().unwrap_or(&serde_json::Map::new()) {
            if key == "last" {
                continue;
            }

            let candles = match value.as_array() {
                Some(arr) if !arr.is_empty() => arr,
                _ => continue,
            };

            found_data = true;

            // Kraken OHLC: [time, open, high, low, close, vwap, volume, count]
            for candle in candles {
                let arr = match candle.as_array() {
                    Some(a) if a.len() >= 8 => a,
                    _ => continue,
                };

                let ts = arr[0].as_i64().unwrap_or(0);
                if ts >= end_ts {
                    continue;
                }

                let open = parse_f64(&arr[1]);
                let high = parse_f64(&arr[2]);
                let low = parse_f64(&arr[3]);
                let close = parse_f64(&arr[4]);
                let vwap = parse_f64(&arr[5]);
                let volume = parse_f64(&arr[6]);
                let trade_count = arr[7].as_u64().unwrap_or(0);
                let quote_volume = volume * vwap;

                all_bars.push(Bar1s {
                    exchange: "kraken".to_string(),
                    symbol: safe_symbol.clone(),
                    ts,
                    open,
                    high,
                    low,
                    close,
                    volume_base: volume,
                    volume_quote: quote_volume,
                    trade_count,
                    vwap,
                    bid: 0.0,
                    ask: 0.0,
                    spread: 0.0,
                });

                cursor = ts;
            }
        }

        if !found_data {
            break;
        }

        // Move past last bar
        cursor += 60; // 1 minute

        // Rate limiting (Kraken has stricter limits)
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    all_bars.sort_by_key(|b| b.ts);
    all_bars.dedup_by_key(|b| b.ts);

    Ok(all_bars)
}

/// Parse a JSON value as f64 (handles both numbers and strings).
fn parse_f64(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0)
}
