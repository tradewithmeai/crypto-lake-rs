use crate::config::{Backfill, Exchange};
use crate::transformer::aggregator::Bar1s;
use crate::transformer::parquet_writer;
use arrow::array::Array;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Run backfill for a specific subset of exchanges (by name).
/// Used by the reconnect-triggered backfill runner.
pub async fn run_for_exchanges(
    exchange_names: &[String],
    exchanges: &[Exchange],
    data_path: &Path,
    backfill_cfg: &Backfill,
    compression: &str,
) {
    let filtered: Vec<Exchange> = exchanges.iter()
        .filter(|e| exchange_names.contains(&e.name))
        .cloned()
        .collect();
    if filtered.is_empty() { return; }
    run(&filtered, data_path, backfill_cfg, compression).await;
}

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
    // Run all exchanges in parallel
    let mut handles = Vec::new();
    for exchange in exchanges {
        let exchange = exchange.clone();
        let data_path = data_path.to_path_buf();
        let gap_threshold = backfill_cfg.gap_threshold_secs;
        let max_backfill = backfill_cfg.max_backfill_secs;
        let compression = compression.to_string();

        handles.push(tokio::spawn(async move {
            backfill_exchange(&exchange, &data_path, gap_threshold, max_backfill, &compression).await;
        }));
    }

    // Wait for all exchanges to complete
    for handle in handles {
        let _ = handle.await;
    }
}

/// Backfill a single exchange (all its symbols sequentially with rate limiting).
async fn backfill_exchange(
    exchange: &Exchange,
    data_path: &Path,
    gap_threshold: u64,
    max_backfill: u64,
    compression: &str,
) {
    let exchange_name = &exchange.name;
    let rest_url = &exchange.rest_url;

    if rest_url.is_empty() {
        info!("Backfill: [{}] no REST URL configured, skipping", exchange_name);
        return;
    }

    let now = Utc::now().timestamp();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();

    for symbol in &exchange.symbols {
        // Determine the filesystem-safe symbol (matches collector logic)
        let safe_symbol = match exchange_name.as_str() {
            "kraken" => symbol.replace('/', "-"),
            _ => symbol.clone(),
        };

        // ── Phase 1: fill internal gaps ──────────────────────────────────────
        // Scan all existing data for holes (gaps caused by the app being offline
        // while it was live-collecting at other times). These will not be found
        // by the trailing-gap fill below.
        let internal_gaps =
            find_internal_gaps(data_path, exchange_name, &safe_symbol, gap_threshold, max_backfill).await;

        if !internal_gaps.is_empty() {
            info!(
                "Backfill: [{}] {} — {} internal gap(s) to fill",
                exchange_name, symbol, internal_gaps.len()
            );
        }

        for (gap_start, gap_end) in &internal_gaps {
            let gap_secs = gap_end - gap_start;
            if gap_secs as u64 > max_backfill {
                warn!(
                    "Backfill: [{}] {} — internal gap {}s exceeds max_backfill {}s, skipping",
                    exchange_name, symbol, gap_secs, max_backfill
                );
                continue;
            }

            info!(
                "Backfill: [{}] {} — filling internal gap {}s ({} → {})",
                exchange_name, symbol, gap_secs, gap_start, gap_end
            );

            // Fetch bars that fall strictly inside the gap (gap_start+1 .. gap_end-1),
            // i.e., bars that have no representation in the parquet files yet.
            let fetch_start = gap_start + 1;
            let fetch_end   = gap_end - 1;
            if fetch_start >= fetch_end {
                continue;
            }

            let bars = match exchange_name.as_str() {
                "binance"  => fetch_binance (&client, rest_url, symbol, fetch_start, fetch_end).await,
                "coinbase" => fetch_coinbase(&client, rest_url, symbol, fetch_start, fetch_end).await,
                "kraken"   => fetch_kraken  (&client, rest_url, symbol, fetch_start, fetch_end).await,
                other => {
                    warn!("Backfill: unknown exchange '{}', skipping", other);
                    continue;
                }
            };

            match bars {
                Ok(bars) if !bars.is_empty() => {
                    info!(
                        "Backfill: [{}] {} — internal gap: fetched {} bars",
                        exchange_name, symbol, bars.len()
                    );
                    if let Err(e) = parquet_writer::write_bars(&bars, data_path, compression).await {
                        warn!(
                            "Backfill: [{}] {} — internal gap write error: {}",
                            exchange_name, symbol, e
                        );
                    }
                }
                Ok(_) => info!("Backfill: [{}] {} — internal gap: no bars returned", exchange_name, symbol),
                Err(e) => warn!("Backfill: [{}] {} — internal gap fetch error: {}", exchange_name, symbol, e),
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // ── Phase 2: fill trailing gap (latest bar → now) ────────────────────
        let last_ts = scan_latest_timestamp(data_path, exchange_name, &safe_symbol).await;

        let gap_secs = match last_ts {
            Some(ts) => now - ts,
            None => {
                info!("Backfill: [{}] {} — no existing data, skipping", exchange_name, symbol);
                continue;
            }
        };

        if gap_secs < gap_threshold as i64 {
            info!(
                "Backfill: [{}] {} — trailing gap {}s < threshold {}s, skipping",
                exchange_name, symbol, gap_secs, gap_threshold
            );
            continue;
        }

        let start_ts = last_ts.unwrap();
        let max_end = start_ts + max_backfill as i64;
        let end_ts = now.min(max_end);

        info!(
            "Backfill: [{}] {} — trailing gap {}s, fetching from {} to {}",
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
                    "Backfill: [{}] {} — trailing: fetched {} bars",
                    exchange_name, symbol, bars.len()
                );
                if let Err(e) = parquet_writer::write_bars(&bars, data_path, compression).await {
                    warn!("Backfill: [{}] {} — trailing write error: {}", exchange_name, symbol, e);
                }
            }
            Ok(_) => {
                info!("Backfill: [{}] {} — trailing: no bars returned", exchange_name, symbol);
            }
            Err(e) => {
                warn!("Backfill: [{}] {} — trailing fetch error: {}", exchange_name, symbol, e);
            }
        }

        // Rate limiting between requests
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Read the min and max `window_start` timestamps from a parquet file using column
/// statistics stored in the file footer — no row data is read.
/// Falls back to a full row scan if statistics are unavailable.
/// Returns (min_ts_secs, max_ts_secs).
fn read_file_ts_range(path: &Path) -> Option<(i64, i64)> {
    let file = std::fs::File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;

    // Locate the window_start column index in the Arrow schema
    let col_idx = builder
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "window_start")?;

    let metadata = builder.metadata();
    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;
    let mut stats_found = false;

    for rg in metadata.row_groups() {
        if col_idx >= rg.num_columns() {
            continue;
        }
        if let Some(stats) = rg.column(col_idx).statistics() {
            // TIMESTAMP_MICROS is stored as INT64 in parquet physical type
            if let Statistics::Int64(s) = stats {
                stats_found = true;
                if let Some(&mn) = s.min_opt() {
                    let secs = mn / 1_000_000;
                    global_min = Some(global_min.map_or(secs, |v: i64| v.min(secs)));
                }
                if let Some(&mx) = s.max_opt() {
                    let secs = mx / 1_000_000;
                    global_max = Some(global_max.map_or(secs, |v: i64| v.max(secs)));
                }
            }
        }
    }

    if stats_found {
        return match (global_min, global_max) {
            (Some(mn), Some(mx)) => Some((mn, mx)),
            _ => None,
        };
    }

    // Fallback: read all rows (older files written without stats)
    let file2 = std::fs::File::open(path).ok()?;
    let builder2 = ParquetRecordBatchReaderBuilder::try_new(file2).ok()?;
    let reader = builder2.build().ok()?;
    let mut mn: Option<i64> = None;
    let mut mx: Option<i64> = None;
    for batch in reader {
        let batch = batch.ok()?;
        let col = batch
            .column_by_name("window_start")?
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?;
        for i in 0..col.len() {
            if !col.is_null(i) {
                let secs = col.value(i) / 1_000_000;
                mn = Some(mn.map_or(secs, |v: i64| v.min(secs)));
                mx = Some(mx.map_or(secs, |v: i64| v.max(secs)));
            }
        }
    }
    match (mn, mx) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    }
}

/// Scan all parquet files for an exchange/symbol and return a sorted list of
/// `(min_ts_secs, max_ts_secs)` ranges, one per file.
fn collect_file_ranges(parquet_dir: &Path) -> Vec<(i64, i64)> {
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    let mut partition_dirs = Vec::new();
    collect_leaf_dirs(parquet_dir, &mut partition_dirs);
    partition_dirs.sort();

    for dir in &partition_dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }
            if let Some((mn, mx)) = read_file_ts_range(&path) {
                ranges.push((mn, mx));
            }
        }
    }

    // Sort by file min timestamp
    ranges.sort_by_key(|r| r.0);
    ranges
}

/// Detect internal time gaps within an exchange/symbol's parquet data.
///
/// Works by reading min/max timestamps from each file's parquet footer statistics,
/// then checking for holes between consecutive files. Only gaps larger than
/// `gap_threshold_secs` are returned. Files older than `max_backfill_secs` before
/// now are skipped — there's no point scanning data we can't fill.
///
/// Returns a list of `(gap_start_exclusive, gap_end_exclusive)` pairs in Unix seconds.
/// These are the exact timestamps of the last bar before the gap and the first bar
/// after it — safe to use as backfill [start+1 .. end-1] boundaries.
async fn find_internal_gaps(
    data_path: &Path,
    exchange: &str,
    symbol: &str,
    gap_threshold_secs: u64,
    max_backfill_secs: u64,
) -> Vec<(i64, i64)> {
    let parquet_dir = data_path.join("parquet").join(exchange).join(symbol);
    if !parquet_dir.exists() {
        return vec![];
    }

    // Cutoff: ignore files whose max timestamp is older than max_backfill_secs
    let earliest_ts = Utc::now().timestamp() - max_backfill_secs as i64;

    // Blocking file I/O — run on the thread pool
    let parquet_dir = parquet_dir.clone();
    let ranges = tokio::task::spawn_blocking(move || {
        collect_file_ranges(&parquet_dir)
            .into_iter()
            .filter(|(_mn, mx)| *mx >= earliest_ts)
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    if ranges.len() < 2 {
        return vec![];
    }

    let mut gaps = Vec::new();
    for i in 1..ranges.len() {
        let prev_max = ranges[i - 1].1;
        let next_min = ranges[i].0;
        let gap_secs = next_min - prev_max;
        if gap_secs > 0 && gap_secs as u64 > gap_threshold_secs {
            gaps.push((prev_max, next_min));
        }
    }

    gaps
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

            // Use the new unified range reader; we only need the max here
            if let Some((_mn, mx)) = read_file_ts_range(&path) {
                latest_ts = Some(latest_ts.map_or(mx, |prev: i64| prev.max(mx)));
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
                source: "backfill_1s".to_string(),
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
                source: "backfill_1m".to_string(),
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
                    source: "backfill_1m".to_string(),
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
