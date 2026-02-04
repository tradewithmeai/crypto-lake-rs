use chrono::{NaiveDate, Utc};
use std::path::Path;
use tokio::fs;
use tracing::{info, warn};

/// Delete raw JSONL directories older than `retention_days`.
///
/// Walks `{base_path}/raw/{exchange}/{symbol}/{date}/` and removes
/// directories where the date folder name is older than the cutoff.
pub async fn cleanup_raw_files(base_path: &Path, retention_days: i64) {
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(retention_days);
    let raw_dir = base_path.join("raw");

    if !raw_dir.exists() {
        return;
    }

    let mut total_removed = 0u64;

    // Walk: raw/{exchange}/{symbol}/{date}/
    let mut exchanges = match fs::read_dir(&raw_dir).await {
        Ok(r) => r,
        Err(e) => {
            warn!("Cannot read raw dir: {}", e);
            return;
        }
    };

    while let Ok(Some(exchange_entry)) = exchanges.next_entry().await {
        let exchange_path = exchange_entry.path();
        if !exchange_path.is_dir() {
            continue;
        }
        // Skip _events directory
        if exchange_path
            .file_name()
            .map_or(false, |n| n.to_string_lossy().starts_with('_'))
        {
            continue;
        }

        let mut symbols = match fs::read_dir(&exchange_path).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        while let Ok(Some(symbol_entry)) = symbols.next_entry().await {
            let symbol_path = symbol_entry.path();
            if !symbol_path.is_dir() {
                continue;
            }

            let mut dates = match fs::read_dir(&symbol_path).await {
                Ok(r) => r,
                Err(_) => continue,
            };

            while let Ok(Some(date_entry)) = dates.next_entry().await {
                let date_path = date_entry.path();
                let dir_name = date_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                // Parse date from directory name (YYYY-MM-DD)
                if let Ok(date) = NaiveDate::parse_from_str(&dir_name, "%Y-%m-%d") {
                    if date < cutoff {
                        match fs::remove_dir_all(&date_path).await {
                            Ok(_) => {
                                total_removed += 1;
                                info!("Cleaned up old raw dir: {:?}", date_path);
                            }
                            Err(e) => {
                                warn!("Failed to remove {:?}: {}", date_path, e);
                            }
                        }
                    }
                }
            }
        }
    }

    if total_removed > 0 {
        info!(
            "Cleanup complete: removed {} directories older than {}",
            total_removed, cutoff
        );
    }
}
