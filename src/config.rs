use serde::Deserialize;
use std::path::PathBuf;

/// Top-level configuration (mirrors config.yml from Python project).
#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub general: General,
    pub exchanges: Vec<Exchange>,
    pub collector: Collector,
    pub transformer: Transformer,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub backfill: Backfill,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    #[serde(default = "default_server_port")]
    pub port: u16,
    #[serde(default = "default_static_dir")]
    pub static_dir: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct General {
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_base_path")]
    pub base_path: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Exchange {
    pub name: String,
    pub symbols: Vec<String>,
    #[serde(default)]
    pub rest_url: String,
    pub wss_url: String,
    /// Bar aggregation interval in seconds. Default 1 (1-second bars).
    /// Set to 60 for exchanges where only 1-minute backfill is available (Coinbase, Kraken).
    #[serde(default = "default_bar_interval_sec")]
    pub bar_interval_sec: u64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Collector {
    #[serde(default = "default_write_interval")]
    pub write_interval_sec: u64,
    #[serde(default = "default_reconnect_backoff")]
    pub reconnect_backoff: u64,
    #[serde(default = "default_max_reconnect_backoff")]
    pub max_reconnect_backoff: u64,
    #[serde(default = "default_reconnect_jitter")]
    pub reconnect_jitter: f64,
    #[serde(default = "default_output_format")]
    pub output_format: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Transformer {
    #[serde(default = "default_resample_interval")]
    pub resample_interval_sec: u64,
    #[serde(default = "default_parquet_compression")]
    pub parquet_compression: String,
    #[serde(default = "default_schedule_minutes")]
    pub schedule_minutes: u64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Backfill {
    #[serde(default = "default_backfill_enabled")]
    pub enabled: bool,
    #[serde(default = "default_gap_threshold_secs")]
    pub gap_threshold_secs: u64,
    #[serde(default = "default_max_backfill_secs")]
    pub max_backfill_secs: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for Backfill {
    fn default() -> Self {
        Self {
            enabled: default_backfill_enabled(),
            gap_threshold_secs: default_gap_threshold_secs(),
            max_backfill_secs: default_max_backfill_secs(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

// ── Defaults ────────────────────────────────────────────────────────────────

fn default_timezone() -> String { "UTC".into() }
fn default_log_level() -> String { "INFO".into() }
fn default_base_path() -> String { "./data".into() }
fn default_write_interval() -> u64 { 60 }
fn default_reconnect_backoff() -> u64 { 2 }
fn default_max_reconnect_backoff() -> u64 { 60 }
fn default_reconnect_jitter() -> f64 { 0.3 }
fn default_output_format() -> String { "jsonl".into() }
fn default_resample_interval() -> u64 { 1 }
fn default_parquet_compression() -> String { "zstd".into() }
fn default_schedule_minutes() -> u64 { 60 }
fn default_server_port() -> u16 { 8000 }
fn default_static_dir() -> String { "./static".into() }
fn default_backfill_enabled() -> bool { true }
fn default_gap_threshold_secs() -> u64 { 60 }
fn default_max_backfill_secs() -> u64 { 2_592_000 } // 30 days
fn default_timeout_secs() -> u64 { 300 }
fn default_bar_interval_sec() -> u64 { 1 }

impl Default for Server {
    fn default() -> Self {
        Self {
            port: default_server_port(),
            static_dir: default_static_dir(),
        }
    }
}

// ── Loader ──────────────────────────────────────────────────────────────────

impl Config {
    /// Load configuration from a YAML file.
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {}", path, e))?;
        let cfg: Config = serde_yaml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file: {}", e))?;
        Ok(cfg)
    }

    /// Resolve the base data path to an absolute path.
    pub fn data_path(&self) -> PathBuf {
        let p = PathBuf::from(&self.general.base_path);
        if p.is_absolute() {
            p
        } else {
            std::env::current_dir().unwrap_or_default().join(p)
        }
    }

    /// Get exchange config by name.
    pub fn exchange(&self, name: &str) -> Option<&Exchange> {
        self.exchanges.iter().find(|e| e.name == name)
    }
}
