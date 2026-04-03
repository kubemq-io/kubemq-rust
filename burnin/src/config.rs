//! YAML configuration matching Go burn-in structure exactly.

use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const ALL_PATTERN_NAMES: &[&str] = &[
    "events",
    "events_store",
    "queue_stream",
    "queue_simple",
    "commands",
    "queries",
];

pub fn is_rpc_pattern(name: &str) -> bool {
    name == "commands" || name == "queries"
}

pub fn is_pubsub_pattern(name: &str) -> bool {
    name == "events" || name == "events_store"
}

pub fn is_queue_pattern(name: &str) -> bool {
    name == "queue_stream" || name == "queue_simple"
}

pub fn channel_type_for_pattern(pattern: &str) -> &'static str {
    match pattern {
        "events" => kubemq::channel_type::EVENTS,
        "events_store" => kubemq::channel_type::EVENTS_STORE,
        "queue_stream" | "queue_simple" => kubemq::channel_type::QUEUES,
        "commands" => kubemq::channel_type::COMMANDS,
        "queries" => kubemq::channel_type::QUERIES,
        _ => kubemq::channel_type::EVENTS,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurnInConfig {
    #[serde(default = "default_version")]
    pub version: String,

    #[serde(default)]
    pub broker: BrokerConfig,

    #[serde(default = "default_mode")]
    pub mode: String,

    #[serde(default = "default_duration")]
    pub duration: String,

    #[serde(default)]
    pub run_id: String,

    #[serde(default = "default_starting_timeout")]
    pub starting_timeout_seconds: u64,

    #[serde(default)]
    pub patterns: PatternsConfig,

    #[serde(default)]
    pub api: ApiConfig,

    #[serde(default)]
    pub queue: QueueConfig,

    #[serde(default)]
    pub rpc: RpcConfig,

    #[serde(default)]
    pub message: MessageConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,

    #[serde(default)]
    pub logging: LoggingConfig,

    #[serde(default)]
    pub forced_disconnect: ForcedDisconnectConfig,

    #[serde(default)]
    pub recovery: RecoveryConfig,

    #[serde(default)]
    pub shutdown: ShutdownConfig,

    #[serde(default)]
    pub output: OutputConfig,

    #[serde(default)]
    pub thresholds: ThresholdsConfig,

    #[serde(default)]
    pub warmup: WarmupConfig,

    #[serde(default)]
    pub cors: CorsConfig,
}

impl Default for BurnInConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            broker: BrokerConfig::default(),
            mode: default_mode(),
            duration: default_duration(),
            run_id: String::new(),
            starting_timeout_seconds: default_starting_timeout(),
            patterns: PatternsConfig::default(),
            api: ApiConfig::default(),
            queue: QueueConfig::default(),
            rpc: RpcConfig::default(),
            message: MessageConfig::default(),
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            forced_disconnect: ForcedDisconnectConfig::default(),
            recovery: RecoveryConfig::default(),
            shutdown: ShutdownConfig::default(),
            output: OutputConfig::default(),
            thresholds: ThresholdsConfig::default(),
            warmup: WarmupConfig::default(),
            cors: CorsConfig::default(),
        }
    }
}

impl BurnInConfig {
    pub fn broker_host(&self) -> String {
        if let Some((host, _)) = self.broker.address.rsplit_once(':') {
            host.to_string()
        } else {
            self.broker.address.clone()
        }
    }

    pub fn broker_port(&self) -> u16 {
        if let Some((_, port_str)) = self.broker.address.rsplit_once(':') {
            port_str.parse().unwrap_or(50000)
        } else {
            50000
        }
    }

    pub fn any_pattern_enabled(&self) -> bool {
        self.patterns.events.enabled
            || self.patterns.events_store.enabled
            || self.patterns.queue_stream.enabled
            || self.patterns.queue_simple.enabled
            || self.patterns.commands.enabled
            || self.patterns.queries.enabled
    }

    pub fn total_channel_count(&self) -> u32 {
        let mut count = 0;
        if self.patterns.events.enabled {
            count += self.patterns.events.channels;
        }
        if self.patterns.events_store.enabled {
            count += self.patterns.events_store.channels;
        }
        if self.patterns.queue_stream.enabled {
            count += self.patterns.queue_stream.channels;
        }
        if self.patterns.queue_simple.enabled {
            count += self.patterns.queue_simple.channels;
        }
        if self.patterns.commands.enabled {
            count += self.patterns.commands.channels;
        }
        if self.patterns.queries.enabled {
            count += self.patterns.queries.channels;
        }
        count
    }

    pub fn enabled_pattern_count(&self) -> usize {
        let mut count = 0;
        if self.patterns.events.enabled { count += 1; }
        if self.patterns.events_store.enabled { count += 1; }
        if self.patterns.queue_stream.enabled { count += 1; }
        if self.patterns.queue_simple.enabled { count += 1; }
        if self.patterns.commands.enabled { count += 1; }
        if self.patterns.queries.enabled { count += 1; }
        count
    }

    pub fn pattern_enabled(&self, name: &str) -> bool {
        match name {
            "events" => self.patterns.events.enabled,
            "events_store" => self.patterns.events_store.enabled,
            "queue_stream" => self.patterns.queue_stream.enabled,
            "queue_simple" => self.patterns.queue_simple.enabled,
            "commands" => self.patterns.commands.enabled,
            "queries" => self.patterns.queries.enabled,
            _ => false,
        }
    }

    pub fn pattern_channels(&self, name: &str) -> u32 {
        match name {
            "events" => self.patterns.events.channels,
            "events_store" => self.patterns.events_store.channels,
            "queue_stream" => self.patterns.queue_stream.channels,
            "queue_simple" => self.patterns.queue_simple.channels,
            "commands" => self.patterns.commands.channels,
            "queries" => self.patterns.queries.channels,
            _ => 0,
        }
    }

    pub fn pattern_rate(&self, name: &str) -> u32 {
        match name {
            "events" => self.patterns.events.rate,
            "events_store" => self.patterns.events_store.rate,
            "queue_stream" => self.patterns.queue_stream.rate,
            "queue_simple" => self.patterns.queue_simple.rate,
            "commands" => self.patterns.commands.rate,
            "queries" => self.patterns.queries.rate,
            _ => 0,
        }
    }

    pub fn validate(&self) -> (Vec<String>, Vec<String>) {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        if self.mode != "soak" && self.mode != "benchmark" && self.mode != "idle" {
            errors.push(format!(
                "mode: must be 'soak', 'benchmark', or 'idle', got '{}'",
                self.mode
            ));
        }

        if parse_duration_opt(&self.duration).is_none() {
            errors.push(format!("duration: invalid format '{}'", self.duration));
        }

        if self.mode != "idle" && !self.any_pattern_enabled() {
            errors.push("no patterns enabled".to_string());
        }

        if self.broker.address.is_empty() {
            errors.push("broker.address is required".to_string());
        }

        for &pname in ALL_PATTERN_NAMES {
            let rate = self.pattern_rate(pname);
            let channels = self.pattern_channels(pname);
            if self.pattern_enabled(pname) {
                if channels == 0 || channels > 1000 {
                    errors.push(format!(
                        "patterns.{}.channels: must be 1-1000, got {}",
                        pname, channels
                    ));
                }
                if rate > 100_000 {
                    warnings.push(format!(
                        "patterns.{}.rate: {} is very high, may cause issues",
                        pname, rate
                    ));
                }
            }
        }

        (errors, warnings)
    }
}

fn default_version() -> String {
    "2".to_string()
}
fn default_mode() -> String {
    "soak".to_string()
}
fn default_duration() -> String {
    "1h".to_string()
}
fn default_starting_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    #[serde(default = "default_broker_address")]
    pub address: String,
    #[serde(default = "default_client_id_prefix")]
    pub client_id_prefix: String,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            address: default_broker_address(),
            client_id_prefix: default_client_id_prefix(),
        }
    }
}

fn default_broker_address() -> String {
    "localhost:50000".to_string()
}
fn default_client_id_prefix() -> String {
    "burnin-rust".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternsConfig {
    #[serde(default)]
    pub events: PatternConfig,
    #[serde(default)]
    pub events_store: PatternConfig,
    #[serde(default)]
    pub queue_stream: QueuePatternConfig,
    #[serde(default)]
    pub queue_simple: QueuePatternConfig,
    #[serde(default)]
    pub commands: RpcPatternConfig,
    #[serde(default)]
    pub queries: RpcPatternConfig,
}

impl Default for PatternsConfig {
    fn default() -> Self {
        Self {
            events: PatternConfig {
                enabled: true,
                channels: 1,
                producers_per_channel: 1,
                consumers_per_channel: 1,
                consumer_group: false,
                rate: 100,
            },
            events_store: PatternConfig {
                enabled: true,
                channels: 1,
                producers_per_channel: 1,
                consumers_per_channel: 1,
                consumer_group: false,
                rate: 100,
            },
            queue_stream: QueuePatternConfig {
                enabled: true,
                channels: 1,
                producers_per_channel: 1,
                consumers_per_channel: 1,
                rate: 50,
            },
            queue_simple: QueuePatternConfig {
                enabled: true,
                channels: 1,
                producers_per_channel: 1,
                consumers_per_channel: 1,
                rate: 50,
            },
            commands: RpcPatternConfig {
                enabled: true,
                channels: 1,
                senders_per_channel: 1,
                responders_per_channel: 1,
                rate: 20,
            },
            queries: RpcPatternConfig {
                enabled: true,
                channels: 1,
                senders_per_channel: 1,
                responders_per_channel: 1,
                rate: 20,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_one")]
    pub channels: u32,
    #[serde(default = "default_one")]
    pub producers_per_channel: u32,
    #[serde(default = "default_one")]
    pub consumers_per_channel: u32,
    #[serde(default)]
    pub consumer_group: bool,
    #[serde(default = "default_rate_100")]
    pub rate: u32,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channels: 1,
            producers_per_channel: 1,
            consumers_per_channel: 1,
            consumer_group: false,
            rate: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePatternConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_one")]
    pub channels: u32,
    #[serde(default = "default_one")]
    pub producers_per_channel: u32,
    #[serde(default = "default_one")]
    pub consumers_per_channel: u32,
    #[serde(default = "default_rate_50")]
    pub rate: u32,
}

impl Default for QueuePatternConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channels: 1,
            producers_per_channel: 1,
            consumers_per_channel: 1,
            rate: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPatternConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_one")]
    pub channels: u32,
    #[serde(default = "default_one")]
    pub senders_per_channel: u32,
    #[serde(default = "default_one")]
    pub responders_per_channel: u32,
    #[serde(default = "default_rate_20")]
    pub rate: u32,
}

impl Default for RpcPatternConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channels: 1,
            senders_per_channel: 1,
            responders_per_channel: 1,
            rate: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            port: 8888,
            enabled: true,
        }
    }
}

fn default_api_port() -> u16 {
    8888
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    #[serde(default = "default_poll_max_messages")]
    pub poll_max_messages: i32,
    #[serde(default = "default_poll_wait_timeout")]
    pub poll_wait_timeout_seconds: i32,
    #[serde(default)]
    pub auto_ack: bool,
    #[serde(default = "default_max_depth")]
    pub max_depth: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            poll_max_messages: 10,
            poll_wait_timeout_seconds: 5,
            auto_ack: false,
            max_depth: 1_000_000,
        }
    }
}

fn default_poll_max_messages() -> i32 {
    10
}
fn default_poll_wait_timeout() -> i32 {
    5
}
fn default_max_depth() -> u64 {
    1_000_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    #[serde(default = "default_rpc_timeout")]
    pub timeout_ms: u64,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self { timeout_ms: 5000 }
    }
}

fn default_rpc_timeout() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageConfig {
    #[serde(default = "default_size_mode")]
    pub size_mode: String,
    #[serde(default = "default_size_bytes")]
    pub size_bytes: usize,
    #[serde(default)]
    pub size_distribution: String,
    #[serde(default = "default_reorder_window")]
    pub reorder_window: u64,
}

impl Default for MessageConfig {
    fn default() -> Self {
        Self {
            size_mode: default_size_mode(),
            size_bytes: 1024,
            size_distribution: "256:80,4096:15,65536:5".to_string(),
            reorder_window: 10000,
        }
    }
}

fn default_size_mode() -> String {
    "fixed".to_string()
}
fn default_size_bytes() -> usize {
    1024
}
fn default_reorder_window() -> u64 {
    10000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_report_interval")]
    pub report_interval: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            port: 8888,
            report_interval: "30s".to_string(),
        }
    }
}

fn default_report_interval() -> String {
    "30s".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: "text".to_string(),
            level: "info".to_string(),
        }
    }
}

fn default_log_format() -> String {
    "text".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForcedDisconnectConfig {
    #[serde(default = "default_zero_str")]
    pub interval: String,
    #[serde(default = "default_5s")]
    pub duration: String,
}

impl Default for ForcedDisconnectConfig {
    fn default() -> Self {
        Self {
            interval: "0".to_string(),
            duration: "5s".to_string(),
        }
    }
}

fn default_zero_str() -> String {
    "0".to_string()
}
fn default_5s() -> String {
    "5s".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryConfig {
    #[serde(default = "default_1s")]
    pub reconnect_interval: String,
    #[serde(default = "default_30s")]
    pub reconnect_max_interval: String,
    #[serde(default = "default_multiplier")]
    pub reconnect_multiplier: f64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            reconnect_interval: "1s".to_string(),
            reconnect_max_interval: "30s".to_string(),
            reconnect_multiplier: 2.0,
        }
    }
}

fn default_1s() -> String {
    "1s".to_string()
}
fn default_30s() -> String {
    "30s".to_string()
}
fn default_multiplier() -> f64 {
    2.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout_seconds: u64,
    #[serde(default = "default_true")]
    pub cleanup_channels: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_seconds: 10,
            cleanup_channels: true,
        }
    }
}

fn default_drain_timeout() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OutputConfig {
    #[serde(default)]
    pub report_file: String,
    #[serde(default)]
    pub sdk_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdsConfig {
    #[serde(default)]
    pub max_loss_pct: f64,
    #[serde(default = "default_events_loss")]
    pub max_events_loss_pct: f64,
    #[serde(default = "default_dup_pct")]
    pub max_duplication_pct: f64,
    #[serde(default = "default_p99")]
    pub max_p99_latency_ms: f64,
    #[serde(default = "default_p999")]
    pub max_p999_latency_ms: f64,
    #[serde(default = "default_throughput")]
    pub min_throughput_pct: f64,
    #[serde(default = "default_error_rate")]
    pub max_error_rate_pct: f64,
    #[serde(default = "default_memory_growth")]
    pub max_memory_growth_factor: f64,
    #[serde(default = "default_downtime")]
    pub max_downtime_pct: f64,
    #[serde(default = "default_max_duration")]
    pub max_duration: String,
}

impl Default for ThresholdsConfig {
    fn default() -> Self {
        Self {
            max_loss_pct: 0.0,
            max_events_loss_pct: 5.0,
            max_duplication_pct: 0.1,
            max_p99_latency_ms: 1000.0,
            max_p999_latency_ms: 5000.0,
            min_throughput_pct: 90.0,
            max_error_rate_pct: 1.0,
            max_memory_growth_factor: 2.0,
            max_downtime_pct: 10.0,
            max_duration: "168h".to_string(),
        }
    }
}

fn default_events_loss() -> f64 {
    5.0
}
fn default_dup_pct() -> f64 {
    0.1
}
fn default_p99() -> f64 {
    1000.0
}
fn default_p999() -> f64 {
    5000.0
}
fn default_throughput() -> f64 {
    90.0
}
fn default_error_rate() -> f64 {
    1.0
}
fn default_memory_growth() -> f64 {
    2.0
}
fn default_downtime() -> f64 {
    10.0
}
fn default_max_duration() -> String {
    "168h".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmupConfig {
    #[serde(default = "default_warmup_parallel")]
    pub max_parallel_channels: u32,
    #[serde(default = "default_warmup_timeout")]
    pub timeout_per_channel_ms: u64,
    #[serde(default)]
    pub warmup_duration: String,
}

impl Default for WarmupConfig {
    fn default() -> Self {
        Self {
            max_parallel_channels: 10,
            timeout_per_channel_ms: 5000,
            warmup_duration: String::new(),
        }
    }
}

fn default_warmup_parallel() -> u32 {
    10
}
fn default_warmup_timeout() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    #[serde(default = "default_cors_origins")]
    pub origins: String,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            origins: "*".to_string(),
        }
    }
}

fn default_cors_origins() -> String {
    "*".to_string()
}

fn default_true() -> bool {
    true
}
fn default_one() -> u32 {
    1
}
fn default_rate_100() -> u32 {
    100
}
fn default_rate_50() -> u32 {
    50
}
fn default_rate_20() -> u32 {
    20
}

/// Parse a duration string supporting d/h/m/s suffixes.
pub fn parse_duration(s: &str) -> Duration {
    parse_duration_opt(s).unwrap_or(Duration::from_secs(900))
}

pub fn parse_duration_opt(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Some(Duration::ZERO);
    }
    if let Some(days) = s.strip_suffix('d') {
        return days.parse::<u64>().ok().map(|d| Duration::from_secs(d * 86400));
    }
    if let Some(hours) = s.strip_suffix('h') {
        return hours.parse::<u64>().ok().map(|h| Duration::from_secs(h * 3600));
    }
    if let Some(mins) = s.strip_suffix('m') {
        return mins.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60));
    }
    if let Some(secs) = s.strip_suffix('s') {
        return secs.parse::<u64>().ok().map(Duration::from_secs);
    }
    s.parse::<u64>().ok().map(Duration::from_secs)
}

pub fn channel_name_for_pattern(run_id: &str, pattern: &str, channel_index: usize) -> String {
    format!("rust_burnin_{}_{}_{:04}", run_id, pattern, channel_index)
}

pub fn consumer_group_name(run_id: &str, pattern: &str, channel_index: usize) -> String {
    format!("rust_burnin_{}_{}_{:04}_group", run_id, pattern, channel_index)
}

const V1_FIELDS: &[(&str, &str)] = &[
    ("events_rate", "use patterns.events.rate"),
    ("events_store_rate", "use patterns.events_store.rate"),
    ("queue_rate", "use patterns.queue_stream.rate"),
    ("commands_rate", "use patterns.commands.rate"),
    ("queries_rate", "use patterns.queries.rate"),
    ("events_channel", "use patterns.events.channels"),
    ("poll_max_messages", "moved to queue.poll_max_messages"),
    ("poll_wait_timeout_seconds", "moved to queue.poll_wait_timeout_seconds"),
    ("producers", "use patterns.<pattern>.producers_per_channel"),
    ("consumers", "use patterns.<pattern>.consumers_per_channel"),
    ("senders", "use patterns.<pattern>.senders_per_channel"),
    ("responders", "use patterns.<pattern>.responders_per_channel"),
];

pub fn detect_v1_format(raw: &serde_json::Value) -> Vec<String> {
    let obj = match raw.as_object() {
        Some(o) => o,
        None => return vec![],
    };
    V1_FIELDS
        .iter()
        .filter(|(key, _)| obj.contains_key(*key))
        .map(|(key, hint)| format!("v1 field '{}' detected: {}", key, hint))
        .collect()
}
