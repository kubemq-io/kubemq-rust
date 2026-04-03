//! v2 RunConfig API types for POST /run/start.

use std::collections::HashMap;

use serde::Deserialize;

use crate::config::{self, BurnInConfig};

#[derive(Debug, Deserialize, Default)]
pub struct RunConfigApi {
    #[serde(default)]
    pub broker: Option<BrokerOverride>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub duration: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub warmup_duration: Option<String>,
    #[serde(default)]
    pub starting_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub patterns: Option<HashMap<String, PatternConfigApi>>,
    #[serde(default)]
    pub queue: Option<QueueOverride>,
    #[serde(default)]
    pub rpc: Option<RpcOverride>,
    #[serde(default)]
    pub message: Option<MessageOverride>,
    #[serde(default)]
    pub thresholds: Option<ThresholdsOverride>,
    #[serde(default)]
    pub shutdown: Option<ShutdownOverride>,
    #[serde(default)]
    pub warmup: Option<WarmupOverride>,
    #[serde(default)]
    pub metrics: Option<MetricsOverride>,
}

#[derive(Debug, Deserialize, Default)]
pub struct BrokerOverride {
    pub address: Option<String>,
    pub client_id_prefix: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct PatternConfigApi {
    pub enabled: Option<bool>,
    pub channels: Option<u32>,
    pub rate: Option<u32>,
    pub producers_per_channel: Option<u32>,
    pub consumers_per_channel: Option<u32>,
    pub senders_per_channel: Option<u32>,
    pub responders_per_channel: Option<u32>,
    pub consumer_group: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct QueueOverride {
    pub poll_max_messages: Option<i32>,
    pub poll_wait_timeout_seconds: Option<i32>,
    pub auto_ack: Option<bool>,
    pub max_depth: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RpcOverride {
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct MessageOverride {
    pub size_mode: Option<String>,
    pub size_bytes: Option<usize>,
    pub size_distribution: Option<String>,
    pub reorder_window: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ThresholdsOverride {
    pub max_loss_pct: Option<f64>,
    pub max_events_loss_pct: Option<f64>,
    pub max_duplication_pct: Option<f64>,
    pub max_p99_latency_ms: Option<f64>,
    pub max_p999_latency_ms: Option<f64>,
    pub min_throughput_pct: Option<f64>,
    pub max_error_rate_pct: Option<f64>,
    pub max_memory_growth_factor: Option<f64>,
    pub max_downtime_pct: Option<f64>,
    pub max_duration: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ShutdownOverride {
    pub drain_timeout_seconds: Option<u64>,
    pub cleanup_channels: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct WarmupOverride {
    pub max_parallel_channels: Option<u32>,
    pub timeout_per_channel_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct MetricsOverride {
    pub report_interval: Option<String>,
}

impl RunConfigApi {
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if let Some(mode) = &self.mode {
            if mode != "soak" && mode != "benchmark" {
                errors.push(format!("mode: must be 'soak' or 'benchmark', got '{}'", mode));
            }
        }

        if let Some(duration) = &self.duration {
            if config::parse_duration_opt(duration).is_none() {
                errors.push(format!("duration: invalid format '{}'", duration));
            }
        }

        if let Some(patterns) = &self.patterns {
            for (name, pc) in patterns {
                if !config::ALL_PATTERN_NAMES.contains(&name.as_str()) {
                    errors.push(format!("patterns.{}: unknown pattern", name));
                }
                if let Some(channels) = pc.channels {
                    if channels == 0 || channels > 1000 {
                        errors.push(format!(
                            "patterns.{}.channels: must be 1-1000, got {}",
                            name, channels
                        ));
                    }
                }
                if let Some(rate) = pc.rate {
                    if rate > 1_000_000 {
                        errors.push(format!(
                            "patterns.{}.rate: must be <= 1000000, got {}",
                            name, rate
                        ));
                    }
                }
            }
        }

        errors
    }

    pub fn enabled_pattern_count(&self, startup: &BurnInConfig) -> usize {
        let mut count = 0;
        for &pname in config::ALL_PATTERN_NAMES {
            let enabled = if let Some(patterns) = &self.patterns {
                if let Some(pc) = patterns.get(pname) {
                    pc.enabled.unwrap_or(startup.pattern_enabled(pname))
                } else {
                    startup.pattern_enabled(pname)
                }
            } else {
                startup.pattern_enabled(pname)
            };
            if enabled {
                count += 1;
            }
        }
        count
    }

    pub fn total_channel_count(&self, startup: &BurnInConfig) -> u32 {
        let mut count = 0;
        for &pname in config::ALL_PATTERN_NAMES {
            let (enabled, channels) = if let Some(patterns) = &self.patterns {
                if let Some(pc) = patterns.get(pname) {
                    (
                        pc.enabled.unwrap_or(startup.pattern_enabled(pname)),
                        pc.channels.unwrap_or(startup.pattern_channels(pname)),
                    )
                } else {
                    (startup.pattern_enabled(pname), startup.pattern_channels(pname))
                }
            } else {
                (startup.pattern_enabled(pname), startup.pattern_channels(pname))
            };
            if enabled {
                count += channels;
            }
        }
        count
    }

    pub fn to_internal_config(&self, startup: &BurnInConfig) -> BurnInConfig {
        let mut cfg = startup.clone();

        if let Some(broker) = &self.broker {
            if let Some(addr) = &broker.address {
                cfg.broker.address = addr.clone();
            }
            if let Some(prefix) = &broker.client_id_prefix {
                cfg.broker.client_id_prefix = prefix.clone();
            }
        }

        if let Some(mode) = &self.mode {
            cfg.mode = mode.clone();
        }
        if let Some(duration) = &self.duration {
            cfg.duration = duration.clone();
        }
        if let Some(timeout) = self.starting_timeout_seconds {
            cfg.starting_timeout_seconds = timeout;
        }

        if let Some(patterns) = &self.patterns {
            for (name, pc) in patterns {
                match name.as_str() {
                    "events" => apply_pubsub_overrides(&mut cfg.patterns.events, pc),
                    "events_store" => apply_pubsub_overrides(&mut cfg.patterns.events_store, pc),
                    "queue_stream" => apply_queue_overrides(&mut cfg.patterns.queue_stream, pc),
                    "queue_simple" => apply_queue_overrides(&mut cfg.patterns.queue_simple, pc),
                    "commands" => apply_rpc_overrides(&mut cfg.patterns.commands, pc),
                    "queries" => apply_rpc_overrides(&mut cfg.patterns.queries, pc),
                    _ => {}
                }
            }
        }

        if let Some(q) = &self.queue {
            if let Some(v) = q.poll_max_messages { cfg.queue.poll_max_messages = v; }
            if let Some(v) = q.poll_wait_timeout_seconds { cfg.queue.poll_wait_timeout_seconds = v; }
            if let Some(v) = q.auto_ack { cfg.queue.auto_ack = v; }
            if let Some(v) = q.max_depth { cfg.queue.max_depth = v; }
        }
        if let Some(r) = &self.rpc {
            if let Some(v) = r.timeout_ms { cfg.rpc.timeout_ms = v; }
        }
        if let Some(m) = &self.message {
            if let Some(v) = &m.size_mode { cfg.message.size_mode = v.clone(); }
            if let Some(v) = m.size_bytes { cfg.message.size_bytes = v; }
            if let Some(v) = &m.size_distribution { cfg.message.size_distribution = v.clone(); }
            if let Some(v) = m.reorder_window { cfg.message.reorder_window = v; }
        }
        if let Some(t) = &self.thresholds {
            if let Some(v) = t.max_loss_pct { cfg.thresholds.max_loss_pct = v; }
            if let Some(v) = t.max_events_loss_pct { cfg.thresholds.max_events_loss_pct = v; }
            if let Some(v) = t.max_duplication_pct { cfg.thresholds.max_duplication_pct = v; }
            if let Some(v) = t.max_p99_latency_ms { cfg.thresholds.max_p99_latency_ms = v; }
            if let Some(v) = t.max_p999_latency_ms { cfg.thresholds.max_p999_latency_ms = v; }
            if let Some(v) = t.min_throughput_pct { cfg.thresholds.min_throughput_pct = v; }
            if let Some(v) = t.max_error_rate_pct { cfg.thresholds.max_error_rate_pct = v; }
            if let Some(v) = t.max_memory_growth_factor { cfg.thresholds.max_memory_growth_factor = v; }
            if let Some(v) = t.max_downtime_pct { cfg.thresholds.max_downtime_pct = v; }
            if let Some(v) = &t.max_duration { cfg.thresholds.max_duration = v.clone(); }
        }
        if let Some(s) = &self.shutdown {
            if let Some(v) = s.drain_timeout_seconds { cfg.shutdown.drain_timeout_seconds = v; }
            if let Some(v) = s.cleanup_channels { cfg.shutdown.cleanup_channels = v; }
        }
        if let Some(w) = &self.warmup {
            if let Some(v) = w.max_parallel_channels { cfg.warmup.max_parallel_channels = v; }
            if let Some(v) = w.timeout_per_channel_ms { cfg.warmup.timeout_per_channel_ms = v; }
        }

        cfg
    }
}

fn apply_pubsub_overrides(target: &mut config::PatternConfig, src: &PatternConfigApi) {
    if let Some(v) = src.enabled { target.enabled = v; }
    if let Some(v) = src.channels { target.channels = v; }
    if let Some(v) = src.rate { target.rate = v; }
    if let Some(v) = src.producers_per_channel { target.producers_per_channel = v; }
    if let Some(v) = src.consumers_per_channel { target.consumers_per_channel = v; }
    if let Some(v) = src.consumer_group { target.consumer_group = v; }
}

fn apply_queue_overrides(target: &mut config::QueuePatternConfig, src: &PatternConfigApi) {
    if let Some(v) = src.enabled { target.enabled = v; }
    if let Some(v) = src.channels { target.channels = v; }
    if let Some(v) = src.rate { target.rate = v; }
    if let Some(v) = src.producers_per_channel { target.producers_per_channel = v; }
    if let Some(v) = src.consumers_per_channel { target.consumers_per_channel = v; }
}

fn apply_rpc_overrides(target: &mut config::RpcPatternConfig, src: &PatternConfigApi) {
    if let Some(v) = src.enabled { target.enabled = v; }
    if let Some(v) = src.channels { target.channels = v; }
    if let Some(v) = src.rate { target.rate = v; }
    if let Some(v) = src.senders_per_channel { target.senders_per_channel = v; }
    if let Some(v) = src.responders_per_channel { target.responders_per_channel = v; }
}

pub fn detect_v1_format(raw: &serde_json::Value) -> Vec<String> {
    config::detect_v1_format(raw)
}
