//! Data-driven burn-in report generation with verdict checks.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{self, BurnInConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub run_id: String,
    pub sdk: String,
    #[serde(rename = "sdk_version")]
    pub version: String,
    pub mode: String,
    pub broker_address: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub duration_seconds: f64,
    pub all_patterns_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub patterns: HashMap<String, PatternStats>,
    pub resources: ResourceStats,
    pub verdict: Verdict,
}

impl Summary {
    pub fn error_report(cfg: &BurnInConfig, error: &str) -> Self {
        let now = Utc::now();
        let version = if cfg.output.sdk_version.is_empty() {
            kubemq::VERSION.to_string()
        } else {
            cfg.output.sdk_version.clone()
        };

        Summary {
            run_id: cfg.run_id.clone(),
            sdk: "rust".to_string(),
            version,
            mode: cfg.mode.clone(),
            broker_address: cfg.broker.address.clone(),
            started_at: now,
            ended_at: now,
            duration_seconds: 0.0,
            all_patterns_enabled: false,
            status: Some("error".to_string()),
            patterns: HashMap::new(),
            resources: ResourceStats::default(),
            verdict: Verdict {
                result: "FAILED".to_string(),
                passed: false,
                warnings: Vec::new(),
                checks: {
                    let mut checks = HashMap::new();
                    checks.insert(
                        "startup".to_string(),
                        CheckResult {
                            passed: false,
                            threshold: "success".to_string(),
                            actual: error.to_string(),
                            advisory: None,
                        },
                    );
                    checks
                },
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternStats {
    pub enabled: bool,
    pub status: String,
    pub sent: u64,
    pub received: u64,
    pub lost: u64,
    pub duplicated: u64,
    pub corrupted: u64,
    pub out_of_order: u64,
    pub loss_pct: f64,
    pub errors: u64,
    pub reconnections: u64,
    pub downtime_seconds: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_p999_ms: f64,
    pub avg_rate: f64,
    pub peak_rate: f64,
    pub target_rate: i32,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub channels: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responses_success: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responses_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responses_error: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceStats {
    pub peak_rss_mb: f64,
    pub baseline_rss_mb: f64,
    pub memory_growth_factor: f64,
    pub peak_workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Verdict {
    pub result: String,
    pub passed: bool,
    pub warnings: Vec<String>,
    pub checks: HashMap<String, CheckResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub passed: bool,
    pub threshold: String,
    pub actual: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advisory: Option<bool>,
}

pub fn generate_verdict(summary: &Summary, cfg: &BurnInConfig) -> Verdict {
    let mut checks = HashMap::new();
    let mut warnings = Vec::new();
    let mut any_fail = false;

    for (pname, ps) in &summary.patterns {
        if !ps.enabled {
            continue;
        }

        let is_rpc = config::is_rpc_pattern(pname);

        // Message loss check (pub/sub + queue patterns)
        if !is_rpc {
            let threshold = if pname == "events" {
                cfg.thresholds.max_events_loss_pct
            } else {
                cfg.thresholds.max_loss_pct
            };

            let check_name = format!("{}_message_loss", pname);
            let passed = ps.loss_pct <= threshold;
            if !passed {
                any_fail = true;
            }
            checks.insert(
                check_name,
                CheckResult {
                    passed,
                    threshold: format!("{:.1}%", threshold),
                    actual: format!("{:.2}%", ps.loss_pct),
                    advisory: None,
                },
            );
        }

        // Corruption check
        {
            let check_name = format!("{}_corruption", pname);
            let passed = ps.corrupted == 0;
            if !passed {
                any_fail = true;
            }
            checks.insert(
                check_name,
                CheckResult {
                    passed,
                    threshold: "0".to_string(),
                    actual: ps.corrupted.to_string(),
                    advisory: None,
                },
            );
        }

        // P99 latency check
        {
            let check_name = format!("{}_p99_latency", pname);
            let passed = ps.latency_p99_ms <= cfg.thresholds.max_p99_latency_ms;
            if !passed {
                any_fail = true;
            }
            checks.insert(
                check_name,
                CheckResult {
                    passed,
                    threshold: format!("{:.0}ms", cfg.thresholds.max_p99_latency_ms),
                    actual: format!("{:.1}ms", ps.latency_p99_ms),
                    advisory: None,
                },
            );
        }

        // Error rate check
        {
            let total_ops = ps.sent + ps.received;
            let error_rate = if total_ops > 0 {
                (ps.errors as f64 / total_ops as f64) * 100.0
            } else {
                0.0
            };
            let check_name = format!("{}_error_rate", pname);
            let passed = error_rate <= cfg.thresholds.max_error_rate_pct;
            if !passed {
                any_fail = true;
            }
            checks.insert(
                check_name,
                CheckResult {
                    passed,
                    threshold: format!("{:.1}%", cfg.thresholds.max_error_rate_pct),
                    actual: format!("{:.2}%", error_rate),
                    advisory: None,
                },
            );
        }

        // Throughput check (soak mode only)
        if cfg.mode == "soak" && ps.target_rate > 0 && !is_rpc {
            let throughput_pct = (ps.avg_rate / ps.target_rate as f64) * 100.0;
            let check_name = format!("{}_throughput", pname);
            let passed = throughput_pct >= cfg.thresholds.min_throughput_pct;
            if !passed {
                warnings.push(format!(
                    "{}: throughput {:.1}% below threshold {:.1}%",
                    pname, throughput_pct, cfg.thresholds.min_throughput_pct
                ));
            }
            checks.insert(
                check_name,
                CheckResult {
                    passed,
                    threshold: format!("{:.1}%", cfg.thresholds.min_throughput_pct),
                    actual: format!("{:.1}%", throughput_pct),
                    advisory: Some(true),
                },
            );
        }
    }

    // Memory growth check
    if summary.resources.baseline_rss_mb > 0.0 {
        let passed = summary.resources.memory_growth_factor <= cfg.thresholds.max_memory_growth_factor;
        if !passed {
            warnings.push(format!(
                "memory growth {:.2}x exceeds threshold {:.1}x",
                summary.resources.memory_growth_factor, cfg.thresholds.max_memory_growth_factor
            ));
        }
        checks.insert(
            "memory_growth".to_string(),
            CheckResult {
                passed,
                threshold: format!("{:.1}x", cfg.thresholds.max_memory_growth_factor),
                actual: format!("{:.2}x", summary.resources.memory_growth_factor),
                advisory: Some(true),
            },
        );
    }

    let has_advisory_only_fails = checks.values().any(|c| !c.passed && c.advisory == Some(true));
    let has_hard_fails = any_fail;

    let (result, passed) = if has_hard_fails {
        ("FAILED".to_string(), false)
    } else if has_advisory_only_fails {
        ("PASSED_WITH_WARNINGS".to_string(), true)
    } else {
        ("PASSED".to_string(), true)
    };

    Verdict {
        result,
        passed,
        warnings,
        checks,
    }
}

pub fn print_console(summary: &Summary) {
    println!();
    println!("================================================================================");
    println!(
        "  KUBEMQ BURN-IN TEST REPORT -- Rust SDK {}",
        summary.version
    );
    println!("================================================================================");
    println!("  SDK:      {}", summary.sdk);
    println!("  Version:  {}", summary.version);
    println!("  Mode:     {}", summary.mode);
    println!("  Broker:   {}", summary.broker_address);
    println!("  Duration: {:.1}s", summary.duration_seconds);
    println!("  Run ID:   {}", summary.run_id);
    println!();
    println!("  PATTERN STATISTICS");
    println!("--------------------------------------------------------------------------------");
    println!(
        "  {:<20} {:>8} {:>8} {:>6} {:>6} {:>6} {:>8} {:>8}",
        "PATTERN", "SENT", "RECV", "LOST", "DUP", "ERR", "P99(ms)", "P999(ms)"
    );
    println!("--------------------------------------------------------------------------------");

    for (pname, ps) in &summary.patterns {
        if !ps.enabled {
            continue;
        }
        let recv_str = if config::is_rpc_pattern(pname) {
            format!("{}", ps.responses_success.unwrap_or(0))
        } else {
            format!("{}", ps.received)
        };
        println!(
            "  {:<20} {:>8} {:>8} {:>6} {:>6} {:>6} {:>8.1} {:>8.1}",
            pname,
            ps.sent,
            recv_str,
            ps.lost,
            ps.duplicated,
            ps.errors,
            ps.latency_p99_ms,
            ps.latency_p999_ms,
        );
    }

    println!("--------------------------------------------------------------------------------");
    println!();
    println!(
        "  VERDICT: {} {}",
        summary.verdict.result,
        if summary.verdict.passed { "✓" } else { "✗" }
    );

    if !summary.verdict.warnings.is_empty() {
        println!("  WARNINGS:");
        for w in &summary.verdict.warnings {
            println!("    - {}", w);
        }
    }

    println!("================================================================================");
    println!();
}
