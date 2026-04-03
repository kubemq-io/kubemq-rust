//! Worker orchestration engine implementing T0-T4 lifecycle with v2 PatternGroup architecture.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rand::Rng;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::RunConfigApi;
use crate::config::{self, BurnInConfig};
use crate::metrics::LatencyAccumulator;
use crate::payload;
use crate::report::{self, Summary};
use crate::tracker::MessageTracker;
use crate::worker::{commands, events, events_store, queries, queue_simple, queue_stream};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunState {
    Idle,
    Starting,
    Running,
    Stopping,
    Stopped,
    Error,
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "idle"),
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Stopping => write!(f, "stopping"),
            Self::Stopped => write!(f, "stopped"),
            Self::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PatternSnapshot {
    pub sent: u64,
    pub received: u64,
    pub errors: u64,
    pub rpc_success: u64,
    pub rpc_timeout: u64,
    pub rpc_error: u64,
}

pub struct ChannelWorkerSet {
    pub channel_name: String,
    pub channel_index: usize,
    pub pattern: String,
    pub tracker: Arc<std::sync::Mutex<MessageTracker>>,
    pub lat_accum: Arc<LatencyAccumulator>,

    pub producer_sent: Vec<Arc<AtomicU64>>,
    pub producer_errors: Vec<Arc<AtomicU64>>,
    pub consumer_received: Vec<Arc<AtomicU64>>,
    pub consumer_errors: Vec<Arc<AtomicU64>>,
    pub rpc_timeouts: Vec<Arc<AtomicU64>>,

    producer_running: Vec<Arc<std::sync::atomic::AtomicBool>>,
    consumer_running: Vec<Arc<std::sync::atomic::AtomicBool>>,

    producer_handles: Vec<JoinHandle<()>>,
    consumer_handles: Vec<JoinHandle<()>>,
    subscriptions: Vec<kubemq::Subscription>,
}

impl ChannelWorkerSet {
    pub fn total_sent(&self) -> u64 {
        self.producer_sent.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn total_received(&self) -> u64 {
        self.consumer_received.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn total_errors(&self) -> u64 {
        let pe: u64 = self.producer_errors.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        let ce: u64 = self.consumer_errors.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        pe + ce
    }

    pub fn total_rpc_success(&self) -> u64 {
        self.producer_sent.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn total_rpc_timeout(&self) -> u64 {
        self.rpc_timeouts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn stop_producers(&self) {
        for flag in &self.producer_running {
            flag.store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    pub fn stop_consumers(&self) {
        for flag in &self.consumer_running {
            flag.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        // Subscriptions are dropped when the ChannelWorkerSet is dropped
    }
}

pub struct PatternGroup {
    pub pattern: String,
    pub rate: u32,
    pub channels_count: u32,
    pub workers: Vec<ChannelWorkerSet>,
    pub lat_accum: Arc<LatencyAccumulator>,
}

impl PatternGroup {
    pub fn total_sent(&self) -> u64 {
        self.workers.iter().map(|w| w.total_sent()).sum()
    }

    pub fn total_received(&self) -> u64 {
        self.workers.iter().map(|w| w.total_received()).sum()
    }

    pub fn total_errors(&self) -> u64 {
        self.workers.iter().map(|w| w.total_errors()).sum()
    }

    pub fn total_rpc_success(&self) -> u64 {
        self.workers.iter().map(|w| w.total_rpc_success()).sum()
    }

    pub fn total_rpc_timeout(&self) -> u64 {
        self.workers.iter().map(|w| w.total_rpc_timeout()).sum()
    }

    pub fn stop_producers(&self) {
        for w in &self.workers {
            w.stop_producers();
        }
    }

    pub fn stop_consumers(&self) {
        for w in &self.workers {
            w.stop_consumers();
        }
    }

    pub fn snapshot(&self) -> PatternSnapshot {
        PatternSnapshot {
            sent: self.total_sent(),
            received: self.total_received(),
            errors: self.total_errors(),
            rpc_success: self.total_rpc_success(),
            rpc_timeout: self.total_rpc_timeout(),
            rpc_error: 0,
        }
    }
}

struct EngineInner {
    state: RunState,
    run_cfg: Option<BurnInConfig>,
    run_id: String,
    run_started_at: Option<DateTime<Utc>>,
    run_ended_at: Option<DateTime<Utc>>,
    producers_started_at: Option<Instant>,
    producers_stopped_at: Option<Instant>,
    run_error: String,
    run_cancel: Option<CancellationToken>,
    pattern_groups: HashMap<String, PatternGroup>,
    last_report: Option<Summary>,
    run_done_tx: Option<tokio::sync::watch::Sender<bool>>,
    producer_stop_snapshot: Option<HashMap<String, PatternSnapshot>>,
}

pub struct Engine {
    startup_cfg: BurnInConfig,
    boot_time: Instant,
    boot_time_utc: DateTime<Utc>,
    inner: Arc<Mutex<EngineInner>>,
}

impl Engine {
    pub fn new(cfg: BurnInConfig) -> Self {
        Self {
            startup_cfg: cfg,
            boot_time: Instant::now(),
            boot_time_utc: Utc::now(),
            inner: Arc::new(Mutex::new(EngineInner {
                state: RunState::Idle,
                run_cfg: None,
                run_id: String::new(),
                run_started_at: None,
                run_ended_at: None,
                producers_started_at: None,
                producers_stopped_at: None,
                run_error: String::new(),
                run_cancel: None,
                pattern_groups: HashMap::new(),
                last_report: None,
                run_done_tx: None,
                producer_stop_snapshot: None,
            })),
        }
    }

    pub fn startup_cfg(&self) -> &BurnInConfig {
        &self.startup_cfg
    }

    pub async fn state(&self) -> RunState {
        self.inner.lock().await.state
    }

    pub async fn run_id(&self) -> String {
        self.inner.lock().await.run_id.clone()
    }

    pub async fn start_run(&self, rc: &RunConfigApi) -> Result<(String, usize), String> {
        let mut inner = self.inner.lock().await;
        match inner.state {
            RunState::Idle | RunState::Stopped | RunState::Error => {}
            other => {
                return Err(format!("cannot start run in state {}", other));
            }
        }

        let run_cfg = rc.to_internal_config(&self.startup_cfg);
        let run_id = if let Some(id) = &rc.run_id {
            if !id.is_empty() { id.clone() } else { generate_run_id() }
        } else {
            generate_run_id()
        };

        let enabled_count = rc.enabled_pattern_count(&self.startup_cfg);

        inner.state = RunState::Starting;
        inner.run_cfg = Some(run_cfg.clone());
        inner.run_id = run_id.clone();
        inner.run_started_at = Some(Utc::now());
        inner.run_ended_at = None;
        inner.producers_started_at = None;
        inner.producers_stopped_at = None;
        inner.run_error = String::new();
        inner.pattern_groups = HashMap::new();
        inner.last_report = None;
        inner.producer_stop_snapshot = None;

        let (done_tx, _done_rx) = tokio::sync::watch::channel(false);
        inner.run_done_tx = Some(done_tx);

        let cancel = CancellationToken::new();
        inner.run_cancel = Some(cancel.clone());

        drop(inner);

        let engine_inner = self.inner.clone();
        let mut run_cfg_owned = run_cfg;
        run_cfg_owned.run_id = run_id.clone();

        tokio::spawn(async move {
            run_lifecycle(engine_inner, run_cfg_owned, cancel).await;
        });

        Ok((run_id, enabled_count))
    }

    pub async fn start_run_from_config(&self, cfg: BurnInConfig) -> Result<String, String> {
        let mut inner = self.inner.lock().await;
        match inner.state {
            RunState::Idle | RunState::Stopped | RunState::Error => {}
            other => {
                return Err(format!("cannot start run in state {}", other));
            }
        }

        let run_id = if cfg.run_id.is_empty() {
            generate_run_id()
        } else {
            cfg.run_id.clone()
        };

        inner.state = RunState::Starting;
        inner.run_cfg = Some(cfg.clone());
        inner.run_id = run_id.clone();
        inner.run_started_at = Some(Utc::now());
        inner.run_ended_at = None;
        inner.run_error = String::new();
        inner.pattern_groups = HashMap::new();
        inner.last_report = None;
        inner.producer_stop_snapshot = None;

        let (done_tx, _done_rx) = tokio::sync::watch::channel(false);
        inner.run_done_tx = Some(done_tx);

        let cancel = CancellationToken::new();
        inner.run_cancel = Some(cancel.clone());

        drop(inner);

        let engine_inner = self.inner.clone();
        let mut run_cfg_owned = cfg;
        run_cfg_owned.run_id = run_id.clone();

        tokio::spawn(async move {
            run_lifecycle(engine_inner, run_cfg_owned, cancel).await;
        });

        Ok(run_id)
    }

    pub async fn stop_run(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        match inner.state {
            RunState::Starting | RunState::Running => {
                inner.state = RunState::Stopping;
                if let Some(cancel) = &inner.run_cancel {
                    cancel.cancel();
                }
                Ok(())
            }
            other => Err(format!("cannot stop run in state {}", other)),
        }
    }

    pub async fn graceful_shutdown(&self) -> bool {
        let state = self.state().await;
        match state {
            RunState::Starting | RunState::Running => {
                let _ = self.stop_run().await;
                self.wait_for_run().await;
            }
            RunState::Stopping => {
                self.wait_for_run().await;
            }
            _ => {}
        }
        let inner = self.inner.lock().await;
        if let Some(rpt) = &inner.last_report {
            rpt.verdict.passed
        } else {
            true
        }
    }

    async fn wait_for_run(&self) {
        let rx = {
            let inner = self.inner.lock().await;
            inner.run_done_tx.as_ref().map(|tx| tx.subscribe())
        };
        if let Some(mut rx) = rx {
            let _ = rx.wait_for(|v| *v).await;
        }
    }

    pub async fn get_info(&self) -> serde_json::Value {
        serde_json::json!({
            "sdk": "rust",
            "sdk_version": self.sdk_version(),
            "burnin_version": "2.0.0",
            "burnin_spec_version": "2",
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "runtime": format!("rustc {}", env!("CARGO_PKG_RUST_VERSION", )),
            "cpus": num_cpus(),
            "pid": std::process::id(),
            "uptime_seconds": self.boot_time.elapsed().as_secs_f64(),
            "started_at": self.boot_time_utc.to_rfc3339(),
            "state": self.state().await.to_string(),
            "broker_address": self.startup_cfg.broker.address,
        })
    }

    pub async fn get_broker_status(&self) -> serde_json::Value {
        let host = self.startup_cfg.broker_host();
        let port = self.startup_cfg.broker_port();
        let start = Instant::now();

        let result = kubemq::KubemqClient::builder()
            .host(&host)
            .port(port)
            .client_id("burnin-ping")
            .build()
            .await;

        match result {
            Ok(client) => {
                match client.ping().await {
                    Ok(info) => {
                        let latency = start.elapsed().as_secs_f64() * 1000.0;
                        let _ = client.close().await;
                        serde_json::json!({
                            "connected": true,
                            "address": self.startup_cfg.broker.address,
                            "ping_latency_ms": latency,
                            "server_version": info.version,
                            "last_ping_at": Utc::now().to_rfc3339(),
                        })
                    }
                    Err(e) => {
                        let _ = client.close().await;
                        serde_json::json!({
                            "connected": false,
                            "address": self.startup_cfg.broker.address,
                            "error": e.to_string(),
                        })
                    }
                }
            }
            Err(e) => {
                serde_json::json!({
                    "connected": false,
                    "address": self.startup_cfg.broker.address,
                    "error": e.to_string(),
                })
            }
        }
    }

    pub async fn get_run_status(&self) -> serde_json::Value {
        let inner = self.inner.lock().await;
        let state = inner.state;

        if state == RunState::Idle {
            return serde_json::json!({"run_id": serde_json::Value::Null, "state": "idle"});
        }

        if state == RunState::Error {
            return serde_json::json!({
                "run_id": inner.run_id,
                "state": "error",
                "error": inner.run_error,
            });
        }

        let mut result = serde_json::json!({
            "run_id": inner.run_id,
            "state": state.to_string(),
            "started_at": inner.run_started_at.map(|t| t.to_rfc3339()),
        });

        if let Some(started) = inner.run_started_at {
            let elapsed = (Utc::now() - started).num_milliseconds() as f64 / 1000.0;
            result["elapsed_seconds"] = serde_json::json!(elapsed);

            if let Some(cfg) = &inner.run_cfg {
                let dur = config::parse_duration(&cfg.duration);
                if !dur.is_zero() {
                    let remaining = dur.as_secs_f64() - elapsed;
                    result["remaining_seconds"] = serde_json::json!(remaining.max(0.0));
                }
            }
        }

        let mut total_sent: u64 = 0;
        let mut total_recv: u64 = 0;
        let mut total_errors: u64 = 0;
        let mut pattern_states = serde_json::Map::new();

        for (pname, pg) in &inner.pattern_groups {
            let sent = pg.total_sent();
            total_sent += sent;
            if config::is_rpc_pattern(pname) {
                total_recv += pg.total_rpc_success();
            } else {
                total_recv += pg.total_received();
            }
            total_errors += pg.total_errors();

            pattern_states.insert(
                pname.clone(),
                serde_json::json!({
                    "state": if state == RunState::Stopped { "stopped" } else { "running" },
                    "channels": pg.channels_count,
                }),
            );
        }

        result["totals"] = serde_json::json!({
            "sent": total_sent,
            "received": total_recv,
            "lost": 0,
            "duplicated": 0,
            "corrupted": 0,
            "out_of_order": 0,
            "errors": total_errors,
            "reconnections": 0,
        });
        result["pattern_states"] = serde_json::Value::Object(pattern_states);
        result["warmup_active"] = serde_json::json!(false);

        result
    }

    pub async fn get_run_full(&self) -> serde_json::Value {
        self.get_run_status().await
    }

    pub async fn get_run_config(&self) -> Option<serde_json::Value> {
        let inner = self.inner.lock().await;
        inner.run_cfg.as_ref().map(|cfg| {
            serde_json::json!({
                "run_id": inner.run_id,
                "state": inner.state.to_string(),
                "config": serde_json::to_value(cfg).unwrap_or_default(),
            })
        })
    }

    pub async fn get_run_report(&self) -> Option<serde_json::Value> {
        let inner = self.inner.lock().await;
        inner.last_report.as_ref().map(|rpt| {
            serde_json::to_value(rpt).unwrap_or_default()
        })
    }

    pub async fn cleanup_channels(&self) -> serde_json::Value {
        let host = self.startup_cfg.broker_host();
        let port = self.startup_cfg.broker_port();

        let client = match kubemq::KubemqClient::builder()
            .host(&host)
            .port(port)
            .client_id("burnin-cleanup")
            .build()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return serde_json::json!({
                    "deleted_channels": [],
                    "failed_channels": [],
                    "message": format!("Could not connect to broker: {}", e),
                });
            }
        };

        let prefix = "rust_burnin_";
        let channel_types = [
            kubemq::channel_type::EVENTS,
            kubemq::channel_type::EVENTS_STORE,
            kubemq::channel_type::QUEUES,
            kubemq::channel_type::COMMANDS,
            kubemq::channel_type::QUERIES,
        ];

        let mut deleted = Vec::new();
        let mut failed = Vec::new();

        for ct in &channel_types {
            if let Ok(channels) = client.list_channels(ct, prefix).await {
                for ch in channels {
                    if client.delete_channel(&ch.name, ct).await.is_ok() {
                        deleted.push(ch.name);
                    } else {
                        failed.push(ch.name);
                    }
                }
            }
        }

        let _ = client.close().await;

        serde_json::json!({
            "deleted_channels": deleted,
            "failed_channels": failed,
            "message": format!("cleaned {} channels", deleted.len()),
        })
    }

    pub async fn metrics_prometheus(&self) -> String {
        let inner = self.inner.lock().await;
        let mut out = String::new();

        for (pname, pg) in &inner.pattern_groups {
            let sent = pg.total_sent();
            let recv = pg.total_received();
            let errs = pg.total_errors();

            out.push_str(&format!(
                "burnin_messages_sent_total{{sdk=\"rust\",pattern=\"{}\"}} {}\n",
                pname, sent
            ));
            out.push_str(&format!(
                "burnin_messages_received_total{{sdk=\"rust\",pattern=\"{}\"}} {}\n",
                pname, recv
            ));
            out.push_str(&format!(
                "burnin_errors_total{{sdk=\"rust\",pattern=\"{}\"}} {}\n",
                pname, errs
            ));

            let p99 = pg.lat_accum.percentile_ms(99.0);
            out.push_str(&format!(
                "burnin_message_latency_seconds{{sdk=\"rust\",pattern=\"{}\",quantile=\"0.99\"}} {:.6}\n",
                pname, p99 / 1000.0
            ));
        }

        out.push_str(&format!(
            "burnin_uptime_seconds{{sdk=\"rust\"}} {:.1}\n",
            self.boot_time.elapsed().as_secs_f64()
        ));

        out
    }

    fn sdk_version(&self) -> String {
        if self.startup_cfg.output.sdk_version.is_empty() {
            kubemq::VERSION.to_string()
        } else {
            self.startup_cfg.output.sdk_version.clone()
        }
    }
}

async fn run_lifecycle(
    engine: Arc<Mutex<EngineInner>>,
    cfg: BurnInConfig,
    cancel: CancellationToken,
) {
    let result = run_lifecycle_inner(&engine, &cfg, &cancel).await;

    let mut inner = engine.lock().await;
    match result {
        Ok(()) => {
            if inner.state == RunState::Stopping || inner.state == RunState::Running {
                inner.state = RunState::Stopped;
            }
        }
        Err(e) => {
            tracing::error!("Run lifecycle error: {}", e);
            if inner.state != RunState::Stopped {
                inner.state = RunState::Error;
                inner.run_error = e.to_string();
            }
            if inner.last_report.is_none() {
                inner.last_report = Some(Summary::error_report(&cfg, &e.to_string()));
            }
        }
    }
    inner.run_ended_at = Some(Utc::now());
    if let Some(tx) = &inner.run_done_tx {
        let _ = tx.send(true);
    }
}

async fn run_lifecycle_inner(
    engine: &Arc<Mutex<EngineInner>>,
    cfg: &BurnInConfig,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let host = cfg.broker_host();
    let port = cfg.broker_port();

    tracing::info!("T0: Connecting to broker {}:{}", host, port);

    let client = kubemq::KubemqClient::builder()
        .host(&host)
        .port(port)
        .client_id(format!("{}-{}", cfg.broker.client_id_prefix, cfg.run_id))
        .build()
        .await?;

    let server_info = client.ping().await?;
    tracing::info!(
        "Connected to broker: host={}, version={}",
        server_info.host,
        server_info.version
    );

    // T0: Build PatternGroups, start consumers
    let mut pattern_groups = HashMap::new();
    for &pname in config::ALL_PATTERN_NAMES {
        if !cfg.pattern_enabled(pname) {
            continue;
        }
        let channels = cfg.pattern_channels(pname);
        let rate = cfg.pattern_rate(pname);
        let lat_accum = Arc::new(LatencyAccumulator::new());

        let mut workers = Vec::new();
        for ch_idx in 1..=channels {
            let channel_name = config::channel_name_for_pattern(&cfg.run_id, pname, ch_idx as usize);

            let cw = create_channel_workers(
                pname,
                &channel_name,
                ch_idx as usize,
                cfg,
                &client,
                lat_accum.clone(),
            )
            .await?;
            workers.push(cw);
        }

        tracing::info!(
            "Pattern {} ready: {} channels, rate={}/s",
            pname,
            channels,
            rate
        );

        pattern_groups.insert(
            pname.to_string(),
            PatternGroup {
                pattern: pname.to_string(),
                rate,
                channels_count: channels,
                workers,
                lat_accum,
            },
        );
    }

    // Warmup for pub/sub patterns (REQ-18)
    let has_pubsub = pattern_groups.keys().any(|p| p == "events" || p == "events_store");
    if has_pubsub {
        tracing::info!("Warmup: sending warmup messages for pub/sub patterns");
        for (pname, pg) in &pattern_groups {
            match pname.as_str() {
                "events" => {
                    for cw in &pg.workers {
                        let body = payload::generate(cfg.message.size_bytes, u64::MAX);
                        let event = kubemq::EventBuilder::new()
                            .channel(&cw.channel_name)
                            .body(body)
                            .build();
                        let _ = client.send_event(event).await;
                    }
                }
                "events_store" => {
                    for cw in &pg.workers {
                        let body = payload::generate(cfg.message.size_bytes, u64::MAX);
                        let event = kubemq::EventStoreBuilder::new()
                            .channel(&cw.channel_name)
                            .body(body)
                            .build();
                        let _ = client.send_event_store(event).await;
                    }
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        for (pname, pg) in &pattern_groups {
            if pname == "events" || pname == "events_store" {
                for cw in &pg.workers {
                    for recv in &cw.consumer_received {
                        recv.store(0, Ordering::SeqCst);
                    }
                    for err in &cw.consumer_errors {
                        err.store(0, Ordering::SeqCst);
                    }
                }
                pg.lat_accum.reset();
                tracing::info!("Warmup: {} counters reset", pname);
            }
        }
    }

    // Store pattern groups
    {
        let mut inner = engine.lock().await;
        if inner.state == RunState::Stopping {
            tracing::info!("Run cancelled during startup");
            let _ = client.close().await;
            inner.state = RunState::Stopped;
            return Ok(());
        }
        inner.pattern_groups = pattern_groups;
    }

    tracing::info!("T0: All consumers started");

    // T1: Check for cancellation during startup
    {
        let mut inner = engine.lock().await;
        if inner.state == RunState::Stopping {
            shutdown_workers(&mut inner, cfg, &client).await;
            return Ok(());
        }
        inner.state = RunState::Running;
        inner.producers_started_at = Some(Instant::now());
    }

    // T2: Start all producers
    {
        let mut inner = engine.lock().await;
        for (_pname, pg) in &mut inner.pattern_groups {
            for cw in &mut pg.workers {
                start_producers(cw, &client, cfg).await;
            }
        }
    }

    tracing::info!(
        "T2: All producers started, running for {}",
        cfg.duration
    );

    // Periodic status reporter (REQ-20)
    let status_cancel = cancel.clone();
    let status_engine = engine.clone();
    let status_started = Instant::now();
    let status_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = status_cancel.cancelled() => break,
                _ = interval.tick() => {
                    let elapsed = status_started.elapsed();
                    let inner = status_engine.lock().await;
                    let mut total_sent: u64 = 0;
                    let mut total_recv: u64 = 0;
                    let mut total_errors: u64 = 0;
                    let mut pattern_info = Vec::new();
                    for (pname, pg) in &inner.pattern_groups {
                        let sent = pg.total_sent();
                        let recv = pg.total_received();
                        let errors = pg.total_errors();
                        total_sent += sent;
                        total_recv += recv;
                        total_errors += errors;
                        pattern_info.push(format!("{}(s:{}/r:{}/e:{})", pname, sent, recv, errors));
                    }
                    drop(inner);
                    pattern_info.sort();
                    tracing::info!(
                        "STATUS [{:.0}s]: sent={} recv={} errors={} | {}",
                        elapsed.as_secs_f64(),
                        total_sent, total_recv, total_errors,
                        pattern_info.join(" ")
                    );
                }
            }
        }
    });

    // Wait for duration or cancellation
    let duration = config::parse_duration(&cfg.duration);
    if !duration.is_zero() {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("Run cancelled");
            }
            _ = tokio::time::sleep(duration) => {
                tracing::info!("Duration elapsed");
            }
        }
    } else {
        cancel.cancelled().await;
        tracing::info!("Run cancelled (no duration set)");
    }

    status_handle.abort();

    // T3-T4: Shutdown
    {
        let mut inner = engine.lock().await;
        if inner.state == RunState::Running {
            inner.state = RunState::Stopping;
        }
        shutdown_workers(&mut inner, cfg, &client).await;
    }

    let _ = client.close().await;
    tracing::info!("Run complete");
    Ok(())
}

async fn shutdown_workers(
    inner: &mut EngineInner,
    cfg: &BurnInConfig,
    client: &kubemq::KubemqClient,
) {
    // T3: Capture snapshot, stop producers
    let mut snapshots = HashMap::new();
    for (pname, pg) in &inner.pattern_groups {
        snapshots.insert(pname.clone(), pg.snapshot());
        pg.stop_producers();
        tracing::info!("Producers stopped for pattern {}", pname);
    }
    inner.producer_stop_snapshot = Some(snapshots);
    inner.producers_stopped_at = Some(Instant::now());

    // Drain
    let drain = Duration::from_secs(cfg.shutdown.drain_timeout_seconds);
    tracing::info!("Draining for {:?}", drain);
    tokio::time::sleep(drain).await;

    // Stop consumers
    for (pname, pg) in &inner.pattern_groups {
        pg.stop_consumers();
        tracing::info!("Consumers stopped for pattern {}", pname);
    }

    // T4: Generate report
    let summary = build_summary(inner, cfg);
    let verdict = report::generate_verdict(&summary, cfg);
    let mut summary = summary;
    summary.verdict = verdict;

    report::print_console(&summary);

    inner.last_report = Some(summary.clone());
    inner.state = RunState::Stopped;

    // Write report file
    if !cfg.output.report_file.is_empty() {
        if let Ok(json) = serde_json::to_string_pretty(&summary) {
            let _ = tokio::fs::write(&cfg.output.report_file, json).await;
            tracing::info!("Report written to {}", cfg.output.report_file);
        }
    }

    // Cleanup channels
    if cfg.shutdown.cleanup_channels {
        cleanup_run_channels(inner, client).await;
    }
}

async fn cleanup_run_channels(inner: &EngineInner, client: &kubemq::KubemqClient) {
    let mut count = 0;
    for (pname, pg) in &inner.pattern_groups {
        let ct = config::channel_type_for_pattern(pname);
        for cw in &pg.workers {
            if client.delete_channel(&cw.channel_name, ct).await.is_ok() {
                count += 1;
            }
        }
    }
    if count > 0 {
        tracing::info!("Cleaned {} channels", count);
    }
}

fn build_summary(inner: &EngineInner, cfg: &BurnInConfig) -> Summary {
    let started = inner.run_started_at.unwrap_or_else(Utc::now);
    let ended = Utc::now();

    let prod_start = inner.producers_started_at.unwrap_or_else(Instant::now);
    let prod_stop = inner.producers_stopped_at.unwrap_or_else(Instant::now);
    let elapsed = prod_stop.duration_since(prod_start).as_secs_f64();

    let mut patterns = HashMap::new();

    for (pname, pg) in &inner.pattern_groups {
        let snap = inner
            .producer_stop_snapshot
            .as_ref()
            .and_then(|s| s.get(pname));

        let (sent, received, errors) = if let Some(s) = snap {
            (s.sent, s.received, s.errors)
        } else {
            (pg.total_sent(), pg.total_received(), pg.total_errors())
        };

        let loss_pct = if sent > 0 && !config::is_rpc_pattern(pname) {
            let lost = sent.saturating_sub(received);
            (lost as f64 / sent as f64) * 100.0
        } else {
            0.0
        };

        let avg_rate = if elapsed > 0.0 { sent as f64 / elapsed } else { 0.0 };
        let total_target = pg.rate as u32 * pg.channels_count;

        let p50 = pg.lat_accum.percentile_ms(50.0);
        let p95 = pg.lat_accum.percentile_ms(95.0);
        let p99 = pg.lat_accum.percentile_ms(99.0);
        let p999 = pg.lat_accum.percentile_ms(99.9);

        let is_rpc = config::is_rpc_pattern(pname);
        let rpc_success = if is_rpc { Some(snap.map(|s| s.rpc_success).unwrap_or_else(|| pg.total_rpc_success())) } else { None };
        let rpc_timeout = if is_rpc { Some(snap.map(|s| s.rpc_timeout).unwrap_or_else(|| pg.total_rpc_timeout())) } else { None };

        patterns.insert(
            pname.clone(),
            report::PatternStats {
                enabled: true,
                status: "stopped".to_string(),
                sent,
                received,
                lost: if is_rpc { 0 } else { sent.saturating_sub(received) },
                duplicated: 0,
                corrupted: 0,
                out_of_order: 0,
                loss_pct,
                errors,
                reconnections: 0,
                downtime_seconds: 0.0,
                latency_p50_ms: p50,
                latency_p95_ms: p95,
                latency_p99_ms: p99,
                latency_p999_ms: p999,
                avg_rate,
                peak_rate: 0.0,
                target_rate: total_target as i32,
                bytes_sent: 0,
                bytes_received: 0,
                channels: pg.channels_count as i32,
                responses_success: rpc_success,
                responses_timeout: rpc_timeout,
                responses_error: if is_rpc { Some(0) } else { None },
            },
        );
    }

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
        started_at: started,
        ended_at: ended,
        duration_seconds: elapsed,
        all_patterns_enabled: cfg.enabled_pattern_count() == 6,
        status: Some("stopped".to_string()),
        patterns,
        resources: report::ResourceStats {
            peak_rss_mb: 0.0,
            baseline_rss_mb: 0.0,
            memory_growth_factor: 1.0,
            peak_workers: 0,
        },
        verdict: report::Verdict::default(),
    }
}

async fn create_channel_workers(
    pattern: &str,
    channel_name: &str,
    channel_index: usize,
    cfg: &BurnInConfig,
    client: &kubemq::KubemqClient,
    lat_accum: Arc<LatencyAccumulator>,
) -> anyhow::Result<ChannelWorkerSet> {
    let tracker = Arc::new(std::sync::Mutex::new(MessageTracker::new(
        cfg.message.reorder_window,
    )));

    let mut cw = ChannelWorkerSet {
        channel_name: channel_name.to_string(),
        channel_index,
        pattern: pattern.to_string(),
        tracker,
        lat_accum,
        producer_sent: Vec::new(),
        producer_errors: Vec::new(),
        consumer_received: Vec::new(),
        consumer_errors: Vec::new(),
        rpc_timeouts: Vec::new(),
        producer_running: Vec::new(),
        consumer_running: Vec::new(),
        producer_handles: Vec::new(),
        consumer_handles: Vec::new(),
        subscriptions: Vec::new(),
    };

    match pattern {
        "events" => {
            let consumer = events::EventsConsumer::new(
                format!("events-c-{}-{}", channel_index, 1),
                channel_name.to_string(),
                if cfg.patterns.events.consumer_group {
                    config::consumer_group_name(&cfg.run_id, pattern, channel_index)
                } else {
                    String::new()
                },
                Some(cw.lat_accum.clone()),
            );
            cw.consumer_received.push(consumer.received.clone());
            cw.consumer_errors.push(consumer.errors.clone());
            cw.consumer_running.push(consumer.running.clone());
            let sub = consumer.run(client.clone()).await?;
            cw.subscriptions.push(sub);
        }
        "events_store" => {
            let consumer = events_store::EventsStoreConsumer::new(
                format!("events_store-c-{}-{}", channel_index, 1),
                channel_name.to_string(),
                if cfg.patterns.events_store.consumer_group {
                    config::consumer_group_name(&cfg.run_id, pattern, channel_index)
                } else {
                    String::new()
                },
                Some(cw.lat_accum.clone()),
            );
            cw.consumer_received.push(consumer.received.clone());
            cw.consumer_errors.push(consumer.errors.clone());
            cw.consumer_running.push(consumer.running.clone());
            let sub = consumer.run(client.clone()).await?;
            cw.subscriptions.push(sub);
        }
        "queue_stream" => {
            let consumer = queue_stream::QueueStreamConsumer::new(
                format!("queue_stream-c-{}-{}", channel_index, 1),
                channel_name.to_string(),
                cfg.queue.poll_max_messages,
                cfg.queue.poll_wait_timeout_seconds,
                cfg.queue.auto_ack,
                Some(cw.lat_accum.clone()),
            );
            cw.consumer_received.push(consumer.received.clone());
            cw.consumer_errors.push(consumer.errors.clone());
            cw.consumer_running.push(consumer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move {
                consumer.run(c).await;
            });
            cw.consumer_handles.push(handle);
        }
        "queue_simple" => {
            let consumer = queue_simple::QueueSimpleConsumer::new(
                format!("queue_simple-c-{}-{}", channel_index, 1),
                channel_name.to_string(),
                cfg.queue.poll_max_messages,
                cfg.queue.poll_wait_timeout_seconds,
                Some(cw.lat_accum.clone()),
            );
            cw.consumer_received.push(consumer.received.clone());
            cw.consumer_errors.push(consumer.errors.clone());
            cw.consumer_running.push(consumer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move {
                consumer.run(c).await;
            });
            cw.consumer_handles.push(handle);
        }
        "commands" => {
            let responder = commands::CommandsResponder::new(
                format!("commands-r-{}-{}", channel_index, 1),
                channel_name.to_string(),
                config::consumer_group_name(&cfg.run_id, pattern, channel_index),
            );
            cw.consumer_received.push(responder.handled.clone());
            cw.consumer_errors.push(responder.errors.clone());
            cw.consumer_running.push(responder.running.clone());
            let sub = responder.run(client.clone()).await?;
            cw.subscriptions.push(sub);
        }
        "queries" => {
            let responder = queries::QueriesResponder::new(
                format!("queries-r-{}-{}", channel_index, 1),
                channel_name.to_string(),
                config::consumer_group_name(&cfg.run_id, pattern, channel_index),
            );
            cw.consumer_received.push(responder.handled.clone());
            cw.consumer_errors.push(responder.errors.clone());
            cw.consumer_running.push(responder.running.clone());
            let sub = responder.run(client.clone()).await?;
            cw.subscriptions.push(sub);
        }
        _ => {}
    }

    tracing::debug!("Consumer ready for {} on {}", pattern, channel_name);
    Ok(cw)
}

async fn start_producers(cw: &mut ChannelWorkerSet, client: &kubemq::KubemqClient, cfg: &BurnInConfig) {
    let pattern = cw.pattern.as_str();
    let channel_name = cw.channel_name.clone();
    let ch_idx = cw.channel_index;
    let msg_size = cfg.message.size_bytes;

    match pattern {
        "events" => {
            let producer = events::EventsProducer::new(
                format!("events-p-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.events.rate,
                msg_size,
            );
            cw.producer_sent.push(producer.sent.clone());
            cw.producer_errors.push(producer.errors.clone());
            cw.producer_running.push(producer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { producer.run(c).await; });
            cw.producer_handles.push(handle);
        }
        "events_store" => {
            let producer = events_store::EventsStoreProducer::new(
                format!("events_store-p-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.events_store.rate,
                msg_size,
            );
            cw.producer_sent.push(producer.sent.clone());
            cw.producer_errors.push(producer.errors.clone());
            cw.producer_running.push(producer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { producer.run(c).await; });
            cw.producer_handles.push(handle);
        }
        "queue_stream" => {
            let producer = queue_stream::QueueStreamProducer::new(
                format!("queue_stream-p-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.queue_stream.rate,
                msg_size,
            );
            cw.producer_sent.push(producer.sent.clone());
            cw.producer_errors.push(producer.errors.clone());
            cw.producer_running.push(producer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { producer.run(c).await; });
            cw.producer_handles.push(handle);
        }
        "queue_simple" => {
            let producer = queue_simple::QueueSimpleProducer::new(
                format!("queue_simple-p-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.queue_simple.rate,
                msg_size,
            );
            cw.producer_sent.push(producer.sent.clone());
            cw.producer_errors.push(producer.errors.clone());
            cw.producer_running.push(producer.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { producer.run(c).await; });
            cw.producer_handles.push(handle);
        }
        "commands" => {
            let sender = commands::CommandsSender::new(
                format!("commands-s-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.commands.rate,
                cfg.rpc.timeout_ms,
                msg_size,
                Some(cw.lat_accum.clone()),
            );
            cw.producer_sent.push(sender.sent.clone());
            cw.producer_errors.push(sender.errors.clone());
            cw.rpc_timeouts.push(sender.timeouts.clone());
            cw.producer_running.push(sender.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { sender.run(c).await; });
            cw.producer_handles.push(handle);
        }
        "queries" => {
            let sender = queries::QueriesSender::new(
                format!("queries-s-{}-{}", ch_idx, 1),
                channel_name,
                cfg.patterns.queries.rate,
                cfg.rpc.timeout_ms,
                msg_size,
                Some(cw.lat_accum.clone()),
            );
            cw.producer_sent.push(sender.sent.clone());
            cw.producer_errors.push(sender.errors.clone());
            cw.producer_running.push(sender.running.clone());
            let c = client.clone();
            let handle = tokio::spawn(async move { sender.run(c).await; });
            cw.producer_handles.push(handle);
        }
        _ => {}
    }
}

fn generate_run_id() -> String {
    let mut rng = rand::thread_rng();
    let val: u32 = rng.gen();
    format!("{:08x}", val)
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
