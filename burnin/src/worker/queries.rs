//! Queries pattern worker -- send query, subscribe as responder, respond with body.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kubemq::{KubemqClient, QueryBuilder, QueryReplyBuilder};

use super::Worker;
use crate::metrics::LatencyAccumulator;
use crate::payload;

/// Queries sender worker.
pub struct QueriesSender {
    pub id: String,
    pub channel: String,
    pub rate: u32,
    pub timeout_ms: u64,
    pub message_size: usize,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) sent: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
    pub(crate) cache_hits: Arc<AtomicU64>,
    pub(crate) lat_accum: Option<Arc<LatencyAccumulator>>,
}

impl QueriesSender {
    pub fn new(
        id: String,
        channel: String,
        rate: u32,
        timeout_ms: u64,
        message_size: usize,
        lat_accum: Option<Arc<LatencyAccumulator>>,
    ) -> Self {
        Self {
            id,
            channel,
            rate,
            timeout_ms,
            message_size,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            sent: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            lat_accum,
        }
    }

    pub async fn run(&self, client: KubemqClient) {
        if self.rate == 0 {
            tracing::warn!("QueriesSender {}: rate is 0, not sending", self.id);
            return;
        }
        self.running.store(true, Ordering::SeqCst);

        let mut seq = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / self.rate as f64));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        while self.running.load(Ordering::SeqCst) {
            if self.paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            interval.tick().await;

            let body = payload::generate(self.message_size, seq);
            let query = QueryBuilder::new()
                .channel(&self.channel)
                .body(body)
                .timeout(Duration::from_millis(self.timeout_ms))
                .build();

            let rpc_start = Instant::now();
            match client.send_query(query).await {
                Ok(resp) => {
                    if resp.executed {
                        self.sent.fetch_add(1, Ordering::SeqCst);
                        if let Some(ref la) = self.lat_accum {
                            la.record(rpc_start.elapsed());
                        }
                        if resp.cache_hit {
                            self.cache_hits.fetch_add(1, Ordering::SeqCst);
                        }
                    } else {
                        self.errors.fetch_add(1, Ordering::SeqCst);
                    }
                    seq += 1;
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::SeqCst);
                    tracing::debug!("Query send error: {}", e);
                }
            }
        }
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    pub fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueriesSender.
#[async_trait]
impl Worker for QueriesSender {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queries"
    }

    async fn start(&self, client: KubemqClient) -> anyhow::Result<()> {
        self.run(client).await;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    async fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    fn status(&self) -> &str {
        if !self.running.load(Ordering::SeqCst) {
            "stopped"
        } else if self.paused.load(Ordering::SeqCst) {
            "paused"
        } else {
            "running"
        }
    }
}

/// Queries responder worker.
pub struct QueriesResponder {
    pub id: String,
    pub channel: String,
    pub group: String,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) handled: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
}

impl QueriesResponder {
    pub fn new(id: String, channel: String, group: String) -> Self {
        Self {
            id,
            channel,
            group,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            handled: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn run(&self, client: KubemqClient) -> anyhow::Result<kubemq::Subscription> {
        self.running.store(true, Ordering::SeqCst);
        let handled = self.handled.clone();
        let errors = self.errors.clone();
        let rc = client.clone();

        let sub = client
            .subscribe_to_queries(
                &self.channel,
                &self.group,
                move |query| {
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"burnin-response".to_vec())
                        .build();
                    let c = rc.clone();
                    let h = handled.clone();
                    let e = errors.clone();
                    Box::pin(async move {
                        match c.send_query_response(reply).await {
                            Ok(()) => {
                                h.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(_) => {
                                e.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                },
                None,
            )
            .await?;

        Ok(sub)
    }

    pub fn handled(&self) -> u64 {
        self.handled.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueriesResponder.
#[async_trait]
impl Worker for QueriesResponder {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queries"
    }

    async fn start(&self, client: KubemqClient) -> anyhow::Result<()> {
        let _sub = self.run(client).await?;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    async fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    fn status(&self) -> &str {
        if !self.running.load(Ordering::SeqCst) {
            "stopped"
        } else if self.paused.load(Ordering::SeqCst) {
            "paused"
        } else {
            "running"
        }
    }
}
