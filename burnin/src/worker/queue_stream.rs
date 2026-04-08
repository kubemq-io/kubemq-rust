//! Queue Stream pattern worker -- upstream batch send, downstream poll with ack.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kubemq::{KubemqClient, PollRequest, QueueMessageBuilder};

use super::Worker;
use crate::metrics::LatencyAccumulator;
use crate::payload;

/// Queue Stream producer (upstream).
pub struct QueueStreamProducer {
    pub id: String,
    pub channel: String,
    pub rate: u32,
    pub message_size: usize,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) sent: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
}

impl QueueStreamProducer {
    pub fn new(id: String, channel: String, rate: u32, message_size: usize) -> Self {
        Self {
            id,
            channel,
            rate,
            message_size,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            sent: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn run(&self, client: KubemqClient) {
        // REQ-M51: Guard against division by zero when rate is 0
        if self.rate == 0 {
            tracing::warn!("QueueStreamProducer {}: rate is 0, not producing", self.id);
            return;
        }
        self.running.store(true, Ordering::SeqCst);

        let upstream = match client.queue_upstream().await {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("Failed to create upstream: {}", e);
                return;
            }
        };

        let mut seq = 0u64;
        let batch_size = std::cmp::max(1, (self.rate / 10).min(100)) as usize;
        let batch_interval = Duration::from_secs_f64(batch_size as f64 / self.rate as f64);
        let mut interval = tokio::time::interval(batch_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        while self.running.load(Ordering::SeqCst) {
            if self.paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            interval.tick().await;

            // Guard: don't send if stopped while waiting for the tick.
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            let mut batch = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let body = payload::generate(self.message_size, seq);
                let msg = QueueMessageBuilder::new()
                    .channel(&self.channel)
                    .body(body)
                    .build();
                batch.push(msg);
                seq += 1;
            }

            match upstream.send(&format!("batch-{}", seq), batch).await {
                Ok(()) => {
                    self.sent.fetch_add(batch_size as u64, Ordering::SeqCst);
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::SeqCst);
                    tracing::warn!("Queue upstream error: {}", e);
                }
            }
        }

        upstream.close();
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueueStreamProducer.
#[async_trait]
impl Worker for QueueStreamProducer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queue_stream"
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

/// Queue Stream consumer (downstream).
pub struct QueueStreamConsumer {
    pub id: String,
    pub channel: String,
    pub poll_max: i32,
    pub poll_timeout: i32,
    pub auto_ack: bool,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) received: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
    pub(crate) lat_accum: Option<Arc<LatencyAccumulator>>,
}

impl QueueStreamConsumer {
    pub fn new(
        id: String,
        channel: String,
        poll_max: i32,
        poll_timeout: i32,
        auto_ack: bool,
        lat_accum: Option<Arc<LatencyAccumulator>>,
    ) -> Self {
        Self {
            id,
            channel,
            poll_max,
            poll_timeout,
            auto_ack,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            received: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            lat_accum,
        }
    }

    pub async fn run(&self, client: KubemqClient) {
        self.running.store(true, Ordering::SeqCst);

        let mut receiver = match client.new_queue_downstream_receiver().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to create downstream receiver: {}", e);
                return;
            }
        };

        while self.running.load(Ordering::SeqCst) {
            if self.paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let poll = PollRequest {
                channel: self.channel.clone(),
                max_items: self.poll_max,
                wait_timeout_seconds: self.poll_timeout,
                auto_ack: self.auto_ack,
            };

            match receiver.poll(poll).await {
                Ok(response) => {
                    let count = response.messages.len() as u64;
                    self.received.fetch_add(count, Ordering::SeqCst);

                    if let Some(ref la) = self.lat_accum {
                        for msg in &response.messages {
                            if let Some(ms) = payload::latency_ms(&msg.message.body) {
                                la.record_ms(ms);
                            }
                        }
                    }

                    if !self.auto_ack && !response.messages.is_empty() {
                        if let Err(e) = response.ack_all().await {
                            tracing::warn!("Ack all error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::SeqCst);
                    tracing::debug!("Queue poll: {}", e);
                }
            }
        }

        let _ = receiver.close().await;
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueueStreamConsumer.
#[async_trait]
impl Worker for QueueStreamConsumer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queue_stream"
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
