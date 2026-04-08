//! Queue Simple pattern worker -- send/receive via simple API.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kubemq::{KubemqClient, QueueMessageBuilder};

use super::Worker;
use crate::metrics::LatencyAccumulator;
use crate::payload;

/// Queue Simple producer.
pub struct QueueSimpleProducer {
    pub id: String,
    pub channel: String,
    pub rate: u32,
    pub message_size: usize,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) sent: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
}

impl QueueSimpleProducer {
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
        if self.rate == 0 {
            tracing::warn!("QueueSimpleProducer {}: rate is 0, not producing", self.id);
            return;
        }
        self.running.store(true, Ordering::SeqCst);

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

            let count = batch.len() as u64;
            match client.send_queue_messages(batch).await {
                Ok(results) => {
                    let mut ok_count = 0u64;
                    let mut err_count = 0u64;
                    for r in &results {
                        if !r.is_error {
                            ok_count += 1;
                        } else {
                            err_count += 1;
                        }
                    }
                    self.sent.fetch_add(ok_count, Ordering::SeqCst);
                    self.errors.fetch_add(err_count, Ordering::SeqCst);
                }
                Err(e) => {
                    self.errors.fetch_add(count, Ordering::SeqCst);
                    tracing::warn!("Queue simple batch send error: {}", e);
                }
            }
        }
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueueSimpleProducer.
#[async_trait]
impl Worker for QueueSimpleProducer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queue_simple"
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

/// Queue Simple consumer.
pub struct QueueSimpleConsumer {
    pub id: String,
    pub channel: String,
    pub max_messages: i32,
    pub wait_time: i32,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) received: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
    pub(crate) lat_accum: Option<Arc<LatencyAccumulator>>,
}

impl QueueSimpleConsumer {
    pub fn new(id: String, channel: String, max_messages: i32, wait_time: i32, lat_accum: Option<Arc<LatencyAccumulator>>) -> Self {
        Self {
            id,
            channel,
            max_messages,
            wait_time,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            received: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            lat_accum,
        }
    }

    pub async fn run(&self, client: KubemqClient) {
        self.running.store(true, Ordering::SeqCst);

        while self.running.load(Ordering::SeqCst) {
            if self.paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            match client
                .receive_queue_messages(&self.channel, self.max_messages, self.wait_time, false)
                .await
            {
                Ok(messages) => {
                    self.received
                        .fetch_add(messages.len() as u64, Ordering::SeqCst);
                    if let Some(ref la) = self.lat_accum {
                        for msg in &messages {
                            if let Some(ms) = payload::latency_ms(&msg.body) {
                                la.record_ms(ms);
                            }
                        }
                    }
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::SeqCst);
                    tracing::debug!("Queue simple receive: {}", e);
                }
            }
        }
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for QueueSimpleConsumer.
#[async_trait]
impl Worker for QueueSimpleConsumer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "queue_simple"
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
