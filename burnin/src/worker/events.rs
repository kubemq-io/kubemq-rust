//! Events pattern worker -- fire-and-forget publish/subscribe.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kubemq::{EventBuilder, KubemqClient};

use super::Worker;
use crate::metrics::LatencyAccumulator;
use crate::payload;

/// Events producer worker.
pub struct EventsProducer {
    pub id: String,
    pub channel: String,
    pub rate: u32,
    pub message_size: usize,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) sent: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
}

impl EventsProducer {
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
            tracing::warn!("EventsProducer {}: rate is 0, not producing", self.id);
            return;
        }
        self.running.store(true, Ordering::SeqCst);

        // Open a bidirectional stream for high-throughput publishing.
        // handle.send() enqueues to a 256-buffer async channel -- near-instant.
        let mut handle = match client.send_event_stream().await {
            Ok(h) => h,
            Err(e) => {
                tracing::error!("EventsProducer {}: failed to open stream: {}", self.id, e);
                return;
            }
        };

        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / self.rate as f64));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        while self.running.load(Ordering::SeqCst) {
            if self.paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Drain any pending errors (non-blocking)
            while let Ok(err) = handle.errors().try_recv() {
                self.errors.fetch_add(1, Ordering::SeqCst);
                tracing::warn!("EventsProducer {}: stream error: {}", self.id, err);
            }

            interval.tick().await;

            let body = payload::generate(self.message_size, self.sent.load(Ordering::SeqCst));
            let event = EventBuilder::new()
                .channel(&self.channel)
                .body(body)
                .build();

            match handle.send(event).await {
                Ok(()) => {
                    self.sent.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    self.errors.fetch_add(1, Ordering::SeqCst);
                    tracing::warn!("EventsProducer {}: stream send error: {}", self.id, e);
                    break; // Stream is broken, exit
                }
            }
        }

        handle.close();
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for EventsProducer.
#[async_trait]
impl Worker for EventsProducer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "events"
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

/// Events consumer worker.
pub struct EventsConsumer {
    pub id: String,
    pub channel: String,
    pub group: String,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) received: Arc<AtomicU64>,
    pub(crate) errors: Arc<AtomicU64>,
    pub(crate) lat_accum: Option<Arc<LatencyAccumulator>>,
}

impl EventsConsumer {
    pub fn new(id: String, channel: String, group: String, lat_accum: Option<Arc<LatencyAccumulator>>) -> Self {
        Self {
            id,
            channel,
            group,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            received: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            lat_accum,
        }
    }

    pub async fn run(&self, client: KubemqClient) -> anyhow::Result<kubemq::Subscription> {
        self.running.store(true, Ordering::SeqCst);
        let received = self.received.clone();
        let errors = self.errors.clone();
        let lat_accum = self.lat_accum.clone();

        let sub = client
            .subscribe_to_events(
                &self.channel,
                &self.group,
                move |event| {
                    received.fetch_add(1, Ordering::SeqCst);
                    if let Some(ref la) = lat_accum {
                        if let Some(ms) = payload::latency_ms(&event.body) {
                            la.record_ms(ms);
                        }
                    }
                    Box::pin(async {})
                },
                Some(Box::new(move |_err| {
                    errors.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async {})
                })),
            )
            .await?;

        Ok(sub)
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::SeqCst)
    }
}

/// REQ-H15: Worker trait implementation for EventsConsumer.
#[async_trait]
impl Worker for EventsConsumer {
    fn id(&self) -> &str {
        &self.id
    }

    fn pattern(&self) -> &str {
        "events"
    }

    async fn start(&self, client: KubemqClient) -> anyhow::Result<()> {
        let _sub = self.run(client).await?;
        // Subscription is kept alive; the consumer loop runs until stopped.
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
