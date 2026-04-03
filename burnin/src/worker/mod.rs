//! Worker trait and common types.

pub mod commands;
pub mod events;
pub mod events_store;
pub mod queries;
pub mod queue_simple;
pub mod queue_stream;

use async_trait::async_trait;
use kubemq::KubemqClient;

/// Worker trait for all burn-in patterns.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Unique worker identifier.
    fn id(&self) -> &str;

    /// Pattern name (events, events_store, commands, queries, queue_stream, queue_simple).
    fn pattern(&self) -> &str;

    /// Start the worker.
    async fn start(&self, client: KubemqClient) -> anyhow::Result<()>;

    /// Stop the worker.
    async fn stop(&self) -> anyhow::Result<()>;

    /// Pause the worker (stop producing/consuming but keep connection).
    async fn pause(&self);

    /// Resume the worker.
    async fn resume(&self);

    /// Get current worker status.
    fn status(&self) -> &str;
}
