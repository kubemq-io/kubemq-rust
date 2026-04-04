//! # KubeMQ Rust SDK
//!
//! Client library for the [KubeMQ](https://kubemq.io) message broker. Supports
//! Events, Events Store, Commands, Queries, and Queues messaging patterns over
//! gRPC with automatic reconnection, TLS/mTLS, and structured error handling.
//!
//! ## Features
//!
//! - **Events (Pub/Sub)** — fire-and-forget fan-out messaging via [`Event`]
//! - **Events Store** — persistent events with replay via [`EventStore`] and [`EventsStoreSubscription`]
//! - **Queues** — transactional message queues with ack/nack/requeue via [`QueueMessage`]
//! - **Commands** — request-response with timeout via [`Command`] and [`CommandResponse`]
//! - **Queries** — request-response with caching via [`Query`] and [`QueryResponse`]
//! - **Auto-reconnection** — configurable retry with exponential backoff via [`RetryPolicy`]
//! - **TLS / mTLS** — secure connections via [`TlsConfig`]
//! - **Auth tokens** — static or dynamic authentication via [`CredentialProvider`]
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use kubemq::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> kubemq::Result<()> {
//!     let client = KubemqClient::builder()
//!         .host("localhost")
//!         .port(50000)
//!         .build()
//!         .await?;
//!
//!     let info = client.ping().await?;
//!     println!("Connected to {} v{}", info.host, info.version);
//!
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Error Handling
//!
//! All fallible operations return [`Result<T>`](crate::Result), which is an alias for
//! `std::result::Result<T, KubemqError>`. Use [`KubemqError::is_retryable()`] to
//! determine if an operation can be retried, and [`KubemqError::code()`] for
//! machine-readable classification.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::result_large_err)]

/// Commonly used types and traits re-exported for convenience.
///
/// ```rust
/// use kubemq::prelude::*;
/// ```
pub mod prelude;

mod auth;
mod channels;
mod client;
mod commands;
mod common;
mod config;
mod connection;
mod env;
mod error;
mod events;
mod events_store;
mod queries;
mod queue_stream;
mod queues;
mod retry;
mod server_info;
mod subscription;
mod tls;
mod validate;
mod version;

#[doc(hidden)]
pub mod proto;
pub(crate) mod transport;

// Re-exports
pub use auth::{CredentialProvider, StaticTokenProvider};
pub use channels::{channel_type, ChannelInfo, ChannelStats};
pub use client::KubemqClient;
pub use commands::{
    Command, CommandBuilder, CommandReceive, CommandReply, CommandReplyBuilder, CommandResponse,
};
pub use config::{AsyncCallback, AsyncNotifyCallback, ClientConfig, ClientConfigBuilder};
pub use connection::ConnectionState;
pub use error::{ErrorCode, KubemqError};
pub use events::{Event, EventBuilder, EventReceive, EventStreamHandle, EventStreamResult};
pub use events_store::{
    EventStore, EventStoreBuilder, EventStoreReceive, EventStoreResult, EventStoreStreamHandle,
    EventsStoreSubscription,
};
pub use queries::{
    Query, QueryBuilder, QueryReceive, QueryReply, QueryReplyBuilder, QueryResponse,
};
pub use queue_stream::{
    queue_downstream_type, PollRequest, PollResponse, QueueDownstreamMessage,
    QueueDownstreamReceiver, QueueDownstreamType, QueueUpstreamHandle, QueueUpstreamResult,
};
pub use queues::{
    AckAllQueueMessagesRequest, AckAllQueueMessagesResponse, QueueMessage, QueueMessageAttributes,
    QueueMessageBuilder, QueuePolicy, QueueSendResult,
};
pub use retry::{JitterMode, RetryPolicy};
pub use server_info::ServerInfo;
pub use subscription::Subscription;
pub use tls::TlsConfig;
pub use version::VERSION;

/// Convenience alias for `std::result::Result<T, KubemqError>`.
pub type Result<T> = std::result::Result<T, KubemqError>;
