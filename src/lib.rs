//! # KubeMQ Rust SDK
//!
//! Client library for the KubeMQ message broker. Supports Events, Events Store,
//! Commands, Queries, and Queues messaging patterns.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use kubemq::prelude::*;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> kubemq::Result<()> {
//!     let client = KubemqClient::builder()
//!         .host("localhost")
//!         .port(50000)
//!         .build()
//!         .await?;
//!
//!     client.ping().await?;
//!     client.close().await?;
//!     Ok(())
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::result_large_err)]

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
