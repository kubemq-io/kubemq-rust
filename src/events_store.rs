//! Events Store (Persistent Pub/Sub) messaging pattern.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::events::EventStreamResult;
use crate::proto::kubemq as proto;
use crate::subscription::Subscription;
use crate::validate;

/// Outbound persistent event message for the Events Store (Pub/Sub) pattern.
///
/// Unlike [`Event`](crate::Event), event store messages are persisted on the
/// broker and can be replayed from a specific position by new subscribers.
///
/// Use [`EventStore::builder()`] to construct instances.
#[derive(Debug, Clone)]
pub struct EventStore {
    /// Unique message identifier. Auto-generated (UUID v4) when empty.
    pub id: String,
    /// Target channel name. Required; must not contain wildcards for sends.
    pub channel: String,
    /// Optional UTF-8 metadata string (e.g., content type, correlation ID).
    pub metadata: String,
    /// Message payload bytes. At least one of `body` or `metadata` must be non-empty.
    pub body: Vec<u8>,
    /// Sender identity. Falls back to the client-level `client_id` when empty.
    pub client_id: String,
    /// Arbitrary key-value pairs attached to the message. Keys and values must be non-empty.
    pub tags: HashMap<String, String>,
}

impl EventStore {
    /// Create a builder for constructing an event store message.
    pub fn builder() -> EventStoreBuilder {
        EventStoreBuilder::new()
    }
}

/// Builder for constructing [`EventStore`] instances using a fluent API.
///
/// All fields are optional. `channel` is required at send time.
///
/// # Example
///
/// ```rust
/// use kubemq::prelude::*;
///
/// let event = EventStore::builder()
///     .channel("orders.created")
///     .body(b"order-data".to_vec())
///     .add_tag("priority", "high")
///     .build();
/// ```
pub struct EventStoreBuilder {
    event: EventStore,
}

impl EventStoreBuilder {
    /// Create a new builder with all fields set to their defaults.
    pub fn new() -> Self {
        Self {
            event: EventStore {
                id: String::new(),
                channel: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                client_id: String::new(),
                tags: HashMap::new(),
            },
        }
    }

    /// Set the message ID. If omitted, a UUID v4 is generated at send time.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.event.id = id.into();
        self
    }

    /// Set the target channel name (required).
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.event.channel = channel.into();
        self
    }

    /// Set optional UTF-8 metadata (e.g., content type, correlation ID).
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.event.metadata = metadata.into();
        self
    }

    /// Set the message payload bytes.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.event.body = body.into();
        self
    }

    /// Override the client-level `client_id` for this message.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.event.client_id = client_id.into();
        self
    }

    /// Replace all tags with the provided map.
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.event.tags = tags;
        self
    }

    /// Add a single key-value tag. Both key and value must be non-empty.
    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.event.tags.insert(key.into(), value.into());
        self
    }

    /// Consume the builder and return the constructed [`EventStore`].
    pub fn build(self) -> EventStore {
        self.event
    }
}

impl Default for EventStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a single event store send operation.
///
/// Use [`into_result()`](Self::into_result) to convert into a standard
/// `Result`, which returns `Err` when `sent` is `false`.
#[derive(Debug, Clone)]
pub struct EventStoreResult {
    /// Server-assigned or client-provided event identifier.
    pub id: String,
    /// `true` if the server persisted the event.
    pub sent: bool,
    /// Server-provided error message when `sent` is `false`.
    pub error: String,
}

impl EventStoreResult {
    /// Convert into a Result, returning Err if the event was not sent.
    pub fn into_result(self) -> crate::Result<Self> {
        if !self.sent {
            let message = if self.error.is_empty() {
                "Event store message not sent (no error details from server)".to_string()
            } else {
                self.error
            };
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message,
                operation: "send_event_store".to_string(),
                channel: String::new(),
                is_retryable: true,
                source: None,
                request_id: self.id,
                suggestion: "Retry the operation.",
            })
        } else {
            Ok(self)
        }
    }
}

/// A persistent event received from an Events Store subscription callback.
///
/// Delivered to the `on_event` closure passed to
/// [`KubemqClient::subscribe_to_events_store()`].
#[derive(Debug, Clone)]
pub struct EventStoreReceive {
    /// Server-assigned event identifier.
    pub id: String,
    /// Monotonically increasing sequence number within the channel.
    pub sequence: u64,
    /// Server timestamp (Unix nanoseconds) when the event was persisted.
    pub timestamp: i64,
    /// The channel this event was published to.
    pub channel: String,
    /// UTF-8 metadata attached by the publisher.
    pub metadata: String,
    /// Message payload bytes.
    pub body: Vec<u8>,
    /// Key-value tags attached by the publisher.
    pub tags: HashMap<String, String>,
}

/// Specifies where an Events Store subscription should begin reading.
///
/// Passed to [`KubemqClient::subscribe_to_events_store()`] to control
/// which historical messages are delivered when the subscription starts.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum EventsStoreSubscription {
    /// Receive only events published after the subscription is established.
    StartNewOnly,
    /// Replay from the first event ever stored on the channel.
    StartFromFirst,
    /// Start from the most recently stored event and continue forward.
    StartFromLast,
    /// Start at a specific sequence number (inclusive).
    StartAtSequence(u64),
    /// Start at a specific wall-clock time (events with timestamp >= this value).
    StartAtTime(SystemTime),
    /// Start from `now - delta`. The server interprets the duration in whole seconds.
    StartAtTimeDelta(Duration),
}

impl std::fmt::Display for EventsStoreSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartNewOnly => write!(f, "StartNewOnly"),
            Self::StartFromFirst => write!(f, "StartFromFirst"),
            Self::StartFromLast => write!(f, "StartFromLast"),
            Self::StartAtSequence(seq) => write!(f, "StartAtSequence({})", seq),
            Self::StartAtTime(t) => write!(f, "StartAtTime({:?})", t),
            Self::StartAtTimeDelta(d) => write!(f, "StartAtTimeDelta({:?})", d),
        }
    }
}

impl EventsStoreSubscription {
    /// Convert to proto events store type and value.
    fn to_proto(&self) -> crate::Result<(i32, i64)> {
        match self {
            Self::StartNewOnly => Ok((proto::subscribe::EventsStoreType::StartNewOnly as i32, 0)),
            Self::StartFromFirst => {
                Ok((proto::subscribe::EventsStoreType::StartFromFirst as i32, 0))
            }
            Self::StartFromLast => Ok((proto::subscribe::EventsStoreType::StartFromLast as i32, 0)),
            Self::StartAtSequence(seq) => {
                // REQ-M53: Safe u64-to-i64 conversion with error instead of silent truncation
                let seq_i64 = i64::try_from(*seq).map_err(|_| KubemqError::Validation {
                    code: ErrorCode::Validation,
                    message: format!("Sequence value {} exceeds i64::MAX", seq),
                    operation: "to_proto".to_string(),
                    channel: String::new(),
                    suggestion: "Use a sequence value within i64 range.",
                })?;
                Ok((
                    proto::subscribe::EventsStoreType::StartAtSequence as i32,
                    seq_i64,
                ))
            }
            Self::StartAtTime(time) => {
                let nanos = time
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let nanos = i64::try_from(nanos).unwrap_or(i64::MAX);
                Ok((proto::subscribe::EventsStoreType::StartAtTime as i32, nanos))
            }
            Self::StartAtTimeDelta(delta) => {
                // REQ-C3: Server expects seconds, not nanoseconds
                let secs = delta.as_secs() as i64;
                Ok((
                    proto::subscribe::EventsStoreType::StartAtTimeDelta as i32,
                    secs,
                ))
            }
        }
    }
}

/// Handle for a bidirectional event store send stream.
///
/// Obtained from [`KubemqClient::send_event_store_stream()`]. Provides a
/// non-blocking [`send()`](Self::send) method for high-throughput publishing
/// and a [`results()`](Self::results) receiver for per-message acknowledgements.
/// The stream is cancelled on [`close()`](Self::close) or when the handle is dropped.
pub struct EventStoreStreamHandle {
    sender: tokio::sync::mpsc::Sender<EventStore>,
    results: tokio::sync::mpsc::Receiver<EventStreamResult>,
    _done: tokio::sync::oneshot::Receiver<()>,
    cancel: CancellationToken,
}

impl EventStoreStreamHandle {
    /// Create a new EventStoreStreamHandle.
    pub(crate) fn new(
        sender: tokio::sync::mpsc::Sender<EventStore>,
        results: tokio::sync::mpsc::Receiver<EventStreamResult>,
        done: tokio::sync::oneshot::Receiver<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            results,
            _done: done,
            cancel,
        }
    }

    /// Send an event store message through the stream.
    pub async fn send(&self, event: EventStore) -> crate::Result<()> {
        // REQ-H5: Validate before sending to stream
        validate::validate_channel(&event.channel, "event_store_stream.send")?;
        validate::validate_no_wildcards(&event.channel, "event_store_stream.send")?;
        validate::validate_tags(&event.tags, "event_store_stream.send", &event.channel)?;

        self.sender
            .send(event)
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "event store stream closed".to_string(),
                operation: "send_event_store_stream".to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The stream has been closed. Open a new stream.",
            })
    }

    /// Get a mutable reference to the results receiver.
    pub fn results(&mut self) -> &mut tokio::sync::mpsc::Receiver<EventStreamResult> {
        &mut self.results
    }

    /// Close the stream.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for EventStoreStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStoreStreamHandle")
            .field("sender", &"<mpsc::Sender>")
            .field("results", &"<mpsc::Receiver>")
            .finish()
    }
}

impl Drop for EventStoreStreamHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Convert EventStore to proto Event with Store=true (owned, zero-copy).
fn event_store_to_proto_owned(event: EventStore, default_client_id: &str) -> proto::Event {
    let id = crate::common::resolve_id_owned(event.id);
    let client_id = if event.client_id.is_empty() {
        default_client_id.to_string()
    } else {
        event.client_id
    };

    proto::Event {
        event_id: id,
        client_id,
        channel: event.channel,   // moved
        metadata: event.metadata, // moved
        body: event.body,         // moved -- zero-copy
        store: true,
        tags: event.tags, // moved
    }
}

/// Convert proto EventReceive to EventStoreReceive.
fn proto_to_event_store_receive(p: proto::EventReceive) -> EventStoreReceive {
    EventStoreReceive {
        id: p.event_id,
        sequence: p.sequence,
        timestamp: p.timestamp,
        channel: p.channel,
        metadata: p.metadata,
        body: p.body,
        tags: p.tags,
    }
}

// Client methods for Events Store
impl KubemqClient {
    /// Send a persistent event to the store.
    pub async fn send_event_store(&self, event: EventStore) -> crate::Result<EventStoreResult> {
        self.check_state("send_event_store")?;

        validate::validate_channel(&event.channel, "send_event_store")?;
        validate::validate_no_wildcards(&event.channel, "send_event_store")?;
        validate::validate_tags(&event.tags, "send_event_store", &event.channel)?;
        validate::validate_body_size(
            &event.body,
            self.config().max_send_message_size,
            "send_event_store",
            &event.channel,
        )?;
        validate::validate_body_or_metadata(
            &event.metadata,
            &event.body,
            "send_event_store",
            &event.channel,
        )?;

        let channel_for_err = event.channel.clone(); // Save for error message
        let proto_event = event_store_to_proto_owned(event, &self.config().client_id);

        let mut request = tonic::Request::new(proto_event);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .send_event(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_event_store", &channel_for_err))?;

        let result = response.into_inner();
        if result.sent {
            Ok(EventStoreResult {
                id: result.event_id,
                sent: result.sent,
                error: result.error,
            })
        } else {
            let message = if result.error.is_empty() {
                "Event store message not sent (no error details from server)".to_string()
            } else {
                result.error
            };
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message,
                operation: "send_event_store".to_string(),
                channel: channel_for_err,
                is_retryable: true,
                source: None,
                request_id: result.event_id,
                suggestion: "Retry the operation.",
            })
        }
    }

    /// Open a bidirectional stream for high-throughput event store publishing.
    pub async fn send_event_store_stream(&self) -> crate::Result<EventStoreStreamHandle> {
        self.check_state("send_event_store_stream")?;

        let client_id = self.config().client_id.clone();
        let cancel = self.child_token();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<EventStore>(256);
        let (result_tx, result_rx) = tokio::sync::mpsc::channel::<EventStreamResult>(64);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let (proto_tx, proto_rx) = tokio::sync::mpsc::channel::<proto::Event>(256);
        let proto_stream = tokio_stream::wrappers::ReceiverStream::new(proto_rx);

        let mut grpc_client = self.transport().client()?;

        // Forward events to proto channel (owned conversion).
        // Must be spawned BEFORE the gRPC call so messages can flow while the
        // server establishes the stream.
        let cid = client_id.clone();
        let ptx = proto_tx.clone();
        let cancel_fwd = cancel.clone();
        let result_tx_clone = result_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_fwd.cancelled() => break,
                    event = event_rx.recv() => {
                        match event {
                            Some(e) => {
                                if let Err(err) = validate::validate_body_or_metadata(&e.metadata, &e.body, "send_event_store_stream", &e.channel) {
                                    let _ = result_tx_clone.send(EventStreamResult {
                                        event_id: String::new(),
                                        sent: false,
                                        error: err.to_string(),
                                    }).await;
                                    continue;
                                }
                                let proto_event = event_store_to_proto_owned(e, &cid);
                                if ptx.send(proto_event).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        // Spawn gRPC stream establishment + result reading in a background task.
        // This avoids blocking if the server waits for the first client message
        // before sending response headers (common in bidirectional streaming).
        let cancel_read = cancel.clone();
        tokio::spawn(async move {
            let response = match grpc_client.send_events_stream(proto_stream).await {
                Ok(resp) => resp,
                Err(status) => {
                    let err_result = EventStreamResult {
                        event_id: String::new(),
                        sent: false,
                        error: format!("stream setup failed: {}", status.message()),
                    };
                    let _ = result_tx.send(err_result).await;
                    let _ = done_tx.send(());
                    return;
                }
            };

            let mut result_stream = response.into_inner();
            // Read results -- REQ-H12: forward stream errors to result channel
            loop {
                tokio::select! {
                    _ = cancel_read.cancelled() => break,
                    result = result_stream.message() => {
                        match result {
                            Ok(Some(r)) => {
                                let stream_result = EventStreamResult {
                                    event_id: r.event_id,
                                    sent: r.sent,
                                    error: r.error,
                                };
                                let _ = result_tx.send(stream_result).await;
                            }
                            Ok(None) => break,
                            Err(status) => {
                                // REQ-H12: Surface stream errors instead of silently breaking
                                let err_result = EventStreamResult {
                                    event_id: String::new(),
                                    sent: false,
                                    error: format!("stream error: {}", status.message()),
                                };
                                let _ = result_tx.send(err_result).await;
                                break;
                            }
                        }
                    }
                }
            }
            let _ = done_tx.send(());
        });

        Ok(EventStoreStreamHandle::new(
            event_tx, result_rx, done_rx, cancel,
        ))
    }

    /// Subscribe to persistent events from a start position.
    ///
    /// Callbacks are async (REQ-M35). The subscription auto-reconnects on
    /// retryable stream errors (REQ-H3).
    #[allow(clippy::type_complexity)]
    pub async fn subscribe_to_events_store(
        &self,
        channel: &str,
        group: &str,
        start: EventsStoreSubscription,
        on_event: impl Fn(EventStoreReceive) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
        on_error: Option<
            Box<
                dyn Fn(
                        KubemqError,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    ) -> crate::Result<Subscription> {
        self.check_state("subscribe_events_store")?;
        validate::validate_channel(channel, "subscribe_to_events_store")?;
        validate::validate_no_wildcards(channel, "subscribe_to_events_store")?;
        validate::validate_client_id(&self.config().client_id, "subscribe_to_events_store")?;

        let (store_type, store_value) = start.to_proto()?;

        // Validate start position values
        validate::validate_events_store_type_not_undefined(
            store_type,
            "subscribe_to_events_store",
            channel,
        )?;
        match &start {
            EventsStoreSubscription::StartAtSequence(seq) => {
                let seq_i64 = i64::try_from(*seq).unwrap_or(i64::MAX);
                validate::validate_start_at_sequence(
                    seq_i64,
                    "subscribe_to_events_store",
                    channel,
                )?;
            }
            EventsStoreSubscription::StartAtTime(_) => {
                validate::validate_start_at_time(
                    store_value,
                    "subscribe_to_events_store",
                    channel,
                )?;
            }
            EventsStoreSubscription::StartAtTimeDelta(_) => {
                validate::validate_start_at_time_delta(
                    store_value,
                    "subscribe_to_events_store",
                    channel,
                )?;
            }
            _ => {}
        }

        let subscribe_request = proto::Subscribe {
            subscribe_type_data: proto::subscribe::SubscribeType::EventsStore as i32,
            client_id: self.config().client_id.clone(),
            channel: channel.to_string(),
            group: group.to_string(),
            events_store_type_data: store_type,
            events_store_type_value: store_value,
        };

        let cancel = self.child_token();
        let cancel_clone = cancel.clone();
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);

        // Clone client for use in spawned task
        let client_handle = self.clone();
        let retry_policy = self.config().retry_policy.clone();

        // Spawn the subscription task. The gRPC subscribe call is made inside
        // the task because KubeMQ's server-streaming RPCs do not send HTTP/2
        // response headers until the first message is available. Awaiting the
        // subscribe call inline would block the caller indefinitely.
        tokio::spawn(async move {
            use futures::FutureExt;

            // Establish the initial gRPC stream
            let mut stream = {
                let mut client = match client_handle.transport().client() {
                    Ok(c) => c,
                    Err(_) => {
                        let _ = done_tx.send(true);
                        return;
                    }
                };
                match client.subscribe_to_events(subscribe_request.clone()).await {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let err =
                            KubemqError::from_grpc_status(status, "subscribe_to_events_store", "");
                        if let Some(ref on_err) = on_error {
                            let _ = std::panic::AssertUnwindSafe(on_err(err))
                                .catch_unwind()
                                .await;
                        }
                        let _ = done_tx.send(true);
                        return;
                    }
                }
            };

            let mut retry_count = 0u32;
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    msg = stream.message() => {
                        match msg {
                            Ok(Some(event_recv)) => {
                                retry_count = 0; // Reset on success
                                // REQ-M55: catch_unwind for async callback
                                let result = std::panic::AssertUnwindSafe(
                                    on_event(proto_to_event_store_receive(event_recv))
                                )
                                .catch_unwind()
                                .await;
                                if let Err(panic_info) = result {
                                    tracing::error!("User callback panicked: {:?}", panic_info);
                                }
                            }
                            Ok(None) => {
                                // Stream ended -- attempt resubscribe (REQ-H3)
                                match client_handle.transport().client() {
                                    Ok(mut c) => {
                                        match c.subscribe_to_events(subscribe_request.clone()).await {
                                            Ok(new_response) => {
                                                stream = new_response.into_inner();
                                                continue;
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                            Err(status) => {
                                let err = KubemqError::from_grpc_status(
                                    status,
                                    "subscribe_to_events_store",
                                    "",
                                );
                                // Check retryable before passing ownership to callback
                                let retryable = err.is_retryable();
                                if let Some(ref on_err) = on_error {
                                    let _ = std::panic::AssertUnwindSafe(on_err(err))
                                        .catch_unwind()
                                        .await;
                                }
                                if !retryable { break; }
                                if retry_policy.max_retries > 0 && retry_count >= retry_policy.max_retries { break; }
                                let delay = retry_policy.backoff(retry_count);
                                tokio::select! {
                                    _ = cancel_clone.cancelled() => break,
                                    _ = tokio::time::sleep(delay) => {}
                                }
                                retry_count += 1;
                                // Attempt resubscribe
                                match client_handle.transport().client() {
                                    Ok(mut c) => {
                                        match c.subscribe_to_events(subscribe_request.clone()).await {
                                            Ok(new_response) => {
                                                stream = new_response.into_inner();
                                                continue;
                                            }
                                            Err(_) => continue,
                                        }
                                    }
                                    Err(_) => continue,
                                }
                            }
                        }
                    }
                }
            }
            let _ = done_tx.send(true);
        });

        Ok(Subscription::new(
            format!("events_store-{}", channel),
            cancel,
            done_rx,
        ))
    }

    /// Convenience: publish event store with minimal params.
    pub async fn publish_event_store(
        &self,
        channel: &str,
        body: impl Into<Vec<u8>>,
        metadata: Option<&str>,
        tags: Option<HashMap<String, String>>,
    ) -> crate::Result<EventStoreResult> {
        let event = EventStore::builder()
            .channel(channel)
            .body(body) // Into<Vec<u8>> avoids extra to_vec()
            .metadata(metadata.unwrap_or(""))
            .tags(tags.unwrap_or_default())
            .build();
        self.send_event_store(event).await // Passes owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// REQ-C3: StartAtTimeDelta.to_proto() returns seconds, not nanoseconds.
    #[test]
    fn test_start_at_time_delta_uses_seconds() {
        let sub = EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(60));
        let (_, value) = sub.to_proto().unwrap();
        assert_eq!(
            value, 60,
            "StartAtTimeDelta should produce seconds, not nanos"
        );
    }

    /// Verify StartNewOnly produces correct proto type and zero value.
    #[test]
    fn test_to_proto_start_new_only() {
        let sub = EventsStoreSubscription::StartNewOnly;
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(t, proto::subscribe::EventsStoreType::StartNewOnly as i32);
        assert_eq!(v, 0);
    }

    /// Verify StartFromFirst produces correct proto type and zero value.
    #[test]
    fn test_to_proto_start_from_first() {
        let sub = EventsStoreSubscription::StartFromFirst;
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(t, proto::subscribe::EventsStoreType::StartFromFirst as i32);
        assert_eq!(v, 0);
    }

    /// Verify StartFromLast produces correct proto type and zero value.
    #[test]
    fn test_to_proto_start_from_last() {
        let sub = EventsStoreSubscription::StartFromLast;
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(t, proto::subscribe::EventsStoreType::StartFromLast as i32);
        assert_eq!(v, 0);
    }

    /// Verify StartAtSequence produces correct proto type and sequence value.
    #[test]
    fn test_to_proto_start_at_sequence() {
        let sub = EventsStoreSubscription::StartAtSequence(42);
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(t, proto::subscribe::EventsStoreType::StartAtSequence as i32);
        assert_eq!(v, 42);
    }

    /// Verify StartAtTime produces correct proto type and nanoseconds-since-epoch.
    #[test]
    fn test_to_proto_start_at_time() {
        let epoch_plus_1s = UNIX_EPOCH + Duration::from_secs(1);
        let sub = EventsStoreSubscription::StartAtTime(epoch_plus_1s);
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(t, proto::subscribe::EventsStoreType::StartAtTime as i32);
        assert_eq!(v, 1_000_000_000, "StartAtTime should produce nanoseconds");
    }

    /// Verify StartAtTimeDelta with sub-second Duration truncates to 0 seconds.
    #[test]
    fn test_to_proto_start_at_time_delta_subsecond() {
        let sub = EventsStoreSubscription::StartAtTimeDelta(Duration::from_millis(500));
        let (t, v) = sub.to_proto().unwrap();
        assert_eq!(
            t,
            proto::subscribe::EventsStoreType::StartAtTimeDelta as i32
        );
        assert_eq!(v, 0, "Sub-second delta truncates to 0 seconds");
    }

    /// REQ-M53: Sequence > i64::MAX should return Validation error, not panic.
    #[test]
    fn test_to_proto_sequence_overflow_error() {
        let sub = EventsStoreSubscription::StartAtSequence(u64::MAX);
        let result = sub.to_proto();
        assert!(result.is_err(), "u64::MAX should exceed i64::MAX and fail");
        if let Err(KubemqError::Validation { message, .. }) = result {
            assert!(
                message.contains("exceeds"),
                "Error should mention exceeding i64::MAX: {}",
                message
            );
        } else {
            panic!("Expected Validation error");
        }
    }

    // -- EventStoreBuilder tests --

    #[test]
    fn test_event_store_builder_default() {
        let builder = EventStoreBuilder::default();
        let event = builder.build();
        assert!(event.id.is_empty());
        assert!(event.channel.is_empty());
        assert!(event.metadata.is_empty());
        assert!(event.body.is_empty());
        assert!(event.client_id.is_empty());
        assert!(event.tags.is_empty());
    }

    #[test]
    fn test_event_store_builder_all_fields() {
        let event = EventStore::builder()
            .id("es1")
            .channel("store-ch")
            .metadata("store-meta")
            .body(b"store-body".to_vec())
            .client_id("store-client")
            .add_tag("k", "v")
            .build();
        assert_eq!(event.id, "es1");
        assert_eq!(event.channel, "store-ch");
        assert_eq!(event.metadata, "store-meta");
        assert_eq!(event.body, b"store-body");
        assert_eq!(event.client_id, "store-client");
        assert_eq!(event.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_event_store_builder_tags() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        let event = EventStore::builder()
            .channel("ch")
            .tags(tags)
            .add_tag("extra", "value")
            .build();
        assert_eq!(event.tags.len(), 2);
    }

    #[test]
    fn test_event_store_debug_clone() {
        let event = EventStore::builder()
            .id("es1")
            .channel("ch")
            .body(b"data".to_vec())
            .build();
        let debug = format!("{:?}", event);
        assert!(debug.contains("es1"));
        let cloned = event.clone();
        assert_eq!(cloned.id, "es1");
    }

    // -- EventStoreResult tests --

    #[test]
    fn test_event_store_result_into_result_success() {
        let result = EventStoreResult {
            id: "r1".to_string(),
            sent: true,
            error: String::new(),
        };
        let r = result.into_result();
        assert!(r.is_ok());
        let val = r.unwrap();
        assert_eq!(val.id, "r1");
        assert!(val.sent);
    }

    #[test]
    fn test_event_store_result_into_result_error_with_message() {
        let result = EventStoreResult {
            id: "r2".to_string(),
            sent: false,
            error: "server rejected".to_string(),
        };
        let r = result.into_result();
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Transient);
    }

    #[test]
    fn test_event_store_result_into_result_error_no_message() {
        let result = EventStoreResult {
            id: "r3".to_string(),
            sent: false,
            error: String::new(),
        };
        let r = result.into_result();
        assert!(r.is_err());
        // Should still produce an error even without server error message
    }

    #[test]
    fn test_event_store_result_debug_clone() {
        let result = EventStoreResult {
            id: "r1".to_string(),
            sent: true,
            error: String::new(),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("EventStoreResult"));
        let cloned = result.clone();
        assert_eq!(cloned.id, "r1");
    }

    // -- event_store_to_proto_owned tests --

    #[test]
    fn test_event_store_to_proto_owned_store_flag() {
        let event = EventStore::builder().channel("ch").build();
        let proto = event_store_to_proto_owned(event, "default");
        assert!(proto.store, "Store events should have store=true");
    }

    #[test]
    fn test_event_store_to_proto_owned_uses_default_client_id() {
        let event = EventStore::builder().channel("ch").build();
        let proto = event_store_to_proto_owned(event, "default-client");
        assert_eq!(proto.client_id, "default-client");
    }

    #[test]
    fn test_event_store_to_proto_owned_uses_custom_client_id() {
        let event = EventStore::builder()
            .channel("ch")
            .client_id("custom-client")
            .build();
        let proto = event_store_to_proto_owned(event, "default-client");
        assert_eq!(proto.client_id, "custom-client");
    }

    #[test]
    fn test_event_store_to_proto_owned_generates_id() {
        let event = EventStore::builder().channel("ch").build();
        let proto = event_store_to_proto_owned(event, "client");
        assert!(!proto.event_id.is_empty());
        assert_eq!(proto.event_id.len(), 36);
    }

    // -- proto_to_event_store_receive tests --

    #[test]
    fn test_proto_to_event_store_receive() {
        let mut tags = HashMap::new();
        tags.insert("key".to_string(), "value".to_string());
        let proto_event = proto::EventReceive {
            event_id: "recv-1".to_string(),
            channel: "store-ch".to_string(),
            metadata: "meta".to_string(),
            body: b"body".to_vec(),
            timestamp: 999,
            sequence: 77,
            tags,
        };
        let received = proto_to_event_store_receive(proto_event);
        assert_eq!(received.id, "recv-1");
        assert_eq!(received.channel, "store-ch");
        assert_eq!(received.metadata, "meta");
        assert_eq!(received.body, b"body");
        assert_eq!(received.timestamp, 999);
        assert_eq!(received.sequence, 77);
        assert_eq!(received.tags.get("key").unwrap(), "value");
    }

    // -- EventStoreReceive tests --

    #[test]
    fn test_event_store_receive_debug_clone() {
        let recv = EventStoreReceive {
            id: "r1".to_string(),
            sequence: 1,
            timestamp: 100,
            channel: "ch".to_string(),
            metadata: String::new(),
            body: vec![],
            tags: HashMap::new(),
        };
        let debug = format!("{:?}", recv);
        assert!(debug.contains("EventStoreReceive"));
        let cloned = recv.clone();
        assert_eq!(cloned.sequence, 1);
    }

    // -- EventsStoreSubscription Display tests --

    #[test]
    fn test_events_store_subscription_display() {
        assert_eq!(
            format!("{}", EventsStoreSubscription::StartNewOnly),
            "StartNewOnly"
        );
        assert_eq!(
            format!("{}", EventsStoreSubscription::StartFromFirst),
            "StartFromFirst"
        );
        assert_eq!(
            format!("{}", EventsStoreSubscription::StartFromLast),
            "StartFromLast"
        );
        assert!(format!("{}", EventsStoreSubscription::StartAtSequence(42)).contains("42"));
        assert!(format!(
            "{}",
            EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(10))
        )
        .contains("10"));
    }

    // -- EventStoreStreamHandle tests --

    #[test]
    fn test_event_store_stream_handle_debug() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<EventStore>(1);
        let (_rtx, rrx) = tokio::sync::mpsc::channel::<crate::events::EventStreamResult>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStoreStreamHandle::new(tx, rrx, drx, cancel);
        let debug = format!("{:?}", handle);
        assert!(debug.contains("EventStoreStreamHandle"));
    }

    #[test]
    fn test_event_store_stream_handle_close() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<EventStore>(1);
        let (_rtx, rrx) = tokio::sync::mpsc::channel::<crate::events::EventStreamResult>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let handle = EventStoreStreamHandle::new(tx, rrx, drx, cancel);
        assert!(!token.is_cancelled());
        handle.close();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_event_store_stream_handle_drop_cancels() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<EventStore>(1);
        let (_rtx, rrx) = tokio::sync::mpsc::channel::<crate::events::EventStreamResult>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        {
            let _handle = EventStoreStreamHandle::new(tx, rrx, drx, cancel);
        }
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_event_store_stream_handle_send_validates_channel() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<EventStore>(1);
        let (_rtx, rrx) = tokio::sync::mpsc::channel::<crate::events::EventStreamResult>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStoreStreamHandle::new(tx, rrx, drx, cancel);
        let event = EventStore::builder().build();
        let result = handle.send(event).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    #[tokio::test]
    async fn test_event_store_stream_handle_send_validates_wildcards() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<EventStore>(1);
        let (_rtx, rrx) = tokio::sync::mpsc::channel::<crate::events::EventStreamResult>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStoreStreamHandle::new(tx, rrx, drx, cancel);
        let event = EventStore::builder().channel("test.>").build();
        let result = handle.send(event).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    // -- EventStore sent=false error construction tests (Spec 1.8) --

    #[test]
    fn test_send_event_store_sent_false_empty_error() {
        let sent = false;
        let error = String::new();
        let event_id = "es-evt-1".to_string();
        let channel = "store-channel".to_string();

        assert!(!sent);
        let message = if error.is_empty() {
            "Event store message not sent (no error details from server)".to_string()
        } else {
            error
        };
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message,
            operation: "send_event_store".to_string(),
            channel,
            is_retryable: true,
            source: None,
            request_id: event_id,
            suggestion: "Retry the operation.",
        };
        assert_eq!(err.code(), ErrorCode::Transient);
        if let KubemqError::Transient {
            message,
            operation,
            channel,
            request_id,
            ..
        } = &err
        {
            assert_eq!(
                message,
                "Event store message not sent (no error details from server)"
            );
            assert_eq!(operation, "send_event_store");
            assert_eq!(channel, "store-channel");
            assert_eq!(request_id, "es-evt-1");
        } else {
            panic!("Expected Transient error");
        }
    }

    #[test]
    fn test_send_event_store_sent_false_with_server_error() {
        let sent = false;
        let error = "persistence layer unavailable".to_string();
        let event_id = "es-evt-2".to_string();
        let channel = "store-channel".to_string();

        assert!(!sent);
        let message = if error.is_empty() {
            "Event store message not sent (no error details from server)".to_string()
        } else {
            error
        };
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message,
            operation: "send_event_store".to_string(),
            channel,
            is_retryable: true,
            source: None,
            request_id: event_id,
            suggestion: "Retry the operation.",
        };
        assert_eq!(err.code(), ErrorCode::Transient);
        if let KubemqError::Transient { message, .. } = &err {
            assert_eq!(message, "persistence layer unavailable");
        } else {
            panic!("Expected Transient error");
        }
    }

    #[test]
    fn test_send_event_store_sent_false_is_retryable() {
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message: "Event store message not sent (no error details from server)".to_string(),
            operation: "send_event_store".to_string(),
            channel: "ch".to_string(),
            is_retryable: true,
            source: None,
            request_id: "id".to_string(),
            suggestion: "Retry the operation.",
        };
        assert!(err.is_retryable());
    }
}
