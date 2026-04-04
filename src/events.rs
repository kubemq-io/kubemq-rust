//! Events (Pub/Sub) messaging pattern.

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq as proto;
use crate::subscription::Subscription;
use crate::validate;

/// Outbound fire-and-forget event message for the Pub/Sub pattern.
///
/// Events are not persisted — they are delivered to all active subscribers
/// and then discarded. For persistent delivery, use [`EventStore`](crate::EventStore).
///
/// Not thread-safe — create a new instance per send.
///
/// Use [`Event::builder()`] to construct instances.
#[derive(Debug, Clone)]
pub struct Event {
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
    /// Arbitrary key-value pairs attached to the event. Keys and values must be non-empty.
    pub tags: HashMap<String, String>,
}

impl Event {
    /// Create a builder for constructing an event.
    pub fn builder() -> EventBuilder {
        EventBuilder::new()
    }
}

/// Builder for constructing [`Event`] instances using a fluent API.
///
/// All fields are optional. `channel` is required at send time.
///
/// # Example
///
/// ```rust
/// use kubemq::prelude::*;
///
/// let event = Event::builder()
///     .channel("notifications")
///     .body(b"hello world".to_vec())
///     .add_tag("source", "sensor-1")
///     .build();
/// ```
pub struct EventBuilder {
    event: Event,
}

impl EventBuilder {
    /// Create a new builder with all fields set to their defaults.
    pub fn new() -> Self {
        Self {
            event: Event {
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

    /// Consume the builder and return the constructed [`Event`].
    pub fn build(self) -> Event {
        self.event
    }
}

impl Default for EventBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// An event received from a subscription callback.
///
/// Delivered to the `on_event` closure passed to
/// [`KubemqClient::subscribe_to_events()`].
#[derive(Debug, Clone)]
pub struct EventReceive {
    /// Server-assigned event identifier.
    pub id: String,
    /// The channel this event was published to.
    pub channel: String,
    /// UTF-8 metadata attached by the publisher.
    pub metadata: String,
    /// Message payload bytes.
    pub body: Vec<u8>,
    /// Server timestamp (Unix nanoseconds) when the event was received.
    pub timestamp: i64,
    /// Monotonically increasing sequence number within the channel.
    pub sequence: u64,
    /// Key-value tags attached by the publisher.
    pub tags: HashMap<String, String>,
}

/// Per-event result returned from a streaming send via [`EventStreamHandle`].
#[derive(Debug, Clone)]
pub struct EventStreamResult {
    /// Identifier of the event this result corresponds to.
    pub event_id: String,
    /// `true` if the server accepted the event.
    pub sent: bool,
    /// Server-provided error message when `sent` is `false`.
    pub error: String,
}

/// Handle for a bidirectional event send stream.
///
/// Obtained from [`KubemqClient::send_event_stream()`]. Provides a
/// non-blocking [`send()`](Self::send) method for high-throughput publishing
/// and an [`errors()`](Self::errors) receiver for asynchronous error
/// notifications. The stream is cancelled on [`close()`](Self::close) or
/// when the handle is dropped.
pub struct EventStreamHandle {
    sender: tokio::sync::mpsc::Sender<Event>,
    errors: tokio::sync::mpsc::Receiver<KubemqError>,
    _done: tokio::sync::oneshot::Receiver<()>,
    cancel: CancellationToken,
}

impl EventStreamHandle {
    /// Create a new EventStreamHandle from its parts.
    pub(crate) fn new(
        sender: tokio::sync::mpsc::Sender<Event>,
        errors: tokio::sync::mpsc::Receiver<KubemqError>,
        done: tokio::sync::oneshot::Receiver<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            errors,
            _done: done,
            cancel,
        }
    }

    /// Send an event through the stream.
    pub async fn send(&self, event: Event) -> crate::Result<()> {
        // REQ-H5: Validate before sending to stream
        validate::validate_channel(&event.channel, "event_stream.send")?;
        validate::validate_no_wildcards(&event.channel, "event_stream.send")?;
        validate::validate_tags(&event.tags, "event_stream.send", &event.channel)?;

        self.sender
            .send(event)
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "event stream closed".to_string(),
                operation: "send_event_stream".to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The stream has been closed. Open a new stream.",
            })
    }

    /// Get a mutable reference to the error receiver.
    pub fn errors(&mut self) -> &mut tokio::sync::mpsc::Receiver<KubemqError> {
        &mut self.errors
    }

    /// Close the stream.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for EventStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStreamHandle")
            .field("sender", &"<mpsc::Sender>")
            .field("errors", &"<mpsc::Receiver>")
            .finish()
    }
}

impl Drop for EventStreamHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Convert our Event to proto Event with Store=false (owned, zero-copy).
fn event_to_proto_owned(event: Event, default_client_id: &str) -> proto::Event {
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
        store: false,
        tags: event.tags, // moved
    }
}

/// Convert proto EventReceive to our EventReceive.
fn proto_to_event_receive(p: proto::EventReceive) -> EventReceive {
    EventReceive {
        id: p.event_id,
        channel: p.channel,
        metadata: p.metadata,
        body: p.body,
        timestamp: p.timestamp,
        sequence: p.sequence,
        tags: p.tags,
    }
}

// Client methods for Events
impl KubemqClient {
    /// Sends a single fire-and-forget [`Event`] to the specified channel.
    ///
    /// Events are delivered to all active subscribers (fan-out). No delivery
    /// guarantee — if no subscriber is listening, the event is dropped.
    /// For persistent delivery, use [`send_event_store()`](Self::send_event_store).
    ///
    /// # Arguments
    ///
    /// * `event` - The [`Event`] to send. Build with [`Event::builder()`].
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `channel` is empty or contains wildcards, or body/metadata are both empty.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    /// * [`KubemqError::Authentication`] (gRPC `UNAUTHENTICATED`) — if the auth token is invalid.
    /// * [`KubemqError::Timeout`] (gRPC `DEADLINE_EXCEEDED`) — if the operation times out. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let event = Event::builder()
    ///     .channel("events.example")
    ///     .body(b"Hello KubeMQ!".to_vec())
    ///     .build();
    ///
    /// client.send_event(event).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_event(&self, event: Event) -> crate::Result<()> {
        self.check_state("send_event")?;

        validate::validate_channel(&event.channel, "send_event")?;
        validate::validate_no_wildcards(&event.channel, "send_event")?; // REQ-H4
        validate::validate_tags(&event.tags, "send_event", &event.channel)?;
        validate::validate_body_size(
            &event.body,
            self.config().max_send_message_size,
            "send_event",
            &event.channel,
        )?;
        validate::validate_body_or_metadata(
            &event.metadata,
            &event.body,
            "send_event",
            &event.channel,
        )?;

        let channel_for_err = event.channel.clone(); // Save for error message
        let proto_event = event_to_proto_owned(event, &self.config().client_id);

        let mut request = tonic::Request::new(proto_event);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .send_event(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_event", &channel_for_err))?;

        let result = response.into_inner();
        if result.sent {
            Ok(())
        } else {
            // REQ-M9: Treat sent=false as error even if error message is empty
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message: if result.error.is_empty() {
                    "Event not sent (no error details from server)".into()
                } else {
                    result.error
                },
                operation: "send_event".to_string(),
                channel: channel_for_err,
                is_retryable: true,
                source: None,
                request_id: result.event_id,
                suggestion: "Retry the operation.",
            })
        }
    }

    /// Opens a bidirectional stream for high-throughput [`Event`] publishing.
    ///
    /// Returns an [`EventStreamHandle`] that provides a non-blocking
    /// [`send()`](EventStreamHandle::send) method. Errors are delivered
    /// asynchronously via [`errors()`](EventStreamHandle::errors).
    /// For single sends, use [`send_event()`](Self::send_event).
    ///
    /// # Returns
    ///
    /// An [`EventStreamHandle`] for sending events and receiving errors.
    ///
    /// # Errors
    ///
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let mut handle = client.send_event_stream().await?;
    /// let event = Event::builder()
    ///     .channel("events.stream")
    ///     .body(b"streamed event".to_vec())
    ///     .build();
    /// handle.send(event).await?;
    /// handle.close();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_event_stream(&self) -> crate::Result<EventStreamHandle> {
        self.check_state("send_event_stream")?;

        let client_id = self.config().client_id.clone();
        let cancel = self.child_token();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(256);
        let (error_tx, error_rx) = tokio::sync::mpsc::channel::<KubemqError>(64);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        // Create the proto event sender channel
        let (proto_tx, proto_rx) = tokio::sync::mpsc::channel::<proto::Event>(256);
        let proto_stream = tokio_stream::wrappers::ReceiverStream::new(proto_rx);

        let mut grpc_client = self.transport().client()?;

        // Spawn task to forward events from user channel to proto channel (owned conversion).
        // Must be spawned BEFORE the gRPC call so messages can flow while the
        // server establishes the stream (avoiding a deadlock where the server
        // waits for the first client message before sending response headers).
        let cid = client_id.clone();
        let ptx = proto_tx.clone();
        let cancel_fwd = cancel.clone();
        let error_tx_clone = error_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_fwd.cancelled() => break,
                    event = event_rx.recv() => {
                        match event {
                            Some(e) => {
                                if let Err(err) = validate::validate_body_or_metadata(&e.metadata, &e.body, "send_event_stream", &e.channel) {
                                    let _ = error_tx_clone.send(err).await;
                                    continue;
                                }
                                let proto_event = event_to_proto_owned(e, &cid);
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
        let error_tx_stream = error_tx.clone();
        tokio::spawn(async move {
            let response = match grpc_client.send_events_stream(proto_stream).await {
                Ok(resp) => resp,
                Err(status) => {
                    let err = KubemqError::from_grpc_status(status, "send_event_stream", "");
                    let _ = error_tx_stream.send(err).await;
                    let _ = done_tx.send(());
                    return;
                }
            };

            let mut result_stream = response.into_inner();
            loop {
                tokio::select! {
                    _ = cancel_read.cancelled() => break,
                    result = result_stream.message() => {
                        match result {
                            Ok(Some(r)) => {
                                if !r.sent {
                                    let message = if r.error.is_empty() {
                                        "Event not sent (no error details from server)".to_string()
                                    } else {
                                        r.error
                                    };
                                    let err = KubemqError::Transient {
                                        code: ErrorCode::Transient,
                                        message,
                                        operation: "send_event_stream".to_string(),
                                        channel: String::new(),
                                        is_retryable: true,
                                        source: None,
                                        request_id: r.event_id,
                                        suggestion: "Check the event and retry.",
                                    };
                                    let _ = error_tx.send(err).await;
                                }
                            }
                            Ok(None) => break,
                            Err(status) => {
                                // REQ-H12: Surface stream errors
                                let err = KubemqError::from_grpc_status(
                                    status,
                                    "send_event_stream",
                                    "",
                                );
                                let _ = error_tx.send(err).await;
                                break;
                            }
                        }
                    }
                }
            }
            let _ = done_tx.send(());
        });

        Ok(EventStreamHandle::new(event_tx, error_rx, done_rx, cancel))
    }

    /// Subscribes to events on a channel.
    ///
    /// The `on_event` callback is invoked for each received [`EventReceive`]. The
    /// subscription automatically reconnects on retryable stream errors with backoff.
    ///
    /// # Arguments
    ///
    /// * `channel` - Channel name to subscribe to. Supports wildcards (`*`, `>`).
    /// * `group` - Consumer group name. Empty string for no group (fan-out).
    /// * `on_event` - Async callback invoked for each [`EventReceive`].
    /// * `on_error` - Optional async error callback for stream errors.
    ///
    /// # Returns
    ///
    /// A [`Subscription`] handle for cancelling the subscription.
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `channel` is empty.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    /// * [`KubemqError::Authentication`] (gRPC `UNAUTHENTICATED`) — if the auth token is invalid.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let sub = client.subscribe_to_events(
    ///     "events.>",
    ///     "",
    ///     |event| Box::pin(async move {
    ///         println!("Received: {}", String::from_utf8_lossy(&event.body));
    ///     }),
    ///     None,
    /// ).await?;
    ///
    /// // Later: cancel the subscription
    /// sub.unsubscribe().await;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::type_complexity)]
    pub async fn subscribe_to_events(
        &self,
        channel: &str,
        group: &str,
        on_event: impl Fn(EventReceive) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
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
        self.check_state("subscribe_events")?;
        validate::validate_channel(channel, "subscribe_to_events")?;
        validate::validate_client_id(&self.config().client_id, "subscribe_to_events")?;

        let subscribe_request = proto::Subscribe {
            subscribe_type_data: proto::subscribe::SubscribeType::Events as i32,
            client_id: self.config().client_id.clone(),
            channel: channel.to_string(),
            group: group.to_string(),
            events_store_type_data: 0,
            events_store_type_value: 0,
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
                        let err = KubemqError::from_grpc_status(status, "subscribe_to_events", "");
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
                                    on_event(proto_to_event_receive(event_recv))
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
                                    "subscribe_to_events",
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
            format!("events-{}", channel),
            cancel,
            done_rx,
        ))
    }

    /// Convenience method to publish an event with minimal parameters.
    ///
    /// Equivalent to building an [`Event`] and calling [`send_event()`](Self::send_event).
    ///
    /// # Arguments
    ///
    /// * `channel` - Target channel name.
    /// * `body` - Message payload bytes.
    /// * `metadata` - Optional UTF-8 metadata string.
    /// * `tags` - Optional key-value tags.
    ///
    /// # Errors
    ///
    /// Same as [`send_event()`](Self::send_event).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// client.publish_event("notifications", b"alert".to_vec(), None, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_event(
        &self,
        channel: &str,
        body: impl Into<Vec<u8>>,
        metadata: Option<&str>,
        tags: Option<HashMap<String, String>>,
    ) -> crate::Result<()> {
        let event = Event::builder()
            .channel(channel)
            .body(body) // Into<Vec<u8>> avoids extra to_vec()
            .metadata(metadata.unwrap_or(""))
            .tags(tags.unwrap_or_default())
            .build();
        self.send_event(event).await // Passes owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- EventBuilder tests --

    #[test]
    fn test_event_builder_default() {
        let builder = EventBuilder::default();
        let event = builder.build();
        assert!(event.id.is_empty());
        assert!(event.channel.is_empty());
        assert!(event.metadata.is_empty());
        assert!(event.body.is_empty());
        assert!(event.client_id.is_empty());
        assert!(event.tags.is_empty());
    }

    #[test]
    fn test_event_builder_all_fields() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "test".to_string());
        let event = Event::builder()
            .id("my-id")
            .channel("test-channel")
            .metadata("meta")
            .body(b"hello".to_vec())
            .client_id("client-1")
            .tags(tags)
            .build();
        assert_eq!(event.id, "my-id");
        assert_eq!(event.channel, "test-channel");
        assert_eq!(event.metadata, "meta");
        assert_eq!(event.body, b"hello");
        assert_eq!(event.client_id, "client-1");
        assert_eq!(event.tags.get("env").unwrap(), "test");
    }

    #[test]
    fn test_event_builder_add_tag() {
        let event = Event::builder()
            .channel("ch")
            .add_tag("key1", "val1")
            .add_tag("key2", "val2")
            .build();
        assert_eq!(event.tags.len(), 2);
        assert_eq!(event.tags.get("key1").unwrap(), "val1");
        assert_eq!(event.tags.get("key2").unwrap(), "val2");
    }

    #[test]
    fn test_event_debug_clone() {
        let event = Event::builder()
            .id("e1")
            .channel("ch")
            .body(b"data".to_vec())
            .build();
        let debug = format!("{:?}", event);
        assert!(debug.contains("e1"));
        let cloned = event.clone();
        assert_eq!(cloned.id, "e1");
        assert_eq!(cloned.body, b"data");
    }

    // -- event_to_proto_owned tests --

    #[test]
    fn test_event_to_proto_owned_with_client_id() {
        let event = Event::builder()
            .id("e1")
            .channel("ch")
            .metadata("meta")
            .body(b"data".to_vec())
            .client_id("my-client")
            .add_tag("k", "v")
            .build();
        let proto = event_to_proto_owned(event, "default-client");
        assert_eq!(proto.event_id, "e1");
        assert_eq!(proto.client_id, "my-client");
        assert_eq!(proto.channel, "ch");
        assert_eq!(proto.metadata, "meta");
        assert_eq!(proto.body, b"data");
        assert!(!proto.store);
        assert_eq!(proto.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_event_to_proto_owned_uses_default_client_id() {
        let event = Event::builder().channel("ch").build();
        let proto = event_to_proto_owned(event, "default-client");
        assert_eq!(proto.client_id, "default-client");
    }

    #[test]
    fn test_event_to_proto_owned_generates_id_when_empty() {
        let event = Event::builder().channel("ch").build();
        let proto = event_to_proto_owned(event, "client");
        assert!(
            !proto.event_id.is_empty(),
            "Should generate UUID for empty id"
        );
        assert_eq!(
            proto.event_id.len(),
            36,
            "Generated ID should be UUID format"
        );
    }

    // -- proto_to_event_receive tests --

    #[test]
    fn test_proto_to_event_receive() {
        let mut tags = HashMap::new();
        tags.insert("source".to_string(), "test".to_string());
        let proto_event = proto::EventReceive {
            event_id: "recv-1".to_string(),
            channel: "events-ch".to_string(),
            metadata: "recv-meta".to_string(),
            body: b"recv-body".to_vec(),
            timestamp: 1234567890,
            sequence: 42,
            tags,
        };
        let received = proto_to_event_receive(proto_event);
        assert_eq!(received.id, "recv-1");
        assert_eq!(received.channel, "events-ch");
        assert_eq!(received.metadata, "recv-meta");
        assert_eq!(received.body, b"recv-body");
        assert_eq!(received.timestamp, 1234567890);
        assert_eq!(received.sequence, 42);
        assert_eq!(received.tags.get("source").unwrap(), "test");
    }

    // -- EventReceive Debug/Clone tests --

    #[test]
    fn test_event_receive_debug_clone() {
        let recv = EventReceive {
            id: "r1".to_string(),
            channel: "ch".to_string(),
            metadata: String::new(),
            body: vec![],
            timestamp: 0,
            sequence: 1,
            tags: HashMap::new(),
        };
        let debug = format!("{:?}", recv);
        assert!(debug.contains("EventReceive"));
        let cloned = recv.clone();
        assert_eq!(cloned.id, "r1");
        assert_eq!(cloned.sequence, 1);
    }

    // -- EventStreamResult tests --

    #[test]
    fn test_event_stream_result_debug_clone() {
        let result = EventStreamResult {
            event_id: "e1".to_string(),
            sent: true,
            error: String::new(),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("EventStreamResult"));
        let cloned = result.clone();
        assert_eq!(cloned.event_id, "e1");
        assert!(cloned.sent);
    }

    // -- EventStreamHandle tests --

    #[test]
    fn test_event_stream_handle_debug() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(1);
        let (_etx, erx) = tokio::sync::mpsc::channel::<KubemqError>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStreamHandle::new(tx, erx, drx, cancel);
        let debug = format!("{:?}", handle);
        assert!(debug.contains("EventStreamHandle"));
    }

    #[test]
    fn test_event_stream_handle_close() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(1);
        let (_etx, erx) = tokio::sync::mpsc::channel::<KubemqError>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let token_clone = cancel.clone();
        let handle = EventStreamHandle::new(tx, erx, drx, cancel);
        assert!(!token_clone.is_cancelled());
        handle.close();
        assert!(token_clone.is_cancelled());
    }

    #[test]
    fn test_event_stream_handle_drop_cancels() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(1);
        let (_etx, erx) = tokio::sync::mpsc::channel::<KubemqError>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let token_clone = cancel.clone();
        {
            let _handle = EventStreamHandle::new(tx, erx, drx, cancel);
        }
        assert!(token_clone.is_cancelled());
    }

    #[tokio::test]
    async fn test_event_stream_handle_send_validates_channel() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(1);
        let (_etx, erx) = tokio::sync::mpsc::channel::<KubemqError>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStreamHandle::new(tx, erx, drx, cancel);
        // Empty channel should fail validation
        let event = Event::builder().build();
        let result = handle.send(event).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    #[tokio::test]
    async fn test_event_stream_handle_send_validates_wildcards() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(1);
        let (_etx, erx) = tokio::sync::mpsc::channel::<KubemqError>(1);
        let (_dtx, drx) = tokio::sync::oneshot::channel::<()>();
        let cancel = CancellationToken::new();
        let handle = EventStreamHandle::new(tx, erx, drx, cancel);
        let event = Event::builder().channel("test.*").build();
        let result = handle.send(event).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    // -- Event Stream sent=false tests (Spec 1.3) --

    #[test]
    fn test_event_stream_sent_false_empty_error_produces_transient() {
        let sent = false;
        let error = String::new();
        let event_id = "test-evt-1".to_string();

        assert!(!sent);
        let message = if error.is_empty() {
            "Event not sent (no error details from server)".to_string()
        } else {
            error
        };
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message,
            operation: "send_event_stream".to_string(),
            channel: String::new(),
            is_retryable: true,
            source: None,
            request_id: event_id,
            suggestion: "Check the event and retry.",
        };
        assert_eq!(err.code(), ErrorCode::Transient);
        if let KubemqError::Transient {
            message,
            operation,
            request_id,
            ..
        } = &err
        {
            assert_eq!(message, "Event not sent (no error details from server)");
            assert_eq!(operation, "send_event_stream");
            assert_eq!(request_id, "test-evt-1");
        } else {
            panic!("Expected Transient error");
        }
    }

    #[test]
    fn test_event_stream_sent_false_with_server_error() {
        let sent = false;
        let error = "broker rejected the event".to_string();
        let event_id = "test-evt-2".to_string();

        assert!(!sent);
        let message = if error.is_empty() {
            "Event not sent (no error details from server)".to_string()
        } else {
            error
        };
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message,
            operation: "send_event_stream".to_string(),
            channel: String::new(),
            is_retryable: true,
            source: None,
            request_id: event_id,
            suggestion: "Check the event and retry.",
        };
        assert_eq!(err.code(), ErrorCode::Transient);
        if let KubemqError::Transient { message, .. } = &err {
            assert_eq!(message, "broker rejected the event");
        } else {
            panic!("Expected Transient error");
        }
    }

    #[test]
    fn test_event_stream_sent_false_error_is_retryable() {
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message: "Event not sent (no error details from server)".to_string(),
            operation: "send_event_stream".to_string(),
            channel: String::new(),
            is_retryable: true,
            source: None,
            request_id: "evt-id".to_string(),
            suggestion: "Check the event and retry.",
        };
        assert!(err.is_retryable());
    }
}
