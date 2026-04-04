//! Commands (RPC) messaging pattern.

use std::collections::HashMap;
use std::time::Duration;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq as proto;
use crate::subscription::Subscription;
use crate::validate;

/// Outbound command request for the RPC (Command/Query) pattern.
///
/// Commands are point-to-point: the sender blocks until exactly one subscriber
/// executes the command and returns a [`CommandResponse`], or the timeout expires.
///
/// Use [`Command::builder()`] to construct instances.
#[derive(Debug, Clone)]
pub struct Command {
    /// Unique request identifier. Auto-generated (UUID v4) when empty.
    pub id: String,
    /// Target channel name. Required; must not contain wildcards.
    pub channel: String,
    /// Optional UTF-8 metadata string (e.g., command name, correlation ID).
    pub metadata: String,
    /// Command payload bytes.
    pub body: Vec<u8>,
    /// Maximum time to wait for a response. Defaults to 5 seconds.
    pub timeout: Duration,
    /// Sender identity. Falls back to the client-level `client_id` when empty.
    pub client_id: String,
    /// Arbitrary key-value pairs attached to the command. Keys and values must be non-empty.
    pub tags: HashMap<String, String>,
    /// OpenTelemetry span context for distributed tracing. Usually left empty.
    pub span: Vec<u8>,
}

impl Command {
    /// Create a builder for constructing a command.
    pub fn builder() -> CommandBuilder {
        CommandBuilder::new()
    }
}

/// Builder for constructing [`Command`] instances using a fluent API.
///
/// All fields are optional. `channel` and `timeout` are required at send time.
///
/// # Example
///
/// ```rust
/// use kubemq::prelude::*;
/// use std::time::Duration;
///
/// let cmd = Command::builder()
///     .channel("device.reboot")
///     .body(b"device-42".to_vec())
///     .timeout(Duration::from_secs(10))
///     .build();
/// ```
pub struct CommandBuilder {
    cmd: Command,
}

impl CommandBuilder {
    /// Create a new builder with all fields set to their defaults (5s timeout).
    pub fn new() -> Self {
        Self {
            cmd: Command {
                id: String::new(),
                channel: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                timeout: Duration::from_secs(5),
                client_id: String::new(),
                tags: HashMap::new(),
                span: Vec::new(),
            },
        }
    }

    /// Set the request ID. If omitted, a UUID v4 is generated at send time.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.cmd.id = id.into();
        self
    }

    /// Set the target channel name (required).
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.cmd.channel = channel.into();
        self
    }

    /// Set optional UTF-8 metadata (e.g., command name, correlation ID).
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.cmd.metadata = metadata.into();
        self
    }

    /// Set the command payload bytes.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.cmd.body = body.into();
        self
    }

    /// Set the maximum time to wait for a response. Must be > 0.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.cmd.timeout = timeout;
        self
    }

    /// Override the client-level `client_id` for this request.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.cmd.client_id = client_id.into();
        self
    }

    /// Replace all tags with the provided map.
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.cmd.tags = tags;
        self
    }

    /// Add a single key-value tag. Both key and value must be non-empty.
    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.cmd.tags.insert(key.into(), value.into());
        self
    }

    /// Consume the builder and return the constructed [`Command`].
    pub fn build(self) -> Command {
        self.cmd
    }
}

impl Default for CommandBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A command received from a subscription callback.
///
/// Delivered to the `on_command` closure passed to
/// [`KubemqClient::subscribe_to_commands()`]. The handler should process
/// the command and send a [`CommandReply`] via
/// [`KubemqClient::send_command_response()`].
#[derive(Debug, Clone)]
pub struct CommandReceive {
    /// Request identifier — use this as `request_id` in the reply.
    pub id: String,
    /// Identity of the sender.
    pub client_id: String,
    /// The channel this command was sent to.
    pub channel: String,
    /// UTF-8 metadata attached by the sender.
    pub metadata: String,
    /// Command payload bytes.
    pub body: Vec<u8>,
    /// Reply channel — use this as `response_to` in the reply.
    pub response_to: String,
    /// Key-value tags attached by the sender.
    pub tags: HashMap<String, String>,
    /// OpenTelemetry span context for distributed tracing.
    pub span: Vec<u8>,
}

/// Response received after sending a command via [`KubemqClient::send_command()`].
///
/// Use [`into_result()`](Self::into_result) to convert into a standard
/// `Result`, which returns `Err` when `executed` is `false`.
#[derive(Debug, Clone)]
pub struct CommandResponse {
    /// Identifier of the original command request.
    pub command_id: String,
    /// Identity of the subscriber that handled the command.
    pub response_client_id: String,
    /// `true` if the subscriber reported successful execution.
    pub executed: bool,
    /// Server timestamp (Unix nanoseconds) when the response was created.
    pub executed_at: i64,
    /// Error message from the subscriber when `executed` is `false`.
    pub error: String,
    /// Key-value tags attached by the subscriber.
    pub tags: HashMap<String, String>,
}

impl CommandResponse {
    /// Convert into a Result, returning Err if the command was not executed.
    pub fn into_result(self) -> crate::Result<Self> {
        if !self.executed {
            let message = if self.error.is_empty() {
                "Command was not executed (no error details from server)".to_string()
            } else {
                self.error
            };
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message,
                operation: "send_command".to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: self.command_id,
                suggestion: "Check the command handler.",
            })
        } else {
            Ok(self)
        }
    }
}

/// Outbound command reply sent by a subscriber via
/// [`KubemqClient::send_command_response()`].
///
/// Copy `id` and `response_to` from the received [`CommandReceive`].
/// If `error` is `None`, the command is marked as executed successfully.
#[derive(Debug, Clone)]
pub struct CommandReply {
    /// Must match [`CommandReceive::id`] of the incoming command.
    pub request_id: String,
    /// Must match [`CommandReceive::response_to`] of the incoming command.
    pub response_to: String,
    /// Optional UTF-8 metadata to include in the reply.
    pub metadata: String,
    /// Optional response payload bytes.
    pub body: Vec<u8>,
    /// Responder identity. Falls back to the client-level `client_id` when empty.
    pub client_id: String,
    /// Timestamp indicating when the command was executed.
    pub executed_at: i64,
    /// Set to `Some(message)` to report a failure; `None` means success.
    pub error: Option<String>,
    /// Arbitrary key-value pairs to attach to the reply.
    pub tags: HashMap<String, String>,
    /// OpenTelemetry span context for distributed tracing.
    pub span: Vec<u8>,
}

impl CommandReply {
    /// Create a builder for constructing a command reply.
    pub fn builder() -> CommandReplyBuilder {
        CommandReplyBuilder::new()
    }
}

/// Builder for constructing [`CommandReply`] instances using a fluent API.
///
/// `request_id` and `response_to` are required and must match the values
/// from the received [`CommandReceive`].
pub struct CommandReplyBuilder {
    reply: CommandReply,
}

impl CommandReplyBuilder {
    /// Create a new builder with all fields set to their defaults.
    pub fn new() -> Self {
        Self {
            reply: CommandReply {
                request_id: String::new(),
                response_to: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                client_id: String::new(),
                executed_at: 0,
                error: None,
                tags: HashMap::new(),
                span: Vec::new(),
            },
        }
    }

    /// Set the request ID (required). Must match [`CommandReceive::id`].
    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.reply.request_id = id.into();
        self
    }

    /// Set the reply channel (required). Must match [`CommandReceive::response_to`].
    pub fn response_to(mut self, channel: impl Into<String>) -> Self {
        self.reply.response_to = channel.into();
        self
    }

    /// Set optional UTF-8 metadata to include in the reply.
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.reply.metadata = metadata.into();
        self
    }

    /// Set optional response payload bytes.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.reply.body = body.into();
        self
    }

    /// Override the client-level `client_id` for this reply.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.reply.client_id = id.into();
        self
    }

    /// Set the execution timestamp.
    pub fn executed_at(mut self, time: i64) -> Self {
        self.reply.executed_at = time;
        self
    }

    /// Report an error. Setting this marks the command as not executed.
    pub fn error(mut self, err: impl Into<String>) -> Self {
        self.reply.error = Some(err.into());
        self
    }

    /// Replace all tags with the provided map.
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.reply.tags = tags;
        self
    }

    /// Consume the builder and return the constructed [`CommandReply`].
    pub fn build(self) -> CommandReply {
        self.reply
    }
}

impl Default for CommandReplyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Client methods for Commands
impl KubemqClient {
    /// Sends a [`Command`] and waits for a [`CommandResponse`] from a subscriber.
    ///
    /// Commands are point-to-point: exactly one subscriber handles the command
    /// and returns a [`CommandResponse`], or the `timeout` expires.
    /// Subscribers register via [`subscribe_to_commands()`](Self::subscribe_to_commands)
    /// and reply via [`send_command_response()`](Self::send_command_response).
    ///
    /// # Arguments
    ///
    /// * `command` - The [`Command`] to send. Build with [`Command::builder()`].
    ///
    /// # Returns
    ///
    /// A [`CommandResponse`] indicating whether the command was executed.
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `channel` is empty, contains wildcards, or `timeout` is zero.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    /// * [`KubemqError::Authentication`] (gRPC `UNAUTHENTICATED`) — if the auth token is invalid.
    /// * [`KubemqError::Timeout`] (gRPC `DEADLINE_EXCEEDED`) — if no subscriber responds in time. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    /// use std::time::Duration;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let cmd = Command::builder()
    ///     .channel("device.reboot")
    ///     .body(b"device-42".to_vec())
    ///     .timeout(Duration::from_secs(10))
    ///     .build();
    ///
    /// let resp = client.send_command(cmd).await?;
    /// println!("Executed: {}", resp.executed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_command(&self, command: Command) -> crate::Result<CommandResponse> {
        self.check_state("send_command")?;

        validate::validate_channel(&command.channel, "send_command")?;
        validate::validate_no_wildcards(&command.channel, "send_command")?;

        // REQ-M8: Safe Duration-to-i32 conversion, no silent truncation
        let timeout_ms = i32::try_from(command.timeout.as_millis()).unwrap_or(i32::MAX);
        validate::validate_timeout_positive(timeout_ms, "send_command", &command.channel)?;
        validate::validate_tags(&command.tags, "send_command", &command.channel)?;
        validate::validate_body_size(
            &command.body,
            self.config().max_send_message_size,
            "send_command",
            &command.channel,
        )?;

        let channel_for_err = command.channel.clone(); // Save for error message

        // REQ-H7: Owned proto conversion -- zero-copy moves
        let proto_request = proto::Request {
            request_id: crate::common::resolve_id_owned(command.id),
            request_type_data: proto::request::RequestType::Command as i32,
            client_id: crate::common::resolve_client_id_owned(command.client_id, self.config()),
            channel: command.channel,   // moved
            metadata: command.metadata, // moved
            body: command.body,         // moved -- zero-copy
            reply_channel: String::new(),
            timeout: timeout_ms,
            cache_key: String::new(),
            cache_ttl: 0,
            span: command.span, // moved
            tags: command.tags, // moved
        };

        let mut request = tonic::Request::new(proto_request);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .send_request(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_command", &channel_for_err))?;

        let resp = response.into_inner();
        Ok(CommandResponse {
            command_id: resp.request_id,
            response_client_id: resp.client_id,
            executed: resp.executed,
            executed_at: resp.timestamp,
            error: resp.error,
            tags: resp.tags,
        })
    }

    /// Subscribes to commands on a channel.
    ///
    /// The `on_command` callback is invoked for each received [`CommandReceive`].
    /// The handler should process the command and send a [`CommandReply`] via
    /// [`send_command_response()`](Self::send_command_response). The subscription
    /// automatically reconnects on retryable stream errors with backoff.
    ///
    /// # Arguments
    ///
    /// * `channel` - Channel name to subscribe to. Must not contain wildcards.
    /// * `group` - Consumer group name. Empty string for no group.
    /// * `on_command` - Async callback invoked for each [`CommandReceive`].
    /// * `on_error` - Optional async error callback for stream errors.
    ///
    /// # Returns
    ///
    /// A [`Subscription`] handle for cancelling the subscription.
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `channel` is empty or contains wildcards.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    /// * [`KubemqError::Authentication`] (gRPC `UNAUTHENTICATED`) — if the auth token is invalid.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: KubemqClient) -> kubemq::Result<()> {
    /// let client_ref = client.clone();
    /// let sub = client.subscribe_to_commands(
    ///     "device.reboot",
    ///     "",
    ///     move |cmd| {
    ///         let c = client_ref.clone();
    ///         Box::pin(async move {
    ///             let reply = CommandReply::builder()
    ///                 .request_id(&cmd.id)
    ///                 .response_to(&cmd.response_to)
    ///                 .build();
    ///             let _ = c.send_command_response(reply).await;
    ///         })
    ///     },
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::type_complexity)]
    pub async fn subscribe_to_commands(
        &self,
        channel: &str,
        group: &str,
        on_command: impl Fn(CommandReceive) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
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
        self.check_state("subscribe_commands")?;
        validate::validate_channel(channel, "subscribe_to_commands")?;
        validate::validate_no_wildcards(channel, "subscribe_to_commands")?;
        validate::validate_client_id(&self.config().client_id, "subscribe_to_commands")?;

        let subscribe_request = proto::Subscribe {
            subscribe_type_data: proto::subscribe::SubscribeType::Commands as i32,
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
                match client
                    .subscribe_to_requests(subscribe_request.clone())
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let err =
                            KubemqError::from_grpc_status(status, "subscribe_to_commands", "");
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
                            Ok(Some(req)) => {
                                retry_count = 0;
                                let cmd_recv = CommandReceive {
                                    id: req.request_id,
                                    client_id: req.client_id,
                                    channel: req.channel,
                                    metadata: req.metadata,
                                    body: req.body,
                                    response_to: req.reply_channel,
                                    tags: req.tags,
                                    span: req.span,
                                };
                                // REQ-M55: catch_unwind for async callback
                                let result = std::panic::AssertUnwindSafe(on_command(cmd_recv))
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
                                        match c.subscribe_to_requests(subscribe_request.clone()).await {
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
                                    "subscribe_to_commands",
                                    "",
                                );
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
                                match client_handle.transport().client() {
                                    Ok(mut c) => {
                                        match c.subscribe_to_requests(subscribe_request.clone()).await {
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
            format!("commands-{}", channel),
            cancel,
            done_rx,
        ))
    }

    /// Sends a command response back to the sender (from a subscriber handler).
    ///
    /// The `request_id` and `response_to` fields must match the values from the
    /// received [`CommandReceive`].
    ///
    /// # Arguments
    ///
    /// * `reply` - The [`CommandReply`] to send. Build with [`CommandReply::builder()`].
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `request_id` or `response_to` is empty.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient, cmd: &CommandReceive) -> kubemq::Result<()> {
    /// let reply = CommandReply::builder()
    ///     .request_id(&cmd.id)
    ///     .response_to(&cmd.response_to)
    ///     .build();
    /// client.send_command_response(reply).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_command_response(&self, reply: CommandReply) -> crate::Result<()> {
        self.check_closed()?;
        validate::validate_request_id(&reply.request_id, "send_command_response")?;
        validate::validate_response_to(&reply.response_to, "send_command_response")?;

        let cid = if reply.client_id.is_empty() {
            self.config().client_id.clone()
        } else {
            reply.client_id
        };

        let error_str = reply.error.unwrap_or_default();
        let executed = error_str.is_empty();

        let proto_response = proto::Response {
            client_id: cid,
            request_id: reply.request_id,
            reply_channel: reply.response_to,
            metadata: reply.metadata,
            body: reply.body,
            cache_hit: false,
            timestamp: reply.executed_at,
            executed,
            error: error_str,
            span: reply.span,
            tags: reply.tags,
        };

        let mut request = tonic::Request::new(proto_response);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        client
            .send_response(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_command_response", ""))?;

        Ok(())
    }

    /// Convenience method to send a command with minimal parameters.
    ///
    /// Equivalent to building a [`Command`] and calling [`send_command()`](Self::send_command).
    ///
    /// # Arguments
    ///
    /// * `channel` - Target channel name.
    /// * `body` - Command payload bytes.
    /// * `timeout` - Maximum time to wait for a response.
    ///
    /// # Errors
    ///
    /// Same as [`send_command()`](Self::send_command).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    /// use std::time::Duration;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let resp = client.send_command_simple("device.reboot", b"42".to_vec(), Duration::from_secs(5)).await?;
    /// println!("Executed: {}", resp.executed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_command_simple(
        &self,
        channel: &str,
        body: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> crate::Result<CommandResponse> {
        let command = Command::builder()
            .channel(channel)
            .body(body)
            .timeout(timeout)
            .build();
        self.send_command(command).await // Passes owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- CommandBuilder tests --

    #[test]
    fn test_command_builder_default() {
        let builder = CommandBuilder::default();
        let cmd = builder.build();
        assert!(cmd.id.is_empty());
        assert!(cmd.channel.is_empty());
        assert!(cmd.metadata.is_empty());
        assert!(cmd.body.is_empty());
        assert_eq!(cmd.timeout, Duration::from_secs(5));
        assert!(cmd.client_id.is_empty());
        assert!(cmd.tags.is_empty());
        assert!(cmd.span.is_empty());
    }

    #[test]
    fn test_command_builder_all_fields() {
        let cmd = Command::builder()
            .id("cmd-1")
            .channel("cmd-channel")
            .metadata("cmd-meta")
            .body(b"cmd-body".to_vec())
            .timeout(Duration::from_secs(10))
            .client_id("cmd-client")
            .add_tag("k", "v")
            .build();
        assert_eq!(cmd.id, "cmd-1");
        assert_eq!(cmd.channel, "cmd-channel");
        assert_eq!(cmd.metadata, "cmd-meta");
        assert_eq!(cmd.body, b"cmd-body");
        assert_eq!(cmd.timeout, Duration::from_secs(10));
        assert_eq!(cmd.client_id, "cmd-client");
        assert_eq!(cmd.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_command_builder_tags() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "test".to_string());
        let cmd = Command::builder().channel("ch").tags(tags).build();
        assert_eq!(cmd.tags.len(), 1);
        assert_eq!(cmd.tags.get("env").unwrap(), "test");
    }

    #[test]
    fn test_command_debug_clone() {
        let cmd = Command::builder().id("c1").channel("ch").build();
        let debug = format!("{:?}", cmd);
        assert!(debug.contains("c1"));
        let cloned = cmd.clone();
        assert_eq!(cloned.id, "c1");
    }

    // -- CommandResponse tests --

    #[test]
    fn test_command_response_into_result_success() {
        let resp = CommandResponse {
            command_id: "r1".to_string(),
            response_client_id: "client".to_string(),
            executed: true,
            executed_at: 12345,
            error: String::new(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_ok());
        let val = r.unwrap();
        assert_eq!(val.command_id, "r1");
        assert!(val.executed);
    }

    #[test]
    fn test_command_response_into_result_error_with_message() {
        let resp = CommandResponse {
            command_id: "r2".to_string(),
            response_client_id: String::new(),
            executed: false,
            executed_at: 0,
            error: "handler error".to_string(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_err());
    }

    #[test]
    fn test_command_response_into_result_error_no_message() {
        let resp = CommandResponse {
            command_id: "r3".to_string(),
            response_client_id: String::new(),
            executed: false,
            executed_at: 0,
            error: String::new(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_err());
    }

    #[test]
    fn test_command_response_debug_clone() {
        let resp = CommandResponse {
            command_id: "r1".to_string(),
            response_client_id: "c".to_string(),
            executed: true,
            executed_at: 100,
            error: String::new(),
            tags: HashMap::new(),
        };
        let debug = format!("{:?}", resp);
        assert!(debug.contains("CommandResponse"));
        let cloned = resp.clone();
        assert_eq!(cloned.command_id, "r1");
    }

    // -- CommandReceive tests --

    #[test]
    fn test_command_receive_debug_clone() {
        let recv = CommandReceive {
            id: "cr1".to_string(),
            client_id: "client".to_string(),
            channel: "ch".to_string(),
            metadata: String::new(),
            body: vec![],
            response_to: "reply-ch".to_string(),
            tags: HashMap::new(),
            span: vec![],
        };
        let debug = format!("{:?}", recv);
        assert!(debug.contains("CommandReceive"));
        let cloned = recv.clone();
        assert_eq!(cloned.response_to, "reply-ch");
    }

    // -- CommandReplyBuilder tests --

    #[test]
    fn test_command_reply_builder_default() {
        let builder = CommandReplyBuilder::default();
        let reply = builder.build();
        assert!(reply.request_id.is_empty());
        assert!(reply.response_to.is_empty());
        assert!(reply.metadata.is_empty());
        assert!(reply.body.is_empty());
        assert!(reply.client_id.is_empty());
        assert_eq!(reply.executed_at, 0);
        assert!(reply.error.is_none());
        assert!(reply.tags.is_empty());
    }

    #[test]
    fn test_command_reply_builder_all_fields() {
        let mut tags = HashMap::new();
        tags.insert("k".to_string(), "v".to_string());
        let reply = CommandReply::builder()
            .request_id("req-1")
            .response_to("reply-ch")
            .metadata("meta")
            .body(b"body".to_vec())
            .client_id("client")
            .executed_at(999)
            .error("some error")
            .tags(tags)
            .build();
        assert_eq!(reply.request_id, "req-1");
        assert_eq!(reply.response_to, "reply-ch");
        assert_eq!(reply.metadata, "meta");
        assert_eq!(reply.body, b"body");
        assert_eq!(reply.client_id, "client");
        assert_eq!(reply.executed_at, 999);
        assert_eq!(reply.error, Some("some error".to_string()));
        assert_eq!(reply.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_command_reply_debug_clone() {
        let reply = CommandReply::builder().request_id("req-1").build();
        let debug = format!("{:?}", reply);
        assert!(debug.contains("CommandReply"));
        let cloned = reply.clone();
        assert_eq!(cloned.request_id, "req-1");
    }

    // -- Command span round-trip tests (Spec 3.10) --

    #[test]
    fn test_command_reply_span_default_empty() {
        let reply = CommandReply::builder()
            .request_id("req-1")
            .response_to("reply-ch")
            .build();
        assert!(reply.span.is_empty(), "Default span should be empty");
    }

    #[test]
    fn test_command_reply_span_forwarded_to_proto() {
        let span_data = vec![10, 20, 30, 40];
        let reply = CommandReply {
            request_id: "req-1".to_string(),
            response_to: "reply-ch".to_string(),
            metadata: String::new(),
            body: Vec::new(),
            client_id: "client".to_string(),
            executed_at: 0,
            error: None,
            tags: HashMap::new(),
            span: span_data.clone(),
        };

        let error_str = reply.error.clone().unwrap_or_default();
        let executed = error_str.is_empty();

        let proto_response = proto::Response {
            client_id: reply.client_id.clone(),
            request_id: reply.request_id.clone(),
            reply_channel: reply.response_to.clone(),
            metadata: reply.metadata.clone(),
            body: reply.body.clone(),
            cache_hit: false,
            timestamp: reply.executed_at,
            executed,
            error: error_str,
            span: reply.span.clone(),
            tags: reply.tags.clone(),
        };

        assert_eq!(proto_response.span, vec![10, 20, 30, 40]);
        assert!(proto_response.executed);
    }
}
