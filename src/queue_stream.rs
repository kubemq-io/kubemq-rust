//! Queue messaging pattern - stream API types and client methods.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq as proto;
use crate::queues::{
    proto_to_queue_message, queue_message_to_proto_owned, QueueMessage, QueueSendResult,
};
use crate::validate;

/// Request types for the queue downstream stream protocol.
///
/// These map 1:1 to the proto `QueuesDownstreamRequestType` enum values.
/// Most users interact with settlement methods on [`PollResponse`] and
/// [`QueueDownstreamMessage`] rather than using these directly.
#[non_exhaustive]
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueDownstreamType {
    /// Fetch messages from the queue.
    Get = 1,
    /// Acknowledge all messages in the current transaction.
    AckAll = 2,
    /// Acknowledge a specific range of messages by sequence.
    AckRange = 3,
    /// Reject all messages in the current transaction (return to queue).
    NackAll = 4,
    /// Reject a specific range of messages by sequence.
    NackRange = 5,
    /// Re-queue all messages to a different channel.
    RequeueAll = 6,
    /// Re-queue a specific range of messages to a different channel.
    RequeueRange = 7,
    /// Query active message offsets.
    ActiveOffsets = 8,
    /// Query the current transaction status.
    TransactionStatus = 9,
    /// Client-initiated stream close.
    CloseByClient = 10,
    /// Server-initiated stream close.
    CloseByServer = 11,
}

/// Backward-compatible `i32` constants for [`QueueDownstreamType`] variants.
///
/// Prefer using the enum directly. These constants exist for compatibility
/// with older code that used raw `i32` values.
pub mod queue_downstream_type {
    use super::QueueDownstreamType;
    pub const GET: i32 = QueueDownstreamType::Get as i32;
    pub const ACK_ALL: i32 = QueueDownstreamType::AckAll as i32;
    pub const ACK_RANGE: i32 = QueueDownstreamType::AckRange as i32;
    pub const NACK_ALL: i32 = QueueDownstreamType::NackAll as i32;
    pub const NACK_RANGE: i32 = QueueDownstreamType::NackRange as i32;
    pub const REQUEUE_ALL: i32 = QueueDownstreamType::RequeueAll as i32;
    pub const REQUEUE_RANGE: i32 = QueueDownstreamType::RequeueRange as i32;
    pub const ACTIVE_OFFSETS: i32 = QueueDownstreamType::ActiveOffsets as i32;
    pub const TRANSACTION_STATUS: i32 = QueueDownstreamType::TransactionStatus as i32;
    pub const CLOSE_BY_CLIENT: i32 = QueueDownstreamType::CloseByClient as i32;
    pub const CLOSE_BY_SERVER: i32 = QueueDownstreamType::CloseByServer as i32;
}

/// Handle for a bidirectional queue upstream (send) stream.
///
/// Obtained from [`KubemqClient::queue_upstream()`]. Provides a
/// non-blocking [`send()`](Self::send) method for high-throughput batch
/// publishing and a [`results()`](Self::results) receiver for per-batch
/// acknowledgements. The stream is cancelled on [`close()`](Self::close)
/// or when the handle is dropped.
pub struct QueueUpstreamHandle {
    sender: tokio::sync::mpsc::Sender<(String, Vec<QueueMessage>)>,
    results: tokio::sync::mpsc::Receiver<QueueUpstreamResult>,
    cancel: CancellationToken,
}

impl QueueUpstreamHandle {
    /// Send a batch of messages with a request ID.
    pub async fn send(&self, request_id: &str, msgs: Vec<QueueMessage>) -> crate::Result<()> {
        // REQ-H5: Validate before sending to stream
        for (i, msg) in msgs.iter().enumerate() {
            let op = format!("queue_upstream.send[{}]", i);
            validate::validate_channel(&msg.channel, &op)?;
            validate::validate_no_wildcards(&msg.channel, &op)?;
            validate::validate_tags(&msg.tags, &op, &msg.channel)?;
        }

        self.sender
            .send((request_id.to_string(), msgs))
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "upstream stream closed".to_string(),
                operation: "queue_upstream.send".to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The upstream stream has been closed. Open a new stream.",
            })
    }

    /// Get a mutable reference to the results receiver.
    pub fn results(&mut self) -> &mut tokio::sync::mpsc::Receiver<QueueUpstreamResult> {
        &mut self.results
    }

    /// Close the upstream stream.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for QueueUpstreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueUpstreamHandle")
            .field("sender", &"<mpsc::Sender>")
            .field("results", &"<mpsc::Receiver>")
            .finish()
    }
}

impl Drop for QueueUpstreamHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Per-batch result from a queue upstream send operation.
///
/// Received via [`QueueUpstreamHandle::results()`].
#[derive(Debug, Clone)]
pub struct QueueUpstreamResult {
    /// The `request_id` from the corresponding [`QueueUpstreamHandle::send()`] call.
    pub ref_request_id: String,
    /// Per-message send results in the same order as the batch.
    pub results: Vec<QueueSendResult>,
    /// `true` if the entire batch operation failed.
    pub is_error: bool,
    /// Batch-level error message when `is_error` is `true`.
    pub error: String,
}

/// Persistent queue downstream receiver for polling and settling messages.
///
/// Obtained from [`KubemqClient::new_queue_downstream_receiver()`]. The
/// receiver holds a bidirectional gRPC stream and provides transactional
/// message processing via [`poll()`](Self::poll).
///
/// `poll()` takes `&mut self` to prevent concurrent calls at compile time.
/// Call [`close()`](Self::close) or drop the receiver to release resources.
pub struct QueueDownstreamReceiver {
    sender: tokio::sync::mpsc::Sender<proto::QueuesDownstreamRequest>,
    receiver: tokio::sync::mpsc::Receiver<proto::QueuesDownstreamResponse>, // REQ-M52: no Arc<Mutex>
    client_id: String,
    cancel: CancellationToken,
}

impl QueueDownstreamReceiver {
    /// Poll for messages. Takes `&mut self` to prevent concurrent calls at compile time (REQ-H14).
    pub async fn poll(&mut self, request: PollRequest) -> crate::Result<PollResponse> {
        // REQ-M10: Input validation
        validate::validate_channel(&request.channel, "poll")?;
        validate::validate_no_wildcards(&request.channel, "poll")?;
        if request.max_items == 0 {
            return Err(KubemqError::Validation {
                code: ErrorCode::Validation,
                message: "max_items must be > 0".to_string(),
                operation: "poll".to_string(),
                channel: request.channel.clone(),
                suggestion: "Set max_items to at least 1.",
            });
        }

        // REQ-M50: Overflow-safe conversion
        let wait_timeout_ms = if request.wait_timeout_seconds < 1 {
            1000
        } else {
            i32::checked_mul(request.wait_timeout_seconds, 1000).unwrap_or(i32::MAX)
        };

        let downstream_req = proto::QueuesDownstreamRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            request_type_data: QueueDownstreamType::Get as i32, // REQ-M32: enum type
            channel: request.channel.clone(),
            max_items: request.max_items,
            wait_timeout: wait_timeout_ms,
            auto_ack: request.auto_ack,
            re_queue_channel: String::new(),
            sequence_range: Vec::new(),
            ref_transaction_id: String::new(),
            metadata: std::collections::HashMap::new(),
        };

        self.sender
            .send(downstream_req)
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "downstream stream closed".to_string(),
                operation: "poll".to_string(),
                channel: request.channel.clone(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The downstream stream has been closed. Create a new receiver.",
            })?;

        // REQ-H13: Local timeout -- wait_timeout + 5s grace
        let timeout_duration = Duration::from_secs(request.wait_timeout_seconds as u64 + 5);
        let resp = tokio::time::timeout(timeout_duration, self.receiver.recv())
            .await
            .map_err(|_| KubemqError::Timeout {
                code: ErrorCode::Timeout,
                message: "Queue poll timed out waiting for server response".into(),
                operation: "poll".into(),
                channel: request.channel.clone(),
                is_retryable: true,
                source: None,
                request_id: String::new(),
                suggestion: "Increase wait_timeout_seconds or check server health",
            })?
            .ok_or_else(|| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "Downstream channel closed".into(),
                operation: "poll".into(),
                channel: request.channel.clone(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "Re-open the downstream receiver.",
            })?;

        // REQ-M46: Check is_error
        if resp.is_error {
            return Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message: resp.error,
                operation: "poll".to_string(),
                channel: request.channel,
                is_retryable: true,
                source: None,
                request_id: resp.ref_request_id,
                suggestion: "Retry the poll operation.",
            });
        }

        let transaction_id = resp.transaction_id.clone();
        let sender_ref = self.sender.clone();
        let client_id = self.client_id.clone();

        let messages: Vec<QueueDownstreamMessage> = resp
            .messages
            .into_iter()
            .map(|m| {
                let seq = m.attributes.as_ref().map(|a| a.sequence).unwrap_or(0);
                QueueDownstreamMessage {
                    message: proto_to_queue_message(m),
                    transaction_id: transaction_id.clone(),
                    sequence: seq,
                    sender: sender_ref.clone(),
                    client_id: client_id.clone(),
                }
            })
            .collect();

        Ok(PollResponse {
            transaction_id: transaction_id.clone(),
            messages,
            is_error: resp.is_error,
            error: resp.error,
            transaction_complete: resp.transaction_complete, // REQ-M2
            sender: sender_ref.clone(),
            client_id: client_id.clone(),
        })
    }

    /// Close the receiver and release resources.
    pub async fn close(&self) -> crate::Result<()> {
        // Send close request
        let close_req = proto::QueuesDownstreamRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            request_type_data: QueueDownstreamType::CloseByClient as i32, // REQ-M32: enum type
            channel: String::new(),
            max_items: 0,
            wait_timeout: 0,
            auto_ack: false,
            re_queue_channel: String::new(),
            sequence_range: Vec::new(),
            ref_transaction_id: String::new(),
            metadata: std::collections::HashMap::new(),
        };
        let _ = self.sender.send(close_req).await;
        self.cancel.cancel();
        Ok(())
    }
}

impl std::fmt::Debug for QueueDownstreamReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueDownstreamReceiver")
            .field("sender", &"<mpsc::Sender>")
            .field("receiver", &"<mpsc::Receiver>")
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl Drop for QueueDownstreamReceiver {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let sender = self.sender.clone();
            handle.spawn(async move {
                let close_req = proto::QueuesDownstreamRequest {
                    request_type_data: QueueDownstreamType::CloseByClient as i32,
                    ..Default::default()
                };
                let _ = sender.send(close_req).await;
            });
        }
    }
}

/// A received queue message with per-message settlement methods.
///
/// Each message can be individually acknowledged ([`ack()`](Self::ack)),
/// rejected ([`nack()`](Self::nack)), or re-queued
/// ([`re_queue()`](Self::re_queue)). Settlement operations are sent
/// over the same downstream stream.
pub struct QueueDownstreamMessage {
    /// The underlying queue message with payload, tags, and attributes.
    pub message: QueueMessage,
    /// Transaction identifier for this poll batch.
    pub transaction_id: String,
    /// Sequence number of this message within the queue.
    pub sequence: u64,
    sender: tokio::sync::mpsc::Sender<proto::QueuesDownstreamRequest>,
    client_id: String,
}

impl std::fmt::Debug for QueueDownstreamMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueDownstreamMessage")
            .field("message", &self.message)
            .field("transaction_id", &self.transaction_id)
            .field("sequence", &self.sequence)
            .finish()
    }
}

impl QueueDownstreamMessage {
    async fn send_settlement(
        &self,
        request_type: i32,
        re_queue_channel: &str,
        operation: &'static str,
    ) -> crate::Result<()> {
        let req = proto::QueuesDownstreamRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            request_type_data: request_type,
            channel: String::new(),
            max_items: 0,
            wait_timeout: 0,
            auto_ack: false,
            re_queue_channel: re_queue_channel.to_string(),
            sequence_range: vec![self.sequence as i64],
            ref_transaction_id: self.transaction_id.clone(),
            metadata: std::collections::HashMap::new(),
        };
        self.sender
            .send(req)
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "downstream stream closed".to_string(),
                operation: operation.to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The downstream stream has been closed.",
            })
    }

    /// Acknowledge this individual message.
    pub async fn ack(&self) -> crate::Result<()> {
        self.send_settlement(queue_downstream_type::ACK_RANGE, "", "ack")
            .await
    }

    /// Reject this individual message (returns to queue).
    pub async fn nack(&self) -> crate::Result<()> {
        self.send_settlement(queue_downstream_type::NACK_RANGE, "", "nack")
            .await
    }

    /// Re-queue this message to the specified channel.
    pub async fn re_queue(&self, channel: &str) -> crate::Result<()> {
        self.send_settlement(queue_downstream_type::REQUEUE_RANGE, channel, "re_queue")
            .await
    }
}

/// Configuration for a queue poll operation.
///
/// Passed to [`QueueDownstreamReceiver::poll()`] or
/// [`KubemqClient::poll_queue()`].
#[derive(Debug, Clone)]
pub struct PollRequest {
    /// Queue channel to poll from (required).
    pub channel: String,
    /// Maximum number of messages to receive. Must be > 0.
    pub max_items: i32,
    /// Maximum seconds to wait for messages. Values < 1 default to 1 second.
    pub wait_timeout_seconds: i32,
    /// When `true`, messages are automatically acknowledged upon delivery.
    pub auto_ack: bool,
}

/// Response from a queue poll operation containing received messages.
///
/// Provides batch settlement methods ([`ack_all()`](Self::ack_all),
/// [`nack_all()`](Self::nack_all), [`re_queue_all()`](Self::re_queue_all))
/// and individual settlement via each [`QueueDownstreamMessage`].
pub struct PollResponse {
    /// Server-assigned transaction identifier for this poll batch.
    pub transaction_id: String,
    /// Received messages with per-message settlement methods.
    pub messages: Vec<QueueDownstreamMessage>,
    /// `true` if the server reported an error.
    pub is_error: bool,
    /// Server-provided error message when `is_error` is `true`.
    pub error: String,
    /// `true` if all messages in the transaction have been settled.
    pub transaction_complete: bool,
    sender: tokio::sync::mpsc::Sender<proto::QueuesDownstreamRequest>,
    client_id: String,
}

impl std::fmt::Debug for PollResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollResponse")
            .field("transaction_id", &self.transaction_id)
            .field("messages", &self.messages)
            .field("is_error", &self.is_error)
            .field("error", &self.error)
            .field("transaction_complete", &self.transaction_complete)
            .finish()
    }
}

impl PollResponse {
    async fn send_batch_settlement(
        &self,
        request_type: i32,
        re_queue_channel: &str,
        operation: &'static str,
    ) -> crate::Result<()> {
        let req = proto::QueuesDownstreamRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            request_type_data: request_type,
            channel: String::new(),
            max_items: 0,
            wait_timeout: 0,
            auto_ack: false,
            re_queue_channel: re_queue_channel.to_string(),
            sequence_range: Vec::new(),
            ref_transaction_id: self.transaction_id.clone(),
            metadata: std::collections::HashMap::new(),
        };
        self.sender
            .send(req)
            .await
            .map_err(|_| KubemqError::Transient {
                code: ErrorCode::Transient,
                message: "downstream stream closed".to_string(),
                operation: operation.to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: String::new(),
                suggestion: "The downstream stream has been closed.",
            })
    }

    /// Acknowledge all messages in this poll response.
    pub async fn ack_all(&self) -> crate::Result<()> {
        self.send_batch_settlement(queue_downstream_type::ACK_ALL, "", "ack_all")
            .await
    }

    /// Reject all messages (return to queue).
    pub async fn nack_all(&self) -> crate::Result<()> {
        self.send_batch_settlement(queue_downstream_type::NACK_ALL, "", "nack_all")
            .await
    }

    /// Re-queue all messages to the specified channel.
    pub async fn re_queue_all(&self, channel: &str) -> crate::Result<()> {
        self.send_batch_settlement(queue_downstream_type::REQUEUE_ALL, channel, "re_queue_all")
            .await
    }
}

// Client methods for Queue Stream API
impl KubemqClient {
    /// Opens a bidirectional queue upstream (send) stream.
    ///
    /// Returns a [`QueueUpstreamHandle`] that provides high-throughput batch
    /// publishing via [`send()`](QueueUpstreamHandle::send) and per-batch
    /// confirmations via [`results()`](QueueUpstreamHandle::results).
    ///
    /// # Returns
    ///
    /// A [`QueueUpstreamHandle`] for sending message batches.
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
    /// let mut handle = client.queue_upstream().await?;
    /// let msgs = vec![
    ///     QueueMessage::builder().channel("tasks").body(b"task-1".to_vec()).build(),
    /// ];
    /// handle.send("batch-1", msgs).await?;
    /// handle.close();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn queue_upstream(&self) -> crate::Result<QueueUpstreamHandle> {
        self.check_state("queue_upstream")?;

        let client_id = self.config().client_id.clone();
        let cancel = self.child_token();

        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<(String, Vec<QueueMessage>)>(256);
        let (result_tx, result_rx) = tokio::sync::mpsc::channel::<QueueUpstreamResult>(64);

        let (proto_tx, proto_rx) = tokio::sync::mpsc::channel::<proto::QueuesUpstreamRequest>(256);
        let proto_stream = tokio_stream::wrappers::ReceiverStream::new(proto_rx);

        let mut grpc_client = self.transport().client()?;

        // Forward messages to proto.
        // Must be spawned BEFORE the gRPC call so messages can flow while the
        // server establishes the stream.
        let cid = client_id.clone();
        let ptx = proto_tx.clone();
        let cancel_fwd = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_fwd.cancelled() => break,
                    item = msg_rx.recv() => {
                        match item {
                            Some((request_id, msgs)) => {
                                let proto_msgs: Vec<proto::QueueMessage> = msgs
                                    .into_iter()
                                    .map(|m| queue_message_to_proto_owned(m, &cid))
                                    .collect();
                                let req = proto::QueuesUpstreamRequest {
                                    request_id,
                                    messages: proto_msgs,
                                };
                                if ptx.send(req).await.is_err() {
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
            let response = match grpc_client.queues_upstream(proto_stream).await {
                Ok(resp) => resp,
                Err(status) => {
                    let err_result = QueueUpstreamResult {
                        ref_request_id: String::new(),
                        results: Vec::new(),
                        is_error: true,
                        error: format!("Stream setup failed: {}", status.message()),
                    };
                    let _ = result_tx.send(err_result).await;
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
                                let upstream_result = QueueUpstreamResult {
                                    ref_request_id: r.ref_request_id,
                                    results: r.results.into_iter().map(|sr| QueueSendResult {
                                        message_id: sr.message_id,
                                        sent_at: sr.sent_at,
                                        expiration_at: sr.expiration_at,
                                        delayed_to: sr.delayed_to,
                                        is_error: sr.is_error,
                                        error: sr.error,
                                    }).collect(),
                                    is_error: r.is_error,
                                    error: r.error,
                                };
                                let _ = result_tx.send(upstream_result).await;
                            }
                            Ok(None) => break,
                            Err(status) => {
                                let err_result = QueueUpstreamResult {
                                    ref_request_id: String::new(),
                                    results: Vec::new(),
                                    is_error: true,
                                    error: format!("Stream error: {}", status.message()),
                                };
                                let _ = result_tx.send(err_result).await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(QueueUpstreamHandle {
            sender: msg_tx,
            results: result_rx,
            cancel,
        })
    }

    /// Creates a persistent queue downstream receiver for polling and settling messages.
    ///
    /// The receiver holds a bidirectional gRPC stream. Use
    /// [`poll()`](QueueDownstreamReceiver::poll) to fetch messages and settle
    /// them via `ack()`, `nack()`, or `re_queue()` on each
    /// [`QueueDownstreamMessage`], or batch-settle via [`PollResponse`] methods.
    ///
    /// # Returns
    ///
    /// A [`QueueDownstreamReceiver`] for polling messages.
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
    /// let mut receiver = client.new_queue_downstream_receiver().await?;
    /// let resp = receiver.poll(PollRequest {
    ///     channel: "tasks".to_string(),
    ///     max_items: 10,
    ///     wait_timeout_seconds: 5,
    ///     auto_ack: false,
    /// }).await?;
    ///
    /// for msg in &resp.messages {
    ///     msg.ack().await?;
    /// }
    /// receiver.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_queue_downstream_receiver(&self) -> crate::Result<QueueDownstreamReceiver> {
        self.check_state("queue_downstream")?;

        let client_id = self.config().client_id.clone();
        let cancel = self.child_token();

        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<proto::QueuesDownstreamRequest>(256);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel::<proto::QueuesDownstreamResponse>(64);

        let proto_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);

        let mut grpc_client = self.transport().client()?;

        // Spawn gRPC stream establishment + response reading in a background task.
        // This avoids blocking if the server waits for the first client message
        // before sending response headers (common in bidirectional streaming).
        let cancel_read = cancel.clone();
        tokio::spawn(async move {
            let response = match grpc_client.queues_downstream(proto_stream).await {
                Ok(resp) => resp,
                Err(status) => {
                    // REQ-H12: Surface setup error as a response with is_error=true
                    let err_resp = proto::QueuesDownstreamResponse {
                        transaction_id: String::new(),
                        ref_request_id: String::new(),
                        request_type_data: 0,
                        messages: Vec::new(),
                        active_offsets: Vec::new(),
                        is_error: true,
                        error: format!("stream setup failed: {}", status.message()),
                        transaction_complete: false,
                        metadata: std::collections::HashMap::new(),
                    };
                    let _ = resp_tx.send(err_resp).await;
                    return;
                }
            };

            let mut result_stream = response.into_inner();
            // Read responses and forward -- REQ-H12: surface stream errors
            loop {
                tokio::select! {
                    _ = cancel_read.cancelled() => break,
                    result = result_stream.message() => {
                        match result {
                            Ok(Some(r)) => {
                                if resp_tx.send(r).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => break,
                            Err(status) => {
                                // REQ-H12: Surface stream error as a response with is_error=true
                                let err_resp = proto::QueuesDownstreamResponse {
                                    transaction_id: String::new(),
                                    ref_request_id: String::new(),
                                    request_type_data: 0,
                                    messages: Vec::new(),
                                    active_offsets: Vec::new(),
                                    is_error: true,
                                    error: format!("stream error: {}", status.message()),
                                    transaction_complete: false,
                                    metadata: std::collections::HashMap::new(),
                                };
                                let _ = resp_tx.send(err_resp).await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(QueueDownstreamReceiver {
            sender: req_tx,
            receiver: resp_rx, // REQ-M52: no Arc<Mutex>
            client_id,
            cancel,
        })
    }

    /// Convenience method for a single poll operation.
    ///
    /// Creates a downstream receiver, polls once, and returns both the response
    /// and the receiver. The caller must settle (ack/nack) messages before
    /// dropping the receiver. For auto-ack use cases, set `auto_ack: true`
    /// in the [`PollRequest`].
    ///
    /// # Arguments
    ///
    /// * `request` - A [`PollRequest`] specifying the channel, max items, and timeout.
    ///
    /// # Returns
    ///
    /// A tuple of ([`PollResponse`], [`QueueDownstreamReceiver`]).
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Validation`] — if `channel` is empty, contains wildcards, or `max_items` is zero.
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    /// * [`KubemqError::Timeout`] — if the poll times out waiting for server response. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let (resp, _receiver) = client.poll_queue(PollRequest {
    ///     channel: "tasks".to_string(),
    ///     max_items: 5,
    ///     wait_timeout_seconds: 10,
    ///     auto_ack: true,
    /// }).await?;
    ///
    /// for msg in &resp.messages {
    ///     println!("Received: {}", String::from_utf8_lossy(&msg.message.body));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn poll_queue(
        &self,
        request: PollRequest,
    ) -> crate::Result<(PollResponse, QueueDownstreamReceiver)> {
        let mut receiver = self.new_queue_downstream_receiver().await?;
        let response = receiver.poll(request).await?;
        Ok((response, receiver))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that dropping a QueueUpstreamHandle cancels its token.
    #[test]
    fn test_upstream_handle_drop_cancels_task() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_result_tx, results) = tokio::sync::mpsc::channel(1);
        {
            let _handle = QueueUpstreamHandle {
                sender,
                results,
                cancel,
            };
            assert!(!token.is_cancelled());
        }
        assert!(token.is_cancelled(), "Drop should cancel the token");
    }

    /// Test that dropping a QueueDownstreamReceiver cancels its token.
    #[test]
    fn test_downstream_receiver_drop_cancels_task() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        {
            let _recv = QueueDownstreamReceiver {
                sender,
                receiver,
                client_id: "test-client".to_string(),
                cancel,
            };
            assert!(!token.is_cancelled());
        }
        assert!(token.is_cancelled(), "Drop should cancel the token");
    }

    /// Test that poll requires &mut self (compile-time enforcement).
    /// This test documents the API contract: poll() takes &mut self,
    /// preventing concurrent poll() calls at the type level.
    #[test]
    fn test_poll_requires_mut_self() {
        // This is a compile-time verification test.
        // The function signature `pub async fn poll(&mut self, ...)` prevents
        // concurrent poll calls. If this test compiles, the contract holds.
        fn _assert_poll_takes_mut_self(r: &mut QueueDownstreamReceiver) {
            let _f = r.poll(PollRequest {
                channel: "ch".to_string(),
                max_items: 1,
                wait_timeout_seconds: 1,
                auto_ack: false,
            });
        }
    }

    /// Test that poll validates empty channel.
    #[tokio::test]
    async fn test_poll_validates_empty_channel() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        let mut recv = QueueDownstreamReceiver {
            sender,
            receiver,
            client_id: "test-client".to_string(),
            cancel,
        };

        let result = recv
            .poll(PollRequest {
                channel: String::new(),
                max_items: 1,
                wait_timeout_seconds: 1,
                auto_ack: false,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    /// Test that poll validates max_items > 0.
    #[tokio::test]
    async fn test_poll_validates_max_items() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        let mut recv = QueueDownstreamReceiver {
            sender,
            receiver,
            client_id: "test-client".to_string(),
            cancel,
        };

        let result = recv
            .poll(PollRequest {
                channel: "test-ch".to_string(),
                max_items: 0,
                wait_timeout_seconds: 1,
                auto_ack: false,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    /// Test that poll validates no wildcards in channel.
    #[tokio::test]
    async fn test_poll_validates_no_wildcards() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        let mut recv = QueueDownstreamReceiver {
            sender,
            receiver,
            client_id: "test-client".to_string(),
            cancel,
        };

        let result = recv
            .poll(PollRequest {
                channel: "test.*".to_string(),
                max_items: 1,
                wait_timeout_seconds: 1,
                auto_ack: false,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    /// Test QueueDownstreamType enum values.
    #[test]
    fn test_queue_downstream_type_values() {
        assert_eq!(QueueDownstreamType::Get as i32, 1);
        assert_eq!(QueueDownstreamType::AckAll as i32, 2);
        assert_eq!(QueueDownstreamType::NackAll as i32, 4);
        assert_eq!(QueueDownstreamType::CloseByClient as i32, 10);
        assert_eq!(QueueDownstreamType::CloseByServer as i32, 11);
    }

    /// Test backward-compatible constants match enum values.
    #[test]
    fn test_queue_downstream_type_constants() {
        assert_eq!(queue_downstream_type::GET, QueueDownstreamType::Get as i32);
        assert_eq!(
            queue_downstream_type::ACK_ALL,
            QueueDownstreamType::AckAll as i32
        );
        assert_eq!(
            queue_downstream_type::CLOSE_BY_CLIENT,
            QueueDownstreamType::CloseByClient as i32
        );
    }

    /// Test Debug implementations.
    #[test]
    fn test_debug_impls() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel::<(String, Vec<QueueMessage>)>(1);
        let (_result_tx, results) = tokio::sync::mpsc::channel::<QueueUpstreamResult>(1);
        let handle = QueueUpstreamHandle {
            sender,
            results,
            cancel,
        };
        let debug_str = format!("{:?}", handle);
        assert!(
            debug_str.contains("QueueUpstreamHandle"),
            "Debug should show type name"
        );

        let cancel2 = CancellationToken::new();
        let (sender2, _rx2) = tokio::sync::mpsc::channel(1);
        let (_tx2, receiver2) = tokio::sync::mpsc::channel(1);
        let recv = QueueDownstreamReceiver {
            sender: sender2,
            receiver: receiver2,
            client_id: "cid".to_string(),
            cancel: cancel2,
        };
        let debug_str2 = format!("{:?}", recv);
        assert!(debug_str2.contains("QueueDownstreamReceiver"));
        assert!(debug_str2.contains("cid"));
    }

    /// Test PollRequest derives.
    #[test]
    fn test_poll_request_debug_clone() {
        let req = PollRequest {
            channel: "ch".to_string(),
            max_items: 5,
            wait_timeout_seconds: 10,
            auto_ack: true,
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("PollRequest"));
        let cloned = req.clone();
        assert_eq!(cloned.channel, "ch");
    }

    /// Test all QueueDownstreamType enum values and their i32 representations.
    #[test]
    fn test_queue_downstream_type_all_values() {
        assert_eq!(QueueDownstreamType::Get as i32, 1);
        assert_eq!(QueueDownstreamType::AckAll as i32, 2);
        assert_eq!(QueueDownstreamType::AckRange as i32, 3);
        assert_eq!(QueueDownstreamType::NackAll as i32, 4);
        assert_eq!(QueueDownstreamType::NackRange as i32, 5);
        assert_eq!(QueueDownstreamType::RequeueAll as i32, 6);
        assert_eq!(QueueDownstreamType::RequeueRange as i32, 7);
        assert_eq!(QueueDownstreamType::ActiveOffsets as i32, 8);
        assert_eq!(QueueDownstreamType::TransactionStatus as i32, 9);
        assert_eq!(QueueDownstreamType::CloseByClient as i32, 10);
        assert_eq!(QueueDownstreamType::CloseByServer as i32, 11);
    }

    /// Test all backward-compatible constants.
    #[test]
    fn test_queue_downstream_type_all_constants() {
        assert_eq!(queue_downstream_type::GET, 1);
        assert_eq!(queue_downstream_type::ACK_ALL, 2);
        assert_eq!(queue_downstream_type::ACK_RANGE, 3);
        assert_eq!(queue_downstream_type::NACK_ALL, 4);
        assert_eq!(queue_downstream_type::NACK_RANGE, 5);
        assert_eq!(queue_downstream_type::REQUEUE_ALL, 6);
        assert_eq!(queue_downstream_type::REQUEUE_RANGE, 7);
        assert_eq!(queue_downstream_type::ACTIVE_OFFSETS, 8);
        assert_eq!(queue_downstream_type::TRANSACTION_STATUS, 9);
        assert_eq!(queue_downstream_type::CLOSE_BY_CLIENT, 10);
        assert_eq!(queue_downstream_type::CLOSE_BY_SERVER, 11);
    }

    /// Test QueueDownstreamType Debug and Clone.
    #[test]
    fn test_queue_downstream_type_debug_clone_eq() {
        let t = QueueDownstreamType::Get;
        let debug = format!("{:?}", t);
        assert!(debug.contains("Get"));
        let cloned = t;
        assert_eq!(t, cloned);
    }

    /// Test QueueUpstreamResult fields.
    #[test]
    fn test_queue_upstream_result_debug_clone() {
        let result = QueueUpstreamResult {
            ref_request_id: "req-1".to_string(),
            results: vec![],
            is_error: false,
            error: String::new(),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("QueueUpstreamResult"));
        assert!(debug.contains("req-1"));
        let cloned = result.clone();
        assert_eq!(cloned.ref_request_id, "req-1");
        assert!(!cloned.is_error);
    }

    /// Test QueueUpstreamResult with error.
    #[test]
    fn test_queue_upstream_result_with_error() {
        let result = QueueUpstreamResult {
            ref_request_id: "req-2".to_string(),
            results: vec![],
            is_error: true,
            error: "send failed".to_string(),
        };
        assert!(result.is_error);
        assert_eq!(result.error, "send failed");
    }

    /// Test QueueUpstreamHandle.close() cancels the token.
    #[test]
    fn test_upstream_handle_close_cancels() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_result_tx, results) = tokio::sync::mpsc::channel(1);
        let handle = QueueUpstreamHandle {
            sender,
            results,
            cancel,
        };
        assert!(!token.is_cancelled());
        handle.close();
        assert!(token.is_cancelled());
    }

    /// Test QueueUpstreamHandle.send() validates channel.
    #[tokio::test]
    async fn test_upstream_handle_send_validates_channel() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_result_tx, results) = tokio::sync::mpsc::channel(1);
        let handle = QueueUpstreamHandle {
            sender,
            results,
            cancel,
        };
        let msg = crate::queues::QueueMessage {
            id: String::new(),
            client_id: String::new(),
            channel: String::new(), // empty channel
            metadata: String::new(),
            body: vec![],
            tags: std::collections::HashMap::new(),
            policy: None,
            attributes: None,
        };
        let result = handle.send("req-1", vec![msg]).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    /// Test QueueUpstreamHandle.send() validates wildcards.
    #[tokio::test]
    async fn test_upstream_handle_send_validates_wildcards() {
        let cancel = CancellationToken::new();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_result_tx, results) = tokio::sync::mpsc::channel(1);
        let handle = QueueUpstreamHandle {
            sender,
            results,
            cancel,
        };
        let msg = crate::queues::QueueMessage {
            id: String::new(),
            client_id: String::new(),
            channel: "test.*".to_string(),
            metadata: String::new(),
            body: vec![],
            tags: std::collections::HashMap::new(),
            policy: None,
            attributes: None,
        };
        let result = handle.send("req-1", vec![msg]).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    /// Test QueueDownstreamReceiver.close() cancels token.
    #[tokio::test]
    async fn test_downstream_receiver_close() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        let recv = QueueDownstreamReceiver {
            sender,
            receiver,
            client_id: "test".to_string(),
            cancel,
        };
        assert!(!token.is_cancelled());
        let _ = recv.close().await;
        assert!(token.is_cancelled());
    }

    /// Test QueueDownstreamMessage Debug.
    #[test]
    fn test_queue_downstream_message_debug() {
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let msg = QueueDownstreamMessage {
            message: crate::queues::QueueMessage {
                id: "msg-1".to_string(),
                client_id: String::new(),
                channel: "ch".to_string(),
                metadata: String::new(),
                body: vec![],
                tags: std::collections::HashMap::new(),
                policy: None,
                attributes: None,
            },
            transaction_id: "tx-1".to_string(),
            sequence: 42,
            sender,
            client_id: "client".to_string(),
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("QueueDownstreamMessage"));
        assert!(debug.contains("tx-1"));
        assert!(debug.contains("42"));
    }

    /// Test PollResponse Debug.
    #[test]
    fn test_poll_response_debug() {
        let (sender, _rx) = tokio::sync::mpsc::channel(1);
        let resp = PollResponse {
            transaction_id: "tx-1".to_string(),
            messages: vec![],
            is_error: false,
            error: String::new(),
            transaction_complete: true,
            sender,
            client_id: "client".to_string(),
        };
        let debug = format!("{:?}", resp);
        assert!(debug.contains("PollResponse"));
        assert!(debug.contains("tx-1"));
        assert!(debug.contains("transaction_complete"));
    }

    /// Test that poll with closed sender returns error.
    #[tokio::test]
    async fn test_poll_closed_sender_returns_error() {
        let cancel = CancellationToken::new();
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        let (_tx, receiver) = tokio::sync::mpsc::channel(1);
        drop(rx); // Close the receiving end
        let mut recv = QueueDownstreamReceiver {
            sender,
            receiver,
            client_id: "test".to_string(),
            cancel,
        };
        let result = recv
            .poll(PollRequest {
                channel: "valid-ch".to_string(),
                max_items: 1,
                wait_timeout_seconds: 1,
                auto_ack: false,
            })
            .await;
        assert!(result.is_err());
    }

    /// Test PollResponse ack_all with closed sender.
    #[tokio::test]
    async fn test_poll_response_ack_all_closed_sender() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let resp = PollResponse {
            transaction_id: "tx".to_string(),
            messages: vec![],
            is_error: false,
            error: String::new(),
            transaction_complete: true,
            sender,
            client_id: "client".to_string(),
        };
        let result = resp.ack_all().await;
        assert!(result.is_err());
    }

    /// Test PollResponse nack_all with closed sender.
    #[tokio::test]
    async fn test_poll_response_nack_all_closed_sender() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let resp = PollResponse {
            transaction_id: "tx".to_string(),
            messages: vec![],
            is_error: false,
            error: String::new(),
            transaction_complete: true,
            sender,
            client_id: "client".to_string(),
        };
        let result = resp.nack_all().await;
        assert!(result.is_err());
    }

    /// Test PollResponse re_queue_all with closed sender.
    #[tokio::test]
    async fn test_poll_response_requeue_all_closed_sender() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let resp = PollResponse {
            transaction_id: "tx".to_string(),
            messages: vec![],
            is_error: false,
            error: String::new(),
            transaction_complete: true,
            sender,
            client_id: "client".to_string(),
        };
        let result = resp.re_queue_all("other-ch").await;
        assert!(result.is_err());
    }

    /// Test QueueDownstreamMessage ack/nack/requeue with closed sender.
    #[tokio::test]
    async fn test_downstream_message_ack_closed() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let msg = QueueDownstreamMessage {
            message: crate::queues::QueueMessage {
                id: String::new(),
                client_id: String::new(),
                channel: "ch".to_string(),
                metadata: String::new(),
                body: vec![],
                tags: std::collections::HashMap::new(),
                policy: None,
                attributes: None,
            },
            transaction_id: "tx".to_string(),
            sequence: 1,
            sender,
            client_id: "client".to_string(),
        };
        assert!(msg.ack().await.is_err());
    }

    #[tokio::test]
    async fn test_downstream_message_nack_closed() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let msg = QueueDownstreamMessage {
            message: crate::queues::QueueMessage {
                id: String::new(),
                client_id: String::new(),
                channel: "ch".to_string(),
                metadata: String::new(),
                body: vec![],
                tags: std::collections::HashMap::new(),
                policy: None,
                attributes: None,
            },
            transaction_id: "tx".to_string(),
            sequence: 1,
            sender,
            client_id: "client".to_string(),
        };
        assert!(msg.nack().await.is_err());
    }

    #[tokio::test]
    async fn test_downstream_message_requeue_closed() {
        let (sender, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let msg = QueueDownstreamMessage {
            message: crate::queues::QueueMessage {
                id: String::new(),
                client_id: String::new(),
                channel: "ch".to_string(),
                metadata: String::new(),
                body: vec![],
                tags: std::collections::HashMap::new(),
                policy: None,
                attributes: None,
            },
            transaction_id: "tx".to_string(),
            sequence: 1,
            sender,
            client_id: "client".to_string(),
        };
        assert!(msg.re_queue("other").await.is_err());
    }
}
