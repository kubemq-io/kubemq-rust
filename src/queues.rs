//! Queue messaging pattern - simple API types and client methods.

use std::collections::HashMap;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq as proto;
use crate::validate;

/// Outbound queue message.
#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: String,
    pub client_id: String,
    pub channel: String,
    pub metadata: String,
    pub body: Vec<u8>,
    pub tags: HashMap<String, String>,
    pub policy: Option<QueuePolicy>,
    pub attributes: Option<QueueMessageAttributes>,
}

impl QueueMessage {
    /// Create a builder for constructing a queue message.
    pub fn builder() -> QueueMessageBuilder {
        QueueMessageBuilder::new()
    }
}

/// Builder for creating queue messages.
pub struct QueueMessageBuilder {
    msg: QueueMessage,
}

impl QueueMessageBuilder {
    pub fn new() -> Self {
        Self {
            msg: QueueMessage {
                id: String::new(),
                client_id: String::new(),
                channel: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                tags: HashMap::new(),
                policy: None,
                attributes: None,
            },
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.msg.id = id.into();
        self
    }

    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.msg.channel = channel.into();
        self
    }

    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.msg.metadata = metadata.into();
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.msg.body = body.into();
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.msg.client_id = client_id.into();
        self
    }

    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.msg.tags = tags;
        self
    }

    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.msg.tags.insert(key.into(), value.into());
        self
    }

    pub fn policy(mut self, policy: QueuePolicy) -> Self {
        self.msg.policy = Some(policy);
        self
    }

    pub fn expiration_seconds(mut self, seconds: i32) -> Self {
        let p = self.msg.policy.get_or_insert_with(QueuePolicy::default);
        p.expiration_seconds = seconds;
        self
    }

    pub fn delay_seconds(mut self, seconds: i32) -> Self {
        let p = self.msg.policy.get_or_insert_with(QueuePolicy::default);
        p.delay_seconds = seconds;
        self
    }

    pub fn max_receive_count(mut self, count: i32) -> Self {
        let p = self.msg.policy.get_or_insert_with(QueuePolicy::default);
        p.max_receive_count = count;
        self
    }

    pub fn max_receive_queue(mut self, queue: impl Into<String>) -> Self {
        let p = self.msg.policy.get_or_insert_with(QueuePolicy::default);
        p.max_receive_queue = queue.into();
        self
    }

    pub fn build(self) -> QueueMessage {
        self.msg
    }
}

impl Default for QueueMessageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Queue message delivery policy.
#[derive(Debug, Clone, Default)]
pub struct QueuePolicy {
    pub expiration_seconds: i32,
    pub delay_seconds: i32,
    pub max_receive_count: i32,
    pub max_receive_queue: String,
}

/// Server-assigned attributes on received queue messages.
#[derive(Debug, Clone)]
pub struct QueueMessageAttributes {
    pub timestamp: i64,
    pub sequence: u64,
    pub md5_of_body: String,
    pub receive_count: i32,
    pub re_routed: bool,
    pub re_routed_from_queue: String,
    pub expiration_at: i64,
    pub delayed_to: i64,
}

/// Result of a single queue message send.
#[derive(Debug, Clone)]
pub struct QueueSendResult {
    pub message_id: String,
    pub sent_at: i64,
    pub expiration_at: i64,
    pub delayed_to: i64,
    pub is_error: bool,
    pub error: String,
}

impl QueueSendResult {
    /// Convert into a Result, returning Err if the send reported an error.
    pub fn into_result(self) -> crate::Result<Self> {
        if self.is_error {
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message: self.error,
                operation: "send_queue_message".to_string(),
                channel: String::new(),
                is_retryable: true,
                source: None,
                request_id: self.message_id,
                suggestion: "Retry the operation.",
            })
        } else {
            Ok(self)
        }
    }
}

/// Request to acknowledge all messages in a queue.
#[derive(Debug, Clone)]
pub struct AckAllQueueMessagesRequest {
    pub request_id: String,
    pub client_id: String,
    pub channel: String,
    pub wait_time_seconds: i32,
}

/// Response from acknowledge all queue messages.
#[derive(Debug, Clone)]
pub struct AckAllQueueMessagesResponse {
    pub request_id: String,
    pub affected_messages: u64,
    pub is_error: bool,
    pub error: String,
}

/// Convert our QueueMessage to proto QueueMessage (borrowed, for batch/stream use).
#[allow(dead_code)]
pub(crate) fn queue_message_to_proto(msg: &QueueMessage, client_id: &str) -> proto::QueueMessage {
    let id = if msg.id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        msg.id.clone()
    };

    let cid = if msg.client_id.is_empty() {
        client_id.to_string()
    } else {
        msg.client_id.clone()
    };

    let policy = msg.policy.as_ref().map(|p| proto::QueueMessagePolicy {
        expiration_seconds: p.expiration_seconds,
        delay_seconds: p.delay_seconds,
        max_receive_count: p.max_receive_count,
        max_receive_queue: p.max_receive_queue.clone(),
    });

    proto::QueueMessage {
        message_id: id,
        client_id: cid,
        channel: msg.channel.clone(),
        metadata: msg.metadata.clone(),
        body: msg.body.clone(),
        tags: msg.tags.clone(),
        attributes: None,
        policy,
    }
}

/// Convert our QueueMessage to proto QueueMessage (owned, zero-copy).
pub(crate) fn queue_message_to_proto_owned(
    msg: QueueMessage,
    client_id: &str,
) -> proto::QueueMessage {
    let id = crate::common::resolve_id_owned(msg.id);
    let cid = if msg.client_id.is_empty() {
        client_id.to_string()
    } else {
        msg.client_id
    };

    let policy = msg.policy.map(|p| proto::QueueMessagePolicy {
        expiration_seconds: p.expiration_seconds,
        delay_seconds: p.delay_seconds,
        max_receive_count: p.max_receive_count,
        max_receive_queue: p.max_receive_queue, // moved
    });

    proto::QueueMessage {
        message_id: id,
        client_id: cid,
        channel: msg.channel,   // moved
        metadata: msg.metadata, // moved
        body: msg.body,         // moved -- zero-copy
        tags: msg.tags,         // moved
        attributes: None,
        policy,
    }
}

/// Convert proto QueueMessage to our QueueMessage.
pub(crate) fn proto_to_queue_message(p: proto::QueueMessage) -> QueueMessage {
    let policy = p.policy.map(|pol| QueuePolicy {
        expiration_seconds: pol.expiration_seconds,
        delay_seconds: pol.delay_seconds,
        max_receive_count: pol.max_receive_count,
        max_receive_queue: pol.max_receive_queue,
    });

    let attributes = p.attributes.map(|attr| QueueMessageAttributes {
        timestamp: attr.timestamp,
        sequence: attr.sequence,
        md5_of_body: attr.md5_of_body,
        receive_count: attr.receive_count,
        re_routed: attr.re_routed,
        re_routed_from_queue: attr.re_routed_from_queue,
        expiration_at: attr.expiration_at,
        delayed_to: attr.delayed_to,
    });

    QueueMessage {
        id: p.message_id,
        client_id: p.client_id,
        channel: p.channel,
        metadata: p.metadata,
        body: p.body,
        tags: p.tags,
        policy,
        attributes,
    }
}

/// Convert proto SendQueueMessageResult to our QueueSendResult.
fn proto_to_send_result(r: proto::SendQueueMessageResult) -> QueueSendResult {
    QueueSendResult {
        message_id: r.message_id,
        sent_at: r.sent_at,
        expiration_at: r.expiration_at,
        delayed_to: r.delayed_to,
        is_error: r.is_error,
        error: r.error,
    }
}

// Client methods for Queues (Simple API)
impl KubemqClient {
    /// Send a single message to a queue.
    pub async fn send_queue_message(&self, msg: QueueMessage) -> crate::Result<QueueSendResult> {
        self.check_state("send_queue_message")?;

        validate::validate_channel(&msg.channel, "send_queue_message")?;
        validate::validate_no_wildcards(&msg.channel, "send_queue_message")?;
        validate::validate_tags(&msg.tags, "send_queue_message", &msg.channel)?;
        validate::validate_body_size(
            &msg.body,
            self.config().max_send_message_size,
            "send_queue_message",
            &msg.channel,
        )?;

        if let Some(ref policy) = msg.policy {
            validate::validate_expiration_seconds(
                policy.expiration_seconds,
                "send_queue_message",
                &msg.channel,
            )?;
            validate::validate_delay_seconds(
                policy.delay_seconds,
                "send_queue_message",
                &msg.channel,
            )?;
            validate::validate_max_receive_count(
                policy.max_receive_count,
                "send_queue_message",
                &msg.channel,
            )?;
        }

        let channel_for_err = msg.channel.clone(); // Save for error message
        let proto_msg = queue_message_to_proto_owned(msg, &self.config().client_id);

        let mut request = tonic::Request::new(proto_msg);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client.send_queue_message(request).await.map_err(|s| {
            KubemqError::from_grpc_status(s, "send_queue_message", &channel_for_err)
        })?;

        Ok(proto_to_send_result(response.into_inner()))
    }

    /// Send multiple messages to a queue in a batch.
    pub async fn send_queue_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> crate::Result<Vec<QueueSendResult>> {
        self.check_state("send_queue_messages")?;

        let client_id = &self.config().client_id;
        let max_size = self.config().max_send_message_size;

        // REQ-H6: Validate each message in the batch
        for (i, msg) in messages.iter().enumerate() {
            let op = format!("send_queue_messages[{}]", i);
            validate::validate_channel(&msg.channel, &op)?;
            validate::validate_no_wildcards(&msg.channel, &op)?;
            validate::validate_tags(&msg.tags, &op, &msg.channel)?;
            validate::validate_body_size(&msg.body, max_size, &op, &msg.channel)?;
            if let Some(ref policy) = msg.policy {
                validate::validate_expiration_seconds(
                    policy.expiration_seconds,
                    &op,
                    &msg.channel,
                )?;
                validate::validate_delay_seconds(policy.delay_seconds, &op, &msg.channel)?;
                validate::validate_max_receive_count(policy.max_receive_count, &op, &msg.channel)?;
            }
        }

        // REQ-H7: Owned proto conversion -- zero-copy moves
        let proto_messages: Vec<proto::QueueMessage> = messages
            .into_iter()
            .map(|m| queue_message_to_proto_owned(m, client_id))
            .collect();

        let batch_request = proto::QueueMessagesBatchRequest {
            batch_id: uuid::Uuid::new_v4().to_string(),
            messages: proto_messages,
        };

        let mut request = tonic::Request::new(batch_request);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .send_queue_messages_batch(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_queue_messages", ""))?;

        let batch_response = response.into_inner();
        Ok(batch_response
            .results
            .into_iter()
            .map(proto_to_send_result)
            .collect())
    }

    /// Receive messages from a queue (simple API).
    pub async fn receive_queue_messages(
        &self,
        channel: &str,
        max_messages: i32,
        wait_time_seconds: i32,
        is_peek: bool,
    ) -> crate::Result<Vec<QueueMessage>> {
        self.check_closed()?;
        validate::validate_channel(channel, "receive_queue_messages")?;
        validate::validate_no_wildcards(channel, "receive_queue_messages")?;
        validate::validate_max_messages(max_messages, "receive_queue_messages", channel)?;
        validate::validate_wait_time_seconds(wait_time_seconds, "receive_queue_messages", channel)?;

        let proto_req = proto::ReceiveQueueMessagesRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: self.config().client_id.clone(),
            channel: channel.to_string(),
            max_number_of_messages: max_messages,
            wait_time_seconds,
            is_peak: is_peek,
        };

        let mut request = tonic::Request::new(proto_req);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .receive_queue_messages(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "receive_queue_messages", channel))?;

        let resp = response.into_inner();
        if resp.is_error {
            return Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message: resp.error,
                operation: "receive_queue_messages".to_string(),
                channel: channel.to_string(),
                is_retryable: true,
                source: None,
                request_id: resp.request_id,
                suggestion: "Check the queue and retry.",
            });
        }

        Ok(resp
            .messages
            .into_iter()
            .map(proto_to_queue_message)
            .collect())
    }

    /// Acknowledge all messages in a queue.
    pub async fn ack_all_queue_messages(
        &self,
        req: &AckAllQueueMessagesRequest,
    ) -> crate::Result<AckAllQueueMessagesResponse> {
        self.check_closed()?;
        validate::validate_channel(&req.channel, "ack_all_queue_messages")?;

        let cid = if req.client_id.is_empty() {
            self.config().client_id.clone()
        } else {
            req.client_id.clone()
        };

        let rid = if req.request_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.request_id.clone()
        };

        let proto_req = proto::AckAllQueueMessagesRequest {
            request_id: rid,
            client_id: cid,
            channel: req.channel.clone(),
            wait_time_seconds: req.wait_time_seconds,
        };

        let mut request = tonic::Request::new(proto_req);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client.ack_all_queue_messages(request).await.map_err(|s| {
            KubemqError::from_grpc_status(s, "ack_all_queue_messages", &req.channel)
        })?;

        let resp = response.into_inner();
        Ok(AckAllQueueMessagesResponse {
            request_id: resp.request_id,
            affected_messages: resp.affected_messages,
            is_error: resp.is_error,
            error: resp.error,
        })
    }

    /// Convenience: send queue message with minimal params.
    pub async fn send_queue_message_simple(
        &self,
        channel: &str,
        body: impl Into<Vec<u8>>,
    ) -> crate::Result<QueueSendResult> {
        let msg = QueueMessage::builder().channel(channel).body(body).build();
        self.send_queue_message(msg).await // Passes owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- QueueMessageBuilder tests --

    #[test]
    fn test_queue_message_builder_default() {
        let builder = QueueMessageBuilder::default();
        let msg = builder.build();
        assert!(msg.id.is_empty());
        assert!(msg.channel.is_empty());
        assert!(msg.body.is_empty());
        assert!(msg.tags.is_empty());
        assert!(msg.policy.is_none());
        assert!(msg.attributes.is_none());
    }

    #[test]
    fn test_queue_message_builder_all_fields() {
        let msg = QueueMessage::builder()
            .id("qm-1")
            .channel("q-channel")
            .metadata("q-meta")
            .body(b"q-body".to_vec())
            .client_id("q-client")
            .add_tag("k", "v")
            .expiration_seconds(60)
            .delay_seconds(10)
            .max_receive_count(5)
            .max_receive_queue("dead-letter")
            .build();
        assert_eq!(msg.id, "qm-1");
        assert_eq!(msg.channel, "q-channel");
        assert_eq!(msg.metadata, "q-meta");
        assert_eq!(msg.body, b"q-body");
        assert_eq!(msg.client_id, "q-client");
        assert_eq!(msg.tags.get("k").unwrap(), "v");
        let policy = msg.policy.as_ref().unwrap();
        assert_eq!(policy.expiration_seconds, 60);
        assert_eq!(policy.delay_seconds, 10);
        assert_eq!(policy.max_receive_count, 5);
        assert_eq!(policy.max_receive_queue, "dead-letter");
    }

    #[test]
    fn test_queue_message_builder_with_full_policy() {
        let policy = QueuePolicy {
            expiration_seconds: 120,
            delay_seconds: 5,
            max_receive_count: 3,
            max_receive_queue: "dlq".to_string(),
        };
        let msg = QueueMessage::builder().channel("ch").policy(policy).build();
        let p = msg.policy.unwrap();
        assert_eq!(p.expiration_seconds, 120);
    }

    #[test]
    fn test_queue_message_builder_tags() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "staging".to_string());
        let msg = QueueMessage::builder().channel("ch").tags(tags).build();
        assert_eq!(msg.tags.len(), 1);
    }

    #[test]
    fn test_queue_message_debug_clone() {
        let msg = QueueMessage::builder().id("qm-1").channel("ch").build();
        let debug = format!("{:?}", msg);
        assert!(debug.contains("qm-1"));
        let cloned = msg.clone();
        assert_eq!(cloned.id, "qm-1");
    }

    // -- QueuePolicy tests --

    #[test]
    fn test_queue_policy_default() {
        let policy = QueuePolicy::default();
        assert_eq!(policy.expiration_seconds, 0);
        assert_eq!(policy.delay_seconds, 0);
        assert_eq!(policy.max_receive_count, 0);
        assert!(policy.max_receive_queue.is_empty());
    }

    #[test]
    fn test_queue_policy_debug_clone() {
        let policy = QueuePolicy {
            expiration_seconds: 30,
            delay_seconds: 5,
            max_receive_count: 3,
            max_receive_queue: "dlq".to_string(),
        };
        let debug = format!("{:?}", policy);
        assert!(debug.contains("QueuePolicy"));
        let cloned = policy.clone();
        assert_eq!(cloned.expiration_seconds, 30);
    }

    // -- QueueSendResult tests --

    #[test]
    fn test_queue_send_result_into_result_success() {
        let result = QueueSendResult {
            message_id: "m1".to_string(),
            sent_at: 100,
            expiration_at: 200,
            delayed_to: 0,
            is_error: false,
            error: String::new(),
        };
        let r = result.into_result();
        assert!(r.is_ok());
        let val = r.unwrap();
        assert_eq!(val.message_id, "m1");
    }

    #[test]
    fn test_queue_send_result_into_result_error() {
        let result = QueueSendResult {
            message_id: "m2".to_string(),
            sent_at: 0,
            expiration_at: 0,
            delayed_to: 0,
            is_error: true,
            error: "queue full".to_string(),
        };
        let r = result.into_result();
        assert!(r.is_err());
    }

    #[test]
    fn test_queue_send_result_debug_clone() {
        let result = QueueSendResult {
            message_id: "m1".to_string(),
            sent_at: 100,
            expiration_at: 200,
            delayed_to: 0,
            is_error: false,
            error: String::new(),
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("QueueSendResult"));
        let cloned = result.clone();
        assert_eq!(cloned.sent_at, 100);
    }

    // -- queue_message_to_proto tests --

    #[test]
    fn test_queue_message_to_proto_borrowed() {
        let msg = QueueMessage::builder()
            .id("qm-1")
            .channel("q-ch")
            .metadata("meta")
            .body(b"body".to_vec())
            .client_id("client-1")
            .add_tag("k", "v")
            .build();
        let proto = queue_message_to_proto(&msg, "default-client");
        assert_eq!(proto.message_id, "qm-1");
        assert_eq!(proto.client_id, "client-1");
        assert_eq!(proto.channel, "q-ch");
        assert_eq!(proto.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_queue_message_to_proto_uses_default_client_id() {
        let msg = QueueMessage::builder().channel("ch").build();
        let proto = queue_message_to_proto(&msg, "default");
        assert_eq!(proto.client_id, "default");
    }

    #[test]
    fn test_queue_message_to_proto_generates_id() {
        let msg = QueueMessage::builder().channel("ch").build();
        let proto = queue_message_to_proto(&msg, "client");
        assert!(!proto.message_id.is_empty());
        assert_eq!(proto.message_id.len(), 36);
    }

    #[test]
    fn test_queue_message_to_proto_with_policy() {
        let msg = QueueMessage::builder()
            .channel("ch")
            .expiration_seconds(60)
            .delay_seconds(10)
            .build();
        let proto = queue_message_to_proto(&msg, "client");
        let policy = proto.policy.unwrap();
        assert_eq!(policy.expiration_seconds, 60);
        assert_eq!(policy.delay_seconds, 10);
    }

    // -- queue_message_to_proto_owned tests --

    #[test]
    fn test_queue_message_to_proto_owned() {
        let msg = QueueMessage::builder()
            .id("qm-1")
            .channel("q-ch")
            .client_id("my-client")
            .build();
        let proto = queue_message_to_proto_owned(msg, "default");
        assert_eq!(proto.message_id, "qm-1");
        assert_eq!(proto.client_id, "my-client");
    }

    #[test]
    fn test_queue_message_to_proto_owned_uses_default() {
        let msg = QueueMessage::builder().channel("ch").build();
        let proto = queue_message_to_proto_owned(msg, "default");
        assert_eq!(proto.client_id, "default");
    }

    // -- proto_to_queue_message tests --

    #[test]
    fn test_proto_to_queue_message_basic() {
        let proto_msg = proto::QueueMessage {
            message_id: "pm1".to_string(),
            client_id: "pc".to_string(),
            channel: "pch".to_string(),
            metadata: "pmeta".to_string(),
            body: b"pbody".to_vec(),
            tags: HashMap::new(),
            attributes: None,
            policy: None,
        };
        let msg = proto_to_queue_message(proto_msg);
        assert_eq!(msg.id, "pm1");
        assert_eq!(msg.client_id, "pc");
        assert_eq!(msg.channel, "pch");
        assert!(msg.policy.is_none());
        assert!(msg.attributes.is_none());
    }

    #[test]
    fn test_proto_to_queue_message_with_policy_and_attributes() {
        let proto_msg = proto::QueueMessage {
            message_id: "pm2".to_string(),
            client_id: "pc".to_string(),
            channel: "pch".to_string(),
            metadata: String::new(),
            body: vec![],
            tags: HashMap::new(),
            attributes: Some(proto::QueueMessageAttributes {
                timestamp: 123,
                sequence: 456,
                md5_of_body: "abc".to_string(),
                receive_count: 2,
                re_routed: true,
                re_routed_from_queue: "old-q".to_string(),
                expiration_at: 789,
                delayed_to: 100,
            }),
            policy: Some(proto::QueueMessagePolicy {
                expiration_seconds: 30,
                delay_seconds: 5,
                max_receive_count: 3,
                max_receive_queue: "dlq".to_string(),
            }),
        };
        let msg = proto_to_queue_message(proto_msg);
        let policy = msg.policy.unwrap();
        assert_eq!(policy.expiration_seconds, 30);
        assert_eq!(policy.max_receive_queue, "dlq");
        let attrs = msg.attributes.unwrap();
        assert_eq!(attrs.timestamp, 123);
        assert_eq!(attrs.sequence, 456);
        assert!(attrs.re_routed);
        assert_eq!(attrs.re_routed_from_queue, "old-q");
    }

    // -- AckAllQueueMessagesRequest/Response tests --

    #[test]
    fn test_ack_all_request_debug_clone() {
        let req = AckAllQueueMessagesRequest {
            request_id: "r1".to_string(),
            client_id: "c".to_string(),
            channel: "ch".to_string(),
            wait_time_seconds: 5,
        };
        let debug = format!("{:?}", req);
        assert!(debug.contains("AckAllQueueMessagesRequest"));
        let cloned = req.clone();
        assert_eq!(cloned.wait_time_seconds, 5);
    }

    #[test]
    fn test_ack_all_response_debug_clone() {
        let resp = AckAllQueueMessagesResponse {
            request_id: "r1".to_string(),
            affected_messages: 10,
            is_error: false,
            error: String::new(),
        };
        let debug = format!("{:?}", resp);
        assert!(debug.contains("AckAllQueueMessagesResponse"));
        let cloned = resp.clone();
        assert_eq!(cloned.affected_messages, 10);
    }

    // -- QueueMessageAttributes tests --

    #[test]
    fn test_queue_message_attributes_debug_clone() {
        let attrs = QueueMessageAttributes {
            timestamp: 100,
            sequence: 1,
            md5_of_body: "abc".to_string(),
            receive_count: 2,
            re_routed: false,
            re_routed_from_queue: String::new(),
            expiration_at: 200,
            delayed_to: 0,
        };
        let debug = format!("{:?}", attrs);
        assert!(debug.contains("QueueMessageAttributes"));
        let cloned = attrs.clone();
        assert_eq!(cloned.sequence, 1);
    }
}
