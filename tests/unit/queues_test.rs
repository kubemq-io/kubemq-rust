//! Tests for QueueMessage type construction and builder.

use kubemq::{QueueMessage, QueueMessageBuilder, QueuePolicy};

#[test]
fn test_queue_message_builder_defaults() {
    let msg = QueueMessage::builder().build();
    assert!(msg.id.is_empty());
    assert!(msg.channel.is_empty());
    assert!(msg.body.is_empty());
    assert!(msg.policy.is_none());
    assert!(msg.attributes.is_none());
}

#[test]
fn test_queue_message_builder_all_fields() {
    let msg = QueueMessage::builder()
        .id("qm-1")
        .channel("queues.test")
        .metadata("meta")
        .body(b"queue-body".to_vec())
        .client_id("client-1")
        .add_tag("k", "v")
        .expiration_seconds(300)
        .delay_seconds(10)
        .max_receive_count(5)
        .max_receive_queue("dead-letter")
        .build();

    assert_eq!(msg.id, "qm-1");
    assert_eq!(msg.channel, "queues.test");
    assert_eq!(msg.body, b"queue-body");
    let policy = msg.policy.as_ref().unwrap();
    assert_eq!(policy.expiration_seconds, 300);
    assert_eq!(policy.delay_seconds, 10);
    assert_eq!(policy.max_receive_count, 5);
    assert_eq!(policy.max_receive_queue, "dead-letter");
}

#[test]
fn test_queue_message_builder_default_impl() {
    let builder = QueueMessageBuilder::default();
    let msg = builder.build();
    assert!(msg.channel.is_empty());
}

#[test]
fn test_queue_policy_default() {
    let policy = QueuePolicy::default();
    assert_eq!(policy.expiration_seconds, 0);
    assert_eq!(policy.delay_seconds, 0);
    assert_eq!(policy.max_receive_count, 0);
    assert!(policy.max_receive_queue.is_empty());
}

#[test]
fn test_queue_message_clone() {
    let msg = QueueMessage::builder()
        .channel("ch")
        .body(b"x".to_vec())
        .build();
    let cloned = msg.clone();
    assert_eq!(msg.channel, cloned.channel);
}

#[test]
fn test_queue_message_policy_builder() {
    let msg = QueueMessage::builder()
        .channel("q")
        .policy(QueuePolicy {
            expiration_seconds: 60,
            delay_seconds: 5,
            max_receive_count: 3,
            max_receive_queue: "dlq".to_string(),
        })
        .build();
    let p = msg.policy.unwrap();
    assert_eq!(p.expiration_seconds, 60);
    assert_eq!(p.max_receive_queue, "dlq");
}

/// REQ-H5: Batch send validates each message, reporting the index of the invalid one.
#[tokio::test]
async fn test_batch_send_validates_each_message() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    // First two messages are valid, third has empty channel
    let messages = vec![
        QueueMessage::builder()
            .channel("valid-1")
            .body(b"ok".to_vec())
            .build(),
        QueueMessage::builder()
            .channel("valid-2")
            .body(b"ok".to_vec())
            .build(),
        QueueMessage::builder()
            .channel("")
            .body(b"bad".to_vec())
            .build(),
    ];

    let result = client.send_queue_messages(messages).await;
    assert!(
        result.is_err(),
        "Batch with empty channel at index 2 should fail validation"
    );
    let err = result.unwrap_err();
    assert_eq!(err.code(), kubemq::ErrorCode::Validation);

    client.close().await.unwrap();
}

/// REQ-M22: Queue batch validation -- wildcard channel should fail validation.
#[tokio::test]
async fn test_queue_batch_validation() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    // Batch with a wildcard channel at index 1 should fail validation
    let messages = vec![
        QueueMessage::builder()
            .channel("valid-1")
            .body(b"ok".to_vec())
            .build(),
        QueueMessage::builder()
            .channel("has.*")
            .body(b"wildcard".to_vec())
            .build(),
    ];

    let result = client.send_queue_messages(messages).await;
    assert!(
        result.is_err(),
        "Batch with wildcard channel should fail validation"
    );
    let err = result.unwrap_err();
    assert_eq!(err.code(), kubemq::ErrorCode::Validation);

    client.close().await.unwrap();
}

/// REQ-M23: Server response with is_error=true should convert to Err.
#[tokio::test]
async fn test_receive_error_response() {
    use kubemq::proto::kubemq as pb;

    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;

    // Set up mock to return is_error=true
    {
        let mut s = state.lock().unwrap();
        s.receive_queue_response = Some(pb::ReceiveQueueMessagesResponse {
            request_id: "req-1".to_string(),
            messages: Vec::new(),
            messages_received: 0,
            messages_expired: 0,
            is_peak: false,
            is_error: true,
            error: "queue not found".to_string(),
        });
    }

    let client = crate::mock_server::build_test_client(addr).await;
    let result = client
        .receive_queue_messages("test-queue", 10, 5, false)
        .await;

    assert!(result.is_err(), "is_error=true should convert to Err");
    let err = result.unwrap_err();
    assert_eq!(err.code(), kubemq::ErrorCode::Transient);

    client.close().await.unwrap();
}

/// Send queue message via mock.
#[tokio::test]
async fn test_send_queue_message_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let msg = QueueMessage::builder()
        .channel("queues.test")
        .body(b"hello".to_vec())
        .build();

    let result = client.send_queue_message(msg).await.unwrap();
    assert!(!result.is_error);

    let s = state.lock().unwrap();
    let captured = s.last_queue_message.as_ref().unwrap();
    assert_eq!(captured.channel, "queues.test");
    drop(s);

    client.close().await.unwrap();
}
