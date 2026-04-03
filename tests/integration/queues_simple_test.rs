//! Integration tests for simple Queues API (requires live KubeMQ broker).

use kubemq::{AckAllQueueMessagesRequest, KubemqClient, QueueMessage};

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-queues")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_send_queue_message() {
    let client = live_client().await;
    let msg = QueueMessage::builder()
        .channel("integration.queues.send")
        .body(b"queue-msg".to_vec())
        .build();
    let result = client.send_queue_message(msg).await.unwrap();
    assert!(!result.is_error, "error: {}", result.error);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_send_and_receive_queue_message() {
    let client = live_client().await;
    let channel = "integration.queues.send-recv";

    let msg = QueueMessage::builder()
        .channel(channel)
        .body(b"receive-test".to_vec())
        .build();
    client.send_queue_message(msg).await.unwrap();

    let messages = client
        .receive_queue_messages(channel, 1, 5, false)
        .await
        .unwrap();
    assert!(!messages.is_empty());
    assert_eq!(messages[0].body, b"receive-test");

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_send_queue_messages_batch() {
    let client = live_client().await;
    let channel = "integration.queues.batch";

    let messages: Vec<QueueMessage> = (0..5)
        .map(|i| {
            QueueMessage::builder()
                .channel(channel)
                .body(format!("batch-{}", i).into_bytes())
                .build()
        })
        .collect();

    let results = client.send_queue_messages(messages).await.unwrap();
    assert_eq!(results.len(), 5);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_peek_queue_messages() {
    let client = live_client().await;
    let channel = "integration.queues.peek";

    let msg = QueueMessage::builder()
        .channel(channel)
        .body(b"peek-test".to_vec())
        .build();
    client.send_queue_message(msg).await.unwrap();

    // Peek should not consume the message
    let messages = client
        .receive_queue_messages(channel, 1, 5, true)
        .await
        .unwrap();
    assert!(!messages.is_empty());

    // Message should still be available
    let messages2 = client
        .receive_queue_messages(channel, 1, 5, true)
        .await
        .unwrap();
    assert!(!messages2.is_empty());

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_ack_all_queue_messages() {
    let client = live_client().await;
    let channel = "integration.queues.ack-all";

    let req = AckAllQueueMessagesRequest {
        request_id: String::new(),
        client_id: String::new(),
        channel: channel.to_string(),
        wait_time_seconds: 5,
    };
    let resp = client.ack_all_queue_messages(&req).await.unwrap();
    assert!(!resp.is_error, "error: {}", resp.error);

    client.close().await.unwrap();
}
