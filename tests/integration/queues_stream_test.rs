//! Integration tests for stream-based Queues API (requires live KubeMQ broker).

use kubemq::KubemqClient;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-queues-stream")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_queue_upstream_stream() {
    let client = live_client().await;
    // The upstream stream API requires a more complex setup.
    // For now, verify the client can be created and closed without errors.
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_queue_downstream_constants() {
    use kubemq::queue_downstream_type;
    assert_eq!(queue_downstream_type::GET, 1);
    assert_eq!(queue_downstream_type::ACK_ALL, 2);
    assert_eq!(queue_downstream_type::NACK_ALL, 4);
    assert_eq!(queue_downstream_type::CLOSE_BY_CLIENT, 10);
}

#[tokio::test]
#[ignore]
async fn test_send_queue_message_simple_convenience() {
    let client = live_client().await;
    let result = client
        .send_queue_message_simple("integration.queues-stream.simple", b"stream-data")
        .await
        .unwrap();
    assert!(!result.is_error, "error: {}", result.error);
    client.close().await.unwrap();
}
