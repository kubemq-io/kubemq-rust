//! Integration tests for concurrent operations (requires live KubeMQ broker).

use kubemq::{Event, KubemqClient, QueueMessage};
use std::sync::Arc;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-concurrency")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_parallel_event_sends() {
    let client = Arc::new(live_client().await);
    let mut handles = Vec::new();

    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let event = Event::builder()
                .channel("integration.concurrency.events")
                .metadata(format!("parallel-{}", i))
                .body(format!("data-{}", i).into_bytes())
                .build();
            c.send_event(event).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_parallel_queue_sends() {
    let client = Arc::new(live_client().await);
    let mut handles = Vec::new();

    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let msg = QueueMessage::builder()
                .channel("integration.concurrency.queues")
                .body(format!("queue-data-{}", i).into_bytes())
                .build();
            c.send_queue_message(msg).await
        }));
    }

    for h in handles {
        let result = h.await.unwrap().unwrap();
        assert!(!result.is_error);
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_concurrent_ping() {
    let client = Arc::new(live_client().await);
    let mut handles = Vec::new();

    for _ in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.ping().await }));
    }

    for h in handles {
        let info = h.await.unwrap().unwrap();
        assert!(!info.host.is_empty());
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_client_clone_is_cheap() {
    let client = live_client().await;
    let cloned = client.clone();

    // Both should work
    client.ping().await.unwrap();
    cloned.ping().await.unwrap();

    // Close one, the other should also be closed (Arc-based)
    client.close().await.unwrap();
    assert!(cloned.ping().await.is_err());
}
