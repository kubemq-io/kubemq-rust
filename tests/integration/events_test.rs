//! Integration tests for Events pattern (requires live KubeMQ broker).

use kubemq::{Event, KubemqClient};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-events")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_send_event() {
    let client = live_client().await;
    let event = Event::builder()
        .channel("integration.events.send")
        .metadata("test")
        .body(b"hello".to_vec())
        .build();
    client.send_event(event).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_subscribe_and_receive_event() {
    let client = live_client().await;
    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();

    let sub = client
        .subscribe_to_events(
            "integration.events.sub",
            "",
            move |_event| {
                let rc = received_clone.clone();
                Box::pin(async move {
                    rc.fetch_add(1, Ordering::SeqCst);
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send an event
    let event = Event::builder()
        .channel("integration.events.sub")
        .metadata("test")
        .body(b"subscribe-test".to_vec())
        .build();
    client.send_event(event).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(received.load(Ordering::SeqCst) >= 1);

    sub.cancel();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_publish_event_convenience() {
    let client = live_client().await;
    client
        .publish_event("integration.events.publish", b"data", Some("meta"), None)
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_ping() {
    let client = live_client().await;
    let info = client.ping().await.unwrap();
    assert!(!info.host.is_empty());
    assert!(!info.version.is_empty());
    client.close().await.unwrap();
}
