//! Integration tests for Events Store pattern (requires live KubeMQ broker).

use kubemq::{EventStore, EventsStoreSubscription, KubemqClient};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-events-store")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_send_event_store() {
    let client = live_client().await;
    let es = EventStore::builder()
        .channel("integration.events-store.send")
        .metadata("test")
        .body(b"persistent".to_vec())
        .build();
    let result = client.send_event_store(es).await.unwrap();
    assert!(result.sent);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_subscribe_events_store_start_new() {
    let client = live_client().await;
    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();

    let sub = client
        .subscribe_to_events_store(
            "integration.events-store.sub-new",
            "",
            EventsStoreSubscription::StartNewOnly,
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

    let es = EventStore::builder()
        .channel("integration.events-store.sub-new")
        .metadata("test")
        .body(b"new-only".to_vec())
        .build();
    client.send_event_store(es).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(received.load(Ordering::SeqCst) >= 1);

    sub.cancel();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_subscribe_events_store_start_from_first() {
    let client = live_client().await;

    // Send a message first
    let es = EventStore::builder()
        .channel("integration.events-store.sub-first")
        .metadata("test")
        .body(b"first".to_vec())
        .build();
    client.send_event_store(es).await.unwrap();

    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();

    let sub = client
        .subscribe_to_events_store(
            "integration.events-store.sub-first",
            "",
            EventsStoreSubscription::StartFromFirst,
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

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(received.load(Ordering::SeqCst) >= 1);

    sub.cancel();
    client.close().await.unwrap();
}
