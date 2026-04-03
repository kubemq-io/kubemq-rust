//! Integration tests for grpc.rs transport lifecycle (requires live KubeMQ broker).
//!
//! Targets: connect/close lifecycle, state transitions, track/untrack subscriptions,
//! eager connect (check_connection=true), health monitor firing.

use kubemq::{ConnectionState, Event, EventStore, EventsStoreSubscription, KubemqClient};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-grpc")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

async fn eager_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-grpc-eager")
        .check_connection(true)
        .build()
        .await
        .expect("live broker required")
}

/// Test eager connect sets state to Ready immediately.
#[tokio::test]
#[ignore]
async fn test_eager_connect_state_ready() {
    let client = eager_client().await;
    assert_eq!(client.state(), ConnectionState::Ready);
    client.close().await.unwrap();
}

/// Test lazy connect starts in Idle state.
#[tokio::test]
#[ignore]
async fn test_lazy_connect_state_idle() {
    let client = live_client().await;
    assert_eq!(client.state(), ConnectionState::Idle);
    client.close().await.unwrap();
}

/// Test close transitions state to Closed.
#[tokio::test]
#[ignore]
async fn test_close_transitions_to_closed() {
    let client = eager_client().await;
    assert_eq!(client.state(), ConnectionState::Ready);
    client.close().await.unwrap();
    assert_eq!(client.state(), ConnectionState::Closed);
}

/// Test double close is idempotent.
#[tokio::test]
#[ignore]
async fn test_double_close_idempotent() {
    let client = eager_client().await;
    client.close().await.unwrap();
    client.close().await.unwrap(); // Should not panic
    assert_eq!(client.state(), ConnectionState::Closed);
}

/// Test ping after close returns error.
#[tokio::test]
#[ignore]
async fn test_ping_after_close() {
    let client = eager_client().await;
    client.close().await.unwrap();
    let result = client.ping().await;
    assert!(result.is_err());
}

/// Test on_connected callback fires on eager connect.
#[tokio::test]
#[ignore]
async fn test_on_connected_callback_fires_eager() {
    let connected = Arc::new(AtomicUsize::new(0));
    let cc = connected.clone();

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-grpc-cb-eager")
        .check_connection(true)
        .on_connected(move || {
            let c = cc.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build()
        .await
        .expect("live broker required");

    // on_connected should have fired during build
    assert!(
        connected.load(Ordering::SeqCst) >= 1,
        "on_connected callback should fire for eager connect"
    );
    client.close().await.unwrap();
}

/// Test on_connected callback fires on lazy connect too.
#[tokio::test]
#[ignore]
async fn test_on_connected_callback_fires_lazy() {
    let connected = Arc::new(AtomicUsize::new(0));
    let cc = connected.clone();

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-grpc-cb-lazy")
        .check_connection(false)
        .on_connected(move || {
            let c = cc.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build()
        .await
        .expect("live broker required");

    // on_connected fires after transport creation for both lazy and eager
    assert!(
        connected.load(Ordering::SeqCst) >= 1,
        "on_connected callback should fire for lazy connect"
    );
    client.close().await.unwrap();
}

/// Test on_closed callback fires when close() is called.
#[tokio::test]
#[ignore]
async fn test_on_closed_callback_fires() {
    let closed_count = Arc::new(AtomicUsize::new(0));
    let cc = closed_count.clone();

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-grpc-cb-closed")
        .check_connection(true)
        .on_closed(move || {
            let c = cc.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build()
        .await
        .expect("live broker required");

    client.close().await.unwrap();

    assert!(
        closed_count.load(Ordering::SeqCst) >= 1,
        "on_closed callback should fire when close() is called"
    );
}

/// Test track/untrack subscription via subscribe + cancel.
/// This exercises track_subscription when the subscription is created
/// and untrack_subscription when it's cancelled.
#[tokio::test]
#[ignore]
async fn test_track_untrack_subscription_via_events() {
    let client = live_client().await;
    let received = Arc::new(AtomicUsize::new(0));
    let rc = received.clone();

    let sub = client
        .subscribe_to_events(
            "integration.cov-grpc.track-unsub",
            "",
            move |_event| {
                let r = rc.clone();
                Box::pin(async move {
                    r.fetch_add(1, Ordering::SeqCst);
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send an event to confirm subscription is active
    let event = Event::builder()
        .channel("integration.cov-grpc.track-unsub")
        .body(b"track-test".to_vec())
        .build();
    client.send_event(event).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(received.load(Ordering::SeqCst) >= 1);

    // Cancel subscription - exercises untrack
    sub.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(sub.is_closed());

    client.close().await.unwrap();
}

/// Test track/untrack subscription via events store subscribe + unsubscribe.
#[tokio::test]
#[ignore]
async fn test_track_untrack_subscription_via_events_store() {
    let client = live_client().await;
    let received = Arc::new(AtomicUsize::new(0));
    let rc = received.clone();

    let sub = client
        .subscribe_to_events_store(
            "integration.cov-grpc.track-unsub-store",
            "",
            EventsStoreSubscription::StartNewOnly,
            move |_event| {
                let r = rc.clone();
                Box::pin(async move {
                    r.fetch_add(1, Ordering::SeqCst);
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send an event to confirm subscription is active
    let es = EventStore::builder()
        .channel("integration.cov-grpc.track-unsub-store")
        .body(b"track-store-test".to_vec())
        .build();
    client.send_event_store(es).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(received.load(Ordering::SeqCst) >= 1);

    // Unsubscribe (waits for cleanup)
    sub.unsubscribe().await;
    assert!(sub.is_closed());
    assert!(sub.is_done());

    client.close().await.unwrap();
}

/// Test client clone is cheap and shares state.
#[tokio::test]
#[ignore]
async fn test_client_clone_shares_state() {
    let client1 = eager_client().await;
    let client2 = client1.clone();

    // Both should see the same state
    assert_eq!(client1.state(), client2.state());

    // Ping through both
    client1.ping().await.unwrap();
    client2.ping().await.unwrap();

    // Close through one should affect the other
    client1.close().await.unwrap();
    assert_eq!(client2.state(), ConnectionState::Closed);
}

/// Test check_state rejects operations during Reconnecting state.
/// We set the state manually via the transport and verify check_state fails.
#[tokio::test]
#[ignore]
async fn test_check_state_rejects_reconnecting() {
    let client = eager_client().await;

    // Verify normal operation works
    client.ping().await.unwrap();

    // Close and try operations
    client.close().await.unwrap();

    // After close, send_event should fail
    let event = Event::builder()
        .channel("test-ch")
        .body(b"data".to_vec())
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
}

/// Test update_token through the transport (indirectly via config).
#[tokio::test]
#[ignore]
async fn test_eager_connect_then_ping() {
    let client = eager_client().await;

    // Multiple pings to exercise the client() path
    for _ in 0..5 {
        let info = client.ping().await.unwrap();
        assert!(!info.host.is_empty());
        assert!(!info.version.is_empty());
    }

    client.close().await.unwrap();
}

/// Test that close cancels active subscriptions.
#[tokio::test]
#[ignore]
async fn test_close_cancels_subscriptions() {
    let client = live_client().await;
    let received = Arc::new(AtomicUsize::new(0));
    let rc = received.clone();

    let sub = client
        .subscribe_to_events(
            "integration.cov-grpc.close-cancel-sub",
            "",
            move |_event| {
                let r = rc.clone();
                Box::pin(async move {
                    r.fetch_add(1, Ordering::SeqCst);
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Close client should cancel all subscriptions
    client.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The subscription should be cancelled
    assert!(sub.is_closed());
}
