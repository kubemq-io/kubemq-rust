//! Integration tests for client.rs coverage gaps (requires live KubeMQ broker).
//!
//! Targets: Drop impl warning path, child_token(), check_state(),
//! close idempotency, drain timeout behavior.

use kubemq::{ConnectionState, Event, KubemqClient};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-client")
        .check_connection(true)
        .build()
        .await
        .expect("live broker required")
}

/// Test Drop impl warning path: client dropped without calling close().
/// This should log a warning but not panic.
#[tokio::test]
#[ignore]
async fn test_client_drop_without_close() {
    {
        let _client = live_client().await;
        // client drops here without close() being called
        // This should trigger the Drop impl warning
    }
    // If we get here without panic, the Drop impl works correctly
}

/// Test that child_token() creates a token cancelled by close().
#[tokio::test]
#[ignore]
async fn test_child_token_cancelled_by_close() {
    let client = live_client().await;

    // Subscribe creates a child token internally
    let received = Arc::new(AtomicUsize::new(0));
    let rc = received.clone();

    let sub = client
        .subscribe_to_events(
            "integration.cov-client.child-token",
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

    // Close should cancel all child tokens
    client.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(sub.is_closed());
}

/// Test check_state after close.
#[tokio::test]
#[ignore]
async fn test_check_state_after_close() {
    let client = live_client().await;
    client.close().await.unwrap();

    // Any operation should fail after close
    let event = Event::builder()
        .channel("test-ch")
        .body(b"data".to_vec())
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
}

/// Test config accessors.
#[tokio::test]
#[ignore]
async fn test_config_accessors() {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-config-test")
        .check_connection(true)
        .drain_timeout(Duration::from_secs(3))
        .rpc_timeout(Duration::from_secs(10))
        .max_receive_message_size(8 * 1024 * 1024)
        .max_send_message_size(8 * 1024 * 1024)
        .build()
        .await
        .unwrap();

    let config = client.config();
    assert_eq!(config.host(), "localhost");
    assert_eq!(config.port(), 50000);
    assert_eq!(config.client_id(), "rust-cov-config-test");
    assert_eq!(config.drain_timeout(), Duration::from_secs(3));
    assert_eq!(config.rpc_timeout(), Duration::from_secs(10));
    assert!(config.check_connection());

    client.close().await.unwrap();
}

/// Test close with drain timeout.
#[tokio::test]
#[ignore]
async fn test_close_with_drain_timeout() {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-cov-drain-timeout")
        .check_connection(true)
        .drain_timeout(Duration::from_millis(100))
        .build()
        .await
        .unwrap();

    // Start a subscription (creates spawned task)
    let _sub = client
        .subscribe_to_events(
            "integration.cov-client.drain",
            "",
            |_event| Box::pin(async {}),
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Close with short drain timeout
    client.close().await.unwrap();
    assert_eq!(client.state(), ConnectionState::Closed);
}

/// Test multiple sequential operations.
#[tokio::test]
#[ignore]
async fn test_sequential_operations() {
    let client = live_client().await;

    // Ping
    let info = client.ping().await.unwrap();
    assert!(!info.host.is_empty());

    // Send event
    let event = Event::builder()
        .channel("integration.cov-client.sequential")
        .body(b"seq-test".to_vec())
        .build();
    client.send_event(event).await.unwrap();

    // Publish event convenience
    client
        .publish_event(
            "integration.cov-client.sequential",
            b"convenience",
            Some("meta"),
            None,
        )
        .await
        .unwrap();

    client.close().await.unwrap();
}

/// Test client state is Ready after eager connect.
#[tokio::test]
#[ignore]
async fn test_state_ready_after_eager_connect() {
    let client = live_client().await;
    assert_eq!(client.state(), ConnectionState::Ready);

    // Operations should work
    client.ping().await.unwrap();

    client.close().await.unwrap();
    assert_eq!(client.state(), ConnectionState::Closed);
}

/// Test client clone behavior.
#[tokio::test]
#[ignore]
async fn test_client_clone_operations() {
    let client1 = live_client().await;
    let client2 = client1.clone();
    let client3 = client2.clone();

    // All clones should work
    client1.ping().await.unwrap();
    client2.ping().await.unwrap();
    client3.ping().await.unwrap();

    // Close one should close all
    client1.close().await.unwrap();
    assert_eq!(client2.state(), ConnectionState::Closed);
    assert_eq!(client3.state(), ConnectionState::Closed);
}

/// Test multiple drop without close (exercises Drop impl for each clone).
#[tokio::test]
#[ignore]
async fn test_multiple_drops() {
    {
        let client = live_client().await;
        let _clone1 = client.clone();
        let _clone2 = client.clone();
        // All drop without close - only the last Arc drop triggers the warning
    }
    // Should not panic
}
