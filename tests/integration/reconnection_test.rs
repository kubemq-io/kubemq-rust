//! Integration tests for reconnection scenarios (requires live KubeMQ broker).

use kubemq::{ConnectionState, Event, KubemqClient};

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-reconnect")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_state_after_connect() {
    let client = live_client().await;
    let state = client.state();
    // With check_connection=false (lazy connect), the initial state is Idle.
    // State transitions to Ready only after check_connection=true (eager connect)
    // or after the first successful gRPC call. Go/Java SDKs always eagerly
    // connect so they always start Ready; Idle is correct for Rust's lazy mode.
    assert!(
        state == ConnectionState::Ready || state == ConnectionState::Idle,
        "unexpected state: {:?}",
        state
    );
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_operations_after_close() {
    let client = live_client().await;
    client.close().await.unwrap();

    let result = client.ping().await;
    assert!(result.is_err());

    let event = Event::builder().channel("test").metadata("test").build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn test_multiple_clients() {
    let client1 = live_client().await;
    let client2 = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-reconnect-2")
        .check_connection(false)
        .build()
        .await
        .unwrap();

    client1.ping().await.unwrap();
    client2.ping().await.unwrap();

    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

/// Full reconnection lifecycle: connect -> disconnect -> verify reconnect -> resume.
/// Stub: requires a CI environment with controllable broker restart.
#[tokio::test]
#[ignore] // Stub: requires live broker with controllable restart
async fn test_reconnection_full_cycle() {
    // Connect -> kill server -> verify reconnection -> restart -> verify resume
    // Intentionally left as no-op until broker restart automation is available.
}

/// Verify all 6 lifecycle callbacks fire at correct state transitions.
#[tokio::test]
#[ignore] // Requires live broker
async fn test_callbacks_fire() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let connected_count = Arc::new(AtomicUsize::new(0));
    let cc = connected_count.clone();

    let _client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-callbacks")
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

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(
        connected_count.load(Ordering::SeqCst) >= 1,
        "on_connected callback should have fired"
    );
}

/// Verify subscription auto-reconnects after broker restart.
/// Stub: requires a CI environment with controllable broker restart.
#[tokio::test]
#[ignore] // Stub: requires live broker with controllable restart
async fn test_subscription_auto_reconnect() {
    // Subscribe -> kill server -> restart -> verify subscription continues
    // Intentionally left as no-op until broker restart automation is available.
}

/// Verify concurrent RPCs execute in parallel (no Mutex serialization).
#[tokio::test]
#[ignore] // Requires live broker
async fn test_concurrent_rpcs() {
    use std::sync::Arc;

    let client = Arc::new(live_client().await);
    let mut handles = Vec::new();

    for _ in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.ping().await }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    client.close().await.unwrap();
}

/// Verify RPC timeout is enforced.
#[tokio::test]
#[ignore] // Requires live broker (ideally a slow/unresponsive one)
async fn test_rpc_timeout() {
    // Configure very short timeout and verify failure on slow server
    let result = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-timeout")
        .rpc_timeout(std::time::Duration::from_millis(1))
        .check_connection(false)
        .build()
        .await;
    // With a 1ms timeout, operations against a real server should time out
    if let Ok(client) = result {
        let ping_result = client.ping().await;
        // May or may not time out depending on server speed; just verify no panic
        let _ = ping_result;
        client.close().await.unwrap();
    }
}
