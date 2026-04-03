//! Tests for ConnectionState and RetryPolicy.

use kubemq::JitterMode;
use kubemq::{ConnectionState, RetryPolicy};
use std::time::Duration;

#[test]
fn test_connection_state_enum_values() {
    // Verify all states are distinct
    let states = [
        ConnectionState::Idle,
        ConnectionState::Ready,
        ConnectionState::Closed,
    ];
    for i in 0..states.len() {
        for j in (i + 1)..states.len() {
            assert_ne!(states[i], states[j]);
        }
    }
}

#[test]
fn test_connection_state_debug() {
    let state = ConnectionState::Ready;
    let debug = format!("{:?}", state);
    assert!(debug.contains("Ready"));
}

#[test]
fn test_connection_state_clone() {
    let state = ConnectionState::Ready;
    let cloned = state;
    assert_eq!(state, cloned);
}

#[test]
fn test_retry_policy_default() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_retries, 3);
    assert_eq!(policy.initial_backoff, Duration::from_millis(100));
    assert_eq!(policy.max_backoff, Duration::from_secs(10));
    assert_eq!(policy.multiplier, 2.0);
    assert_eq!(policy.jitter_mode, JitterMode::Full);
}

#[test]
fn test_retry_policy_custom() {
    let policy = RetryPolicy {
        max_retries: 10,
        initial_backoff: Duration::from_millis(500),
        max_backoff: Duration::from_secs(60),
        multiplier: 3.0,
        jitter_mode: JitterMode::None,
    };
    assert_eq!(policy.max_retries, 10);
    assert_eq!(policy.multiplier, 3.0);
}

/// Client starts in a valid state after connection.
#[tokio::test]
async fn test_client_state_after_connect() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;
    // After lazy connect (check_connection=false), state should be Idle.
    // State transitions to Ready only after the first successful gRPC call
    // or when check_connection=true (eager connect).
    let state = client.state();
    assert!(
        state == ConnectionState::Ready || state == ConnectionState::Idle,
        "unexpected state: {:?}",
        state
    );
    client.close().await.unwrap();
}

/// After close, operations should fail with ClientClosed.
#[tokio::test]
async fn test_client_closed_rejects_operations() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;
    client.close().await.unwrap();

    let result = client.ping().await;
    assert!(result.is_err());
    // close is idempotent
    client.close().await.unwrap();
}

/// Test ConnectionState Display trait.
#[test]
fn test_connection_state_display_all() {
    assert_eq!(format!("{}", ConnectionState::Idle), "Idle");
    assert_eq!(format!("{}", ConnectionState::Ready), "Ready");
    assert_eq!(format!("{}", ConnectionState::Closed), "Closed");
}

/// Test backoff with None jitter mode.
#[test]
fn test_retry_backoff_none_jitter() {
    let policy = RetryPolicy {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter_mode: JitterMode::None,
    };
    // With no jitter, backoff is deterministic
    assert_eq!(policy.backoff(0), Duration::from_millis(100));
    assert_eq!(policy.backoff(1), Duration::from_millis(200));
    assert_eq!(policy.backoff(2), Duration::from_millis(400));
}

/// Test backoff capping at max_backoff.
#[test]
fn test_retry_backoff_capped() {
    let policy = RetryPolicy {
        max_retries: 10,
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(5),
        multiplier: 10.0,
        jitter_mode: JitterMode::None,
    };
    assert_eq!(policy.backoff(5), Duration::from_secs(5));
}

/// Test backoff with Full jitter stays bounded.
#[test]
fn test_retry_backoff_full_jitter_bounded() {
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter_mode: JitterMode::Full,
    };
    for _ in 0..50 {
        let b = policy.backoff(0);
        assert!(b <= Duration::from_millis(100));
    }
}

/// Test backoff with Equal jitter stays in [half, full].
#[test]
fn test_retry_backoff_equal_jitter_bounded() {
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter_mode: JitterMode::Equal,
    };
    for _ in 0..50 {
        let b = policy.backoff(0);
        assert!(b >= Duration::from_millis(50));
        assert!(b <= Duration::from_millis(100));
    }
}

/// Test RetryPolicy Debug and Clone.
#[test]
fn test_retry_policy_debug_clone() {
    let policy = RetryPolicy {
        max_retries: 7,
        initial_backoff: Duration::from_millis(250),
        max_backoff: Duration::from_secs(20),
        multiplier: 1.5,
        jitter_mode: JitterMode::Equal,
    };
    let debug = format!("{:?}", policy);
    assert!(debug.contains("RetryPolicy"));

    let cloned = policy.clone();
    assert_eq!(cloned.max_retries, 7);
    assert_eq!(cloned.multiplier, 1.5);
    assert_eq!(cloned.jitter_mode, JitterMode::Equal);
}

/// Test client clone shares state.
#[tokio::test]
async fn test_client_clone_shares_state() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;
    let cloned = client.clone();
    // Both should report the same state
    assert_eq!(client.state(), cloned.state());
    // Closing one should close both
    client.close().await.unwrap();
    let result = cloned.ping().await;
    assert!(result.is_err());
}
