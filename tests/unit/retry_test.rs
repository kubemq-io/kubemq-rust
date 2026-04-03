//! Tests for RetryPolicy and backoff calculation.

use kubemq::{JitterMode, RetryPolicy};
use std::time::Duration;

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
fn test_backoff_no_jitter_exponential() {
    let policy = RetryPolicy {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(60),
        multiplier: 2.0,
        jitter_mode: JitterMode::None,
    };
    assert_eq!(policy.backoff(0), Duration::from_millis(100));
    assert_eq!(policy.backoff(1), Duration::from_millis(200));
    assert_eq!(policy.backoff(2), Duration::from_millis(400));
    assert_eq!(policy.backoff(3), Duration::from_millis(800));
}

#[test]
fn test_backoff_capped_at_max() {
    let policy = RetryPolicy {
        max_retries: 10,
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(5),
        multiplier: 10.0,
        jitter_mode: JitterMode::None,
    };
    // attempt 5 would be 1 * 10^5 = 100_000s, but capped at 5s
    assert_eq!(policy.backoff(5), Duration::from_secs(5));
}

#[test]
fn test_backoff_full_jitter_bounded() {
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter_mode: JitterMode::Full,
    };
    for _ in 0..50 {
        let b = policy.backoff(0);
        assert!(b <= Duration::from_millis(100), "got {:?}", b);
    }
}

#[test]
fn test_backoff_equal_jitter_bounded() {
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter_mode: JitterMode::Equal,
    };
    for _ in 0..50 {
        let b = policy.backoff(0);
        assert!(b >= Duration::from_millis(50), "got {:?}", b);
        assert!(b <= Duration::from_millis(100), "got {:?}", b);
    }
}

#[test]
fn test_jitter_mode_values() {
    assert_ne!(JitterMode::None, JitterMode::Full);
    assert_ne!(JitterMode::Full, JitterMode::Equal);
    assert_ne!(JitterMode::None, JitterMode::Equal);
}

#[test]
fn test_retry_policy_clone() {
    let policy = RetryPolicy {
        max_retries: 5,
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(30),
        multiplier: 3.0,
        jitter_mode: JitterMode::None,
    };
    let cloned = policy.clone();
    assert_eq!(policy.max_retries, cloned.max_retries);
    assert_eq!(policy.multiplier, cloned.multiplier);
}
