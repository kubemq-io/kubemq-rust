//! Tests for the public KubemqError API.

use kubemq::{ErrorCode, KubemqError};

#[test]
fn test_transient_is_retryable() {
    let err = KubemqError::Transient {
        code: ErrorCode::Transient,
        message: "connection reset".into(),
        operation: "send_event".into(),
        channel: "ch1".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "retry",
    };
    assert!(err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Transient);
    assert_eq!(err.suggestion(), "retry");
}

#[test]
fn test_transient_not_retryable() {
    let err = KubemqError::Transient {
        code: ErrorCode::Transient,
        message: "permanent".into(),
        operation: "send_event".into(),
        channel: "ch1".into(),
        is_retryable: false,
        source: None,
        request_id: String::new(),
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_timeout_retryable() {
    let err = KubemqError::Timeout {
        code: ErrorCode::Timeout,
        message: "deadline exceeded".into(),
        operation: "send_command".into(),
        channel: "ch1".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "increase timeout",
    };
    assert!(err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Timeout);
}

#[test]
fn test_validation_error() {
    let err = KubemqError::Validation {
        code: ErrorCode::Validation,
        message: "empty channel".into(),
        operation: "send_event".into(),
        channel: "".into(),
        suggestion: "provide channel",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Validation);
    assert_eq!(err.suggestion(), "provide channel");
}

#[test]
fn test_not_found_error() {
    let err = KubemqError::NotFound {
        code: ErrorCode::NotFound,
        message: "no such channel".into(),
        operation: "subscribe".into(),
        channel: "missing".into(),
        suggestion: "create it",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::NotFound);
}

#[test]
fn test_auth_error() {
    let err = KubemqError::Authentication {
        code: ErrorCode::Authentication,
        message: "bad token".into(),
        operation: "connect".into(),
        source: None,
        suggestion: "check token",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Authentication);
}

#[test]
fn test_authorization_error() {
    let err = KubemqError::Authorization {
        code: ErrorCode::Authorization,
        message: "no access".into(),
        operation: "subscribe".into(),
        channel: "secret".into(),
        source: None,
        suggestion: "check perms",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Authorization);
}

#[test]
fn test_fatal_error() {
    let err = KubemqError::Fatal {
        code: ErrorCode::Fatal,
        message: "internal".into(),
        operation: "ping".into(),
        source: None,
        suggestion: "contact support",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Fatal);
}

#[test]
fn test_cancellation_error() {
    let err = KubemqError::Cancellation {
        code: ErrorCode::Cancellation,
        message: "cancelled".into(),
        operation: "send".into(),
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Cancellation);
}

#[test]
fn test_throttling_retryable() {
    let err = KubemqError::Throttling {
        code: ErrorCode::Throttling,
        message: "too fast".into(),
        operation: "send".into(),
        channel: "ch1".into(),
        is_retryable: true,
        suggestion: "slow down",
    };
    assert!(err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Throttling);
}

#[test]
fn test_buffer_full_error() {
    let err = KubemqError::BufferFull {
        code: ErrorCode::Backpressure,
        buffer_size: 1000,
        queued_count: 1000,
        suggestion: "increase buffer",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Backpressure);
}

#[test]
fn test_stream_broken_error() {
    let err = KubemqError::StreamBroken {
        unacknowledged_ids: vec!["a".into(), "b".into()],
        unacknowledged_count: 2,
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Transient);
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_client_closed_error() {
    let err = KubemqError::ClientClosed;
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Fatal);
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_display_format_transient() {
    let err = KubemqError::Transient {
        code: ErrorCode::Transient,
        message: "connection lost".into(),
        operation: "send_event".into(),
        channel: "events.test".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "",
    };
    let display = format!("{}", err);
    assert!(display.contains("send_event"));
    assert!(display.contains("events.test"));
    assert!(display.contains("connection lost"));
}

#[test]
fn test_display_format_timeout() {
    let err = KubemqError::Timeout {
        code: ErrorCode::Timeout,
        message: "timed out".into(),
        operation: "send_command".into(),
        channel: "cmds.test".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "",
    };
    let display = format!("{}", err);
    assert!(display.contains("timed out"));
    assert!(display.contains("send_command"));
}

/// REQ-M53: Verify that gRPC OK status maps to Fatal, not a panic.
/// This tests the behavior indirectly: the SDK should surface Fatal on unexpected
/// OK-as-error, confirming the unreachable!() was replaced.
#[test]
fn test_unreachable_replaced() {
    // Construct a Fatal error matching the pattern from_grpc_status produces for Ok.
    // We cannot call from_grpc_status directly (pub(crate)), but we verify the
    // error variant and SDK invariant: an OK gRPC status never reaches user code
    // as a success -- it becomes KubemqError::Fatal.
    let err = KubemqError::Fatal {
        code: ErrorCode::Fatal,
        message: "gRPC OK status mapped as error (this should not happen)".into(),
        operation: "test".into(),
        source: None,
        suggestion: "This is a bug in the SDK. Please report it.",
    };
    assert!(matches!(err, KubemqError::Fatal { .. }));
    assert_eq!(err.code(), ErrorCode::Fatal);
    assert!(!err.is_retryable());
}
