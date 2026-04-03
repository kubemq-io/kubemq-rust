//! Additional unit tests for error.rs coverage gaps.
//!
//! Targets: remaining Display variants, suggestion() for all error types,
//! ErrorCode Display, From<io::Error>, Transport/Handler variants.

use kubemq::{ErrorCode, KubemqError};

// -- ErrorCode Display tests --

#[test]
fn test_error_code_display_transient() {
    assert_eq!(format!("{}", ErrorCode::Transient), "Transient");
}

#[test]
fn test_error_code_display_timeout() {
    assert_eq!(format!("{}", ErrorCode::Timeout), "Timeout");
}

#[test]
fn test_error_code_display_throttling() {
    assert_eq!(format!("{}", ErrorCode::Throttling), "Throttling");
}

#[test]
fn test_error_code_display_authentication() {
    assert_eq!(format!("{}", ErrorCode::Authentication), "Authentication");
}

#[test]
fn test_error_code_display_authorization() {
    assert_eq!(format!("{}", ErrorCode::Authorization), "Authorization");
}

#[test]
fn test_error_code_display_validation() {
    assert_eq!(format!("{}", ErrorCode::Validation), "Validation");
}

#[test]
fn test_error_code_display_not_found() {
    assert_eq!(format!("{}", ErrorCode::NotFound), "NotFound");
}

#[test]
fn test_error_code_display_fatal() {
    assert_eq!(format!("{}", ErrorCode::Fatal), "Fatal");
}

#[test]
fn test_error_code_display_cancellation() {
    assert_eq!(format!("{}", ErrorCode::Cancellation), "Cancellation");
}

#[test]
fn test_error_code_display_backpressure() {
    assert_eq!(format!("{}", ErrorCode::Backpressure), "Backpressure");
}

// -- KubemqError Display variants --

#[test]
fn test_display_authentication() {
    let err = KubemqError::Authentication {
        code: ErrorCode::Authentication,
        message: "bad token".into(),
        operation: "connect".into(),
        source: None,
        suggestion: "check token",
    };
    let display = format!("{}", err);
    assert!(display.contains("authentication failed"));
    assert!(display.contains("bad token"));
}

#[test]
fn test_display_authorization() {
    let err = KubemqError::Authorization {
        code: ErrorCode::Authorization,
        message: "no access".into(),
        operation: "subscribe".into(),
        channel: "secret-ch".into(),
        source: None,
        suggestion: "check perms",
    };
    let display = format!("{}", err);
    assert!(display.contains("authorization failed"));
    assert!(display.contains("secret-ch"));
}

#[test]
fn test_display_validation() {
    let err = KubemqError::Validation {
        code: ErrorCode::Validation,
        message: "empty channel".into(),
        operation: "send_event".into(),
        channel: "".into(),
        suggestion: "provide channel",
    };
    let display = format!("{}", err);
    assert!(display.contains("validation failed"));
    assert!(display.contains("empty channel"));
}

#[test]
fn test_display_not_found() {
    let err = KubemqError::NotFound {
        code: ErrorCode::NotFound,
        message: "no such channel".into(),
        operation: "subscribe".into(),
        channel: "missing-ch".into(),
        suggestion: "create it",
    };
    let display = format!("{}", err);
    assert!(display.contains("not found"));
    assert!(display.contains("no such channel"));
}

#[test]
fn test_display_fatal() {
    let err = KubemqError::Fatal {
        code: ErrorCode::Fatal,
        message: "internal error".into(),
        operation: "ping".into(),
        source: None,
        suggestion: "contact support",
    };
    let display = format!("{}", err);
    assert!(display.contains("fatal error"));
    assert!(display.contains("internal error"));
}

#[test]
fn test_display_cancellation() {
    let err = KubemqError::Cancellation {
        code: ErrorCode::Cancellation,
        message: "user cancelled".into(),
        operation: "send_event".into(),
    };
    let display = format!("{}", err);
    assert!(display.contains("operation cancelled"));
    assert!(display.contains("user cancelled"));
}

#[test]
fn test_display_throttling() {
    let err = KubemqError::Throttling {
        code: ErrorCode::Throttling,
        message: "too many requests".into(),
        operation: "send_event".into(),
        channel: "events.fast".into(),
        is_retryable: true,
        suggestion: "slow down",
    };
    let display = format!("{}", err);
    assert!(display.contains("throttled"));
    assert!(display.contains("too many requests"));
}

#[test]
fn test_display_buffer_full() {
    let err = KubemqError::BufferFull {
        code: ErrorCode::Backpressure,
        buffer_size: 1000,
        queued_count: 1000,
        suggestion: "increase buffer",
    };
    let display = format!("{}", err);
    assert!(display.contains("backpressure"));
    assert!(display.contains("buffer full"));
    assert!(display.contains("1000"));
}

#[test]
fn test_display_stream_broken() {
    let err = KubemqError::StreamBroken {
        unacknowledged_ids: vec!["a".into(), "b".into()],
        unacknowledged_count: 2,
    };
    let display = format!("{}", err);
    assert!(display.contains("stream broken"));
    assert!(display.contains("2"));
}

#[test]
fn test_display_client_closed() {
    let err = KubemqError::ClientClosed;
    let display = format!("{}", err);
    assert!(display.contains("client closed"));
}

#[test]
fn test_display_transport() {
    let source = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let err = KubemqError::Transport {
        operation: "connect".into(),
        source: Box::new(source),
    };
    let display = format!("{}", err);
    assert!(display.contains("transport error"));
    assert!(display.contains("connect"));
}

#[test]
fn test_display_handler() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "callback error");
    let err = KubemqError::Handler {
        handler: "on_event".into(),
        source: Box::new(source),
    };
    let display = format!("{}", err);
    assert!(display.contains("handler error"));
    assert!(display.contains("on_event"));
}

// -- suggestion() for all variants --

#[test]
fn test_suggestion_transient() {
    let err = KubemqError::Transient {
        code: ErrorCode::Transient,
        message: "test".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "retry the operation",
    };
    assert_eq!(err.suggestion(), "retry the operation");
}

#[test]
fn test_suggestion_timeout() {
    let err = KubemqError::Timeout {
        code: ErrorCode::Timeout,
        message: "test".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: true,
        source: None,
        request_id: String::new(),
        suggestion: "increase timeout",
    };
    assert_eq!(err.suggestion(), "increase timeout");
}

#[test]
fn test_suggestion_authentication() {
    let err = KubemqError::Authentication {
        code: ErrorCode::Authentication,
        message: "test".into(),
        operation: "connect".into(),
        source: None,
        suggestion: "check token",
    };
    assert_eq!(err.suggestion(), "check token");
}

#[test]
fn test_suggestion_authorization() {
    let err = KubemqError::Authorization {
        code: ErrorCode::Authorization,
        message: "test".into(),
        operation: "subscribe".into(),
        channel: "ch".into(),
        source: None,
        suggestion: "check permissions",
    };
    assert_eq!(err.suggestion(), "check permissions");
}

#[test]
fn test_suggestion_not_found() {
    let err = KubemqError::NotFound {
        code: ErrorCode::NotFound,
        message: "test".into(),
        operation: "subscribe".into(),
        channel: "ch".into(),
        suggestion: "create channel",
    };
    assert_eq!(err.suggestion(), "create channel");
}

#[test]
fn test_suggestion_fatal() {
    let err = KubemqError::Fatal {
        code: ErrorCode::Fatal,
        message: "test".into(),
        operation: "ping".into(),
        source: None,
        suggestion: "contact support",
    };
    assert_eq!(err.suggestion(), "contact support");
}

#[test]
fn test_suggestion_throttling() {
    let err = KubemqError::Throttling {
        code: ErrorCode::Throttling,
        message: "test".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: true,
        suggestion: "slow down",
    };
    assert_eq!(err.suggestion(), "slow down");
}

#[test]
fn test_suggestion_buffer_full() {
    let err = KubemqError::BufferFull {
        code: ErrorCode::Backpressure,
        buffer_size: 100,
        queued_count: 100,
        suggestion: "increase buffer size",
    };
    assert_eq!(err.suggestion(), "increase buffer size");
}

#[test]
fn test_suggestion_stream_broken_empty() {
    let err = KubemqError::StreamBroken {
        unacknowledged_ids: vec![],
        unacknowledged_count: 0,
    };
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_suggestion_client_closed_empty() {
    let err = KubemqError::ClientClosed;
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_suggestion_transport_empty() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Transport {
        operation: "connect".into(),
        source: Box::new(source),
    };
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_suggestion_handler_empty() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Handler {
        handler: "callback".into(),
        source: Box::new(source),
    };
    assert_eq!(err.suggestion(), "");
}

#[test]
fn test_suggestion_cancellation_empty() {
    let err = KubemqError::Cancellation {
        code: ErrorCode::Cancellation,
        message: "cancelled".into(),
        operation: "send".into(),
    };
    assert_eq!(
        err.suggestion(),
        "Operation was cancelled. Check if the client is closing."
    );
}

// -- code() for all variants --

#[test]
fn test_code_transport() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Transport {
        operation: "connect".into(),
        source: Box::new(source),
    };
    assert_eq!(err.code(), ErrorCode::Fatal);
}

#[test]
fn test_code_handler() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Handler {
        handler: "callback".into(),
        source: Box::new(source),
    };
    assert_eq!(err.code(), ErrorCode::Fatal);
}

#[test]
fn test_code_buffer_full() {
    let err = KubemqError::BufferFull {
        code: ErrorCode::Backpressure,
        buffer_size: 100,
        queued_count: 100,
        suggestion: "",
    };
    assert_eq!(err.code(), ErrorCode::Backpressure);
}

#[test]
fn test_code_stream_broken() {
    let err = KubemqError::StreamBroken {
        unacknowledged_ids: vec![],
        unacknowledged_count: 0,
    };
    assert_eq!(err.code(), ErrorCode::Transient);
}

// -- is_retryable() for all variants --

#[test]
fn test_is_retryable_throttling_true() {
    let err = KubemqError::Throttling {
        code: ErrorCode::Throttling,
        message: "test".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: true,
        suggestion: "",
    };
    assert!(err.is_retryable());
}

#[test]
fn test_is_retryable_throttling_false() {
    let err = KubemqError::Throttling {
        code: ErrorCode::Throttling,
        message: "test".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: false,
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_authentication_false() {
    let err = KubemqError::Authentication {
        code: ErrorCode::Authentication,
        message: "test".into(),
        operation: "connect".into(),
        source: None,
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_authorization_false() {
    let err = KubemqError::Authorization {
        code: ErrorCode::Authorization,
        message: "test".into(),
        operation: "subscribe".into(),
        channel: "ch".into(),
        source: None,
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_not_found_false() {
    let err = KubemqError::NotFound {
        code: ErrorCode::NotFound,
        message: "test".into(),
        operation: "subscribe".into(),
        channel: "ch".into(),
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_fatal_false() {
    let err = KubemqError::Fatal {
        code: ErrorCode::Fatal,
        message: "test".into(),
        operation: "ping".into(),
        source: None,
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_buffer_full_false() {
    let err = KubemqError::BufferFull {
        code: ErrorCode::Backpressure,
        buffer_size: 100,
        queued_count: 100,
        suggestion: "",
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_transport_false() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Transport {
        operation: "connect".into(),
        source: Box::new(source),
    };
    assert!(!err.is_retryable());
}

#[test]
fn test_is_retryable_handler_false() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "err");
    let err = KubemqError::Handler {
        handler: "callback".into(),
        source: Box::new(source),
    };
    assert!(!err.is_retryable());
}

// -- ErrorCode Clone/Copy/PartialEq/Hash --

#[test]
fn test_error_code_clone_copy() {
    let code = ErrorCode::Transient;
    let copied = code;
    let cloned = code;
    assert_eq!(code, copied);
    assert_eq!(code, cloned);
}

#[test]
fn test_error_code_ne() {
    assert_ne!(ErrorCode::Transient, ErrorCode::Fatal);
    assert_ne!(ErrorCode::Timeout, ErrorCode::Cancellation);
    assert_ne!(ErrorCode::Throttling, ErrorCode::Backpressure);
}

#[test]
fn test_error_code_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(ErrorCode::Transient);
    set.insert(ErrorCode::Fatal);
    set.insert(ErrorCode::Transient); // duplicate
    assert_eq!(set.len(), 2);
}

#[test]
fn test_error_code_debug() {
    let debug = format!("{:?}", ErrorCode::Transient);
    assert_eq!(debug, "Transient");
    let debug = format!("{:?}", ErrorCode::Backpressure);
    assert_eq!(debug, "Backpressure");
}

// -- From<io::Error> --

#[test]
fn test_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let kubemq_err: KubemqError = io_err.into();
    assert_eq!(kubemq_err.code(), ErrorCode::Fatal);
    assert!(!kubemq_err.is_retryable());
    let display = format!("{}", kubemq_err);
    assert!(display.contains("file not found"));
}

#[test]
fn test_from_io_error_permission_denied() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
    let kubemq_err: KubemqError = io_err.into();
    assert_eq!(kubemq_err.code(), ErrorCode::Fatal);
    let display = format!("{}", kubemq_err);
    assert!(display.contains("permission denied"));
}

// -- Timeout with is_retryable false --

#[test]
fn test_timeout_not_retryable() {
    let err = KubemqError::Timeout {
        code: ErrorCode::Timeout,
        message: "final timeout".into(),
        operation: "send".into(),
        channel: "ch".into(),
        is_retryable: false,
        source: None,
        request_id: String::new(),
        suggestion: "give up",
    };
    assert!(!err.is_retryable());
    assert_eq!(err.code(), ErrorCode::Timeout);
}

// -- Transient with non-empty request_id --

#[test]
fn test_transient_with_request_id() {
    let err = KubemqError::Transient {
        code: ErrorCode::Transient,
        message: "connection lost".into(),
        operation: "send_event".into(),
        channel: "ch1".into(),
        is_retryable: true,
        source: None,
        request_id: "req-12345".into(),
        suggestion: "retry",
    };
    assert!(err.is_retryable());
    let display = format!("{}", err);
    assert!(display.contains("connection lost"));
}
