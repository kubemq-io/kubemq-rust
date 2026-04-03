//! Tests for validation rules.
//! Note: validation functions are pub(crate), so we test them indirectly
//! through the public API (e.g., building events with invalid channels).

use kubemq::{Command, ErrorCode, Event};
use std::collections::HashMap;

/// Attempting to send an event with empty channel should fail validation.
#[tokio::test]
async fn test_empty_channel_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder().channel("").metadata("test").build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    client.close().await.unwrap();
}

/// Channel with whitespace should fail.
#[tokio::test]
async fn test_whitespace_channel_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder()
        .channel("events test")
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}

/// Channel ending with dot should fail.
#[tokio::test]
async fn test_trailing_dot_channel_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder().channel("events.").metadata("test").build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}

/// Channel too long (>256 chars).
#[tokio::test]
async fn test_long_channel_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let long_channel = "a".repeat(257);
    let event = Event::builder()
        .channel(long_channel)
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}

/// Channel with invalid chars should fail.
#[tokio::test]
async fn test_invalid_char_channel_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder()
        .channel("events@test")
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}

/// Valid channel passes.
#[tokio::test]
async fn test_valid_channel_accepted() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder()
        .channel("events-test_1.2")
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_ok());
    client.close().await.unwrap();
}

/// Tag with empty key should fail.
#[tokio::test]
async fn test_empty_tag_key_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let mut tags = HashMap::new();
    tags.insert(String::new(), "value".to_string());
    let event = Event::builder()
        .channel("test-ch")
        .metadata("test")
        .tags(tags)
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}

/// Command with zero timeout should fail.
#[tokio::test]
async fn test_command_zero_timeout_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let cmd = Command::builder()
        .channel("cmds.test")
        .metadata("test")
        .timeout(std::time::Duration::ZERO)
        .build();
    let result = client.send_command(cmd).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}
