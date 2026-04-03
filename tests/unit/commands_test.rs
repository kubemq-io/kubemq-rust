//! Tests for Command type construction and builder.

use kubemq::{Command, CommandBuilder, CommandReply, CommandReplyBuilder};
use std::time::Duration;

#[test]
fn test_command_builder_defaults() {
    let cmd = Command::builder().build();
    assert!(cmd.id.is_empty());
    assert!(cmd.channel.is_empty());
    assert_eq!(cmd.timeout, Duration::from_secs(5)); // default timeout
    assert!(cmd.body.is_empty());
}

#[test]
fn test_command_builder_all_fields() {
    let cmd = Command::builder()
        .id("cmd-1")
        .channel("commands.test")
        .metadata("meta")
        .body(b"request".to_vec())
        .timeout(Duration::from_secs(10))
        .client_id("client-1")
        .add_tag("k", "v")
        .build();

    assert_eq!(cmd.id, "cmd-1");
    assert_eq!(cmd.channel, "commands.test");
    assert_eq!(cmd.metadata, "meta");
    assert_eq!(cmd.body, b"request");
    assert_eq!(cmd.timeout, Duration::from_secs(10));
    assert_eq!(cmd.client_id, "client-1");
    assert_eq!(cmd.tags.get("k").unwrap(), "v");
}

#[test]
fn test_command_builder_default_impl() {
    let builder = CommandBuilder::default();
    let cmd = builder.build();
    assert!(cmd.channel.is_empty());
}

#[test]
fn test_command_clone() {
    let cmd = Command::builder().channel("ch").body(b"x".to_vec()).build();
    let cloned = cmd.clone();
    assert_eq!(cmd.channel, cloned.channel);
    assert_eq!(cmd.body, cloned.body);
}

#[test]
fn test_command_reply_builder_defaults() {
    let reply = CommandReply::builder().build();
    assert!(reply.request_id.is_empty());
    assert!(reply.response_to.is_empty());
    assert!(reply.error.is_none());
}

#[test]
fn test_command_reply_builder_all_fields() {
    let reply = CommandReply::builder()
        .request_id("req-1")
        .response_to("reply-ch")
        .metadata("meta")
        .body(b"resp".to_vec())
        .client_id("responder")
        .executed_at(1234567890)
        .error("something failed")
        .build();

    assert_eq!(reply.request_id, "req-1");
    assert_eq!(reply.response_to, "reply-ch");
    assert_eq!(reply.error, Some("something failed".to_string()));
    assert_eq!(reply.executed_at, 1234567890);
}

#[test]
fn test_command_reply_builder_default_impl() {
    let builder = CommandReplyBuilder::default();
    let reply = builder.build();
    assert!(reply.request_id.is_empty());
}

/// Send command via mock.
#[tokio::test]
async fn test_send_command_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let cmd = Command::builder()
        .channel("commands.test")
        .metadata("meta")
        .body(b"payload".to_vec())
        .timeout(Duration::from_secs(5))
        .build();

    let response = client.send_command(cmd).await.unwrap();
    assert!(response.executed);

    let s = state.lock().unwrap();
    let captured = s.last_request.as_ref().unwrap();
    assert_eq!(captured.channel, "commands.test");
    drop(s);

    client.close().await.unwrap();
}

/// Send command response via mock.
#[tokio::test]
async fn test_send_command_response_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let reply = CommandReply::builder()
        .request_id("req-123")
        .response_to("reply-ch")
        .build();

    client.send_command_response(reply).await.unwrap();
    client.close().await.unwrap();
}
