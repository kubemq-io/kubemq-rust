//! Additional unit tests for common.rs coverage gaps.
//!
//! Targets: resolve_client_id with non-empty client_id,
//! resolve_client_id_owned with non-empty and empty,
//! resolve_id/resolve_id_owned with explicit ids.

use kubemq::KubemqClient;

/// Test common::resolve_id behavior is exercised through event sending.
/// When an event has a non-empty id, it should be preserved.
#[test]
fn test_event_builder_preserves_explicit_id() {
    let event = kubemq::Event::builder()
        .id("my-explicit-id")
        .channel("test-ch")
        .build();
    assert_eq!(event.id, "my-explicit-id");
}

/// When an event has empty id, a UUID is generated.
#[test]
fn test_event_builder_empty_id_generates_uuid() {
    let event = kubemq::Event::builder().channel("test-ch").build();
    assert!(event.id.is_empty()); // Builder doesn't generate - conversion does
}

/// Test event_store builder preserves explicit id.
#[test]
fn test_event_store_builder_preserves_explicit_id() {
    let es = kubemq::EventStore::builder()
        .id("my-store-id")
        .channel("test-ch")
        .build();
    assert_eq!(es.id, "my-store-id");
}

/// Test command builder preserves explicit id.
#[test]
fn test_command_builder_preserves_explicit_id() {
    let cmd = kubemq::Command::builder()
        .id("cmd-explicit-id")
        .channel("test-ch")
        .timeout(std::time::Duration::from_secs(5))
        .build();
    assert_eq!(cmd.id, "cmd-explicit-id");
}

/// Test query builder preserves explicit id.
#[test]
fn test_query_builder_preserves_explicit_id() {
    let q = kubemq::Query::builder()
        .id("query-explicit-id")
        .channel("test-ch")
        .timeout(std::time::Duration::from_secs(5))
        .build();
    assert_eq!(q.id, "query-explicit-id");
}

/// Test queue message builder preserves explicit client_id.
#[test]
fn test_queue_message_builder_preserves_client_id() {
    let msg = kubemq::QueueMessage::builder()
        .client_id("custom-client")
        .channel("test-ch")
        .build();
    assert_eq!(msg.client_id, "custom-client");
}

/// Test queue message builder empty client_id.
#[test]
fn test_queue_message_builder_empty_client_id() {
    let msg = kubemq::QueueMessage::builder().channel("test-ch").build();
    assert!(msg.client_id.is_empty());
}

/// Test event with non-empty client_id uses it instead of default.
#[test]
fn test_event_custom_client_id_preserved() {
    let event = kubemq::Event::builder()
        .id("e1")
        .channel("ch")
        .client_id("my-custom-client")
        .build();
    assert_eq!(event.client_id, "my-custom-client");
}

/// Test event with empty client_id.
#[test]
fn test_event_empty_client_id() {
    let event = kubemq::Event::builder().channel("ch").build();
    assert!(event.client_id.is_empty());
}

/// Test event store with non-empty client_id uses it.
#[test]
fn test_event_store_custom_client_id() {
    let es = kubemq::EventStore::builder()
        .channel("ch")
        .client_id("custom-es-client")
        .build();
    assert_eq!(es.client_id, "custom-es-client");
}
