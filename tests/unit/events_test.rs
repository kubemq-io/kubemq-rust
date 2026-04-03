//! Tests for Event type construction, builder, and sending via mock.

use kubemq::{Event, EventBuilder};
use std::collections::HashMap;

#[test]
fn test_event_builder_defaults() {
    let event = Event::builder().build();
    assert!(event.id.is_empty());
    assert!(event.channel.is_empty());
    assert!(event.metadata.is_empty());
    assert!(event.body.is_empty());
    assert!(event.client_id.is_empty());
    assert!(event.tags.is_empty());
}

#[test]
fn test_event_builder_all_fields() {
    let mut tags = HashMap::new();
    tags.insert("env".to_string(), "test".to_string());

    let event = Event::builder()
        .id("evt-1")
        .channel("events.test")
        .metadata("some-meta")
        .body(b"hello world".to_vec())
        .client_id("my-client")
        .tags(tags.clone())
        .build();

    assert_eq!(event.id, "evt-1");
    assert_eq!(event.channel, "events.test");
    assert_eq!(event.metadata, "some-meta");
    assert_eq!(event.body, b"hello world");
    assert_eq!(event.client_id, "my-client");
    assert_eq!(event.tags, tags);
}

#[test]
fn test_event_builder_add_tag() {
    let event = Event::builder()
        .add_tag("key1", "val1")
        .add_tag("key2", "val2")
        .build();
    assert_eq!(event.tags.len(), 2);
    assert_eq!(event.tags.get("key1").unwrap(), "val1");
}

#[test]
fn test_event_builder_default_impl() {
    let builder = EventBuilder::default();
    let event = builder.build();
    assert!(event.channel.is_empty());
}

#[test]
fn test_event_clone() {
    let event = Event::builder()
        .channel("ch1")
        .body(b"data".to_vec())
        .build();
    let cloned = event.clone();
    assert_eq!(event.channel, cloned.channel);
    assert_eq!(event.body, cloned.body);
}

/// Send event via mock server and verify it was received.
#[tokio::test]
async fn test_send_event_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder()
        .channel("events.test")
        .metadata("test-meta")
        .body(b"payload".to_vec())
        .build();

    client.send_event(event).await.unwrap();

    // Verify mock received the event
    let s = state.lock().unwrap();
    let captured = s.last_event.as_ref().unwrap();
    assert_eq!(captured.channel, "events.test");
    assert_eq!(captured.metadata, "test-meta");
    assert_eq!(captured.body, b"payload");
    assert!(!captured.store); // Regular events have store=false
    drop(s);

    client.close().await.unwrap();
}

/// Send event with server error returns Err.
#[tokio::test]
async fn test_send_event_server_error() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    {
        let mut s = state.lock().unwrap();
        s.send_event_error = Some(tonic::Status::unavailable("server down"));
    }
    let client = crate::mock_server::build_test_client(addr).await;

    let event = Event::builder()
        .channel("events.test")
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_err());

    client.close().await.unwrap();
}

/// REQ-H4: Wildcards are rejected on send_event (publish).
#[tokio::test]
async fn test_wildcard_validation_on_send_event() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let wildcard_channels = vec!["events.*", "events.>", "*.test", ">"];
    for channel in wildcard_channels {
        let event = Event::builder().channel(channel).metadata("test").build();
        let result = client.send_event(event).await;
        assert!(
            result.is_err(),
            "Channel '{}' should be rejected by wildcard validation",
            channel
        );
        let err = result.unwrap_err();
        assert_eq!(
            err.code(),
            kubemq::ErrorCode::Validation,
            "Channel '{}' should produce Validation error, got {:?}",
            channel,
            err.code()
        );
    }

    client.close().await.unwrap();
}

/// REQ-H5: Empty channel is rejected on event stream send.
#[tokio::test]
async fn test_stream_send_validates_channel() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    // Empty channel should be rejected at the send_event level
    let event = Event::builder().channel("").metadata("test").build();
    let result = client.send_event(event).await;
    assert!(result.is_err(), "Empty channel should be rejected");
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);

    // Valid channel should pass
    let event = Event::builder()
        .channel("test-channel")
        .metadata("test")
        .build();
    let result = client.send_event(event).await;
    assert!(result.is_ok(), "Valid channel should be accepted");

    client.close().await.unwrap();
}

/// REQ-M55: Panicking async callback should be caught by catch_unwind.
#[tokio::test]
async fn test_callback_panic_caught() {
    use futures::FutureExt;
    use std::future::Future;
    use std::pin::Pin;

    // Simulate the panic-safe callback pattern used in subscription loops
    let panicking_callback = |_: kubemq::EventReceive| -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async { panic!("user callback panic") })
    };

    // Create a dummy EventReceive
    let event = kubemq::EventReceive {
        id: "test".to_string(),
        channel: "test".to_string(),
        metadata: String::new(),
        body: Vec::new(),
        timestamp: 0,
        sequence: 0,
        tags: HashMap::new(),
    };

    // The catch_unwind pattern from REQ-M55 should catch this
    let result = std::panic::AssertUnwindSafe(panicking_callback(event))
        .catch_unwind()
        .await;

    assert!(
        result.is_err(),
        "Panic should be caught by catch_unwind on the Future"
    );
    // If we reach here, the panic was caught and the test task survived
}
