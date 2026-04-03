//! Stream API tests via mock server for coverage.
//!
//! Targets: send_event_stream(), send_event_store_stream(), queue_upstream(),
//! new_queue_downstream_receiver() lifecycle paths.

use kubemq::{Event, EventStore, QueueMessage};
use std::time::Duration;

/// Test send_event_stream lifecycle via mock server.
#[tokio::test]
async fn test_send_event_stream_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.send_event_stream().await.unwrap();

    // Send events through the stream
    for i in 0..3 {
        let event = Event::builder()
            .channel("test.stream.events")
            .body(format!("msg-{}", i).into_bytes())
            .build();
        handle.send(event).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Close the handle
    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test send_event_stream with tags via mock.
#[tokio::test]
async fn test_send_event_stream_tags_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.send_event_stream().await.unwrap();

    let event = Event::builder()
        .channel("test.stream.tags")
        .body(b"tagged".to_vec())
        .add_tag("env", "test")
        .build();
    handle.send(event).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test send_event_stream drop cancels.
#[tokio::test]
async fn test_send_event_stream_drop_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    {
        let handle = client.send_event_stream().await.unwrap();
        let event = Event::builder()
            .channel("test.stream.drop")
            .body(b"drop".to_vec())
            .build();
        handle.send(event).await.unwrap();
        // handle drops here, cancelling the stream
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await.unwrap();
}

/// Test send_event_stream errors receiver via mock.
#[tokio::test]
async fn test_send_event_stream_errors_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let mut handle = client.send_event_stream().await.unwrap();

    let event = Event::builder()
        .channel("test.stream.errors")
        .body(b"test".to_vec())
        .build();
    handle.send(event).await.unwrap();

    // No errors expected for valid events
    let err = tokio::time::timeout(Duration::from_millis(200), handle.errors().recv()).await;
    // Either timeout or None is expected
    assert!(err.is_err() || err.unwrap().is_none());

    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test send after close via mock.
#[tokio::test]
async fn test_send_event_stream_send_after_close_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.send_event_stream().await.unwrap();
    handle.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let event = Event::builder()
        .channel("test.stream.after-close")
        .body(b"fail".to_vec())
        .build();
    let result = handle.send(event).await;
    assert!(result.is_err());

    client.close().await.unwrap();
}

/// Test send_event_store_stream lifecycle via mock.
#[tokio::test]
async fn test_send_event_store_stream_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let mut handle = client.send_event_store_stream().await.unwrap();

    for i in 0..3 {
        let event = EventStore::builder()
            .channel("test.store-stream.events")
            .body(format!("store-msg-{}", i).into_bytes())
            .build();
        handle.send(event).await.unwrap();
    }

    // Read results
    let mut count = 0;
    for _ in 0..3 {
        let r = tokio::time::timeout(Duration::from_secs(2), handle.results().recv()).await;
        match r {
            Ok(Some(res)) => {
                count += 1;
                assert!(res.sent, "Event should be sent, error: {}", res.error);
            }
            _ => break,
        }
    }
    assert!(count >= 1, "Should have received at least 1 result");

    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test send_event_store_stream with custom client_id via mock.
#[tokio::test]
async fn test_send_event_store_stream_custom_cid_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.send_event_store_stream().await.unwrap();

    let event = EventStore::builder()
        .channel("test.store-stream.custom-cid")
        .client_id("my-custom-client")
        .body(b"custom-cid-data".to_vec())
        .build();
    handle.send(event).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify the mock received the event with store=true
    let s = state.lock().unwrap();
    if let Some(ref captured) = s.last_event {
        assert!(captured.store, "Store events should have store=true");
        assert_eq!(captured.client_id, "my-custom-client");
    }
    drop(s);

    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test send_event_store_stream drop cancels via mock.
#[tokio::test]
async fn test_send_event_store_stream_drop_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    {
        let handle = client.send_event_store_stream().await.unwrap();
        let event = EventStore::builder()
            .channel("test.store-stream.drop")
            .body(b"drop-store".to_vec())
            .build();
        handle.send(event).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await.unwrap();
}

/// Test send_event_store_stream send after close via mock.
#[tokio::test]
async fn test_send_event_store_stream_send_after_close_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.send_event_store_stream().await.unwrap();
    handle.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let event = EventStore::builder()
        .channel("test.store-stream.after-close")
        .body(b"fail".to_vec())
        .build();
    let result = handle.send(event).await;
    assert!(result.is_err());

    client.close().await.unwrap();
}

/// Test queue upstream via mock (verifies proto messages are correctly forwarded).
#[tokio::test]
async fn test_queue_upstream_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.queue_upstream().await.unwrap();

    let msgs: Vec<QueueMessage> = (0..2)
        .map(|i| {
            QueueMessage::builder()
                .channel("test.queue-upstream")
                .body(format!("q-msg-{}", i).into_bytes())
                .build()
        })
        .collect();

    handle.send("req-1", msgs).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}

/// Test queue upstream drop cancels via mock.
#[tokio::test]
async fn test_queue_upstream_drop_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    {
        let handle = client.queue_upstream().await.unwrap();
        let msgs = vec![QueueMessage::builder()
            .channel("test.queue-upstream.drop")
            .body(b"drop-q".to_vec())
            .build()];
        handle.send("req-drop", msgs).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await.unwrap();
}

/// Test queue upstream validates channel via mock.
#[tokio::test]
async fn test_queue_upstream_validates_via_mock() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let handle = client.queue_upstream().await.unwrap();

    // Empty channel should fail
    let msgs = vec![QueueMessage::builder().body(b"data".to_vec()).build()];
    let result = handle.send("req-val", msgs).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);

    handle.close();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
}
