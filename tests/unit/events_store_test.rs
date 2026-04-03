//! Tests for EventStore types, builder, and subscription start positions.

use kubemq::{EventStore, EventStoreBuilder, EventsStoreSubscription};
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn test_event_store_builder_defaults() {
    let es = EventStore::builder().build();
    assert!(es.id.is_empty());
    assert!(es.channel.is_empty());
    assert!(es.body.is_empty());
}

#[test]
fn test_event_store_builder_all_fields() {
    let es = EventStore::builder()
        .id("es-1")
        .channel("store.ch")
        .metadata("meta")
        .body(b"data".to_vec())
        .client_id("client-1")
        .add_tag("k", "v")
        .build();

    assert_eq!(es.id, "es-1");
    assert_eq!(es.channel, "store.ch");
    assert_eq!(es.metadata, "meta");
    assert_eq!(es.body, b"data");
    assert_eq!(es.client_id, "client-1");
    assert_eq!(es.tags.get("k").unwrap(), "v");
}

#[test]
fn test_event_store_builder_default_trait() {
    let builder = EventStoreBuilder::default();
    let es = builder.build();
    assert!(es.channel.is_empty());
}

#[test]
fn test_event_store_clone() {
    let es = EventStore::builder()
        .channel("ch")
        .body(b"x".to_vec())
        .build();
    let cloned = es.clone();
    assert_eq!(es.channel, cloned.channel);
}

/// All 6 EventsStoreSubscription variants can be constructed.
#[test]
fn test_events_store_subscription_start_new_only() {
    let sub = EventsStoreSubscription::StartNewOnly;
    assert!(matches!(sub, EventsStoreSubscription::StartNewOnly));
}

#[test]
fn test_events_store_subscription_start_from_first() {
    let sub = EventsStoreSubscription::StartFromFirst;
    assert!(matches!(sub, EventsStoreSubscription::StartFromFirst));
}

#[test]
fn test_events_store_subscription_start_from_last() {
    let sub = EventsStoreSubscription::StartFromLast;
    assert!(matches!(sub, EventsStoreSubscription::StartFromLast));
}

#[test]
fn test_events_store_subscription_start_at_sequence() {
    let sub = EventsStoreSubscription::StartAtSequence(42);
    if let EventsStoreSubscription::StartAtSequence(seq) = sub {
        assert_eq!(seq, 42);
    } else {
        panic!("expected StartAtSequence");
    }
}

#[test]
fn test_events_store_subscription_start_at_time() {
    let t = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let sub = EventsStoreSubscription::StartAtTime(t);
    if let EventsStoreSubscription::StartAtTime(time) = sub {
        assert_eq!(time, t);
    } else {
        panic!("expected StartAtTime");
    }
}

#[test]
fn test_events_store_subscription_start_at_time_delta() {
    let delta = Duration::from_secs(3600);
    let sub = EventsStoreSubscription::StartAtTimeDelta(delta);
    if let EventsStoreSubscription::StartAtTimeDelta(d) = sub {
        assert_eq!(d, delta);
    } else {
        panic!("expected StartAtTimeDelta");
    }
}

/// Send event store via mock.
#[tokio::test]
async fn test_send_event_store_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let es = EventStore::builder()
        .channel("store.test")
        .metadata("meta")
        .body(b"payload".to_vec())
        .build();

    let result = client.send_event_store(es).await.unwrap();
    assert!(result.sent);

    let s = state.lock().unwrap();
    let captured = s.last_event.as_ref().unwrap();
    assert_eq!(captured.channel, "store.test");
    assert!(captured.store); // Store events have store=true
    drop(s);

    client.close().await.unwrap();
}
