//! Tests for Query type construction, builder, and cache fields.

use kubemq::{Query, QueryBuilder, QueryReply, QueryReplyBuilder};
use std::time::Duration;

#[test]
fn test_query_builder_defaults() {
    let q = Query::builder().build();
    assert!(q.id.is_empty());
    assert!(q.channel.is_empty());
    assert_eq!(q.timeout, Duration::from_secs(5));
    assert!(q.cache_key.is_empty());
    assert_eq!(q.cache_ttl, Duration::ZERO);
}

#[test]
fn test_query_builder_all_fields() {
    let q = Query::builder()
        .id("q-1")
        .channel("queries.test")
        .metadata("meta")
        .body(b"request".to_vec())
        .timeout(Duration::from_secs(10))
        .client_id("client-1")
        .cache_key("my-cache-key")
        .cache_ttl(Duration::from_secs(60))
        .add_tag("env", "test")
        .build();

    assert_eq!(q.id, "q-1");
    assert_eq!(q.channel, "queries.test");
    assert_eq!(q.cache_key, "my-cache-key");
    assert_eq!(q.cache_ttl, Duration::from_secs(60));
    assert_eq!(q.tags.get("env").unwrap(), "test");
}

#[test]
fn test_query_builder_default_impl() {
    let builder = QueryBuilder::default();
    let q = builder.build();
    assert!(q.channel.is_empty());
}

#[test]
fn test_query_clone() {
    let q = Query::builder()
        .channel("ch")
        .body(b"x".to_vec())
        .cache_key("ck")
        .build();
    let cloned = q.clone();
    assert_eq!(q.channel, cloned.channel);
    assert_eq!(q.cache_key, cloned.cache_key);
}

#[test]
fn test_query_reply_builder_defaults() {
    let reply = QueryReply::builder().build();
    assert!(reply.request_id.is_empty());
    assert!(reply.response_to.is_empty());
    assert!(!reply.cache_hit);
    assert!(reply.error.is_none());
}

#[test]
fn test_query_reply_builder_all_fields() {
    let reply = QueryReply::builder()
        .request_id("req-1")
        .response_to("reply-ch")
        .metadata("meta")
        .body(b"response".to_vec())
        .client_id("responder")
        .executed_at(1234567890)
        .error("query failed")
        .build();

    assert_eq!(reply.request_id, "req-1");
    assert_eq!(reply.response_to, "reply-ch");
    assert_eq!(reply.error, Some("query failed".to_string()));
}

#[test]
fn test_query_reply_builder_default_impl() {
    let builder = QueryReplyBuilder::default();
    let reply = builder.build();
    assert!(reply.request_id.is_empty());
}

/// Send query via mock.
#[tokio::test]
async fn test_send_query_via_mock() {
    let (addr, state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let q = Query::builder()
        .channel("queries.test")
        .metadata("meta")
        .body(b"query-payload".to_vec())
        .timeout(Duration::from_secs(5))
        .build();

    let response = client.send_query(q).await.unwrap();
    assert!(response.executed);

    let s = state.lock().unwrap();
    let captured = s.last_request.as_ref().unwrap();
    assert_eq!(captured.channel, "queries.test");
    drop(s);

    client.close().await.unwrap();
}

/// Query with cache_key but zero cache_ttl should fail validation.
#[tokio::test]
async fn test_query_cache_key_without_ttl_rejected() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;

    let q = Query::builder()
        .channel("queries.test")
        .metadata("meta")
        .timeout(Duration::from_secs(5))
        .cache_key("my-key")
        .cache_ttl(Duration::ZERO)
        .build();

    let result = client.send_query(q).await;
    assert!(result.is_err());
    client.close().await.unwrap();
}
