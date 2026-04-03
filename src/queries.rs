//! Queries (RPC with Cache) messaging pattern.

use std::collections::HashMap;
use std::time::Duration;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq as proto;
use crate::subscription::Subscription;
use crate::validate;

/// Outbound query request.
#[derive(Debug, Clone)]
pub struct Query {
    pub id: String,
    pub channel: String,
    pub metadata: String,
    pub body: Vec<u8>,
    pub timeout: Duration,
    pub client_id: String,
    pub cache_key: String,
    pub cache_ttl: Duration,
    pub tags: HashMap<String, String>,
    pub span: Vec<u8>,
}

impl Query {
    /// Create a builder for constructing a query.
    pub fn builder() -> QueryBuilder {
        QueryBuilder::new()
    }
}

/// Builder for creating queries.
pub struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            query: Query {
                id: String::new(),
                channel: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                timeout: Duration::from_secs(5),
                client_id: String::new(),
                cache_key: String::new(),
                cache_ttl: Duration::ZERO,
                tags: HashMap::new(),
                span: Vec::new(),
            },
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.query.id = id.into();
        self
    }

    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.query.channel = channel.into();
        self
    }

    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.query.metadata = metadata.into();
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.query.body = body.into();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.query.timeout = timeout;
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.query.client_id = client_id.into();
        self
    }

    pub fn cache_key(mut self, key: impl Into<String>) -> Self {
        self.query.cache_key = key.into();
        self
    }

    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.query.cache_ttl = ttl;
        self
    }

    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.query.tags = tags;
        self
    }

    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.tags.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> Query {
        self.query
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Received query from subscription.
#[derive(Debug, Clone)]
pub struct QueryReceive {
    pub id: String,
    pub channel: String,
    pub client_id: String,
    pub metadata: String,
    pub body: Vec<u8>,
    pub response_to: String,
    pub tags: HashMap<String, String>,
    pub span: Vec<u8>,
}

/// Result of sending a query (response from subscriber).
#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub query_id: String,
    pub executed: bool,
    pub executed_at: i64,
    pub metadata: String,
    pub response_client_id: String,
    pub body: Vec<u8>,
    pub cache_hit: bool,
    pub error: String,
    pub tags: HashMap<String, String>,
}

impl QueryResponse {
    /// Convert into a Result, returning Err if the query was not executed.
    pub fn into_result(self) -> crate::Result<Self> {
        if !self.executed {
            let message = if self.error.is_empty() {
                "Query was not executed (no error details from server)".to_string()
            } else {
                self.error
            };
            Err(KubemqError::Transient {
                code: ErrorCode::Transient,
                message,
                operation: "send_query".to_string(),
                channel: String::new(),
                is_retryable: false,
                source: None,
                request_id: self.query_id,
                suggestion: "Check the query handler.",
            })
        } else {
            Ok(self)
        }
    }
}

/// Outbound query reply (sent by subscriber).
#[derive(Debug, Clone)]
pub struct QueryReply {
    pub request_id: String,
    pub response_to: String,
    pub metadata: String,
    pub body: Vec<u8>,
    pub client_id: String,
    pub executed_at: i64,
    pub error: Option<String>,
    pub tags: HashMap<String, String>,
    pub cache_hit: bool,
    pub span: Vec<u8>,
}

impl QueryReply {
    /// Create a builder for constructing a query reply.
    pub fn builder() -> QueryReplyBuilder {
        QueryReplyBuilder::new()
    }
}

/// Builder for creating query replies.
pub struct QueryReplyBuilder {
    reply: QueryReply,
}

impl QueryReplyBuilder {
    pub fn new() -> Self {
        Self {
            reply: QueryReply {
                request_id: String::new(),
                response_to: String::new(),
                metadata: String::new(),
                body: Vec::new(),
                client_id: String::new(),
                executed_at: 0,
                error: None,
                tags: HashMap::new(),
                cache_hit: false,
                span: Vec::new(),
            },
        }
    }

    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.reply.request_id = id.into();
        self
    }

    pub fn response_to(mut self, channel: impl Into<String>) -> Self {
        self.reply.response_to = channel.into();
        self
    }

    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.reply.metadata = metadata.into();
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.reply.body = body.into();
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.reply.client_id = id.into();
        self
    }

    pub fn executed_at(mut self, time: i64) -> Self {
        self.reply.executed_at = time;
        self
    }

    pub fn error(mut self, err: impl Into<String>) -> Self {
        self.reply.error = Some(err.into());
        self
    }

    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.reply.tags = tags;
        self
    }

    pub fn build(self) -> QueryReply {
        self.reply
    }
}

impl Default for QueryReplyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Client methods for Queries
impl KubemqClient {
    /// Send a query and wait for response.
    pub async fn send_query(&self, query: Query) -> crate::Result<QueryResponse> {
        self.check_state("send_query")?;

        validate::validate_channel(&query.channel, "send_query")?;
        validate::validate_no_wildcards(&query.channel, "send_query")?;

        // REQ-M8: Safe Duration-to-i32 conversion, no silent truncation
        let timeout_ms = i32::try_from(query.timeout.as_millis()).unwrap_or(i32::MAX);
        validate::validate_timeout_positive(timeout_ms, "send_query", &query.channel)?;
        validate::validate_tags(&query.tags, "send_query", &query.channel)?;

        let cache_ttl_secs = i32::try_from(query.cache_ttl.as_secs()).unwrap_or(i32::MAX);
        validate::validate_cache_ttl(
            &query.cache_key,
            cache_ttl_secs,
            "send_query",
            &query.channel,
        )?;

        validate::validate_body_size(
            &query.body,
            self.config().max_send_message_size,
            "send_query",
            &query.channel,
        )?;

        let channel_for_err = query.channel.clone(); // Save for error message

        // REQ-H7: Owned proto conversion -- zero-copy moves
        let proto_request = proto::Request {
            request_id: crate::common::resolve_id_owned(query.id),
            request_type_data: proto::request::RequestType::Query as i32,
            client_id: crate::common::resolve_client_id_owned(query.client_id, self.config()),
            channel: query.channel,   // moved
            metadata: query.metadata, // moved
            body: query.body,         // moved -- zero-copy
            reply_channel: String::new(),
            timeout: timeout_ms,
            cache_key: query.cache_key, // moved
            cache_ttl: cache_ttl_secs,
            span: query.span, // moved
            tags: query.tags, // moved
        };

        let mut request = tonic::Request::new(proto_request);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        let response = client
            .send_request(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_query", &channel_for_err))?;

        let resp = response.into_inner();
        Ok(QueryResponse {
            query_id: resp.request_id,
            executed: resp.executed,
            executed_at: resp.timestamp,
            metadata: resp.metadata,
            response_client_id: resp.client_id,
            body: resp.body,
            cache_hit: resp.cache_hit,
            error: resp.error,
            tags: resp.tags,
        })
    }

    /// Subscribe to queries on a channel.
    ///
    /// Callbacks are async (REQ-M35). The subscription auto-reconnects on
    /// retryable stream errors (REQ-H3).
    #[allow(clippy::type_complexity)]
    pub async fn subscribe_to_queries(
        &self,
        channel: &str,
        group: &str,
        on_query: impl Fn(QueryReceive) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
        on_error: Option<
            Box<
                dyn Fn(
                        KubemqError,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    ) -> crate::Result<Subscription> {
        self.check_state("subscribe_queries")?;
        validate::validate_channel(channel, "subscribe_to_queries")?;
        validate::validate_no_wildcards(channel, "subscribe_to_queries")?;
        validate::validate_client_id(&self.config().client_id, "subscribe_to_queries")?;

        let subscribe_request = proto::Subscribe {
            subscribe_type_data: proto::subscribe::SubscribeType::Queries as i32,
            client_id: self.config().client_id.clone(),
            channel: channel.to_string(),
            group: group.to_string(),
            events_store_type_data: 0,
            events_store_type_value: 0,
        };

        let cancel = self.child_token();
        let cancel_clone = cancel.clone();
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);

        // Clone client for use in spawned task
        let client_handle = self.clone();
        let retry_policy = self.config().retry_policy.clone();

        // Spawn the subscription task. The gRPC subscribe call is made inside
        // the task because KubeMQ's server-streaming RPCs do not send HTTP/2
        // response headers until the first message is available. Awaiting the
        // subscribe call inline would block the caller indefinitely.
        tokio::spawn(async move {
            use futures::FutureExt;

            // Establish the initial gRPC stream
            let mut stream = {
                let mut client = match client_handle.transport().client() {
                    Ok(c) => c,
                    Err(_) => {
                        let _ = done_tx.send(true);
                        return;
                    }
                };
                match client
                    .subscribe_to_requests(subscribe_request.clone())
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let err = KubemqError::from_grpc_status(status, "subscribe_to_queries", "");
                        if let Some(ref on_err) = on_error {
                            let _ = std::panic::AssertUnwindSafe(on_err(err))
                                .catch_unwind()
                                .await;
                        }
                        let _ = done_tx.send(true);
                        return;
                    }
                }
            };

            let mut retry_count = 0u32;
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    msg = stream.message() => {
                        match msg {
                            Ok(Some(req)) => {
                                retry_count = 0;
                                let query_recv = QueryReceive {
                                    id: req.request_id,
                                    channel: req.channel,
                                    client_id: req.client_id,
                                    metadata: req.metadata,
                                    body: req.body,
                                    response_to: req.reply_channel,
                                    tags: req.tags,
                                    span: req.span,
                                };
                                // REQ-M55: catch_unwind for async callback
                                let result = std::panic::AssertUnwindSafe(on_query(query_recv))
                                    .catch_unwind()
                                    .await;
                                if let Err(panic_info) = result {
                                    tracing::error!("User callback panicked: {:?}", panic_info);
                                }
                            }
                            Ok(None) => {
                                // Stream ended -- attempt resubscribe (REQ-H3)
                                match client_handle.transport().client() {
                                    Ok(mut c) => {
                                        match c.subscribe_to_requests(subscribe_request.clone()).await {
                                            Ok(new_response) => {
                                                stream = new_response.into_inner();
                                                continue;
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                            Err(status) => {
                                let err = KubemqError::from_grpc_status(
                                    status,
                                    "subscribe_to_queries",
                                    "",
                                );
                                let retryable = err.is_retryable();
                                if let Some(ref on_err) = on_error {
                                    let _ = std::panic::AssertUnwindSafe(on_err(err))
                                        .catch_unwind()
                                        .await;
                                }
                                if !retryable { break; }
                                if retry_policy.max_retries > 0 && retry_count >= retry_policy.max_retries { break; }
                                let delay = retry_policy.backoff(retry_count);
                                tokio::select! {
                                    _ = cancel_clone.cancelled() => break,
                                    _ = tokio::time::sleep(delay) => {}
                                }
                                retry_count += 1;
                                match client_handle.transport().client() {
                                    Ok(mut c) => {
                                        match c.subscribe_to_requests(subscribe_request.clone()).await {
                                            Ok(new_response) => {
                                                stream = new_response.into_inner();
                                                continue;
                                            }
                                            Err(_) => continue,
                                        }
                                    }
                                    Err(_) => continue,
                                }
                            }
                        }
                    }
                }
            }
            let _ = done_tx.send(true);
        });

        Ok(Subscription::new(
            format!("queries-{}", channel),
            cancel,
            done_rx,
        ))
    }

    /// Send a query response (from subscriber handler).
    pub async fn send_query_response(&self, reply: QueryReply) -> crate::Result<()> {
        self.check_closed()?;
        validate::validate_request_id(&reply.request_id, "send_query_response")?;
        validate::validate_response_to(&reply.response_to, "send_query_response")?;

        let cid = if reply.client_id.is_empty() {
            self.config().client_id.clone()
        } else {
            reply.client_id
        };

        let error_str = reply.error.unwrap_or_default();
        let executed = error_str.is_empty();

        let proto_response = proto::Response {
            client_id: cid,
            request_id: reply.request_id,
            reply_channel: reply.response_to,
            metadata: reply.metadata,
            body: reply.body,
            cache_hit: reply.cache_hit,
            timestamp: reply.executed_at,
            executed,
            error: error_str,
            span: reply.span,
            tags: reply.tags,
        };

        let mut request = tonic::Request::new(proto_response);
        request.set_timeout(self.config().rpc_timeout);
        let mut client = self.transport().client()?;
        client
            .send_response(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "send_query_response", ""))?;

        Ok(())
    }

    /// Convenience: send query with minimal params.
    pub async fn send_query_simple(
        &self,
        channel: &str,
        body: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> crate::Result<QueryResponse> {
        let query = Query::builder()
            .channel(channel)
            .body(body)
            .timeout(timeout)
            .build();
        self.send_query(query).await // Passes owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- QueryBuilder tests --

    #[test]
    fn test_query_builder_default() {
        let builder = QueryBuilder::default();
        let query = builder.build();
        assert!(query.id.is_empty());
        assert!(query.channel.is_empty());
        assert!(query.metadata.is_empty());
        assert!(query.body.is_empty());
        assert_eq!(query.timeout, Duration::from_secs(5));
        assert!(query.client_id.is_empty());
        assert!(query.cache_key.is_empty());
        assert_eq!(query.cache_ttl, Duration::ZERO);
        assert!(query.tags.is_empty());
        assert!(query.span.is_empty());
    }

    #[test]
    fn test_query_builder_all_fields() {
        let query = Query::builder()
            .id("q-1")
            .channel("query-ch")
            .metadata("q-meta")
            .body(b"q-body".to_vec())
            .timeout(Duration::from_secs(15))
            .client_id("q-client")
            .cache_key("my-cache-key")
            .cache_ttl(Duration::from_secs(60))
            .add_tag("k", "v")
            .build();
        assert_eq!(query.id, "q-1");
        assert_eq!(query.channel, "query-ch");
        assert_eq!(query.metadata, "q-meta");
        assert_eq!(query.body, b"q-body");
        assert_eq!(query.timeout, Duration::from_secs(15));
        assert_eq!(query.client_id, "q-client");
        assert_eq!(query.cache_key, "my-cache-key");
        assert_eq!(query.cache_ttl, Duration::from_secs(60));
        assert_eq!(query.tags.get("k").unwrap(), "v");
    }

    #[test]
    fn test_query_builder_tags() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        let query = Query::builder().channel("ch").tags(tags).build();
        assert_eq!(query.tags.len(), 1);
    }

    #[test]
    fn test_query_debug_clone() {
        let query = Query::builder().id("q1").channel("ch").build();
        let debug = format!("{:?}", query);
        assert!(debug.contains("q1"));
        let cloned = query.clone();
        assert_eq!(cloned.id, "q1");
    }

    // -- QueryResponse tests --

    #[test]
    fn test_query_response_into_result_success() {
        let resp = QueryResponse {
            query_id: "r1".to_string(),
            executed: true,
            executed_at: 100,
            metadata: "resp-meta".to_string(),
            response_client_id: "client".to_string(),
            body: b"resp-body".to_vec(),
            cache_hit: true,
            error: String::new(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_ok());
        let val = r.unwrap();
        assert_eq!(val.query_id, "r1");
        assert!(val.cache_hit);
    }

    #[test]
    fn test_query_response_into_result_error() {
        let resp = QueryResponse {
            query_id: "r2".to_string(),
            executed: false,
            executed_at: 0,
            metadata: String::new(),
            response_client_id: String::new(),
            body: vec![],
            cache_hit: false,
            error: "handler error".to_string(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_err());
    }

    #[test]
    fn test_query_response_into_result_error_no_message() {
        let resp = QueryResponse {
            query_id: "r3".to_string(),
            executed: false,
            executed_at: 0,
            metadata: String::new(),
            response_client_id: String::new(),
            body: vec![],
            cache_hit: false,
            error: String::new(),
            tags: HashMap::new(),
        };
        let r = resp.into_result();
        assert!(r.is_err());
    }

    #[test]
    fn test_query_response_debug_clone() {
        let resp = QueryResponse {
            query_id: "r1".to_string(),
            executed: true,
            executed_at: 0,
            metadata: String::new(),
            response_client_id: String::new(),
            body: vec![],
            cache_hit: false,
            error: String::new(),
            tags: HashMap::new(),
        };
        let debug = format!("{:?}", resp);
        assert!(debug.contains("QueryResponse"));
        let cloned = resp.clone();
        assert_eq!(cloned.query_id, "r1");
    }

    // -- QueryReceive tests --

    #[test]
    fn test_query_receive_debug_clone() {
        let recv = QueryReceive {
            id: "qr1".to_string(),
            channel: "ch".to_string(),
            client_id: "client".to_string(),
            metadata: String::new(),
            body: vec![],
            response_to: "reply-ch".to_string(),
            tags: HashMap::new(),
            span: vec![],
        };
        let debug = format!("{:?}", recv);
        assert!(debug.contains("QueryReceive"));
        let cloned = recv.clone();
        assert_eq!(cloned.response_to, "reply-ch");
    }

    // -- QueryReply / QueryReplyBuilder tests --

    #[test]
    fn test_query_reply_builder_default() {
        let builder = crate::queries::QueryReplyBuilder::default();
        let reply = builder.build();
        assert!(reply.request_id.is_empty());
        assert!(reply.response_to.is_empty());
        assert!(reply.error.is_none());
        assert!(!reply.cache_hit);
    }

    #[test]
    fn test_query_reply_builder_all_fields() {
        let mut tags = HashMap::new();
        tags.insert("k".to_string(), "v".to_string());
        let reply = QueryReply::builder()
            .request_id("req-1")
            .response_to("reply-ch")
            .metadata("meta")
            .body(b"body".to_vec())
            .client_id("client")
            .executed_at(999)
            .error("some error")
            .tags(tags)
            .build();
        assert_eq!(reply.request_id, "req-1");
        assert_eq!(reply.response_to, "reply-ch");
        assert_eq!(reply.error, Some("some error".to_string()));
        assert!(!reply.cache_hit); // Default is false, no builder method for it
    }

    #[test]
    fn test_query_reply_debug_clone() {
        let reply = QueryReply::builder().request_id("req-1").build();
        let debug = format!("{:?}", reply);
        assert!(debug.contains("QueryReply"));
        let cloned = reply.clone();
        assert_eq!(cloned.request_id, "req-1");
    }

    // -- Query span round-trip tests (Spec 3.10) --

    #[test]
    fn test_query_reply_span_default_empty() {
        let reply = QueryReply::builder()
            .request_id("req-1")
            .response_to("reply-ch")
            .build();
        assert!(reply.span.is_empty(), "Default span should be empty");
    }

    #[test]
    fn test_query_reply_span_forwarded_to_proto() {
        let span_data = vec![50, 60, 70, 80];
        let reply = QueryReply {
            request_id: "req-1".to_string(),
            response_to: "reply-ch".to_string(),
            metadata: String::new(),
            body: Vec::new(),
            client_id: "client".to_string(),
            executed_at: 0,
            error: None,
            tags: HashMap::new(),
            cache_hit: false,
            span: span_data.clone(),
        };

        let error_str = reply.error.clone().unwrap_or_default();
        let executed = error_str.is_empty();

        let proto_response = proto::Response {
            client_id: reply.client_id.clone(),
            request_id: reply.request_id.clone(),
            reply_channel: reply.response_to.clone(),
            metadata: reply.metadata.clone(),
            body: reply.body.clone(),
            cache_hit: reply.cache_hit,
            timestamp: reply.executed_at,
            executed,
            error: error_str,
            span: reply.span.clone(),
            tags: reply.tags.clone(),
        };

        assert_eq!(proto_response.span, vec![50, 60, 70, 80]);
        assert!(proto_response.executed);
    }
}
