//! Channel management types and operations.

use std::collections::HashMap;

use serde::Deserialize;

use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::proto::kubemq::{self as pb, request::RequestType};
use crate::Result;

/// Well-known internal channel for channel management requests.
const REQUEST_CHANNEL: &str = "kubemq.cluster.internal.requests";

/// Default timeout for channel management operations (10 seconds).
const DEFAULT_TIMEOUT_MS: i32 = 10_000;

/// Channel type constants.
pub mod channel_type {
    pub const EVENTS: &str = "events";
    pub const EVENTS_STORE: &str = "events_store";
    pub const COMMANDS: &str = "commands";
    pub const QUERIES: &str = "queries";
    pub const QUEUES: &str = "queues";
}

/// Valid channel types for validation.
const VALID_CHANNEL_TYPES: &[&str] = &[
    channel_type::EVENTS,
    channel_type::EVENTS_STORE,
    channel_type::COMMANDS,
    channel_type::QUERIES,
    channel_type::QUEUES,
];

/// Validate that a channel type string is one of the valid types.
fn validate_channel_type(channel_type: &str, operation: &str) -> Result<()> {
    if channel_type.is_empty() {
        return Err(KubemqError::Validation {
            code: ErrorCode::Validation,
            message: "channel type is required".to_string(),
            operation: operation.to_string(),
            channel: String::new(),
            suggestion: "Use one of: events, events_store, commands, queries, queues",
        });
    }
    if !VALID_CHANNEL_TYPES.contains(&channel_type) {
        return Err(KubemqError::Validation {
            code: ErrorCode::Validation,
            message: format!(
                "invalid channel type {:?}; must be one of: events, events_store, commands, queries, queues",
                channel_type
            ),
            operation: operation.to_string(),
            channel: String::new(),
            suggestion: "Use one of: events, events_store, commands, queries, queues",
        });
    }
    Ok(())
}

/// Information about a KubeMQ channel.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub name: String,
    pub channel_type: String,
    pub last_activity: i64,
    pub is_active: bool,
    pub incoming: Option<ChannelStats>,
    pub outgoing: Option<ChannelStats>,
}

/// Directional channel statistics.
#[derive(Debug, Clone, Default)]
pub struct ChannelStats {
    pub messages: i64,
    pub volume: i64,
    pub responses: i64,
    pub waiting: i64,
    pub expired: i64,
    pub delayed: i64,
}

// -- JSON deserialization types for parsing server response --

#[derive(Deserialize)]
struct ChannelListItem {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "type")]
    channel_type: String,
    #[serde(default, rename = "lastActivity")]
    last_activity: i64,
    #[serde(default, rename = "isActive")]
    is_active: bool,
    #[serde(default)]
    incoming: Option<ChannelStatJson>,
    #[serde(default)]
    outgoing: Option<ChannelStatJson>,
}

#[derive(Deserialize)]
struct ChannelStatJson {
    #[serde(default)]
    messages: i64,
    #[serde(default)]
    volume: i64,
    #[serde(default)]
    responses: i64,
    #[serde(default)]
    waiting: i64,
    #[serde(default)]
    expired: i64,
    #[serde(default)]
    delayed: i64,
}

impl From<&ChannelStatJson> for ChannelStats {
    fn from(s: &ChannelStatJson) -> Self {
        Self {
            messages: s.messages,
            volume: s.volume,
            responses: s.responses,
            waiting: s.waiting,
            expired: s.expired,
            delayed: s.delayed,
        }
    }
}

fn parse_channel_list(data: &[u8]) -> Result<Vec<ChannelInfo>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let items: Vec<ChannelListItem> =
        serde_json::from_slice(data).map_err(|e| KubemqError::Fatal {
            code: ErrorCode::Fatal,
            message: format!("failed to parse channel list JSON: {}", e),
            operation: "list_channels".to_string(),
            source: Some(Box::new(e)),
            suggestion: "This may indicate a server version mismatch.",
        })?;
    let out = items
        .into_iter()
        .map(|item| ChannelInfo {
            name: item.name,
            channel_type: item.channel_type,
            last_activity: item.last_activity,
            is_active: item.is_active,
            incoming: item.incoming.as_ref().map(ChannelStats::from),
            outgoing: item.outgoing.as_ref().map(ChannelStats::from),
        })
        .collect();
    Ok(out)
}

// -- Generic channel management methods on KubemqClient --

impl KubemqClient {
    /// Internal helper for create/delete channel operations.
    async fn channel_mutate(
        &self,
        name: &str,
        channel_type: &str,
        metadata: &str,
        operation: &str,
        error_suggestion: &'static str,
    ) -> Result<()> {
        self.check_closed()?;
        if name.is_empty() {
            return Err(KubemqError::Validation {
                code: ErrorCode::Validation,
                message: "channel name is required".to_string(),
                operation: operation.to_string(),
                channel: String::new(),
                suggestion: "Provide a non-empty channel name.",
            });
        }
        validate_channel_type(channel_type, operation)?;

        let client_id = self.config().client_id.clone();
        let mut tags = HashMap::new();
        tags.insert("channel_type".to_string(), channel_type.to_string());
        tags.insert("channel".to_string(), name.to_string());
        tags.insert("client_id".to_string(), client_id.clone());

        let proto_request = pb::Request {
            request_id: uuid::Uuid::new_v4().to_string(),
            request_type_data: RequestType::Query as i32,
            client_id,
            channel: REQUEST_CHANNEL.to_string(),
            metadata: metadata.to_string(),
            body: Vec::new(),
            reply_channel: String::new(),
            timeout: DEFAULT_TIMEOUT_MS,
            cache_key: String::new(),
            cache_ttl: 0,
            span: Vec::new(),
            tags,
        };

        let mut request = tonic::Request::new(proto_request);
        request.set_timeout(self.config().rpc_timeout);
        let mut grpc_client = self.transport().client()?;
        let response = grpc_client
            .send_request(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, operation, name))?;

        let resp = response.into_inner();
        if !resp.error.is_empty() {
            return Err(KubemqError::Fatal {
                code: ErrorCode::Fatal,
                message: format!("{}: {}", operation.replace('_', " "), resp.error),
                operation: operation.to_string(),
                source: None,
                suggestion: error_suggestion,
            });
        }
        if !resp.executed {
            return Err(KubemqError::Fatal {
                code: ErrorCode::Fatal,
                message: format!("Operation not executed by server for channel '{}'", name),
                operation: operation.to_string(),
                source: None,
                suggestion: "Check broker logs for details.",
            });
        }
        Ok(())
    }

    /// Create a channel of the specified type.
    ///
    /// `channel_type` must be one of the constants in [`channel_type`].
    pub async fn create_channel(&self, name: &str, channel_type: &str) -> Result<()> {
        self.channel_mutate(
            name,
            channel_type,
            "create-channel",
            "create_channel",
            "Check that the channel name and type are valid.",
        )
        .await
    }

    /// Delete a channel of the specified type.
    ///
    /// `channel_type` must be one of the constants in [`channel_type`].
    pub async fn delete_channel(&self, name: &str, channel_type: &str) -> Result<()> {
        self.channel_mutate(
            name,
            channel_type,
            "delete-channel",
            "delete_channel",
            "Check that the channel exists and the type is correct.",
        )
        .await
    }

    /// List channels of the specified type, optionally filtered by a search string.
    ///
    /// `channel_type` must be one of the constants in [`channel_type`].
    /// Pass an empty string for `search` to list all channels of the given type.
    pub async fn list_channels(
        &self,
        channel_type: &str,
        search: &str,
    ) -> Result<Vec<ChannelInfo>> {
        self.check_closed()?;
        validate_channel_type(channel_type, "list_channels")?;

        let client_id = self.config().client_id.clone();
        let mut tags = HashMap::new();
        tags.insert("channel_type".to_string(), channel_type.to_string());
        tags.insert("client_id".to_string(), client_id.clone());
        if !search.is_empty() {
            tags.insert("channel_search".to_string(), search.to_string());
        }

        let proto_request = pb::Request {
            request_id: uuid::Uuid::new_v4().to_string(),
            request_type_data: RequestType::Query as i32,
            client_id,
            channel: REQUEST_CHANNEL.to_string(),
            metadata: "list-channels".to_string(),
            body: Vec::new(),
            reply_channel: String::new(),
            timeout: DEFAULT_TIMEOUT_MS,
            cache_key: String::new(),
            cache_ttl: 0,
            span: Vec::new(),
            tags,
        };

        let mut request = tonic::Request::new(proto_request);
        request.set_timeout(self.config().rpc_timeout);
        let mut grpc_client = self.transport().client()?;
        let response = grpc_client
            .send_request(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "list_channels", ""))?;

        let resp = response.into_inner();
        if !resp.error.is_empty() {
            return Err(KubemqError::Fatal {
                code: ErrorCode::Fatal,
                message: format!("list channels: {}", resp.error),
                operation: "list_channels".to_string(),
                source: None,
                suggestion: "Check the channel type and try again.",
            });
        }
        if !resp.executed {
            return Err(KubemqError::Fatal {
                code: ErrorCode::Fatal,
                message: "Operation not executed by server".to_string(),
                operation: "list_channels".to_string(),
                source: None,
                suggestion: "Check broker logs for details.",
            });
        }
        parse_channel_list(&resp.body)
    }

    // -- Convenience methods: Create --

    /// Create an events channel with the given name.
    pub async fn create_events_channel(&self, name: &str) -> Result<()> {
        self.create_channel(name, channel_type::EVENTS).await
    }

    /// Create an events-store channel with the given name.
    pub async fn create_events_store_channel(&self, name: &str) -> Result<()> {
        self.create_channel(name, channel_type::EVENTS_STORE).await
    }

    /// Create a commands channel with the given name.
    pub async fn create_commands_channel(&self, name: &str) -> Result<()> {
        self.create_channel(name, channel_type::COMMANDS).await
    }

    /// Create a queries channel with the given name.
    pub async fn create_queries_channel(&self, name: &str) -> Result<()> {
        self.create_channel(name, channel_type::QUERIES).await
    }

    /// Create a queues channel with the given name.
    pub async fn create_queues_channel(&self, name: &str) -> Result<()> {
        self.create_channel(name, channel_type::QUEUES).await
    }

    // -- Convenience methods: Delete --

    /// Delete an events channel with the given name.
    pub async fn delete_events_channel(&self, name: &str) -> Result<()> {
        self.delete_channel(name, channel_type::EVENTS).await
    }

    /// Delete an events-store channel with the given name.
    pub async fn delete_events_store_channel(&self, name: &str) -> Result<()> {
        self.delete_channel(name, channel_type::EVENTS_STORE).await
    }

    /// Delete a commands channel with the given name.
    pub async fn delete_commands_channel(&self, name: &str) -> Result<()> {
        self.delete_channel(name, channel_type::COMMANDS).await
    }

    /// Delete a queries channel with the given name.
    pub async fn delete_queries_channel(&self, name: &str) -> Result<()> {
        self.delete_channel(name, channel_type::QUERIES).await
    }

    /// Delete a queues channel with the given name.
    pub async fn delete_queues_channel(&self, name: &str) -> Result<()> {
        self.delete_channel(name, channel_type::QUEUES).await
    }

    // -- Convenience methods: List --

    /// List events channels, optionally filtered by search string.
    pub async fn list_events_channels(&self, search: &str) -> Result<Vec<ChannelInfo>> {
        self.list_channels(channel_type::EVENTS, search).await
    }

    /// List events-store channels, optionally filtered by search string.
    pub async fn list_events_store_channels(&self, search: &str) -> Result<Vec<ChannelInfo>> {
        self.list_channels(channel_type::EVENTS_STORE, search).await
    }

    /// List commands channels, optionally filtered by search string.
    pub async fn list_commands_channels(&self, search: &str) -> Result<Vec<ChannelInfo>> {
        self.list_channels(channel_type::COMMANDS, search).await
    }

    /// List queries channels, optionally filtered by search string.
    pub async fn list_queries_channels(&self, search: &str) -> Result<Vec<ChannelInfo>> {
        self.list_channels(channel_type::QUERIES, search).await
    }

    /// List queues channels, optionally filtered by search string.
    pub async fn list_queues_channels(&self, search: &str) -> Result<Vec<ChannelInfo>> {
        self.list_channels(channel_type::QUEUES, search).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_channel_type_valid() {
        assert!(validate_channel_type("events", "test").is_ok());
        assert!(validate_channel_type("events_store", "test").is_ok());
        assert!(validate_channel_type("commands", "test").is_ok());
        assert!(validate_channel_type("queries", "test").is_ok());
        assert!(validate_channel_type("queues", "test").is_ok());
    }

    #[test]
    fn test_validate_channel_type_empty() {
        let err = validate_channel_type("", "test").unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    #[test]
    fn test_validate_channel_type_invalid() {
        let err = validate_channel_type("invalid_type", "test").unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    #[test]
    fn test_parse_channel_list_empty() {
        let result = parse_channel_list(b"").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_channel_list_valid() {
        let json = r#"[
            {
                "name": "test-channel",
                "type": "events",
                "lastActivity": 1234567890,
                "isActive": true,
                "incoming": {
                    "messages": 100,
                    "volume": 2048,
                    "responses": 0,
                    "waiting": 0,
                    "expired": 0,
                    "delayed": 0
                },
                "outgoing": null
            }
        ]"#;
        let result = parse_channel_list(json.as_bytes()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "test-channel");
        assert_eq!(result[0].channel_type, "events");
        assert_eq!(result[0].last_activity, 1234567890);
        assert!(result[0].is_active);
        assert!(result[0].incoming.is_some());
        let incoming = result[0].incoming.as_ref().unwrap();
        assert_eq!(incoming.messages, 100);
        assert_eq!(incoming.volume, 2048);
        assert!(result[0].outgoing.is_none());
    }

    #[test]
    fn test_parse_channel_list_invalid_json() {
        let result = parse_channel_list(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_channel_list_multiple_channels() {
        let json = r#"[
            {"name": "ch1", "type": "events", "lastActivity": 100, "isActive": true},
            {"name": "ch2", "type": "queues", "lastActivity": 200, "isActive": false}
        ]"#;
        let result = parse_channel_list(json.as_bytes()).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "ch1");
        assert_eq!(result[0].channel_type, "events");
        assert!(result[0].is_active);
        assert_eq!(result[1].name, "ch2");
        assert_eq!(result[1].channel_type, "queues");
        assert!(!result[1].is_active);
    }

    #[test]
    fn test_parse_channel_list_with_both_stats() {
        let json = r#"[{
            "name": "test-ch",
            "type": "events",
            "lastActivity": 999,
            "isActive": true,
            "incoming": {"messages": 10, "volume": 1024, "responses": 0, "waiting": 0, "expired": 0, "delayed": 0},
            "outgoing": {"messages": 5, "volume": 512, "responses": 3, "waiting": 1, "expired": 2, "delayed": 0}
        }]"#;
        let result = parse_channel_list(json.as_bytes()).unwrap();
        assert_eq!(result.len(), 1);
        let ch = &result[0];
        assert!(ch.incoming.is_some());
        assert!(ch.outgoing.is_some());
        let inc = ch.incoming.as_ref().unwrap();
        assert_eq!(inc.messages, 10);
        assert_eq!(inc.volume, 1024);
        let out = ch.outgoing.as_ref().unwrap();
        assert_eq!(out.messages, 5);
        assert_eq!(out.responses, 3);
        assert_eq!(out.waiting, 1);
        assert_eq!(out.expired, 2);
    }

    #[test]
    fn test_parse_channel_list_missing_optional_fields() {
        // JSON with no incoming/outgoing
        let json = r#"[{"name": "bare-ch"}]"#;
        let result = parse_channel_list(json.as_bytes()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "bare-ch");
        assert_eq!(result[0].channel_type, "");
        assert_eq!(result[0].last_activity, 0);
        assert!(!result[0].is_active);
        assert!(result[0].incoming.is_none());
        assert!(result[0].outgoing.is_none());
    }

    #[test]
    fn test_validate_channel_type_case_sensitive() {
        // Channel types are case-sensitive
        assert!(validate_channel_type("Events", "test").is_err());
        assert!(validate_channel_type("EVENTS", "test").is_err());
        assert!(validate_channel_type("QUEUES", "test").is_err());
    }

    #[test]
    fn test_channel_type_constants() {
        assert_eq!(channel_type::EVENTS, "events");
        assert_eq!(channel_type::EVENTS_STORE, "events_store");
        assert_eq!(channel_type::COMMANDS, "commands");
        assert_eq!(channel_type::QUERIES, "queries");
        assert_eq!(channel_type::QUEUES, "queues");
    }

    #[test]
    fn test_channel_info_debug_clone() {
        let info = ChannelInfo {
            name: "test".to_string(),
            channel_type: "events".to_string(),
            last_activity: 12345,
            is_active: true,
            incoming: Some(ChannelStats {
                messages: 1,
                volume: 100,
                ..Default::default()
            }),
            outgoing: None,
        };
        let debug = format!("{:?}", info);
        assert!(debug.contains("test"));
        let cloned = info.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.last_activity, 12345);
    }

    #[test]
    fn test_channel_stats_default() {
        let stats = ChannelStats::default();
        assert_eq!(stats.messages, 0);
        assert_eq!(stats.volume, 0);
        assert_eq!(stats.responses, 0);
        assert_eq!(stats.waiting, 0);
        assert_eq!(stats.expired, 0);
        assert_eq!(stats.delayed, 0);
    }

    #[test]
    fn test_channel_stats_debug_clone() {
        let stats = ChannelStats {
            messages: 42,
            volume: 1024,
            responses: 10,
            waiting: 5,
            expired: 1,
            delayed: 2,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("42"));
        let cloned = stats.clone();
        assert_eq!(cloned.messages, 42);
        assert_eq!(cloned.delayed, 2);
    }

    #[test]
    fn test_channel_stat_json_to_channel_stats() {
        let json_stat = ChannelStatJson {
            messages: 100,
            volume: 2048,
            responses: 50,
            waiting: 10,
            expired: 5,
            delayed: 3,
        };
        let stats = ChannelStats::from(&json_stat);
        assert_eq!(stats.messages, 100);
        assert_eq!(stats.volume, 2048);
        assert_eq!(stats.responses, 50);
        assert_eq!(stats.waiting, 10);
        assert_eq!(stats.expired, 5);
        assert_eq!(stats.delayed, 3);
    }

    #[test]
    fn test_parse_channel_list_empty_array() {
        let result = parse_channel_list(b"[]").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_validate_channel_type_all_valid_types() {
        for t in VALID_CHANNEL_TYPES {
            assert!(
                validate_channel_type(t, "test").is_ok(),
                "Expected '{}' to be valid",
                t
            );
        }
    }

    // -- Channel executed=false error construction tests (Spec 1.6) --

    #[test]
    fn test_create_channel_executed_false_error() {
        let name = "test-channel";
        let err = KubemqError::Fatal {
            code: ErrorCode::Fatal,
            message: format!("Operation not executed by server for channel '{}'", name),
            operation: "create_channel".to_string(),
            source: None,
            suggestion: "Check broker logs for details.",
        };
        assert_eq!(err.code(), ErrorCode::Fatal);
        if let KubemqError::Fatal {
            message, operation, ..
        } = &err
        {
            assert!(message.contains("test-channel"));
            assert_eq!(operation, "create_channel");
        } else {
            panic!("Expected Fatal error");
        }
    }

    #[test]
    fn test_delete_channel_executed_false_error() {
        let name = "delete-me";
        let err = KubemqError::Fatal {
            code: ErrorCode::Fatal,
            message: format!("Operation not executed by server for channel '{}'", name),
            operation: "delete_channel".to_string(),
            source: None,
            suggestion: "Check broker logs for details.",
        };
        assert_eq!(err.code(), ErrorCode::Fatal);
        if let KubemqError::Fatal {
            message, operation, ..
        } = &err
        {
            assert!(message.contains("delete-me"));
            assert_eq!(operation, "delete_channel");
        } else {
            panic!("Expected Fatal error");
        }
    }

    #[test]
    fn test_list_channels_executed_false_error() {
        let err = KubemqError::Fatal {
            code: ErrorCode::Fatal,
            message: "Operation not executed by server".to_string(),
            operation: "list_channels".to_string(),
            source: None,
            suggestion: "Check broker logs for details.",
        };
        assert_eq!(err.code(), ErrorCode::Fatal);
        if let KubemqError::Fatal {
            message, operation, ..
        } = &err
        {
            assert!(message.contains("not executed"));
            assert_eq!(operation, "list_channels");
        } else {
            panic!("Expected Fatal error");
        }
    }

    #[test]
    fn test_executed_false_error_is_not_retryable() {
        let err = KubemqError::Fatal {
            code: ErrorCode::Fatal,
            message: "Operation not executed by server for channel 'test'".to_string(),
            operation: "create_channel".to_string(),
            source: None,
            suggestion: "Check broker logs for details.",
        };
        assert!(!err.is_retryable());
    }
}
