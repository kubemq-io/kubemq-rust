//! Validation rules (V-1 through V-26) from spec SS16.

use crate::error::{ErrorCode, KubemqError};
use std::collections::HashMap;

/// Channel name regex: `^[a-zA-Z0-9._\-/>*]+$`
fn is_valid_channel_chars(channel: &str) -> bool {
    channel
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | '>' | '*'))
}

/// Maximum channel name length.
const MAX_CHANNEL_LENGTH: usize = 256;

/// Maximum tag key length in bytes.
const MAX_TAG_KEY_LENGTH: usize = 256;

/// Maximum tag value length in bytes.
const MAX_TAG_VALUE_LENGTH: usize = 4096;

fn validation_error(
    message: &str,
    operation: &str,
    channel: &str,
    suggestion: &'static str,
) -> KubemqError {
    KubemqError::Validation {
        code: ErrorCode::Validation,
        message: message.to_string(),
        operation: operation.to_string(),
        channel: channel.to_string(),
        suggestion,
    }
}

/// V-1: client_id cannot be empty.
pub(crate) fn validate_client_id(client_id: &str, operation: &str) -> crate::Result<()> {
    if client_id.is_empty() {
        return Err(validation_error(
            "clientId is required",
            operation,
            "",
            "Provide a non-empty client_id.",
        ));
    }
    Ok(())
}

/// V-2: channel cannot be empty.
pub(crate) fn validate_channel(channel: &str, operation: &str) -> crate::Result<()> {
    if channel.is_empty() {
        return Err(validation_error(
            "channel name is required",
            operation,
            "",
            "Provide a non-empty channel name.",
        ));
    }
    // V-4: Channel cannot contain whitespace.
    if channel.chars().any(|c| c.is_whitespace()) {
        return Err(validation_error(
            "invalid characters in channel name",
            operation,
            channel,
            "Channel names must not contain whitespace.",
        ));
    }
    // V-5: Channel cannot end with '.'.
    if channel.ends_with('.') {
        return Err(validation_error(
            "channel name cannot end with '.'",
            operation,
            channel,
            "Remove trailing '.' from channel name.",
        ));
    }
    // V-7: Channel name max 256 chars.
    if channel.len() > MAX_CHANNEL_LENGTH {
        return Err(validation_error(
            "channel name exceeds maximum length",
            operation,
            channel,
            "Channel names must be at most 256 characters.",
        ));
    }
    // V-8: Channel regex.
    if !is_valid_channel_chars(channel) {
        return Err(validation_error(
            "invalid characters in channel name",
            operation,
            channel,
            "Channel names must match ^[a-zA-Z0-9._\\-/>*]+$.",
        ));
    }
    Ok(())
}

/// V-3: Wildcards (`*`, `>`) blocked except for Events subscribe.
pub(crate) fn validate_no_wildcards(channel: &str, operation: &str) -> crate::Result<()> {
    if channel.contains('*') || channel.contains('>') {
        return Err(validation_error(
            "wildcards not allowed for this operation",
            operation,
            channel,
            "Remove wildcard characters from channel name.",
        ));
    }
    Ok(())
}

/// V-6: At least one of metadata/body must be non-empty.
pub(crate) fn validate_body_or_metadata(
    metadata: &str,
    body: &[u8],
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if metadata.is_empty() && body.is_empty() {
        return Err(validation_error(
            "at least one of metadata or body is required",
            operation,
            channel,
            "Provide metadata, body, or both.",
        ));
    }
    Ok(())
}

/// V-9, V-10, V-11: Validate tags.
pub(crate) fn validate_tags(
    tags: &HashMap<String, String>,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    for (key, value) in tags {
        // V-9: Tag key cannot be empty.
        if key.is_empty() {
            return Err(validation_error(
                "tag key must be non-empty",
                operation,
                channel,
                "All tag keys must be non-empty strings.",
            ));
        }
        // V-10: Tag key max 256 bytes.
        if key.len() > MAX_TAG_KEY_LENGTH {
            return Err(validation_error(
                "tag key exceeds maximum length",
                operation,
                channel,
                "Tag keys must be at most 256 bytes.",
            ));
        }
        // V-11: Tag value max 4096 bytes.
        if value.len() > MAX_TAG_VALUE_LENGTH {
            return Err(validation_error(
                "tag value exceeds maximum length",
                operation,
                channel,
                "Tag values must be at most 4096 bytes.",
            ));
        }
    }
    Ok(())
}

/// V-12: Body size <= max_send_message_size.
pub(crate) fn validate_body_size(
    body: &[u8],
    max_size: usize,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if body.len() > max_size {
        return Err(validation_error(
            &format!("body exceeds maximum size ({} > {})", body.len(), max_size),
            operation,
            channel,
            "Reduce body size or increase max_send_message_size.",
        ));
    }
    Ok(())
}

/// V-13: Command/Query timeout must be > 0.
pub(crate) fn validate_timeout_positive(
    timeout_ms: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if timeout_ms <= 0 {
        return Err(validation_error(
            "timeout must be positive",
            operation,
            channel,
            "Set a positive timeout value (in milliseconds).",
        ));
    }
    Ok(())
}

/// V-14: CacheKey set => CacheTTL > 0.
pub(crate) fn validate_cache_ttl(
    cache_key: &str,
    cache_ttl: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if !cache_key.is_empty() && cache_ttl <= 0 {
        return Err(validation_error(
            "CacheTTL must be > 0 when CacheKey is set",
            operation,
            channel,
            "Set CacheTTL to a positive value when using CacheKey.",
        ));
    }
    Ok(())
}

/// V-15: SendResponse requestId non-empty.
pub(crate) fn validate_request_id(request_id: &str, operation: &str) -> crate::Result<()> {
    if request_id.is_empty() {
        return Err(validation_error(
            "requestId is required",
            operation,
            "",
            "Provide a non-empty requestId.",
        ));
    }
    Ok(())
}

/// V-16: SendResponse responseTo non-empty.
pub(crate) fn validate_response_to(response_to: &str, operation: &str) -> crate::Result<()> {
    if response_to.is_empty() {
        return Err(validation_error(
            "responseTo is required",
            operation,
            "",
            "Provide a non-empty responseTo.",
        ));
    }
    Ok(())
}

/// V-17: EventsStoreType must not be Undefined.
pub(crate) fn validate_events_store_type_not_undefined(
    store_type: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if store_type == 0 {
        return Err(validation_error(
            "EventsStoreType must not be Undefined",
            operation,
            channel,
            "Specify a valid EventsStoreSubscription type.",
        ));
    }
    Ok(())
}

/// V-18: StartAtSequence value > 0.
pub(crate) fn validate_start_at_sequence(
    sequence: i64,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if sequence <= 0 {
        return Err(validation_error(
            "StartAtSequence value must be > 0",
            operation,
            channel,
            "Provide a positive sequence number.",
        ));
    }
    Ok(())
}

/// V-19: StartAtTime value > 0.
pub(crate) fn validate_start_at_time(
    time_value: i64,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if time_value <= 0 {
        return Err(validation_error(
            "StartAtTime value must be > 0",
            operation,
            channel,
            "Provide a positive timestamp.",
        ));
    }
    Ok(())
}

/// V-20: StartAtTimeDelta value > 0.
pub(crate) fn validate_start_at_time_delta(
    delta: i64,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if delta <= 0 {
        return Err(validation_error(
            "StartAtTimeDelta value must be > 0",
            operation,
            channel,
            "Provide a positive time delta.",
        ));
    }
    Ok(())
}

/// V-22: MaxNumberOfMessages >= 1 and <= 1024.
pub(crate) fn validate_max_messages(
    max_messages: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if !(1..=1024).contains(&max_messages) {
        return Err(validation_error(
            "MaxNumberOfMessages out of range (must be 1..=1024)",
            operation,
            channel,
            "Set MaxNumberOfMessages between 1 and 1024.",
        ));
    }
    Ok(())
}

/// V-23: WaitTimeSeconds >= 0 and <= 3600.
pub(crate) fn validate_wait_time_seconds(
    wait_time: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if !(0..=3600).contains(&wait_time) {
        return Err(validation_error(
            "WaitTimeSeconds out of range (must be 0..=3600)",
            operation,
            channel,
            "Set WaitTimeSeconds between 0 and 3600.",
        ));
    }
    Ok(())
}

/// V-24: Policy.ExpirationSeconds >= 0.
pub(crate) fn validate_expiration_seconds(
    expiration: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if expiration < 0 {
        return Err(validation_error(
            "ExpirationSeconds must be non-negative",
            operation,
            channel,
            "Set ExpirationSeconds to 0 or a positive value.",
        ));
    }
    Ok(())
}

/// V-25: Policy.DelaySeconds >= 0.
pub(crate) fn validate_delay_seconds(
    delay: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if delay < 0 {
        return Err(validation_error(
            "DelaySeconds must be non-negative",
            operation,
            channel,
            "Set DelaySeconds to 0 or a positive value.",
        ));
    }
    Ok(())
}

/// V-26: Policy.MaxReceiveCount >= 0.
pub(crate) fn validate_max_receive_count(
    max_receive: i32,
    operation: &str,
    channel: &str,
) -> crate::Result<()> {
    if max_receive < 0 {
        return Err(validation_error(
            "MaxReceiveCount must be non-negative",
            operation,
            channel,
            "Set MaxReceiveCount to 0 or a positive value.",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_client_id_empty() {
        assert!(validate_client_id("", "test").is_err());
        assert!(validate_client_id("my-client", "test").is_ok());
    }

    #[test]
    fn test_v2_channel_empty() {
        assert!(validate_channel("", "test").is_err());
        assert!(validate_channel("events.test", "test").is_ok());
    }

    #[test]
    fn test_v3_wildcards_blocked() {
        assert!(validate_no_wildcards("events.*", "test").is_err());
        assert!(validate_no_wildcards("events.>", "test").is_err());
        assert!(validate_no_wildcards("events.test", "test").is_ok());
    }

    #[test]
    fn test_v4_channel_whitespace() {
        assert!(validate_channel("events test", "test").is_err());
        assert!(validate_channel("events\ttest", "test").is_err());
    }

    #[test]
    fn test_v5_channel_trailing_dot() {
        assert!(validate_channel("events.", "test").is_err());
    }

    #[test]
    fn test_v6_body_or_metadata() {
        assert!(validate_body_or_metadata("", &[], "test", "ch1").is_err());
        assert!(validate_body_or_metadata("meta", &[], "test", "ch1").is_ok());
        assert!(validate_body_or_metadata("", b"body", "test", "ch1").is_ok());
    }

    #[test]
    fn test_v7_channel_max_length() {
        let long_channel = "a".repeat(257);
        assert!(validate_channel(&long_channel, "test").is_err());
        let ok_channel = "a".repeat(256);
        assert!(validate_channel(&ok_channel, "test").is_ok());
    }

    #[test]
    fn test_v8_channel_invalid_chars() {
        assert!(validate_channel("events@test", "test").is_err());
        assert!(validate_channel("events#test", "test").is_err());
        assert!(validate_channel("events/test", "test").is_ok());
        assert!(validate_channel("events-test_1.2", "test").is_ok());
    }

    #[test]
    fn test_v9_v10_v11_tags() {
        let mut tags = HashMap::new();
        tags.insert(String::new(), "value".to_string());
        assert!(validate_tags(&tags, "test", "ch1").is_err());

        let mut tags = HashMap::new();
        tags.insert("a".repeat(257), "value".to_string());
        assert!(validate_tags(&tags, "test", "ch1").is_err());

        let mut tags = HashMap::new();
        tags.insert("key".to_string(), "a".repeat(4097));
        assert!(validate_tags(&tags, "test", "ch1").is_err());

        let mut tags = HashMap::new();
        tags.insert("key".to_string(), "value".to_string());
        assert!(validate_tags(&tags, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v12_body_size() {
        let body = vec![0u8; 1001];
        assert!(validate_body_size(&body, 1000, "test", "ch1").is_err());
        assert!(validate_body_size(&body, 1001, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v13_timeout_positive() {
        assert!(validate_timeout_positive(0, "test", "ch1").is_err());
        assert!(validate_timeout_positive(-1, "test", "ch1").is_err());
        assert!(validate_timeout_positive(1, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v14_cache_ttl() {
        assert!(validate_cache_ttl("key", 0, "test", "ch1").is_err());
        assert!(validate_cache_ttl("key", 1, "test", "ch1").is_ok());
        assert!(validate_cache_ttl("", 0, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v15_request_id() {
        assert!(validate_request_id("", "test").is_err());
        assert!(validate_request_id("abc", "test").is_ok());
    }

    #[test]
    fn test_v16_response_to() {
        assert!(validate_response_to("", "test").is_err());
        assert!(validate_response_to("abc", "test").is_ok());
    }

    #[test]
    fn test_v17_events_store_type() {
        assert!(validate_events_store_type_not_undefined(0, "test", "ch1").is_err());
        assert!(validate_events_store_type_not_undefined(1, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v18_start_at_sequence() {
        assert!(validate_start_at_sequence(0, "test", "ch1").is_err());
        assert!(validate_start_at_sequence(-1, "test", "ch1").is_err());
        assert!(validate_start_at_sequence(1, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v19_start_at_time() {
        assert!(validate_start_at_time(0, "test", "ch1").is_err());
        assert!(validate_start_at_time(1, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v20_start_at_time_delta() {
        assert!(validate_start_at_time_delta(0, "test", "ch1").is_err());
        assert!(validate_start_at_time_delta(1, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v22_max_messages() {
        assert!(validate_max_messages(0, "test", "ch1").is_err());
        assert!(validate_max_messages(1025, "test", "ch1").is_err());
        assert!(validate_max_messages(1, "test", "ch1").is_ok());
        assert!(validate_max_messages(1024, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v23_wait_time_seconds() {
        assert!(validate_wait_time_seconds(-1, "test", "ch1").is_err());
        assert!(validate_wait_time_seconds(3601, "test", "ch1").is_err());
        assert!(validate_wait_time_seconds(0, "test", "ch1").is_ok());
        assert!(validate_wait_time_seconds(3600, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v24_expiration_seconds() {
        assert!(validate_expiration_seconds(-1, "test", "ch1").is_err());
        assert!(validate_expiration_seconds(0, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v25_delay_seconds() {
        assert!(validate_delay_seconds(-1, "test", "ch1").is_err());
        assert!(validate_delay_seconds(0, "test", "ch1").is_ok());
    }

    #[test]
    fn test_v26_max_receive_count() {
        assert!(validate_max_receive_count(-1, "test", "ch1").is_err());
        assert!(validate_max_receive_count(0, "test", "ch1").is_ok());
    }
}
