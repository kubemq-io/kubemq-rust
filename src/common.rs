//! Shared helper functions to reduce duplication across messaging patterns.

use crate::config::ClientConfig;

/// Resolve event/message ID: use provided ID if non-empty, otherwise generate UUID.
#[allow(dead_code)]
pub(crate) fn resolve_id(user_id: &str) -> String {
    if user_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        user_id.to_string()
    }
}

/// Resolve event/message ID from an owned string.
pub(crate) fn resolve_id_owned(user_id: String) -> String {
    if user_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        user_id
    }
}

/// Resolve client ID: use provided if non-empty, otherwise use config default.
#[allow(dead_code)]
pub(crate) fn resolve_client_id(user_client_id: &str, config: &ClientConfig) -> String {
    if user_client_id.is_empty() {
        config.client_id.clone()
    } else {
        user_client_id.to_string()
    }
}

/// Resolve client ID from an owned string.
pub(crate) fn resolve_client_id_owned(user_client_id: String, config: &ClientConfig) -> String {
    if user_client_id.is_empty() {
        config.client_id.clone()
    } else {
        user_client_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_id_empty_generates_uuid() {
        let id = resolve_id("");
        assert!(!id.is_empty(), "Empty input should generate a UUID");
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(id.len(), 36, "Generated ID should be a UUID string");
    }

    #[test]
    fn test_resolve_id_non_empty_preserved() {
        let id = resolve_id("my-id");
        assert_eq!(id, "my-id");
    }

    #[test]
    fn test_resolve_id_owned_empty_generates_uuid() {
        let id = resolve_id_owned(String::new());
        assert!(!id.is_empty());
        assert_eq!(id.len(), 36);
    }

    #[test]
    fn test_resolve_id_owned_non_empty_preserved() {
        let input = "owned-id".to_string();
        let id = resolve_id_owned(input);
        assert_eq!(id, "owned-id");
    }

    #[test]
    fn test_resolve_id_generates_unique_values() {
        let id1 = resolve_id("");
        let id2 = resolve_id("");
        assert_ne!(id1, id2, "Two generated IDs should be unique");
    }
}
