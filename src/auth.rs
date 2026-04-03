/// Credential provider for authentication.
///
/// The SDK guarantees serialized invocation: at most one get_token() call
/// is outstanding at any time.
///
/// # Concurrency
///
/// Each `KubemqClient` instance spawns its own refresh task. If the same
/// `CredentialProvider` is shared across multiple clients (via `Arc`), the
/// provider MUST be safe for concurrent `get_token()` calls. The per-client
/// serialization guarantee applies only within a single client.
///
/// Uses `async_trait` for dyn dispatch compatibility (native async fn in
/// traits does not support `dyn Trait` on Rust 1.75-1.85).
#[async_trait::async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Get the current authentication token.
    ///
    /// Returns:
    /// - token: the auth token string
    /// - expires_at: optional expiry time. When Some, the SDK schedules
    ///   proactive refresh. When None, refreshes only on UNAUTHENTICATED.
    async fn get_token(&self) -> crate::Result<(String, Option<std::time::SystemTime>)>;
}

/// Static token provider that never expires.
pub struct StaticTokenProvider {
    token: String,
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &"<redacted>")
            .finish()
    }
}

impl StaticTokenProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for StaticTokenProvider {
    async fn get_token(&self) -> crate::Result<(String, Option<std::time::SystemTime>)> {
        Ok((self.token.clone(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_token_provider() {
        let provider = StaticTokenProvider::new("my-token");
        let (token, expires_at) = provider.get_token().await.unwrap();
        assert_eq!(token, "my-token");
        assert!(expires_at.is_none());
    }

    #[tokio::test]
    async fn test_static_token_provider_from_string() {
        let provider = StaticTokenProvider::new(String::from("string-token"));
        let (token, _) = provider.get_token().await.unwrap();
        assert_eq!(token, "string-token");
    }

    #[test]
    fn test_static_token_provider_debug_redacts() {
        let provider = StaticTokenProvider::new("secret");
        let debug = format!("{:?}", provider);
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret"));
    }

    #[tokio::test]
    async fn test_static_token_provider_multiple_calls() {
        let provider = StaticTokenProvider::new("token");
        // Multiple calls should return the same token
        for _ in 0..5 {
            let (token, expires_at) = provider.get_token().await.unwrap();
            assert_eq!(token, "token");
            assert!(expires_at.is_none());
        }
    }

    #[tokio::test]
    async fn test_static_token_provider_as_trait_object() {
        let provider: Box<dyn CredentialProvider> = Box::new(StaticTokenProvider::new("dyn-token"));
        let (token, _) = provider.get_token().await.unwrap();
        assert_eq!(token, "dyn-token");
    }

    #[test]
    fn test_static_token_provider_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StaticTokenProvider>();
    }
}
