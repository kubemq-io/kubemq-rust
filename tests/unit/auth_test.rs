//! Tests for StaticTokenProvider.

use kubemq::{CredentialProvider, StaticTokenProvider};

#[tokio::test]
async fn test_static_token_returns_token() {
    let provider = StaticTokenProvider::new("my-secret-token");
    let (token, expires_at) = provider.get_token().await.unwrap();
    assert_eq!(token, "my-secret-token");
    assert!(expires_at.is_none()); // static tokens never expire
}

#[tokio::test]
async fn test_static_token_empty() {
    let provider = StaticTokenProvider::new("");
    let (token, expires_at) = provider.get_token().await.unwrap();
    assert!(token.is_empty());
    assert!(expires_at.is_none());
}

#[tokio::test]
async fn test_static_token_multiple_calls() {
    let provider = StaticTokenProvider::new("token-123");
    for _ in 0..10 {
        let (token, _) = provider.get_token().await.unwrap();
        assert_eq!(token, "token-123");
    }
}

#[tokio::test]
async fn test_static_token_from_string() {
    let token_string = String::from("dynamic-token");
    let provider = StaticTokenProvider::new(token_string);
    let (token, _) = provider.get_token().await.unwrap();
    assert_eq!(token, "dynamic-token");
}

/// StaticTokenProvider implements Send + Sync (required by CredentialProvider).
#[test]
fn test_static_token_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StaticTokenProvider>();
}
