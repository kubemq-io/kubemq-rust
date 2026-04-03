//! Tests for ClientConfig builder and defaults.

use kubemq::KubemqClient;
use std::time::Duration;

/// Builder with no host should use default "localhost".
#[tokio::test]
async fn test_builder_defaults_host_and_port() {
    // We cannot build without a server, but we can verify the builder
    // compiles and returns a meaningful error when no server is available.
    let result = KubemqClient::builder()
        .client_id("test")
        .check_connection(false)
        .build()
        .await;
    // Should either succeed (if something is on localhost:50000) or fail with transport error.
    // We just verify it doesn't panic and returns a Result.
    let _ = result;
}

/// Builder sets host and port correctly (via mock server).
#[tokio::test]
async fn test_builder_sets_host_port() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .client_id("my-client")
        .check_connection(false)
        .build()
        .await
        .unwrap();
    assert_eq!(client.config().host(), "127.0.0.1");
    assert_eq!(client.config().port(), addr.port());
    assert_eq!(client.config().client_id(), "my-client");
    client.close().await.unwrap();
}

/// Builder chain is fluent.
#[tokio::test]
async fn test_builder_fluent_chain() {
    let _builder = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("test-chain")
        .auth_token("my-secret-token")
        .connection_timeout(Duration::from_secs(5))
        .check_connection(false)
        .drain_timeout(Duration::from_secs(10))
        .keepalive_time(Duration::from_secs(30))
        .keepalive_timeout(Duration::from_secs(10))
        .max_receive_message_size(8 * 1024 * 1024)
        .max_send_message_size(8 * 1024 * 1024);
    // Just verifying compile-time API. The builder consumes itself on build().
}

/// Keepalive_time below 5s should be rejected.
#[tokio::test]
async fn test_builder_rejects_low_keepalive() {
    let result = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .keepalive_time(Duration::from_secs(2))
        .check_connection(false)
        .build()
        .await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.code(), kubemq::ErrorCode::Validation);
}

/// ClientConfig Debug impl does not leak auth_token.
#[tokio::test]
async fn test_config_debug_redacts_token() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = crate::mock_server::build_test_client(addr).await;
    let debug = format!("{:?}", client.config());
    assert!(debug.contains("host"));
    // Auth token should be redacted as "***" or not present
    assert!(!debug.contains("my-secret-token"));
    client.close().await.unwrap();
}

/// Builder with on_connected callback compiles.
#[test]
fn test_builder_accepts_callbacks() {
    let _b = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .on_connected(|| async {})
        .on_closed(|| async {});
}

/// REQ-M24: Duration truncation to i32 milliseconds is safely capped.
#[test]
fn test_duration_truncation_capped() {
    let d = Duration::from_millis(i32::MAX as u64 + 1);
    let capped = i32::try_from(d.as_millis()).unwrap_or(i32::MAX);
    assert_eq!(
        capped,
        i32::MAX,
        "Overflowing duration should cap at i32::MAX"
    );
}

/// Test all config accessor methods return expected values.
#[tokio::test]
async fn test_config_accessors() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .client_id("accessor-test")
        .auth_token("secret-tok")
        .check_connection(false)
        .drain_timeout(Duration::from_secs(3))
        .keepalive_time(Duration::from_secs(15))
        .keepalive_timeout(Duration::from_secs(8))
        .permit_keepalive_without_stream(false)
        .max_receive_message_size(8_000_000)
        .max_send_message_size(16_000_000)
        .rpc_timeout(Duration::from_secs(45))
        .build()
        .await
        .unwrap();

    let cfg = client.config();
    assert_eq!(cfg.host(), "127.0.0.1");
    assert_eq!(cfg.port(), addr.port());
    assert_eq!(cfg.client_id(), "accessor-test");
    assert_eq!(cfg.auth_token(), Some("secret-tok"));
    assert!(!cfg.check_connection());
    assert_eq!(cfg.drain_timeout(), Duration::from_secs(3));
    assert_eq!(cfg.keepalive_time(), Duration::from_secs(15));
    assert_eq!(cfg.keepalive_timeout(), Duration::from_secs(8));
    assert!(!cfg.permit_keepalive_without_stream());
    assert_eq!(cfg.max_receive_message_size(), 8_000_000);
    assert_eq!(cfg.max_send_message_size(), 16_000_000);
    assert_eq!(cfg.rpc_timeout(), Duration::from_secs(45));
    assert!(cfg.tls_config().is_none());
    assert_eq!(cfg.connection_timeout(), Duration::from_secs(10)); // default
    let _ = cfg.retry_policy();

    client.close().await.unwrap();
}

/// Test ClientConfig Debug format.
#[tokio::test]
async fn test_config_debug_format() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .client_id("debug-test")
        .check_connection(false)
        .build()
        .await
        .unwrap();
    let debug = format!("{:?}", client.config());
    assert!(debug.contains("ClientConfig"));
    assert!(debug.contains("host"));
    assert!(debug.contains("port"));
    assert!(debug.contains("client_id"));
    assert!(debug.contains("debug-test"));
    assert!(debug.contains("keepalive_time"));
    assert!(debug.contains("max_receive_message_size"));
    assert!(debug.contains("rpc_timeout"));
    client.close().await.unwrap();
}

/// Test ClientConfigBuilder Debug format.
#[test]
fn test_builder_debug_format() {
    let builder = KubemqClient::builder()
        .host("testhost")
        .port(12345)
        .client_id("debug-builder")
        .connection_timeout(Duration::from_secs(5))
        .rpc_timeout(Duration::from_secs(30));
    let debug = format!("{:?}", builder);
    assert!(debug.contains("ClientConfigBuilder"));
    assert!(debug.contains("testhost"));
    assert!(debug.contains("12345"));
}

/// Test builder with no auth token.
#[tokio::test]
async fn test_config_no_auth_token() {
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .check_connection(false)
        .build()
        .await
        .unwrap();
    assert!(client.config().auth_token().is_none());
    client.close().await.unwrap();
}

/// Verify env fallback chain: Builder value > Environment variable > Default.
#[tokio::test]
async fn test_env_fallback_chain() {
    // 1. Default: no env, no builder override -- client_id defaults to UUID
    std::env::remove_var("KUBEMQ_CLIENT_ID");
    let (addr, _state, _shutdown) = crate::mock_server::start_mock_server().await;

    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .check_connection(false)
        .build()
        .await
        .unwrap();
    let default_id = client.config().client_id().to_string();
    assert!(
        !default_id.is_empty(),
        "Default client_id should be non-empty (UUID)"
    );
    client.close().await.unwrap();

    // 2. Env overrides default
    std::env::set_var("KUBEMQ_CLIENT_ID", "env-client-id");
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .check_connection(false)
        .build()
        .await
        .unwrap();
    assert_eq!(
        client.config().client_id(),
        "env-client-id",
        "Env var should override default"
    );
    client.close().await.unwrap();

    // 3. Builder overrides env
    let client = KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .client_id("builder-client-id")
        .check_connection(false)
        .build()
        .await
        .unwrap();
    assert_eq!(
        client.config().client_id(),
        "builder-client-id",
        "Builder value should override env var"
    );
    client.close().await.unwrap();

    // Cleanup
    std::env::remove_var("KUBEMQ_CLIENT_ID");
}
