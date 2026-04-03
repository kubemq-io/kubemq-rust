//! Tests for TlsConfig construction.

use kubemq::TlsConfig;

#[test]
fn test_tls_config_default() {
    let config = TlsConfig::default();
    assert!(config.ca_cert_file.is_none());
    assert!(config.ca_cert_pem.is_none());
    assert!(config.cert_file.is_none());
    assert!(config.key_file.is_none());
    assert!(config.cert_pem.is_none());
    assert!(config.key_pem.is_none());
    assert!(config.server_name.is_none());
}

#[test]
fn test_tls_config_with_ca_cert_file() {
    let config = TlsConfig {
        ca_cert_file: Some("/path/to/ca.pem".to_string()),
        ..Default::default()
    };
    assert!(config.ca_cert_file.is_some());
}

#[test]
fn test_tls_config_with_ca_cert_pem() {
    let config = TlsConfig {
        ca_cert_pem: Some(b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec()),
        ..Default::default()
    };
    assert!(config.ca_cert_pem.is_some());
}

#[test]
fn test_tls_config_mtls() {
    let config = TlsConfig {
        ca_cert_file: Some("/path/to/ca.pem".to_string()),
        cert_file: Some("/path/to/client.pem".to_string()),
        key_file: Some("/path/to/client-key.pem".to_string()),
        ..Default::default()
    };
    assert!(config.cert_file.is_some());
    assert!(config.key_file.is_some());
}

#[test]
fn test_tls_config_server_name() {
    let config = TlsConfig {
        server_name: Some("kubemq.example.com".to_string()),
        ..Default::default()
    };
    assert_eq!(config.server_name.as_deref(), Some("kubemq.example.com"));
}

#[test]
fn test_tls_config_clone() {
    let config = TlsConfig {
        ca_cert_file: Some("/ca.pem".to_string()),
        server_name: Some("test".to_string()),
        ..Default::default()
    };
    let cloned = config.clone();
    assert_eq!(config.ca_cert_file, cloned.ca_cert_file);
    assert_eq!(config.server_name, cloned.server_name);
}

#[test]
fn test_tls_config_debug() {
    let config = TlsConfig::default();
    let debug = format!("{:?}", config);
    assert!(debug.contains("TlsConfig"));
}

/// REQ-M5: mTLS half-configuration (cert without key) should be rejected.
/// We test this indirectly via the builder, since to_tonic_tls_config is pub(crate).
#[tokio::test]
async fn test_mtls_half_config_error() {
    // Provide cert_file but no key_file -- should fail during build when TLS is
    // configured. We use a non-existent file path; the cert file I/O error happens
    // before the half-config validation when cert_pem is None, so use PEM bytes.
    let config = TlsConfig {
        cert_pem: Some(b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec()),
        key_file: None,
        key_pem: None,
        ..Default::default()
    };
    let result = kubemq::KubemqClient::builder()
        .host("127.0.0.1")
        .port(50000)
        .tls_config(config)
        .check_connection(false)
        .build()
        .await;
    assert!(
        result.is_err(),
        "mTLS half-config (cert without key) should fail"
    );
    let err = result.err().unwrap();
    assert_eq!(
        err.code(),
        kubemq::ErrorCode::Validation,
        "Half-config error should be Validation, got: {:?}",
        err
    );
}
