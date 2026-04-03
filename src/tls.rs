/// TLS configuration for connecting to a KubeMQ server.
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// Path to CA certificate file for server verification.
    pub ca_cert_file: Option<String>,
    /// PEM-encoded CA certificate bytes for server verification.
    pub ca_cert_pem: Option<Vec<u8>>,
    /// Path to client certificate file (mTLS).
    pub cert_file: Option<String>,
    /// Path to client private key file (mTLS).
    pub key_file: Option<String>,
    /// PEM-encoded client certificate bytes (mTLS).
    pub cert_pem: Option<Vec<u8>>,
    /// PEM-encoded client private key bytes (mTLS).
    pub key_pem: Option<Vec<u8>>,
    /// Override the server name for TLS verification.
    pub server_name: Option<String>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("ca_cert_file", &self.ca_cert_file)
            .field(
                "ca_cert_pem",
                &self
                    .ca_cert_pem
                    .as_ref()
                    .map(|v| format!("[{} bytes]", v.len())),
            )
            .field("cert_file", &self.cert_file)
            .field("key_file", &"[REDACTED]")
            .field(
                "cert_pem",
                &self
                    .cert_pem
                    .as_ref()
                    .map(|v| format!("[{} bytes]", v.len())),
            )
            .field("key_pem", &self.key_pem.as_ref().map(|_| "[REDACTED]"))
            .field("server_name", &self.server_name)
            .finish()
    }
}

impl TlsConfig {
    /// Build a tonic TLS config from this configuration.
    ///
    /// REQ-M34: Uses async `tokio::fs::read` instead of blocking `std::fs::read`.
    pub(crate) async fn to_tonic_tls_config(
        &self,
    ) -> crate::Result<tonic::transport::ClientTlsConfig> {
        let mut tls_config = tonic::transport::ClientTlsConfig::new();

        // Server name override
        if let Some(ref server_name) = self.server_name {
            tls_config = tls_config.domain_name(server_name.clone());
        }

        // CA certificate -- REQ-M34: async file I/O
        if let Some(ref ca_pem) = self.ca_cert_pem {
            let cert = tonic::transport::Certificate::from_pem(ca_pem.clone());
            tls_config = tls_config.ca_certificate(cert);
        } else if let Some(ref ca_file) = self.ca_cert_file {
            let pem = tokio::fs::read(ca_file)
                .await
                .map_err(|e| crate::KubemqError::Fatal {
                    code: crate::ErrorCode::Fatal,
                    message: format!("failed to read CA cert file '{}': {}", ca_file, e),
                    operation: "tls_config".to_string(),
                    source: Some(Box::new(e)),
                    suggestion: "Check that the CA certificate file exists and is readable.",
                })?;
            let cert = tonic::transport::Certificate::from_pem(pem);
            tls_config = tls_config.ca_certificate(cert);
        }

        // Client identity (mTLS) -- REQ-M34: async file I/O
        let client_cert = if let Some(ref pem) = self.cert_pem {
            Some(pem.clone())
        } else if let Some(ref path) = self.cert_file {
            Some(
                tokio::fs::read(path)
                    .await
                    .map_err(|e| crate::KubemqError::Fatal {
                        code: crate::ErrorCode::Fatal,
                        message: format!("failed to read client cert file '{}': {}", path, e),
                        operation: "tls_config".to_string(),
                        source: Some(Box::new(e)),
                        suggestion:
                            "Check that the client certificate file exists and is readable.",
                    })?,
            )
        } else {
            None
        };
        let client_key = if let Some(ref pem) = self.key_pem {
            Some(pem.clone())
        } else if let Some(ref path) = self.key_file {
            Some(
                tokio::fs::read(path)
                    .await
                    .map_err(|e| crate::KubemqError::Fatal {
                        code: crate::ErrorCode::Fatal,
                        message: format!("failed to read client key file '{}': {}", path, e),
                        operation: "tls_config".to_string(),
                        source: Some(Box::new(e)),
                        suggestion: "Check that the client key file exists and is readable.",
                    })?,
            )
        } else {
            None
        };

        // REQ-M5: mTLS half-configuration validation
        match (client_cert.is_some(), client_key.is_some()) {
            (true, true) => {
                let identity =
                    tonic::transport::Identity::from_pem(client_cert.unwrap(), client_key.unwrap());
                tls_config = tls_config.identity(identity);
            }
            (false, false) => { /* No mTLS -- OK */ }
            _ => {
                return Err(crate::KubemqError::Validation {
                    code: crate::ErrorCode::Validation,
                    message: "mTLS requires both client certificate and key".to_string(),
                    operation: "tls_config".to_string(),
                    channel: String::new(),
                    suggestion: "Provide both cert_file/cert_pem and key_file/key_pem, or neither.",
                });
            }
        }

        Ok(tls_config)
    }

    /// Check if TLS is enabled (any TLS option is set).
    #[allow(dead_code)]
    pub(crate) fn is_enabled(&self) -> bool {
        self.ca_cert_file.is_some()
            || self.ca_cert_pem.is_some()
            || self.cert_file.is_some()
            || self.key_file.is_some()
            || self.cert_pem.is_some()
            || self.key_pem.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- is_enabled tests --

    #[test]
    fn test_is_enabled_default_false() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_is_enabled_ca_cert_file() {
        let config = TlsConfig {
            ca_cert_file: Some("/path/to/ca.pem".to_string()),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_is_enabled_ca_cert_pem() {
        let config = TlsConfig {
            ca_cert_pem: Some(vec![1, 2, 3]),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_is_enabled_cert_file() {
        let config = TlsConfig {
            cert_file: Some("/path/to/cert.pem".to_string()),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_is_enabled_key_file() {
        let config = TlsConfig {
            key_file: Some("/path/to/key.pem".to_string()),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_is_enabled_cert_pem() {
        let config = TlsConfig {
            cert_pem: Some(vec![4, 5, 6]),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_is_enabled_key_pem() {
        let config = TlsConfig {
            key_pem: Some(vec![7, 8, 9]),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    // -- to_tonic_tls_config tests --

    #[tokio::test]
    async fn test_to_tonic_tls_config_minimal() {
        // No CA, no mTLS, no insecure -- should produce a valid config
        let config = TlsConfig::default();
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_with_server_name() {
        let config = TlsConfig {
            server_name: Some("example.com".to_string()),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_with_ca_pem() {
        // Use a syntactically valid (but not real) PEM -- tonic accepts the bytes
        let config = TlsConfig {
            ca_cert_pem: Some(
                b"-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n".to_vec(),
            ),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        // tonic's Certificate::from_pem just stores bytes; validation happens at connect time
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_ca_cert_file_not_found() {
        let config = TlsConfig {
            ca_cert_file: Some("/nonexistent/ca-cert.pem".to_string()),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::Fatal);
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_ca_cert_from_file() {
        // Write a temp CA cert file and read it back
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        tokio::fs::write(
            &ca_path,
            b"-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n",
        )
        .await
        .unwrap();

        let config = TlsConfig {
            ca_cert_file: Some(ca_path.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_mtls_half_config_cert_only() {
        let config = TlsConfig {
            cert_pem: Some(
                b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec(),
            ),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::Validation);
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_mtls_half_config_key_only() {
        let config = TlsConfig {
            key_pem: Some(
                b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----".to_vec(),
            ),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::Validation);
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_mtls_full_pem() {
        let config = TlsConfig {
            cert_pem: Some(
                b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec(),
            ),
            key_pem: Some(
                b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----".to_vec(),
            ),
            ..Default::default()
        };
        // Both provided, so mTLS validation passes (connect-time will fail with bad certs)
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_cert_file_not_found() {
        let config = TlsConfig {
            cert_file: Some("/nonexistent/client.pem".to_string()),
            key_pem: Some(
                b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----".to_vec(),
            ),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::Fatal);
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_key_file_not_found() {
        let config = TlsConfig {
            cert_pem: Some(
                b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec(),
            ),
            key_file: Some("/nonexistent/key.pem".to_string()),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::Fatal);
    }

    #[tokio::test]
    async fn test_to_tonic_tls_config_mtls_from_files() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("client.pem");
        let key_path = dir.path().join("client-key.pem");
        tokio::fs::write(
            &cert_path,
            b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
        )
        .await
        .unwrap();
        tokio::fs::write(
            &key_path,
            b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
        )
        .await
        .unwrap();

        let config = TlsConfig {
            cert_file: Some(cert_path.to_str().unwrap().to_string()),
            key_file: Some(key_path.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let result = config.to_tonic_tls_config().await;
        assert!(result.is_ok());
    }

    // -- Debug redaction tests --

    #[test]
    fn test_debug_redacts_key_file() {
        let config = TlsConfig {
            key_file: Some("/secret/key.pem".to_string()),
            ..Default::default()
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("/secret/key.pem"));
    }

    #[test]
    fn test_debug_redacts_key_pem() {
        let config = TlsConfig {
            key_pem: Some(vec![1, 2, 3, 4, 5]),
            ..Default::default()
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("[1, 2, 3, 4, 5]"));
    }

    #[test]
    fn test_debug_shows_pem_lengths() {
        let config = TlsConfig {
            ca_cert_pem: Some(vec![0; 42]),
            cert_pem: Some(vec![0; 100]),
            ..Default::default()
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("[42 bytes]"));
        assert!(debug.contains("[100 bytes]"));
    }
}
