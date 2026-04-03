use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::CredentialProvider;
use crate::client::KubemqClient;
use crate::error::{ErrorCode, KubemqError};
use crate::retry::RetryPolicy;
use crate::tls::TlsConfig;
use crate::transport::grpc::GrpcTransport;

/// Async callback that takes a parameter and returns a future.
pub type AsyncCallback<T> =
    Box<dyn Fn(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
/// Async callback with no parameter.
pub type AsyncNotifyCallback =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// Named constants for default values.
const DEFAULT_MAX_RECEIVE_MESSAGE_SIZE: usize = 4_194_304; // 4 MB
const DEFAULT_MAX_SEND_MESSAGE_SIZE: usize = 104_857_600; // 100 MB
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_KEEPALIVE_TIME: Duration = Duration::from_secs(10);
const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Client configuration. Immutable after build.
#[non_exhaustive]
pub struct ClientConfig {
    // Connection
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) client_id: String,
    pub(crate) auth_token: Option<String>,

    // TLS
    pub(crate) tls_config: Option<TlsConfig>,

    // Connection behavior
    pub(crate) connection_timeout: Duration,
    pub(crate) check_connection: bool,
    pub(crate) drain_timeout: Duration,

    // Keepalive
    pub(crate) keepalive_time: Duration,
    pub(crate) keepalive_timeout: Duration,
    pub(crate) permit_keepalive_without_stream: bool,

    // Message limits
    pub(crate) max_receive_message_size: usize,
    pub(crate) max_send_message_size: usize,

    // Retry
    pub(crate) retry_policy: RetryPolicy,

    // RPC
    pub(crate) rpc_timeout: Duration,

    // State callbacks
    pub(crate) on_connected: Option<AsyncNotifyCallback>,
    pub(crate) on_closed: Option<AsyncNotifyCallback>,

    // Auth
    pub(crate) credential_provider: Option<Arc<dyn CredentialProvider>>,
}

impl ClientConfig {
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }
    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }
    pub fn check_connection(&self) -> bool {
        self.check_connection
    }
    pub fn drain_timeout(&self) -> Duration {
        self.drain_timeout
    }
    pub fn keepalive_time(&self) -> Duration {
        self.keepalive_time
    }
    pub fn keepalive_timeout(&self) -> Duration {
        self.keepalive_timeout
    }
    pub fn permit_keepalive_without_stream(&self) -> bool {
        self.permit_keepalive_without_stream
    }
    pub fn max_receive_message_size(&self) -> usize {
        self.max_receive_message_size
    }
    pub fn max_send_message_size(&self) -> usize {
        self.max_send_message_size
    }
    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("client_id", &self.client_id)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "***"))
            .field("tls_config", &self.tls_config)
            .field("connection_timeout", &self.connection_timeout)
            .field("check_connection", &self.check_connection)
            .field("drain_timeout", &self.drain_timeout)
            .field("keepalive_time", &self.keepalive_time)
            .field("keepalive_timeout", &self.keepalive_timeout)
            .field(
                "permit_keepalive_without_stream",
                &self.permit_keepalive_without_stream,
            )
            .field("max_receive_message_size", &self.max_receive_message_size)
            .field("max_send_message_size", &self.max_send_message_size)
            .field("retry_policy", &self.retry_policy)
            .field("rpc_timeout", &self.rpc_timeout)
            .finish_non_exhaustive()
    }
}

/// Builder for creating a `ClientConfig` and connecting a `KubemqClient`.
pub struct ClientConfigBuilder {
    host: Option<String>,
    port: Option<u16>,
    client_id: Option<String>,
    auth_token: Option<String>,
    tls_config: Option<TlsConfig>,
    connection_timeout: Option<Duration>,
    check_connection: Option<bool>,
    drain_timeout: Option<Duration>,
    keepalive_time: Option<Duration>,
    keepalive_timeout: Option<Duration>,
    permit_keepalive_without_stream: Option<bool>,
    max_receive_message_size: Option<usize>,
    max_send_message_size: Option<usize>,
    retry_policy: Option<RetryPolicy>,
    rpc_timeout: Option<Duration>,
    on_connected: Option<AsyncNotifyCallback>,
    on_closed: Option<AsyncNotifyCallback>,
    credential_provider: Option<Arc<dyn CredentialProvider>>,
}

impl ClientConfigBuilder {
    pub(crate) fn new() -> Self {
        Self {
            host: None,
            port: None,
            client_id: None,
            auth_token: None,
            tls_config: None,
            connection_timeout: None,
            check_connection: None,
            drain_timeout: None,
            keepalive_time: None,
            keepalive_timeout: None,
            permit_keepalive_without_stream: None,
            max_receive_message_size: None,
            max_send_message_size: None,
            retry_policy: None,
            rpc_timeout: None,
            on_connected: None,
            on_closed: None,
            credential_provider: None,
        }
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    pub fn check_connection(mut self, check: bool) -> Self {
        self.check_connection = Some(check);
        self
    }

    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }

    pub fn keepalive_time(mut self, interval: Duration) -> Self {
        self.keepalive_time = Some(interval);
        self
    }

    pub fn keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.keepalive_timeout = Some(timeout);
        self
    }

    pub fn permit_keepalive_without_stream(mut self, permit: bool) -> Self {
        self.permit_keepalive_without_stream = Some(permit);
        self
    }

    pub fn max_receive_message_size(mut self, size: usize) -> Self {
        self.max_receive_message_size = Some(size);
        self
    }

    pub fn max_send_message_size(mut self, size: usize) -> Self {
        self.max_send_message_size = Some(size);
        self
    }

    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    pub fn credential_provider(mut self, provider: impl CredentialProvider + 'static) -> Self {
        self.credential_provider = Some(Arc::new(provider));
        self
    }

    pub fn on_connected<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_connected = Some(Box::new(move || Box::pin(f())));
        self
    }

    pub fn on_closed<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_closed = Some(Box::new(move || Box::pin(f())));
        self
    }

    /// Build and connect the client.
    ///
    /// Precedence: Builder method > Environment variable > Compiled default.
    pub async fn build(self) -> crate::Result<KubemqClient> {
        // 1. Resolve env var fallbacks
        let host = self
            .host
            .or_else(crate::env::get_host)
            .unwrap_or_else(|| "localhost".to_string());
        let port = self.port.or_else(crate::env::get_port).unwrap_or(50000);
        let client_id = self
            .client_id
            .or_else(crate::env::get_client_id)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let auth_token = self.auth_token.or_else(crate::env::get_auth_token);
        let connection_timeout = self
            .connection_timeout
            .or_else(crate::env::get_connection_timeout)
            .unwrap_or(DEFAULT_CONNECTION_TIMEOUT);
        let keepalive_time = self
            .keepalive_time
            .or_else(crate::env::get_keepalive_time)
            .unwrap_or(DEFAULT_KEEPALIVE_TIME);
        let keepalive_timeout = self
            .keepalive_timeout
            .or_else(crate::env::get_keepalive_timeout)
            .unwrap_or(DEFAULT_KEEPALIVE_TIMEOUT);
        let max_receive_message_size = self
            .max_receive_message_size
            .or_else(crate::env::get_max_receive_size)
            .unwrap_or(DEFAULT_MAX_RECEIVE_MESSAGE_SIZE);

        // Build TLS config from env if not provided via builder
        let tls_config = if self.tls_config.is_some() {
            self.tls_config
        } else {
            let cert_file = crate::env::get_tls_cert_file();
            let cert_data = crate::env::get_tls_cert_data();
            let client_cert = crate::env::get_tls_client_cert();
            let client_key = crate::env::get_tls_client_key();
            if cert_file.is_some()
                || cert_data.is_some()
                || client_cert.is_some()
                || client_key.is_some()
            {
                Some(TlsConfig {
                    ca_cert_file: cert_file,
                    ca_cert_pem: cert_data.map(|d| {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD
                            .decode(&d)
                            .unwrap_or_else(|_| d.into_bytes())
                    }),
                    cert_file: client_cert,
                    key_file: client_key,
                    ..Default::default()
                })
            } else {
                None
            }
        };

        // 2. Validate config values
        if keepalive_time < Duration::from_secs(5) {
            return Err(KubemqError::Validation {
                code: ErrorCode::Validation,
                message: "keepalive interval must be >= 5 seconds".into(),
                operation: "ClientConfigBuilder::build".into(),
                channel: String::new(),
                suggestion: "Set keepalive_time to at least 5 seconds.",
            });
        }

        // 3. Construct ClientConfig
        let config = ClientConfig {
            host,
            port,
            client_id,
            auth_token,
            tls_config,
            connection_timeout,
            check_connection: self.check_connection.unwrap_or(false),
            drain_timeout: self.drain_timeout.unwrap_or(DEFAULT_DRAIN_TIMEOUT),
            keepalive_time,
            keepalive_timeout,
            permit_keepalive_without_stream: self.permit_keepalive_without_stream.unwrap_or(true),
            max_receive_message_size,
            max_send_message_size: self
                .max_send_message_size
                .unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE),
            retry_policy: self.retry_policy.unwrap_or_default(),
            rpc_timeout: self.rpc_timeout.unwrap_or(DEFAULT_RPC_TIMEOUT),
            on_connected: self.on_connected,
            on_closed: self.on_closed,
            credential_provider: self.credential_provider,
        };

        // 4. Create transport and connect (takes ownership of config)
        let transport = GrpcTransport::connect(config).await?;

        // 5. Return KubemqClient (config is now owned by transport)
        Ok(KubemqClient::from_transport(transport))
    }
}

impl std::fmt::Debug for ClientConfigBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfigBuilder")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("client_id", &self.client_id)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_config", &self.tls_config.is_some())
            .field("connection_timeout", &self.connection_timeout)
            .field("rpc_timeout", &self.rpc_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- ClientConfigBuilder Debug tests --

    #[test]
    fn test_builder_debug_redacts_token() {
        let builder = ClientConfigBuilder::new()
            .host("localhost")
            .port(50000)
            .auth_token("my-secret");
        let debug = format!("{:?}", builder);
        assert!(debug.contains("ClientConfigBuilder"));
        assert!(debug.contains("localhost"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("my-secret"));
    }

    #[test]
    fn test_builder_debug_no_token() {
        let builder = ClientConfigBuilder::new().host("localhost");
        let debug = format!("{:?}", builder);
        assert!(debug.contains("ClientConfigBuilder"));
        assert!(!debug.contains("<redacted>"));
    }

    // -- ClientConfigBuilder setter coverage --

    #[test]
    fn test_builder_all_setters_compile() {
        let _builder = ClientConfigBuilder::new()
            .host("host")
            .port(5000)
            .client_id("cid")
            .auth_token("tok")
            .tls_config(TlsConfig::default())
            .connection_timeout(Duration::from_secs(5))
            .check_connection(true)
            .drain_timeout(Duration::from_secs(10))
            .keepalive_time(Duration::from_secs(30))
            .keepalive_timeout(Duration::from_secs(10))
            .permit_keepalive_without_stream(false)
            .max_receive_message_size(8_000_000)
            .max_send_message_size(16_000_000)
            .retry_policy(RetryPolicy::default())
            .rpc_timeout(Duration::from_secs(30))
            .on_connected(|| async {})
            .on_closed(|| async {})
            .credential_provider(crate::auth::StaticTokenProvider::new("test"));
    }

    // -- Default constants validation --

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_MAX_RECEIVE_MESSAGE_SIZE, 4_194_304);
        assert_eq!(DEFAULT_MAX_SEND_MESSAGE_SIZE, 104_857_600);
        assert_eq!(DEFAULT_RPC_TIMEOUT, Duration::from_secs(60));
        assert_eq!(DEFAULT_DRAIN_TIMEOUT, Duration::from_secs(5));
        assert_eq!(DEFAULT_CONNECTION_TIMEOUT, Duration::from_secs(10));
        assert_eq!(DEFAULT_KEEPALIVE_TIME, Duration::from_secs(10));
        assert_eq!(DEFAULT_KEEPALIVE_TIMEOUT, Duration::from_secs(5));
    }
}
