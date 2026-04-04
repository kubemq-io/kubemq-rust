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

/// Async callback signature for subscription message handlers.
///
/// Receives a message of type `T` and returns a pinned future.
/// Used by [`KubemqClient::subscribe_to_events()`](crate::KubemqClient::subscribe_to_events),
/// [`KubemqClient::subscribe_to_commands()`](crate::KubemqClient::subscribe_to_commands),
/// [`KubemqClient::subscribe_to_queries()`](crate::KubemqClient::subscribe_to_queries), and
/// [`KubemqClient::subscribe_to_events_store()`](crate::KubemqClient::subscribe_to_events_store).
pub type AsyncCallback<T> =
    Box<dyn Fn(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
/// Async notification callback for lifecycle events.
///
/// Used by [`ClientConfigBuilder::on_connected()`] and [`ClientConfigBuilder::on_closed()`].
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

/// Client configuration for connecting to a KubeMQ broker.
///
/// Immutable after construction via [`ClientConfigBuilder::build()`].
/// Access configuration values through the getter methods below.
#[non_exhaustive]
pub struct ClientConfig {
    /// Broker hostname (default: `"localhost"`).
    pub(crate) host: String,
    /// Broker port number (default: `50000`).
    pub(crate) port: u16,
    /// Client identifier sent with every request.
    pub(crate) client_id: String,
    /// Authentication token for gRPC `authorization` metadata.
    pub(crate) auth_token: Option<String>,

    /// TLS configuration for secure connections.
    pub(crate) tls_config: Option<TlsConfig>,

    /// Timeout for establishing the initial connection.
    pub(crate) connection_timeout: Duration,
    /// Whether to verify connectivity during build.
    pub(crate) check_connection: bool,
    /// Timeout for draining tasks during graceful shutdown.
    pub(crate) drain_timeout: Duration,

    /// HTTP/2 keepalive ping interval.
    pub(crate) keepalive_time: Duration,
    /// HTTP/2 keepalive ping timeout.
    pub(crate) keepalive_timeout: Duration,
    /// Whether keepalive pings are sent without active streams.
    pub(crate) permit_keepalive_without_stream: bool,

    /// Maximum inbound message size in bytes.
    pub(crate) max_receive_message_size: usize,
    /// Maximum outbound message size in bytes.
    pub(crate) max_send_message_size: usize,

    /// Retry policy for automatic reconnection.
    pub(crate) retry_policy: RetryPolicy,

    /// RPC timeout for request-response operations.
    pub(crate) rpc_timeout: Duration,

    /// Callback invoked on connection (re)establishment.
    pub(crate) on_connected: Option<AsyncNotifyCallback>,
    /// Callback invoked on connection close.
    pub(crate) on_closed: Option<AsyncNotifyCallback>,

    /// Dynamic credential provider for authentication.
    pub(crate) credential_provider: Option<Arc<dyn CredentialProvider>>,
}

impl ClientConfig {
    /// Returns the broker hostname.
    pub fn host(&self) -> &str {
        &self.host
    }
    /// Returns the broker port number.
    pub fn port(&self) -> u16 {
        self.port
    }
    /// Returns the client identifier sent with every request.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    /// Returns the configured auth token, if any.
    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }
    /// Returns the TLS configuration, if any.
    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }
    /// Returns the connection timeout duration.
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }
    /// Returns whether the builder verifies connectivity at build time.
    pub fn check_connection(&self) -> bool {
        self.check_connection
    }
    /// Returns the drain timeout for graceful shutdown.
    pub fn drain_timeout(&self) -> Duration {
        self.drain_timeout
    }
    /// Returns the HTTP/2 keepalive interval.
    pub fn keepalive_time(&self) -> Duration {
        self.keepalive_time
    }
    /// Returns the HTTP/2 keepalive timeout.
    pub fn keepalive_timeout(&self) -> Duration {
        self.keepalive_timeout
    }
    /// Returns whether keepalive pings are sent without active streams.
    pub fn permit_keepalive_without_stream(&self) -> bool {
        self.permit_keepalive_without_stream
    }
    /// Returns the maximum inbound message size in bytes.
    pub fn max_receive_message_size(&self) -> usize {
        self.max_receive_message_size
    }
    /// Returns the maximum outbound message size in bytes.
    pub fn max_send_message_size(&self) -> usize {
        self.max_send_message_size
    }
    /// Returns the RPC timeout for request-response operations.
    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }
    /// Returns the retry policy for reconnection.
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

/// Builder for creating a [`ClientConfig`] and connecting a [`KubemqClient`](crate::KubemqClient).
///
/// Use [`KubemqClient::builder()`](crate::KubemqClient::builder) to create a new builder.
///
/// Configuration precedence: Builder method > Environment variable > Compiled default.
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

    /// Sets the broker hostname.
    ///
    /// Defaults to `"localhost"`. If not set and `KUBEMQ_ADDRESS` is defined,
    /// the host is parsed from that environment variable.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Sets the broker port number.
    ///
    /// Defaults to `50000`. If not set and `KUBEMQ_ADDRESS` is defined,
    /// the port is parsed from that environment variable.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the client identifier sent with every request.
    ///
    /// If not set, a random UUID is generated.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Sets the authentication token for the connection.
    ///
    /// The token is sent as gRPC `authorization` metadata on every request.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Sets the TLS configuration for secure connections.
    ///
    /// See [`TlsConfig`] for available options (server TLS and mutual TLS).
    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Sets the timeout for establishing the initial connection.
    ///
    /// Defaults to 10 seconds.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Enables connectivity verification during [`build()`](Self::build).
    ///
    /// When `true`, the builder pings the broker to verify the connection
    /// before returning the client. Defaults to `false`.
    pub fn check_connection(mut self, check: bool) -> Self {
        self.check_connection = Some(check);
        self
    }

    /// Sets the drain timeout for graceful shutdown.
    ///
    /// During [`KubemqClient::close()`](crate::KubemqClient::close), background tasks
    /// are given this duration to complete. Defaults to 5 seconds.
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }

    /// Sets the HTTP/2 keepalive interval.
    ///
    /// Must be at least 5 seconds. Defaults to 10 seconds.
    pub fn keepalive_time(mut self, interval: Duration) -> Self {
        self.keepalive_time = Some(interval);
        self
    }

    /// Sets the HTTP/2 keepalive timeout.
    ///
    /// If the peer does not respond to a keepalive ping within this duration,
    /// the connection is considered dead. Defaults to 5 seconds.
    pub fn keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.keepalive_timeout = Some(timeout);
        self
    }

    /// Sets whether keepalive pings are sent when there are no active streams.
    ///
    /// Defaults to `true`.
    pub fn permit_keepalive_without_stream(mut self, permit: bool) -> Self {
        self.permit_keepalive_without_stream = Some(permit);
        self
    }

    /// Sets the maximum inbound message size in bytes.
    ///
    /// Defaults to 4 MB (4,194,304 bytes).
    pub fn max_receive_message_size(mut self, size: usize) -> Self {
        self.max_receive_message_size = Some(size);
        self
    }

    /// Sets the maximum outbound message size in bytes.
    ///
    /// Defaults to 100 MB (104,857,600 bytes).
    pub fn max_send_message_size(mut self, size: usize) -> Self {
        self.max_send_message_size = Some(size);
        self
    }

    /// Sets the retry policy for automatic reconnection.
    ///
    /// See [`RetryPolicy`] for configuring backoff and jitter.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Sets the timeout for RPC (request-response) operations.
    ///
    /// Applies to Commands and Queries. Defaults to 60 seconds.
    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    /// Sets a dynamic credential provider for authentication.
    ///
    /// The provider's [`get_token()`](crate::CredentialProvider::get_token) method is called
    /// before each gRPC request to obtain the current token.
    pub fn credential_provider(mut self, provider: impl CredentialProvider + 'static) -> Self {
        self.credential_provider = Some(Arc::new(provider));
        self
    }

    /// Registers a callback invoked when the client connects (or reconnects).
    pub fn on_connected<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_connected = Some(Box::new(move || Box::pin(f())));
        self
    }

    /// Registers a callback invoked when the client connection is closed.
    pub fn on_closed<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_closed = Some(Box::new(move || Box::pin(f())));
        self
    }

    /// Builds the [`KubemqClient`](crate::KubemqClient), establishing a connection to the broker.
    ///
    /// Configuration precedence: Builder method > Environment variable > Compiled default.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example() -> kubemq::Result<()> {
    /// let client = KubemqClient::builder()
    ///     .host("localhost")
    ///     .port(50000)
    ///     .client_id("my-service")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// * [`KubemqError::Transient`](crate::KubemqError::Transient) — if the broker is unreachable (when `check_connection` is enabled).
    /// * [`KubemqError::Validation`](crate::KubemqError::Validation) — if required configuration is invalid.
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
