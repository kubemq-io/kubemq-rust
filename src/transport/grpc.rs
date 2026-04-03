use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::config::ClientConfig;
use crate::connection::ConnectionState;
use crate::error::KubemqError;
use crate::server_info::ServerInfo;
use crate::transport::interceptor::AuthInterceptor;

type ProtoClient =
    crate::proto::kubemq::kubemq_client::KubemqClient<InterceptedService<Channel, AuthInterceptor>>;

// Map ConnectionState to u8 for atomic storage.
fn state_to_u8(s: ConnectionState) -> u8 {
    match s {
        ConnectionState::Idle => 0,
        ConnectionState::Ready => 2,
        ConnectionState::Closed => 4,
    }
}

fn u8_to_state(v: u8) -> ConnectionState {
    match v {
        0 => ConnectionState::Idle,
        2 => ConnectionState::Ready,
        4 => ConnectionState::Closed,
        _ => ConnectionState::Closed,
    }
}

/// gRPC transport layer wrapping a tonic channel.
pub(crate) struct GrpcTransport {
    // StdMutex for client: only used for short critical sections (clone/take),
    // never held across .await points.
    client: StdMutex<Option<ProtoClient>>,
    config: ClientConfig,
    state: AtomicU8,
    #[allow(dead_code)]
    token: Arc<RwLock<Option<String>>>,
    cancel: CancellationToken,
}

impl GrpcTransport {
    /// Connect to a KubeMQ server using the given configuration.
    pub(crate) async fn connect(config: ClientConfig) -> crate::Result<Self> {
        let scheme = if config.tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{}://{}:{}", scheme, config.host, config.port);

        let mut endpoint =
            Channel::from_shared(uri.clone()).map_err(|e| KubemqError::Transport {
                operation: "connect".to_string(),
                source: Box::new(e),
            })?;

        // Configure endpoint -- removed explicit flow control windows (REQ-M4),
        // letting tonic use its defaults (1MB).
        endpoint = endpoint
            .tcp_nodelay(true)
            .keep_alive_timeout(config.keepalive_timeout)
            .http2_keep_alive_interval(config.keepalive_time)
            .keep_alive_while_idle(config.permit_keepalive_without_stream)
            .connect_timeout(config.connection_timeout);

        // TLS config
        if let Some(ref tls_config) = config.tls_config {
            let tonic_tls = tls_config.to_tonic_tls_config().await?;
            endpoint = endpoint
                .tls_config(tonic_tls)
                .map_err(|e| KubemqError::Transport {
                    operation: "tls_config".to_string(),
                    source: Box::new(e),
                })?;
        }

        // Set up auth token
        let token = Arc::new(RwLock::new(config.auth_token.clone()));

        // Set up cancellation token
        let cancel = CancellationToken::new();

        // If credential_provider is set, get initial token
        if let Some(ref provider) = config.credential_provider {
            let (token_str, expires_at) = provider.get_token().await?;
            *token.write().unwrap() = Some(token_str);

            // Spawn periodic refresh task if token has an expiration
            if let Some(expiry) = expires_at {
                let provider_clone = Arc::clone(provider);
                let token_clone = token.clone();
                let cancel_clone = cancel.clone();
                // Refresh margin: renew 30 seconds before expiry
                const REFRESH_MARGIN: Duration = Duration::from_secs(30);

                tokio::spawn(async move {
                    let mut next_expiry = expiry;
                    loop {
                        // Calculate sleep duration: time until expiry minus margin
                        let now = std::time::SystemTime::now();
                        let sleep_duration = next_expiry
                            .duration_since(now)
                            .unwrap_or(Duration::from_secs(1))
                            .checked_sub(REFRESH_MARGIN)
                            .unwrap_or(Duration::from_secs(1));

                        tokio::select! {
                            _ = cancel_clone.cancelled() => break,
                            _ = tokio::time::sleep(sleep_duration) => {
                                // Attempt token refresh with retry
                                let mut retry_delay = Duration::from_secs(1);
                                const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
                                const MAX_RETRIES: u32 = 5;

                                for attempt in 0..MAX_RETRIES {
                                    match provider_clone.get_token().await {
                                        Ok((new_token, new_expiry)) => {
                                            *token_clone.write().unwrap() = Some(new_token);
                                            tracing::debug!("Token refreshed successfully");
                                            if let Some(exp) = new_expiry {
                                                next_expiry = exp;
                                            }
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Token refresh failed (attempt {}/{}): {}",
                                                attempt + 1, MAX_RETRIES, e
                                            );
                                            if attempt + 1 < MAX_RETRIES {
                                                tokio::select! {
                                                    _ = cancel_clone.cancelled() => break,
                                                    _ = tokio::time::sleep(retry_delay) => {}
                                                }
                                                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                                            } else {
                                                tracing::error!(
                                                    "Token refresh exhausted all {} retries", MAX_RETRIES
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }

        // Connect
        let channel = if config.check_connection {
            endpoint
                .connect()
                .await
                .map_err(|e| KubemqError::Transport {
                    operation: "connect".to_string(),
                    source: Box::new(e),
                })?
        } else {
            endpoint.connect_lazy()
        };

        let interceptor = AuthInterceptor::new(token.clone());
        let client = crate::proto::kubemq::kubemq_client::KubemqClient::with_interceptor(
            channel,
            interceptor,
        )
        .max_decoding_message_size(config.max_receive_message_size)
        .max_encoding_message_size(config.max_send_message_size);

        // Determine initial state: Idle for lazy connect, Ready for eager connect (REQ-M16)
        let initial_state = if config.check_connection {
            ConnectionState::Ready
        } else {
            ConnectionState::Idle
        };

        // Fire on_connected callback for both eager and lazy connect.
        // For eager connect, the connection has been verified. For lazy
        // connect, the transport is created and ready (connection happens
        // on first RPC). Both Go and Java SDKs fire on_connected after
        // NewGRPC/build succeeds.
        if let Some(cb) = &config.on_connected {
            cb().await;
        }

        Ok(Self {
            client: StdMutex::new(Some(client)),
            config,
            state: AtomicU8::new(state_to_u8(initial_state)),
            token,
            cancel,
        })
    }

    /// Get the current connection state.
    pub(crate) fn state(&self) -> ConnectionState {
        u8_to_state(self.state.load(Ordering::Acquire))
    }

    /// Get a clone of the client (cheap Arc clone per call). Returns Err if closed.
    pub(crate) fn client(&self) -> crate::Result<ProtoClient> {
        let guard = self.client.lock().unwrap();
        guard.clone().ok_or(KubemqError::ClientClosed)
    }

    /// Get a reference to the config.
    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Ping the server.
    pub(crate) async fn ping(&self) -> crate::Result<ServerInfo> {
        let mut client = self.client()?;
        let mut request = tonic::Request::new(crate::proto::kubemq::Empty {});
        request.set_timeout(self.config.rpc_timeout);
        let response = client
            .ping(request)
            .await
            .map_err(|s| KubemqError::from_grpc_status(s, "ping", ""))?;

        let result = response.into_inner();
        Ok(ServerInfo {
            host: result.host,
            version: result.version,
            server_start_time: result.server_start_time,
            server_up_time_seconds: result.server_up_time_seconds,
        })
    }

    /// Close the transport. Releases the gRPC channel via Option.
    pub(crate) async fn close(&self) {
        self.state
            .store(state_to_u8(ConnectionState::Closed), Ordering::Release);
        self.cancel.cancel();
        // Drop ProtoClient, Channel, TCP connection
        {
            let mut guard = self.client.lock().unwrap();
            *guard = None;
        }
    }

    /// Get a clone of the cancellation token.
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionState;

    // -- state_to_u8 / u8_to_state roundtrip tests --

    #[test]
    fn test_state_to_u8_idle() {
        assert_eq!(state_to_u8(ConnectionState::Idle), 0);
    }

    #[test]
    fn test_state_to_u8_ready() {
        assert_eq!(state_to_u8(ConnectionState::Ready), 2);
    }

    #[test]
    fn test_state_to_u8_closed() {
        assert_eq!(state_to_u8(ConnectionState::Closed), 4);
    }

    #[test]
    fn test_u8_to_state_roundtrip() {
        let states = [
            ConnectionState::Idle,
            ConnectionState::Ready,
            ConnectionState::Closed,
        ];
        for state in &states {
            assert_eq!(u8_to_state(state_to_u8(*state)), *state);
        }
    }

    #[test]
    fn test_u8_to_state_unknown_defaults_to_closed() {
        assert_eq!(u8_to_state(5), ConnectionState::Closed);
        assert_eq!(u8_to_state(100), ConnectionState::Closed);
        assert_eq!(u8_to_state(255), ConnectionState::Closed);
    }

    #[test]
    fn test_u8_to_state_all_valid_values() {
        assert_eq!(u8_to_state(0), ConnectionState::Idle);
        assert_eq!(u8_to_state(2), ConnectionState::Ready);
        assert_eq!(u8_to_state(4), ConnectionState::Closed);
    }
}
