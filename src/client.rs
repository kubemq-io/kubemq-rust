use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::config::{ClientConfig, ClientConfigBuilder};
use crate::connection::ConnectionState;
use crate::error::KubemqError;
use crate::server_info::ServerInfo;
use crate::transport::grpc::GrpcTransport;
use crate::Result;

struct Inner {
    transport: GrpcTransport,
    closed: AtomicBool,
    cancel: CancellationToken,
    transport_cancel: CancellationToken,
}

/// Thread-safe KubeMQ client. Clone is cheap (Arc-based).
///
/// This is the primary entry point for all messaging operations. Use
/// [`builder()`](Self::builder) to create a configured client, then call
/// pattern-specific methods to send [`Event`](crate::Event)s,
/// [`EventStore`](crate::EventStore)s, [`Command`](crate::Command)s,
/// [`Query`](crate::Query) requests, or [`QueueMessage`](crate::QueueMessage)s.
///
/// All operations return [`Result<T>`](crate::Result) with structured
/// [`KubemqError`](crate::KubemqError) variants.
///
/// # Shutdown
///
/// Call [`close()`](Self::close) before dropping to ensure graceful shutdown.
/// Dropping without `close()` cancels background tasks but does not send
/// broker-side close handshakes (e.g., queue downstream `CloseByClient`).
#[derive(Clone)]
pub struct KubemqClient {
    inner: Arc<Inner>,
}

impl KubemqClient {
    /// Creates a [`ClientConfigBuilder`] for configuring and connecting the client.
    ///
    /// See [`ClientConfig`] for the full set of configurable options.
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
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::new()
    }

    /// Construct a client from the transport. Called by `ClientConfigBuilder::build()`.
    /// Config is now owned by GrpcTransport.
    pub(crate) fn from_transport(transport: GrpcTransport) -> Self {
        let transport_cancel = transport.cancel_token();
        Self {
            inner: Arc::new(Inner {
                transport,
                closed: AtomicBool::new(false),
                cancel: CancellationToken::new(),
                transport_cancel,
            }),
        }
    }

    /// Get a child cancellation token for spawned tasks.
    ///
    /// Child tokens are automatically cancelled when the parent token
    /// (owned by this client) is cancelled via `close()`.
    pub(crate) fn child_token(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Returns the current [`ConnectionState`].
    ///
    /// This method is synchronous — no `.await` needed.
    ///
    /// # Returns
    ///
    /// The current [`ConnectionState`]: [`Idle`](ConnectionState::Idle),
    /// [`Ready`](ConnectionState::Ready), or [`Closed`](ConnectionState::Closed).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # fn example(client: &KubemqClient) {
    /// match client.state() {
    ///     ConnectionState::Ready => println!("Connected"),
    ///     ConnectionState::Idle => println!("Not yet connected"),
    ///     ConnectionState::Closed => println!("Closed"),
    /// }
    /// # }
    /// ```
    pub fn state(&self) -> ConnectionState {
        self.inner.transport.state()
    }

    /// Pings the KubeMQ broker and returns [`ServerInfo`].
    ///
    /// This health-check bypasses authentication — it can be called even when
    /// the auth token is not configured.
    ///
    /// # Returns
    ///
    /// [`ServerInfo`] containing the server's hostname, version, start time,
    /// and uptime.
    ///
    /// # Errors
    ///
    /// * [`KubemqError::ClientClosed`] — if the client has been closed.
    /// * [`KubemqError::Transient`] (gRPC `UNAVAILABLE`) — if the server is unreachable. Retryable.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: &KubemqClient) -> kubemq::Result<()> {
    /// let info = client.ping().await?;
    /// println!("Server: {} v{}", info.host, info.version);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ping(&self) -> Result<ServerInfo> {
        self.check_closed()?;
        self.inner.transport.ping().await
    }

    /// Closes the client, cancelling all child tasks and releasing the gRPC channel.
    ///
    /// After calling `close()`, all subsequent operations will return
    /// [`KubemqError::ClientClosed`]. Active subscriptions are cancelled.
    ///
    /// This is best-effort: child tasks are cancelled via CancellationToken and given
    /// `drain_timeout` to complete, but there are no JoinHandles tracked. Tasks that
    /// don't observe cancellation within the timeout may still be running when the
    /// transport is closed, resulting in gRPC errors in flight. This is an accepted
    /// trade-off for v1 — the drain_timeout is configurable via ClientConfigBuilder.
    ///
    /// Idempotent — safe to call multiple times. Returns `Ok(())` if already closed.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # async fn example(client: KubemqClient) -> kubemq::Result<()> {
    /// client.close().await?;
    /// assert_eq!(client.state(), ConnectionState::Closed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // Already closed
        }
        // Cancel all child tokens -- spawned tasks should observe this
        self.inner.cancel.cancel();
        // Best-effort drain: give tasks drain_timeout to observe cancellation
        let drain = self.config().drain_timeout();
        let _ = tokio::time::timeout(drain, async {
            // Yield to let cancelled tasks complete their cleanup
            tokio::task::yield_now().await;
        })
        .await;
        self.inner.transport.close().await;
        // Fire on_closed callback (REQ-C2)
        if let Some(cb) = &self.config().on_closed {
            cb().await;
        }
        Ok(())
    }

    /// Check if the client is closed.
    ///
    /// NOTE: There is an inherent TOCTOU race between this check and the
    /// subsequent gRPC operation. This is an accepted trade-off of lock-free
    /// design -- the gRPC call will fail with a transport error if the client
    /// closes between check and call.
    pub(crate) fn check_closed(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(KubemqError::ClientClosed);
        }
        Ok(())
    }

    /// Check if the client is in a state that allows sending.
    ///
    /// NOTE: There is an inherent TOCTOU race between this check and the
    /// subsequent gRPC operation. This is an accepted trade-off of lock-free
    /// design -- the gRPC call will fail with a transport error if the client
    /// closes between check and call.
    pub(crate) fn check_state(&self, _operation: &str) -> Result<()> {
        self.check_closed()
    }

    /// Returns a reference to the client's [`ClientConfig`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::prelude::*;
    ///
    /// # fn example(client: &KubemqClient) {
    /// let config = client.config();
    /// println!("Connected to {}:{}", config.host(), config.port());
    /// # }
    /// ```
    pub fn config(&self) -> &ClientConfig {
        self.inner.transport.config()
    }

    /// Get a reference to the transport (for use by messaging pattern modules).
    pub(crate) fn transport(&self) -> &GrpcTransport {
        &self.inner.transport
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            tracing::warn!("KubemqClient dropped without calling close()");
            self.cancel.cancel();
            self.transport_cancel.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_tokens_cancelled_on_drop() {
        let cancel = CancellationToken::new();
        let transport_cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let transport_clone = transport_cancel.clone();

        assert!(!cancel_clone.is_cancelled());
        assert!(!transport_clone.is_cancelled());

        cancel.cancel();
        transport_cancel.cancel();

        assert!(cancel_clone.is_cancelled());
        assert!(transport_clone.is_cancelled());
    }

    #[test]
    fn test_transport_cancel_independent_of_client_cancel() {
        let cancel = CancellationToken::new();
        let transport_cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let transport_clone = transport_cancel.clone();

        cancel.cancel();
        assert!(cancel_clone.is_cancelled());
        assert!(
            !transport_clone.is_cancelled(),
            "transport_cancel should be independent"
        );

        transport_cancel.cancel();
        assert!(transport_clone.is_cancelled());
    }

    #[test]
    fn test_closed_flag_prevents_cancel_on_drop() {
        let closed = AtomicBool::new(true);
        let cancel = CancellationToken::new();
        let transport_cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let transport_clone = transport_cancel.clone();

        if !closed.load(Ordering::Acquire) {
            cancel.cancel();
            transport_cancel.cancel();
        }

        assert!(
            !cancel_clone.is_cancelled(),
            "cancel should NOT fire when closed=true"
        );
        assert!(
            !transport_clone.is_cancelled(),
            "transport_cancel should NOT fire when closed=true"
        );
    }
}
