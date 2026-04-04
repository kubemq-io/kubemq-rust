//! Subscription handle for managing active subscriptions.

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Handle for an active event, event-store, command, or query subscription.
///
/// Provides methods to cancel the subscription, check its status, and wait
/// for completion. When dropped, the subscription is automatically cancelled
/// via the internal [`CancellationToken`].
///
/// Obtained from [`subscribe_to_events()`](crate::KubemqClient::subscribe_to_events),
/// [`subscribe_to_events_store()`](crate::KubemqClient::subscribe_to_events_store),
/// [`subscribe_to_commands()`](crate::KubemqClient::subscribe_to_commands), or
/// [`subscribe_to_queries()`](crate::KubemqClient::subscribe_to_queries).
pub struct Subscription {
    id: String,
    cancel: CancellationToken,
    done: watch::Receiver<bool>,
}

impl Subscription {
    /// Create a new subscription handle.
    ///
    /// `id` is an opaque identifier for the subscription.
    /// `cancel` is used to signal cancellation to the subscription task.
    /// `done` receives `true` when the subscription task has terminated.
    pub(crate) fn new(id: String, cancel: CancellationToken, done: watch::Receiver<bool>) -> Self {
        Self { id, cancel, done }
    }

    /// Cancel the subscription and wait for cleanup. Safe to call multiple times.
    pub async fn unsubscribe(&self) {
        self.cancel.cancel();
        let mut done = self.done.clone();
        let _ = done.wait_for(|&v| v).await;
    }

    /// Cancel without waiting.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Check if the subscription has terminated.
    pub fn is_done(&self) -> bool {
        *self.done.borrow()
    }

    /// Get a future that completes when the subscription terminates.
    pub async fn done(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|&v| v).await;
    }

    /// Get the subscription ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Check if the subscription has been cancelled.
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Get a clone of the cancellation token (for internal use).
    #[allow(dead_code)]
    pub(crate) fn token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("is_cancelled", &self.cancel.is_cancelled())
            .field("is_done", &*self.done.borrow())
            .finish()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_cancel() {
        let cancel = CancellationToken::new();
        let (done_tx, done_rx) = watch::channel(false);
        let sub = Subscription::new("test-1".to_string(), cancel.clone(), done_rx);

        assert_eq!(sub.id(), "test-1");
        assert!(!sub.is_closed());
        assert!(!sub.is_done());

        sub.cancel();
        assert!(sub.is_closed());
        assert!(cancel.is_cancelled());

        // Simulate task completion
        let _ = done_tx.send(true);
        assert!(sub.is_done());
    }

    #[tokio::test]
    async fn test_subscription_unsubscribe() {
        let cancel = CancellationToken::new();
        let (done_tx, done_rx) = watch::channel(false);
        let sub = Subscription::new("test-2".to_string(), cancel, done_rx);

        // Spawn a task that completes the done signal after cancel
        let token = sub.token();
        tokio::spawn(async move {
            token.cancelled().await;
            let _ = done_tx.send(true);
        });

        sub.unsubscribe().await;
        assert!(sub.is_closed());
        assert!(sub.is_done());
    }

    #[tokio::test]
    async fn test_subscription_drop_cancels() {
        let cancel = CancellationToken::new();
        let (_done_tx, done_rx) = watch::channel(false);
        let token = cancel.clone();

        {
            let _sub = Subscription::new("test-3".to_string(), cancel, done_rx);
            assert!(!token.is_cancelled());
        }
        // After drop, should be cancelled
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_subscription_done_waits() {
        let cancel = CancellationToken::new();
        let (done_tx, done_rx) = watch::channel(false);
        let sub = Subscription::new("test-4".to_string(), cancel, done_rx);

        // Signal done after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = done_tx.send(true);
        });

        sub.done().await;
        assert!(sub.is_done());
    }
}
