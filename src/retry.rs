use std::time::Duration;

use rand::Rng;

/// Jitter mode applied to exponential backoff calculations.
///
/// Jitter spreads retry attempts over time to avoid thundering-herd
/// effects when many clients reconnect simultaneously.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterMode {
    /// No jitter — backoff duration is deterministic.
    None,
    /// Full jitter — random value in `[0, backoff]`.
    Full,
    /// Equal jitter — random value in `[backoff/2, backoff]`.
    Equal,
}

/// Retry policy for transient operation errors and subscription reconnection.
///
/// Uses exponential backoff with configurable jitter. Applied automatically
/// by the SDK for subscription reconnection and available for manual use
/// with [`backoff()`](Self::backoff).
///
/// # Defaults
///
/// | Field | Value |
/// |-------|-------|
/// | `max_retries` | 3 |
/// | `initial_backoff` | 100ms |
/// | `max_backoff` | 10s |
/// | `multiplier` | 2.0 |
/// | `jitter_mode` | [`JitterMode::Full`] |
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts. 0 means unlimited retries.
    pub max_retries: u32,
    /// Backoff duration for the first retry attempt.
    pub initial_backoff: Duration,
    /// Upper bound for backoff duration (caps exponential growth).
    pub max_backoff: Duration,
    /// Exponential multiplier applied per attempt (e.g., 2.0 for doubling).
    pub multiplier: f64,
    /// Jitter strategy applied to the computed backoff.
    pub jitter_mode: JitterMode,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_mode: JitterMode::Full,
        }
    }
}

impl RetryPolicy {
    /// Calculate backoff duration for a given attempt.
    pub fn backoff(&self, attempt: u32) -> Duration {
        compute_backoff(
            attempt,
            self.initial_backoff,
            self.max_backoff,
            self.multiplier,
            self.jitter_mode,
        )
    }
}

pub(crate) fn compute_backoff(
    attempt: u32,
    initial: Duration,
    max: Duration,
    multiplier: f64,
    jitter: JitterMode,
) -> Duration {
    let base = initial.as_secs_f64() * multiplier.powi(attempt as i32);
    let capped = base.min(max.as_secs_f64());
    let jittered = match jitter {
        JitterMode::None => capped,
        JitterMode::Full => rand::rng().random::<f64>() * capped,
        JitterMode::Equal => {
            let half = capped / 2.0;
            half + rand::rng().random::<f64>() * half
        }
    };
    Duration::from_secs_f64(jittered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(10));
        assert_eq!(policy.multiplier, 2.0);
        assert_eq!(policy.jitter_mode, JitterMode::Full);
    }

    #[test]
    fn test_backoff_no_jitter() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_mode: JitterMode::None,
        };
        let b0 = policy.backoff(0);
        assert_eq!(b0, Duration::from_millis(100));
        let b1 = policy.backoff(1);
        assert_eq!(b1, Duration::from_millis(200));
        let b2 = policy.backoff(2);
        assert_eq!(b2, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_capped() {
        let policy = RetryPolicy {
            max_retries: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            multiplier: 10.0,
            jitter_mode: JitterMode::None,
        };
        let b = policy.backoff(5);
        assert_eq!(b, Duration::from_secs(5));
    }

    #[test]
    fn test_backoff_full_jitter_bounded() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_mode: JitterMode::Full,
        };
        for _ in 0..100 {
            let b = policy.backoff(0);
            assert!(b <= Duration::from_millis(100));
        }
    }

    #[test]
    fn test_backoff_equal_jitter_bounded() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_mode: JitterMode::Equal,
        };
        for _ in 0..100 {
            let b = policy.backoff(0);
            assert!(b >= Duration::from_millis(50));
            assert!(b <= Duration::from_millis(100));
        }
    }
}
