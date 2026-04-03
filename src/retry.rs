use std::time::Duration;

use rand::Rng;

/// Jitter mode for backoff calculations.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterMode {
    None,
    Full,
    Equal,
}

/// Retry policy for transient operation errors.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
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
