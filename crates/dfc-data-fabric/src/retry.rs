use std::future::Future;
use std::time::Duration;

use dfc_core::{DfcError, DfcMetrics};
use rand::Rng;
use tokio::time::sleep;

/// Default retry policy for transient upstream failures (5xx / 429).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    pub fn from_env() -> Self {
        let max_attempts = std::env::var("DFC_RETRY_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);
        Self {
            max_attempts,
            ..Self::default()
        }
    }
}

pub async fn with_retry<F, Fut, T>(
    policy: &RetryPolicy,
    metrics: Option<&DfcMetrics>,
    mut operation: F,
) -> Result<T, DfcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DfcError>>,
{
    let mut attempt = 0u32;
    let mut backoff = policy.initial_backoff;
    loop {
        attempt += 1;
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) if err.is_retryable() && attempt < policy.max_attempts => {
                if let Some(metrics) = metrics {
                    metrics.inc_retries();
                }
                tracing::warn!(attempt, error = %err, "retrying upstream operation");
                sleep(jitter(backoff)).await;
                backoff = (backoff * 2).min(policy.max_backoff);
            }
            Err(err) => return Err(err),
        }
    }
}

/// Equal-jitter backoff: returns a duration in `[backoff/2, backoff]`.
///
/// M1: deterministic doubling (the previous behaviour) synchronises a
/// thundering herd at every upstream restart. Equal-jitter caps the wait at
/// the same ceiling but spreads the wake-ups across half the window.
fn jitter(backoff: Duration) -> Duration {
    let half = backoff / 2;
    let extra_ms = backoff
        .checked_sub(half)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if extra_ms == 0 {
        return backoff;
    }
    let extra = rand::thread_rng().gen_range(0..=extra_ms);
    half + Duration::from_millis(extra)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_inside_half_to_full_window() {
        let base = Duration::from_millis(400);
        for _ in 0..100 {
            let d = jitter(base);
            assert!(d >= base / 2, "{:?} < half of {:?}", d, base);
            assert!(d <= base, "{:?} > full {:?}", d, base);
        }
    }

    #[test]
    fn jitter_handles_zero_and_one_ms() {
        assert_eq!(jitter(Duration::from_millis(0)), Duration::from_millis(0));
        assert_eq!(jitter(Duration::from_millis(1)), Duration::from_millis(1));
    }
}
