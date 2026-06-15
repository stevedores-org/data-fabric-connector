use std::future::Future;
use std::time::Duration;

use dfc_core::{DfcError, DfcMetrics};
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
                sleep(backoff).await;
                backoff = (backoff * 2).min(policy.max_backoff);
            }
            Err(err) => return Err(err),
        }
    }
}
