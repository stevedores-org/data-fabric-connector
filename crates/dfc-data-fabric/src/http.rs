use dfc_core::{DfcError, DfcMetrics};
use reqwest::{Client, RequestBuilder, Response};

use crate::retry::{with_retry, RetryPolicy};

pub async fn send_with_retry(
    _client: &Client,
    build: impl Fn() -> RequestBuilder,
    system: &str,
    policy: &RetryPolicy,
    metrics: Option<&DfcMetrics>,
) -> Result<Response, DfcError> {
    with_retry(policy, metrics, || async {
        let resp = build()
            .send()
            .await
            .map_err(|err| DfcError::upstream(system, err.to_string(), None))?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let message = format!("status {status}");
            return Err(DfcError::upstream(system, message, Some(status)));
        }
        Ok(resp)
    })
    .await
}

pub async fn send_allowing_status(
    _client: &Client,
    build: impl Fn() -> RequestBuilder,
    system: &str,
    policy: &RetryPolicy,
    metrics: Option<&DfcMetrics>,
) -> Result<Response, DfcError> {
    with_retry(policy, metrics, || async {
        let resp = build()
            .send()
            .await
            .map_err(|err| DfcError::upstream(system, err.to_string(), None))?;
        let status = resp.status().as_u16();
        if resp.status().is_server_error() || status == 429 {
            return Err(DfcError::upstream(
                system,
                format!("status {status}"),
                Some(status),
            ));
        }
        Ok(resp)
    })
    .await
}
