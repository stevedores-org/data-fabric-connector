use async_trait::async_trait;
use dfc_core::{DfcError, ReplayRequest, ReplayResponse, RollbackRequest, RollbackResponse};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use urlencoding::encode as urlenc;

use crate::config::AivcsConfig;
use crate::review::{ReviewDecisionPayload, ReviewDecisionResult, ReviewFragments};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOperation {
    pub operation_id: String,
    pub status: String,
}

#[async_trait]
pub trait AivcsClient: Send + Sync {
    async fn request_replay(&self, req: &ReplayRequest) -> Result<ReplayResponse, DfcError>;
    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError>;
    async fn fetch_review_fragments(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<ReviewFragments, DfcError>;
    async fn submit_review_decision(
        &self,
        payload: &ReviewDecisionPayload,
    ) -> Result<ReviewDecisionResult, DfcError>;
}

pub struct HttpAivcsClient {
    http: Client,
    config: AivcsConfig,
}

impl HttpAivcsClient {
    pub fn new(config: AivcsConfig) -> Self {
        Self {
            http: Client::new(),
            config,
        }
    }
}

#[async_trait]
impl AivcsClient for HttpAivcsClient {
    async fn request_replay(&self, req: &ReplayRequest) -> Result<ReplayResponse, DfcError> {
        let url = format!("{}/v1/replay", self.config.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))?;

        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                "aivcs-api",
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))
    }

    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError> {
        let url = format!("{}/v1/rollback", self.config.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))?;

        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                "aivcs-api",
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))
    }

    async fn fetch_review_fragments(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<ReviewFragments, DfcError> {
        // H5: review_id is caller-controlled.
        let url = format!(
            "{}/v1/hitl/reviews/{}",
            self.config.base_url.trim_end_matches('/'),
            urlenc(review_id)
        );
        let resp = self
            .http
            .get(&url)
            .header("X-Tenant-Id", tenant_id)
            .send()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))?;

        if resp.status().as_u16() == 404 {
            return Err(DfcError::NotFound(format!("review/{review_id}")));
        }
        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                "aivcs-api",
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))
    }

    async fn submit_review_decision(
        &self,
        payload: &ReviewDecisionPayload,
    ) -> Result<ReviewDecisionResult, DfcError> {
        let url = format!(
            "{}/v1/hitl/reviews/{}/decision",
            self.config.base_url.trim_end_matches('/'),
            urlenc(&payload.review_id)
        );
        let resp = self
            .http
            .post(&url)
            .header("X-Tenant-Id", &payload.tenant_id)
            .json(payload)
            .send()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))?;

        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                "aivcs-api",
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream("aivcs-api", e.to_string(), None))
    }
}

/// In-memory stub for local development and tests (E1).
#[derive(Debug, Default)]
pub struct MockAivcsClient;

#[async_trait]
impl AivcsClient for MockAivcsClient {
    async fn request_replay(&self, req: &ReplayRequest) -> Result<ReplayResponse, DfcError> {
        let mut snapshot_ids = Vec::new();
        if let Some(ref from) = req.from_snapshot {
            snapshot_ids.push(from.clone());
        }
        let target = req
            .to_snapshot
            .clone()
            .or_else(|| req.target_snapshot_id.clone());
        if let Some(to) = target {
            if snapshot_ids.last() != Some(&to) {
                snapshot_ids.push(to);
            }
        }

        Ok(ReplayResponse {
            replay_id: format!("replay_{}", short_idempotency_key(&req.idempotency_key)),
            status: "accepted".into(),
            snapshot_ids,
            data_fabric_event_id: None,
            aivcs_operation_id: Some("aivcs_op_stub".into()),
        })
    }

    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError> {
        Ok(RollbackResponse {
            rollback_id: format!("rollback_{}", short_idempotency_key(&req.idempotency_key)),
            status: "accepted".into(),
            data_fabric_event_id: None,
            aivcs_operation_id: Some("aivcs_op_stub".into()),
        })
    }

    async fn fetch_review_fragments(
        &self,
        _tenant_id: &str,
        review_id: &str,
    ) -> Result<ReviewFragments, DfcError> {
        Ok(ReviewFragments {
            diff_ref: Some(format!("aivcs:diff:{review_id}")),
            evidence_graph_ref: Some(format!("data-fabric:graph:{review_id}")),
            policy_decision: Some(serde_json::json!({
                "status": "pending",
                "policy_set": "default"
            })),
            rollback_plan: Some(serde_json::json!({
                "strategy": "snapshot_rollback",
                "status": "ready"
            })),
        })
    }

    async fn submit_review_decision(
        &self,
        payload: &ReviewDecisionPayload,
    ) -> Result<ReviewDecisionResult, DfcError> {
        Ok(ReviewDecisionResult {
            operation_id: format!(
                "aivcs_op_{}",
                short_idempotency_key(&payload.idempotency_key)
            ),
        })
    }
}

/// Truncate `idempotency_key` to at most 8 *characters* (not bytes).
///
/// Slicing on byte indices (e.g. `&key[..8.min(key.len())]`) panics when the
/// byte index falls inside a multi-byte UTF-8 character. Callers cannot assume
/// idempotency keys are ASCII, so we iterate by `char` instead.
fn short_idempotency_key(key: &str) -> String {
    key.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_replay_accepts_multibyte_idempotency_key() {
        // C1 regression: byte-slice on `&key[..8]` panicked when byte 8 fell
        // inside a multi-byte UTF-8 char. Reachable by any caller in Mock
        // (default) mode.
        let mock = MockAivcsClient;
        let mut req = ReplayRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            run_id: "run-1".into(),
            task_id: None,
            from_snapshot: None,
            to_snapshot: None,
            target_snapshot_id: None,
            mode: None,
            idempotency_key: "a🚀🚀🚀b".into(),
        };
        // pre-fix this panics inside the async task with "byte index 8 is not
        // a char boundary"; post-fix it returns Ok and truncates by char.
        let resp = mock.request_replay(&req).await.expect("must not panic");
        assert_eq!(resp.replay_id, "replay_a🚀🚀🚀b");

        let rollback = RollbackRequest {
            tenant_id: "tenant-a".into(),
            branch_id: "branch-1".into(),
            target_snapshot_id: "snap-1".into(),
            reason: "rollback test".into(),
            idempotency_key: req.idempotency_key.clone(),
        };
        let resp = mock.request_rollback(&rollback).await.unwrap();
        assert_eq!(resp.rollback_id, "rollback_a🚀🚀🚀b");

        // C1: third site — submit_review_decision had the same byte-slice bug.
        let payload = ReviewDecisionPayload {
            tenant_id: "tenant-a".into(),
            review_id: "review-1".into(),
            decision: "approve".into(),
            comment: None,
            idempotency_key: req.idempotency_key.clone(),
            run_id: None,
            task_id: None,
        };
        let resp = mock
            .submit_review_decision(&payload)
            .await
            .expect("must not panic");
        assert_eq!(resp.operation_id, "aivcs_op_a🚀🚀🚀b");

        // Long-input truncation lands cleanly on the 8th char.
        req.idempotency_key = "🚀".repeat(20);
        let resp = mock.request_replay(&req).await.unwrap();
        assert_eq!(resp.replay_id.chars().filter(|c| *c == '🚀').count(), 8);
    }
}
