use async_trait::async_trait;
use dfc_core::{DfcError, ReplayRequest, ReplayResponse, RollbackRequest, RollbackResponse};
use reqwest::Client;
use serde::{Deserialize, Serialize};

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
        let url = format!(
            "{}/v1/hitl/reviews/{}",
            self.config.base_url.trim_end_matches('/'),
            review_id
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
            payload.review_id
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
            replay_id: format!(
                "replay_{}",
                &req.idempotency_key[..8.min(req.idempotency_key.len())]
            ),
            status: "accepted".into(),
            snapshot_ids,
            data_fabric_event_id: None,
            aivcs_operation_id: Some("aivcs_op_stub".into()),
        })
    }

    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError> {
        Ok(RollbackResponse {
            rollback_id: format!(
                "rollback_{}",
                &req.idempotency_key[..8.min(req.idempotency_key.len())]
            ),
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
                &payload.idempotency_key[..8.min(payload.idempotency_key.len())]
            ),
        })
    }
}
