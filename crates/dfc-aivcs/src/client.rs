use async_trait::async_trait;
use dfc_core::{DfcError, ReplayRequest, ReplayResponse, RollbackRequest, RollbackResponse};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::config::AivcsConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOperation {
    pub operation_id: String,
    pub status: String,
}

#[async_trait]
pub trait AivcsClient: Send + Sync {
    async fn request_replay(&self, req: &ReplayRequest) -> Result<ReplayResponse, DfcError>;
    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError>;
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
            .map_err(|e| DfcError::Upstream {
                system: "aivcs-api".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(DfcError::Upstream {
                system: "aivcs-api".into(),
                message: format!("status {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| DfcError::Upstream {
            system: "aivcs-api".into(),
            message: e.to_string(),
        })
    }

    async fn request_rollback(&self, req: &RollbackRequest) -> Result<RollbackResponse, DfcError> {
        let url = format!("{}/v1/rollback", self.config.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| DfcError::Upstream {
                system: "aivcs-api".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(DfcError::Upstream {
                system: "aivcs-api".into(),
                message: format!("status {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| DfcError::Upstream {
            system: "aivcs-api".into(),
            message: e.to_string(),
        })
    }
}

/// In-memory stub for local development and tests (E1).
#[derive(Debug, Default)]
pub struct MockAivcsClient;

#[async_trait]
impl AivcsClient for MockAivcsClient {
    async fn request_replay(&self, req: &ReplayRequest) -> Result<ReplayResponse, DfcError> {
        Ok(ReplayResponse {
            replay_id: format!(
                "replay_{}",
                &req.idempotency_key[..8.min(req.idempotency_key.len())]
            ),
            status: "accepted".into(),
            snapshot_ids: req
                .from_snapshot
                .clone()
                .into_iter()
                .chain(req.to_snapshot.clone())
                .collect(),
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
}
