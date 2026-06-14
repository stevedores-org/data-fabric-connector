use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    DryRun,
    Execute,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplayRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub repo: Option<String>,
    pub run_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub from_snapshot: Option<String>,
    #[serde(default)]
    pub to_snapshot: Option<String>,
    #[serde(default)]
    pub target_snapshot_id: Option<String>,
    #[serde(default)]
    pub mode: Option<ReplayMode>,
    pub idempotency_key: String,
}

impl ReplayRequest {
    pub fn validate(&self) -> Result<(), crate::DfcError> {
        if self.tenant_id.trim().is_empty() {
            return Err(crate::DfcError::Validation("tenant_id is required".into()));
        }
        if self.run_id.trim().is_empty() {
            return Err(crate::DfcError::Validation("run_id is required".into()));
        }
        if self.idempotency_key.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "idempotency_key is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResponse {
    pub replay_id: String,
    pub status: String,
    #[serde(default)]
    pub snapshot_ids: Vec<String>,
    #[serde(default)]
    pub data_fabric_event_id: Option<String>,
    #[serde(default)]
    pub aivcs_operation_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RollbackRequest {
    pub tenant_id: String,
    pub branch_id: String,
    pub target_snapshot_id: String,
    pub reason: String,
    pub idempotency_key: String,
}

impl RollbackRequest {
    pub fn validate(&self) -> Result<(), crate::DfcError> {
        if self.tenant_id.trim().is_empty() {
            return Err(crate::DfcError::Validation("tenant_id is required".into()));
        }
        if self.branch_id.trim().is_empty() {
            return Err(crate::DfcError::Validation("branch_id is required".into()));
        }
        if self.target_snapshot_id.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "target_snapshot_id is required".into(),
            ));
        }
        if self.reason.trim().is_empty() {
            return Err(crate::DfcError::Validation("reason is required".into()));
        }
        if self.idempotency_key.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "idempotency_key is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResponse {
    pub rollback_id: String,
    pub status: String,
    #[serde(default)]
    pub data_fabric_event_id: Option<String>,
    #[serde(default)]
    pub aivcs_operation_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_request_validate_requires_run_id() {
        let req = ReplayRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            run_id: "   ".into(),
            task_id: None,
            from_snapshot: None,
            to_snapshot: None,
            target_snapshot_id: None,
            mode: None,
            idempotency_key: "key-1".into(),
        };
        assert!(req.validate().is_err());
    }

    #[test]
    fn rollback_request_validate_requires_reason() {
        let req = RollbackRequest {
            tenant_id: "tenant-a".into(),
            branch_id: "branch-1".into(),
            target_snapshot_id: "snap-1".into(),
            reason: "   ".into(),
            idempotency_key: "key-1".into(),
        };
        assert!(req.validate().is_err());
    }
}
