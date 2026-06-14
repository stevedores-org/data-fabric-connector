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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResponse {
    pub rollback_id: String,
    pub status: String,
    #[serde(default)]
    pub data_fabric_event_id: Option<String>,
    #[serde(default)]
    pub aivcs_operation_id: Option<String>,
}
