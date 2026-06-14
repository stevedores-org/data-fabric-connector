use serde::{Deserialize, Serialize};

/// Fragments assembled from aivcs-api for a HITL review bundle.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReviewFragments {
    #[serde(default)]
    pub diff_ref: Option<String>,
    #[serde(default)]
    pub evidence_graph_ref: Option<String>,
    #[serde(default)]
    pub policy_decision: Option<serde_json::Value>,
    #[serde(default)]
    pub rollback_plan: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecisionPayload {
    pub tenant_id: String,
    pub review_id: String,
    pub decision: String,
    #[serde(default)]
    pub comment: Option<String>,
    pub idempotency_key: String,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecisionResult {
    pub operation_id: String,
}
