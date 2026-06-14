use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    Approved,
    Rejected,
    RequestedChanges,
    Escalated,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReviewDecisionRequest {
    pub decision: ReviewDecision,
    pub reviewer: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecisionResponse {
    pub review_id: String,
    pub data_fabric_event_id: String,
    pub aivcs_operation_id: String,
}
