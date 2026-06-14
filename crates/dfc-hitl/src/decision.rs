use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    Approved,
    Rejected,
    RequestedChanges,
    Escalated,
}

impl ReviewDecision {
    pub fn data_fabric_event_type(self) -> &'static str {
        match self {
            Self::Approved => "human.approved",
            Self::RequestedChanges => "human.requested_changes",
            Self::Rejected => "human.rejected",
            Self::Escalated => "human.escalated",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::RequestedChanges => "requested_changes",
            Self::Escalated => "escalated",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReviewDecisionRequest {
    pub decision: ReviewDecision,
    #[serde(default, alias = "reason")]
    pub comment: Option<String>,
    #[serde(default)]
    pub reviewer: Option<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecisionResponse {
    pub review_id: String,
    pub data_fabric_event_id: String,
    pub aivcs_operation_id: String,
}
