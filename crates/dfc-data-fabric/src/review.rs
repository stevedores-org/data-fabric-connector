use serde::{Deserialize, Serialize};

/// Validation and audit fragments sourced from data-fabric for a review bundle.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataFabricReviewFragment {
    #[serde(default)]
    pub validation_result: Option<serde_json::Value>,
}
