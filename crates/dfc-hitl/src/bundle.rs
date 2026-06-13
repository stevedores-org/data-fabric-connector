use dfc_core::{CorrelationId, DfcError};
use dfc_data_fabric::DataFabricClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlReviewBundle {
    pub schema_version: String,
    pub review_id: String,
    pub tenant_id: String,
    pub correlation_id: Option<CorrelationId>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub aivcs_snapshot_ref: Option<String>,
    pub branch_ref: Option<String>,
    pub diff_ref: Option<String>,
    pub validation_result: Option<serde_json::Value>,
    pub evidence_graph_ref: Option<String>,
    pub policy_decision: Option<serde_json::Value>,
    pub rollback_plan: Option<serde_json::Value>,
    pub revision: u64,
}

pub struct ReviewBundleAssembler<C: DataFabricClient> {
    data_fabric: Arc<C>,
}

impl<C: DataFabricClient> ReviewBundleAssembler<C> {
    pub fn new(data_fabric: Arc<C>) -> Self {
        Self { data_fabric }
    }

    pub async fn assemble(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<HitlReviewBundle, DfcError> {
        let correlation = self
            .data_fabric
            .get_correlation(tenant_id, "review", review_id)
            .await
            .ok();

        Ok(HitlReviewBundle {
            schema_version: dfc_core::SCHEMA_VERSION.into(),
            review_id: review_id.into(),
            tenant_id: tenant_id.into(),
            correlation_id: correlation.as_ref().map(|c| c.correlation_id.clone()),
            run_id: correlation.as_ref().and_then(|c| c.data_fabric_run_id.clone()),
            task_id: correlation
                .as_ref()
                .and_then(|c| c.data_fabric_task_id.clone()),
            aivcs_snapshot_ref: correlation
                .as_ref()
                .and_then(|c| c.aivcs_snapshot_id.clone())
                .map(|s| format!("aivcs:snapshot:{s}")),
            branch_ref: correlation
                .as_ref()
                .and_then(|c| c.aivcs_branch.clone()),
            diff_ref: None,
            validation_result: None,
            evidence_graph_ref: None,
            policy_decision: None,
            rollback_plan: Some(serde_json::json!({
                "strategy": "snapshot_rollback",
                "status": "stub"
            })),
            revision: 1,
        })
    }
}
