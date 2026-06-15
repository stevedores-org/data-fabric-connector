use dfc_aivcs::{AivcsClient, ReviewFragments};
use dfc_core::{CorrelationId, DfcError};
use dfc_data_fabric::{DataFabricClient, DataFabricReviewFragment};
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

impl HitlReviewBundle {
    pub fn etag(&self) -> String {
        format!("\"rev-{}\"", self.revision)
    }
}

pub struct ReviewBundleAssembler {
    data_fabric: Arc<dyn DataFabricClient>,
    aivcs: Arc<dyn AivcsClient>,
}

impl ReviewBundleAssembler {
    pub fn new(data_fabric: Arc<dyn DataFabricClient>, aivcs: Arc<dyn AivcsClient>) -> Self {
        Self { data_fabric, aivcs }
    }

    pub async fn assemble(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<HitlReviewBundle, DfcError> {
        let correlation_fut = self
            .data_fabric
            .get_correlation(tenant_id, "review", review_id);
        let aivcs_fut = self.aivcs.fetch_review_fragments(tenant_id, review_id);
        let fabric_fut = self.data_fabric.fetch_review_fragment(tenant_id, review_id);
        let revision_fut = self.data_fabric.review_revision(tenant_id, review_id);

        let (correlation_res, aivcs_res, fabric_res, revision_res) =
            tokio::join!(correlation_fut, aivcs_fut, fabric_fut, revision_fut);

        let correlation = match correlation_res {
            Ok(record) => record,
            Err(DfcError::NotFound(_)) => {
                return Err(DfcError::NotFound(format!("review/{review_id}")));
            }
            Err(err) => return Err(err),
        };

        let fragments = aivcs_res?;
        let fabric_fragment = fabric_res?;
        let revision = revision_res?;

        Ok(build_bundle(
            tenant_id,
            review_id,
            Some(correlation),
            fragments,
            fabric_fragment,
            revision,
        ))
    }
}

fn build_bundle(
    tenant_id: &str,
    review_id: &str,
    correlation: Option<dfc_core::CorrelationRecord>,
    fragments: ReviewFragments,
    fabric_fragment: DataFabricReviewFragment,
    revision: u64,
) -> HitlReviewBundle {
    HitlReviewBundle {
        schema_version: dfc_core::SCHEMA_VERSION.into(),
        review_id: review_id.into(),
        tenant_id: tenant_id.into(),
        correlation_id: correlation.as_ref().map(|c| c.correlation_id.clone()),
        run_id: correlation
            .as_ref()
            .and_then(|c| c.data_fabric_run_id.clone()),
        task_id: correlation
            .as_ref()
            .and_then(|c| c.data_fabric_task_id.clone()),
        aivcs_snapshot_ref: correlation
            .as_ref()
            .and_then(|c| c.aivcs_snapshot_id.clone())
            .map(|s| format!("aivcs:snapshot:{s}")),
        branch_ref: correlation.as_ref().and_then(|c| c.aivcs_branch.clone()),
        diff_ref: fragments.diff_ref,
        validation_result: fabric_fragment.validation_result,
        evidence_graph_ref: fragments.evidence_graph_ref,
        policy_decision: fragments.policy_decision,
        rollback_plan: fragments.rollback_plan,
        revision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfc_aivcs::MockAivcsClient;
    use dfc_data_fabric::MockDataFabricClient;

    #[tokio::test]
    async fn assemble_returns_404_when_review_unknown() {
        let assembler = ReviewBundleAssembler::new(
            Arc::new(MockDataFabricClient::default()) as Arc<dyn DataFabricClient>,
            Arc::new(MockAivcsClient) as Arc<dyn AivcsClient>,
        );
        let err = assembler
            .assemble("tenant-a", "missing-review")
            .await
            .unwrap_err();
        assert!(matches!(err, DfcError::NotFound(_)));
    }
}
