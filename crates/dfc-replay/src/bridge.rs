use dfc_aivcs::AivcsClient;
use dfc_core::{
    DfcError, ReplayRequest, ReplayResponse, RollbackRequest, RollbackResponse, SourceSystem,
};
use dfc_data_fabric::DataFabricClient;
use serde_json::json;
use std::sync::Arc;

use crate::audit::{
    replay_response_from_event, rollback_response_from_event, terminal_event, AuditContext,
};

pub struct ReplayBridge<C: DataFabricClient, A: AivcsClient> {
    data_fabric: Arc<C>,
    aivcs: Arc<A>,
}

impl<C: DataFabricClient, A: AivcsClient> ReplayBridge<C, A> {
    pub fn new(data_fabric: Arc<C>, aivcs: Arc<A>) -> Self {
        Self { data_fabric, aivcs }
    }

    pub async fn handle_replay(
        &self,
        audit: AuditContext,
        req: ReplayRequest,
    ) -> Result<ReplayResponse, DfcError> {
        req.validate()?;

        let completed_key = format!("{}:completed", req.idempotency_key);
        if let Some(stored) = self
            .data_fabric
            .get_event_by_idempotency(&req.tenant_id, &completed_key)
            .await?
        {
            return replay_response_from_event(&stored);
        }

        let lineage = self
            .data_fabric
            .resolve_snapshot_lineage(
                &req.tenant_id,
                &req.run_id,
                req.task_id.as_deref(),
                req.target_snapshot_id.as_deref(),
            )
            .await?;

        let audit = AuditContext::new(
            audit.actor,
            audit.correlation_id.or(lineage.correlation_id.clone()),
        );

        let mut requested = terminal_event(
            "aivcs.replay.requested",
            &req.tenant_id,
            &format!("{}:requested", req.idempotency_key),
            SourceSystem::AivcsApi,
            &audit,
            json!({
                "run_id": req.run_id,
                "task_id": req.task_id,
                "target_snapshot_id": req.target_snapshot_id,
                "snapshot_ids": lineage.snapshot_ids,
            }),
        );
        requested.run_id = Some(req.run_id.clone());
        requested.task_id = req.task_id.clone();
        self.data_fabric.ingest_event(&requested).await?;

        let mut aivcs_req = req.clone();
        if aivcs_req.from_snapshot.is_none() {
            aivcs_req.from_snapshot = lineage.from_snapshot.clone();
        }
        if aivcs_req.to_snapshot.is_none() {
            aivcs_req.to_snapshot = lineage.to_snapshot.clone();
        }
        if aivcs_req.target_snapshot_id.is_none() {
            aivcs_req.target_snapshot_id = lineage.to_snapshot.clone();
        }

        match self.aivcs.request_replay(&aivcs_req).await {
            Ok(mut response) => {
                if response.snapshot_ids.is_empty() {
                    response.snapshot_ids = lineage.snapshot_ids.clone();
                }

                let mut completed = terminal_event(
                    "aivcs.replay.completed",
                    &aivcs_req.tenant_id,
                    &completed_key,
                    SourceSystem::AivcsApi,
                    &audit,
                    json!({
                        "replay_id": response.replay_id,
                        "status": response.status,
                        "snapshot_ids": response.snapshot_ids,
                        "aivcs_operation_id": response.aivcs_operation_id,
                    }),
                );
                completed.run_id = Some(aivcs_req.run_id);
                completed.task_id = aivcs_req.task_id;
                let stored = self.data_fabric.ingest_event(&completed).await?;
                response.data_fabric_event_id = stored.data_fabric_event_id.clone();
                Ok(response)
            }
            Err(err) => {
                let reason = err.to_string();
                let mut failed = terminal_event(
                    "aivcs.replay.failed",
                    &aivcs_req.tenant_id,
                    &format!("{}:failed", aivcs_req.idempotency_key),
                    SourceSystem::AivcsApi,
                    &audit,
                    json!({
                        "run_id": aivcs_req.run_id,
                        "task_id": aivcs_req.task_id,
                        "reason": reason,
                        "dlq": {
                            "queued": true,
                            "reason": reason,
                            "epic": "E6"
                        }
                    }),
                );
                failed.run_id = Some(aivcs_req.run_id);
                failed.task_id = aivcs_req.task_id;
                let _ = self.data_fabric.ingest_event(&failed).await;
                Err(err)
            }
        }
    }

    pub async fn handle_rollback(
        &self,
        audit: AuditContext,
        req: RollbackRequest,
    ) -> Result<RollbackResponse, DfcError> {
        req.validate()?;

        let completed_key = format!("{}:completed", req.idempotency_key);
        if let Some(stored) = self
            .data_fabric
            .get_event_by_idempotency(&req.tenant_id, &completed_key)
            .await?
        {
            return rollback_response_from_event(&stored);
        }

        let correlation = self
            .data_fabric
            .get_correlation(&req.tenant_id, "branch", &req.branch_id)
            .await
            .ok();
        let audit = AuditContext::new(
            audit.actor,
            audit
                .correlation_id
                .or_else(|| correlation.as_ref().map(|c| c.correlation_id.clone())),
        );

        let requested = terminal_event(
            "aivcs.rollback.requested",
            &req.tenant_id,
            &format!("{}:requested", req.idempotency_key),
            SourceSystem::AivcsApi,
            &audit,
            json!({
                "branch_id": req.branch_id,
                "target_snapshot_id": req.target_snapshot_id,
                "reason": req.reason,
            }),
        );
        self.data_fabric.ingest_event(&requested).await?;

        match self.aivcs.request_rollback(&req).await {
            Ok(mut response) => {
                let completed = terminal_event(
                    "aivcs.rollback.completed",
                    &req.tenant_id,
                    &completed_key,
                    SourceSystem::AivcsApi,
                    &audit,
                    json!({
                        "rollback_id": response.rollback_id,
                        "status": response.status,
                        "branch_id": req.branch_id,
                        "target_snapshot_id": req.target_snapshot_id,
                        "aivcs_operation_id": response.aivcs_operation_id,
                    }),
                );
                let stored = self.data_fabric.ingest_event(&completed).await?;
                response.data_fabric_event_id = stored.data_fabric_event_id.clone();
                Ok(response)
            }
            Err(err) => {
                let reason = err.to_string();
                let failed = terminal_event(
                    "aivcs.rollback.failed",
                    &req.tenant_id,
                    &format!("{}:failed", req.idempotency_key),
                    SourceSystem::AivcsApi,
                    &audit,
                    json!({
                        "branch_id": req.branch_id,
                        "target_snapshot_id": req.target_snapshot_id,
                        "reason": reason,
                        "dlq": {
                            "queued": true,
                            "reason": reason,
                            "epic": "E6"
                        }
                    }),
                );
                let _ = self.data_fabric.ingest_event(&failed).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfc_aivcs::MockAivcsClient;
    use dfc_data_fabric::MockDataFabricClient;

    #[tokio::test]
    async fn replay_resolves_lineage_and_returns_snapshot_chain() {
        let data_fabric = Arc::new(MockDataFabricClient::default());
        data_fabric
            .store_correlation(&dfc_core::CorrelationRecord {
                correlation_id: dfc_core::CorrelationId("corr_run".into()),
                tenant_id: "tenant-a".into(),
                repo: None,
                kind: None,
                source_system: None,
                source_id: None,
                target_system: None,
                target_id: None,
                data_fabric_run_id: Some("run-1".into()),
                data_fabric_task_id: None,
                aivcs_snapshot_id: Some("snap_base".into()),
                aivcs_branch: None,
                links: Default::default(),
            })
            .await
            .unwrap();

        let bridge = ReplayBridge::new(data_fabric.clone(), Arc::new(MockAivcsClient));
        let response = bridge
            .handle_replay(
                AuditContext::new("operator-1", None),
                ReplayRequest {
                    tenant_id: "tenant-a".into(),
                    repo: None,
                    run_id: "run-1".into(),
                    task_id: None,
                    from_snapshot: None,
                    to_snapshot: None,
                    target_snapshot_id: Some("snap_target".into()),
                    mode: None,
                    idempotency_key: "replay-key-1".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(response.status, "accepted");
        assert_eq!(response.snapshot_ids, vec!["snap_base", "snap_target"]);
    }
}
