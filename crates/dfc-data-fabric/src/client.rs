use async_trait::async_trait;
use dfc_core::{CorrelationRecord, DfcError, DfcEvent, DfcMetrics};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::DataFabricConfig;
use crate::http::{send_allowing_status, send_with_retry};
use crate::lineage::SnapshotLineage;
use crate::retry::RetryPolicy;
use crate::review::DataFabricReviewFragment;

const UPSTREAM: &str = "data-fabric";

#[async_trait]
pub trait DataFabricClient: Send + Sync {
    async fn ingest_event(&self, event: &DfcEvent) -> Result<DfcEvent, DfcError>;
    async fn get_event_by_idempotency(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DfcEvent>, DfcError>;
    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError>;
    async fn get_correlation(
        &self,
        tenant_id: &str,
        kind: &str,
        id: &str,
    ) -> Result<CorrelationRecord, DfcError>;
    async fn review_revision(&self, tenant_id: &str, review_id: &str) -> Result<u64, DfcError>;
    async fn bump_review_revision(&self, tenant_id: &str, review_id: &str)
        -> Result<u64, DfcError>;
    async fn fetch_review_fragment(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<DataFabricReviewFragment, DfcError>;
    async fn resolve_snapshot_lineage(
        &self,
        tenant_id: &str,
        run_id: &str,
        task_id: Option<&str>,
        target_snapshot_id: Option<&str>,
    ) -> Result<SnapshotLineage, DfcError>;
}

pub struct HttpDataFabricClient {
    http: Client,
    config: DataFabricConfig,
    retry_policy: RetryPolicy,
    metrics: Option<Arc<DfcMetrics>>,
}

impl HttpDataFabricClient {
    pub fn new(config: DataFabricConfig) -> Self {
        Self {
            http: Client::new(),
            config,
            retry_policy: RetryPolicy::from_env(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<DfcMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

#[async_trait]
impl DataFabricClient for HttpDataFabricClient {
    async fn ingest_event(&self, event: &DfcEvent) -> Result<DfcEvent, DfcError> {
        let url = format!("{}/v1/events", self.config.base_url.trim_end_matches('/'));
        let resp = send_with_retry(
            &self.http,
            || {
                self.http
                    .post(&url)
                    .header("X-Tenant-Id", &self.config.tenant_id)
                    .json(event)
            },
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        resp.json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))
    }

    async fn get_event_by_idempotency(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DfcEvent>, DfcError> {
        let url = format!(
            "{}/v1/events/by-idempotency/{}",
            self.config.base_url.trim_end_matches('/'),
            idempotency_key
        );
        let resp = send_allowing_status(
            &self.http,
            || self.http.get(&url).header("X-Tenant-Id", tenant_id),
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                UPSTREAM,
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))
            .map(Some)
    }

    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError> {
        let url = format!(
            "{}/v1/correlations",
            self.config.base_url.trim_end_matches('/')
        );
        let resp = send_with_retry(
            &self.http,
            || {
                self.http
                    .post(&url)
                    .header("X-Tenant-Id", &record.tenant_id)
                    .json(record)
            },
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;
        let _ = resp;
        Ok(())
    }

    async fn get_correlation(
        &self,
        tenant_id: &str,
        kind: &str,
        id: &str,
    ) -> Result<CorrelationRecord, DfcError> {
        let url = format!(
            "{}/v1/correlations/{}/{}",
            self.config.base_url.trim_end_matches('/'),
            kind,
            id
        );
        let resp = send_allowing_status(
            &self.http,
            || self.http.get(&url).header("X-Tenant-Id", tenant_id),
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        if resp.status().as_u16() == 404 {
            return Err(DfcError::NotFound(format!("{kind}/{id}")));
        }
        if !resp.status().is_success() {
            return Err(DfcError::upstream(
                UPSTREAM,
                format!("status {}", resp.status()),
                Some(resp.status().as_u16()),
            ));
        }

        resp.json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))
    }

    async fn review_revision(&self, tenant_id: &str, review_id: &str) -> Result<u64, DfcError> {
        let url = format!(
            "{}/v1/hitl/reviews/{}/revision",
            self.config.base_url.trim_end_matches('/'),
            review_id
        );
        let resp = send_with_retry(
            &self.http,
            || self.http.get(&url).header("X-Tenant-Id", tenant_id),
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        #[derive(Deserialize)]
        struct RevisionBody {
            revision: u64,
        }
        let body: RevisionBody = resp
            .json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))?;
        Ok(body.revision)
    }

    async fn bump_review_revision(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<u64, DfcError> {
        let url = format!(
            "{}/v1/hitl/reviews/{}/revision",
            self.config.base_url.trim_end_matches('/'),
            review_id
        );
        let resp = send_with_retry(
            &self.http,
            || self.http.post(&url).header("X-Tenant-Id", tenant_id),
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        #[derive(Deserialize)]
        struct RevisionBody {
            revision: u64,
        }
        let body: RevisionBody = resp
            .json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))?;
        Ok(body.revision)
    }

    async fn fetch_review_fragment(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<DataFabricReviewFragment, DfcError> {
        let url = format!(
            "{}/v1/hitl/reviews/{}/fragment",
            self.config.base_url.trim_end_matches('/'),
            review_id
        );
        let resp = send_with_retry(
            &self.http,
            || self.http.get(&url).header("X-Tenant-Id", tenant_id),
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        resp.json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))
    }

    async fn resolve_snapshot_lineage(
        &self,
        tenant_id: &str,
        run_id: &str,
        task_id: Option<&str>,
        target_snapshot_id: Option<&str>,
    ) -> Result<SnapshotLineage, DfcError> {
        let url = format!(
            "{}/v1/runs/{}/snapshot-lineage",
            self.config.base_url.trim_end_matches('/'),
            run_id
        );
        let task = task_id.map(str::to_string);
        let target = target_snapshot_id.map(str::to_string);
        let resp = send_with_retry(
            &self.http,
            || {
                let mut req = self.http.get(&url).header("X-Tenant-Id", tenant_id);
                if let Some(task_id) = task.as_deref() {
                    req = req.query(&[("task_id", task_id)]);
                }
                if let Some(target_snapshot_id) = target.as_deref() {
                    req = req.query(&[("target_snapshot_id", target_snapshot_id)]);
                }
                req
            },
            UPSTREAM,
            &self.retry_policy,
            self.metrics.as_deref(),
        )
        .await?;

        resp.json()
            .await
            .map_err(|e| DfcError::upstream(UPSTREAM, e.to_string(), None))
    }
}

type CorrelationKey = (String, String, String);
type ReviewKey = (String, String);

#[derive(Debug, Default)]
pub struct MockDataFabricClient {
    correlations: Arc<RwLock<HashMap<CorrelationKey, CorrelationRecord>>>,
    events: Arc<RwLock<HashMap<String, DfcEvent>>>,
    review_revisions: Arc<RwLock<HashMap<ReviewKey, u64>>>,
}

impl MockDataFabricClient {
    pub async fn event_count(&self) -> usize {
        self.events.read().await.len()
    }
}

#[async_trait]
impl DataFabricClient for MockDataFabricClient {
    async fn ingest_event(&self, event: &DfcEvent) -> Result<DfcEvent, DfcError> {
        event.validate()?;
        let key = format!("{}:{}", event.tenant_id, event.idempotency_key);
        let mut events = self.events.write().await;
        if let Some(existing) = events.get(&key) {
            return Ok(existing.clone());
        }
        let mut stored = event.clone();
        stored.data_fabric_event_id = Some(stored.event_id.0.clone());
        events.insert(key, stored.clone());
        Ok(stored)
    }

    async fn get_event_by_idempotency(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<DfcEvent>, DfcError> {
        let key = format!("{tenant_id}:{idempotency_key}");
        let events = self.events.read().await;
        Ok(events.get(&key).cloned())
    }

    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError> {
        let mut correlations = self.correlations.write().await;
        let keys = correlation_lookup_keys(record);

        for (kind, id) in &keys {
            for ((existing_tenant, existing_kind, existing_id), _) in correlations.iter() {
                if existing_kind == kind
                    && existing_id == id
                    && existing_tenant != &record.tenant_id
                {
                    return Err(DfcError::Conflict(format!(
                        "tenant mismatch: ID {id} of kind {kind} is already mapped to tenant {existing_tenant}"
                    )));
                }
            }
        }

        for (kind, id) in keys {
            let key = (record.tenant_id.clone(), kind, id);
            correlations.insert(key, record.clone());
        }
        Ok(())
    }

    async fn get_correlation(
        &self,
        tenant_id: &str,
        kind: &str,
        id: &str,
    ) -> Result<CorrelationRecord, DfcError> {
        let correlations = self.correlations.read().await;
        let key = (tenant_id.to_string(), kind.to_string(), id.to_string());
        if let Some(rec) = correlations.get(&key) {
            return Ok(rec.clone());
        }
        // If the key is not present for the requested tenant, check whether the
        // ID exists under a different tenant. If so, surface a TenantMismatch
        // error to make cross-tenant lookups explicit (403) and allow auditing.
        for ((existing_tenant, existing_kind, existing_id), _) in correlations.iter() {
            if existing_kind == kind && existing_id == id {
                return Err(DfcError::TenantMismatch {
                    expected: tenant_id.to_string(),
                    actual: existing_tenant.clone(),
                });
            }
        }
        Err(DfcError::NotFound(format!("{kind}/{id}")))
    }

    async fn review_revision(&self, tenant_id: &str, review_id: &str) -> Result<u64, DfcError> {
        let revisions = self.review_revisions.read().await;
        Ok(revisions
            .get(&(tenant_id.to_string(), review_id.to_string()))
            .copied()
            .unwrap_or(1))
    }

    async fn bump_review_revision(
        &self,
        tenant_id: &str,
        review_id: &str,
    ) -> Result<u64, DfcError> {
        let mut revisions = self.review_revisions.write().await;
        let key = (tenant_id.to_string(), review_id.to_string());
        let next = revisions.get(&key).copied().unwrap_or(1) + 1;
        revisions.insert(key, next);
        Ok(next)
    }

    async fn fetch_review_fragment(
        &self,
        _tenant_id: &str,
        review_id: &str,
    ) -> Result<DataFabricReviewFragment, DfcError> {
        Ok(DataFabricReviewFragment {
            validation_result: Some(serde_json::json!({
                "status": "passed",
                "review_id": review_id,
                "checks": ["lint", "policy"]
            })),
        })
    }

    async fn resolve_snapshot_lineage(
        &self,
        tenant_id: &str,
        run_id: &str,
        _task_id: Option<&str>,
        target_snapshot_id: Option<&str>,
    ) -> Result<SnapshotLineage, DfcError> {
        let correlation = self.get_correlation(tenant_id, "run", run_id).await.ok();
        let from_snapshot = correlation
            .as_ref()
            .and_then(|record| record.aivcs_snapshot_id.clone());
        let to_snapshot = target_snapshot_id
            .map(str::to_string)
            .or_else(|| from_snapshot.clone());

        let mut snapshot_ids = Vec::new();
        if let Some(ref from) = from_snapshot {
            snapshot_ids.push(from.clone());
        }
        if let Some(ref to) = to_snapshot {
            if snapshot_ids.last() != Some(to) {
                snapshot_ids.push(to.clone());
            }
        }
        if snapshot_ids.is_empty() {
            snapshot_ids.push(format!("snap_{run_id}"));
            if let Some(target) = target_snapshot_id {
                let target = target.to_string();
                if snapshot_ids.last() != Some(&target) {
                    snapshot_ids.push(target);
                }
            }
        }

        Ok(SnapshotLineage {
            correlation_id: correlation.map(|record| record.correlation_id.clone()),
            snapshot_ids,
            from_snapshot,
            to_snapshot,
        })
    }
}

fn correlation_lookup_keys(record: &CorrelationRecord) -> Vec<(String, String)> {
    let mut keys = Vec::new();

    // 1. Index kind and source_id / target_id if present
    if let Some(kind) = &record.kind {
        let kind_str = kind.as_str().to_string();
        if let Some(source_id) = &record.source_id {
            if !source_id.trim().is_empty() {
                keys.push((kind_str.clone(), source_id.clone()));
            }
        }
        if let Some(target_id) = &record.target_id {
            if !target_id.trim().is_empty() {
                keys.push((kind_str, target_id.clone()));
            }
        }
    }

    // 2. Index flat mapping fields
    if let Some(run_id) = &record.data_fabric_run_id {
        if !run_id.trim().is_empty() {
            keys.push(("run".into(), run_id.clone()));
        }
    }
    if let Some(task_id) = &record.data_fabric_task_id {
        if !task_id.trim().is_empty() {
            keys.push(("task".into(), task_id.clone()));
        }
    }
    if let Some(snapshot_id) = &record.aivcs_snapshot_id {
        if !snapshot_id.trim().is_empty() {
            keys.push(("snapshot".into(), snapshot_id.clone()));
        }
    }
    if let Some(branch) = &record.aivcs_branch {
        if !branch.trim().is_empty() {
            keys.push(("branch".into(), branch.clone()));
        }
    }

    // 3. Index links metadata fields
    if let Some(review_id) = record.links.get("review_id").and_then(|v| v.as_str()) {
        if !review_id.trim().is_empty() {
            keys.push(("review".into(), review_id.to_string()));
        }
    }
    if let Some(session_id) = record
        .links
        .get("session_id")
        .or_else(|| record.links.get("session"))
        .and_then(|v| v.as_str())
    {
        if !session_id.trim().is_empty() {
            keys.push(("session".into(), session_id.to_string()));
        }
    }
    if let Some(pr_id) = record
        .links
        .get("pr_id")
        .or_else(|| record.links.get("pr"))
        .and_then(|v| v.as_str())
    {
        if !pr_id.trim().is_empty() {
            keys.push(("pr".into(), pr_id.to_string()));
        }
    }

    keys.sort();
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_correlation_returns_tenant_mismatch_if_present_elsewhere() {
        let client = MockDataFabricClient::default();

        let req = dfc_core::CorrelateRequest {
            tenant_id: "tenant-b".into(),
            repo: None,
            kind: None,
            source_system: None,
            source_id: None,
            target_system: None,
            target_id: None,
            data_fabric_run_id: Some("run-123".into()),
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({}),
        };
        let record = dfc_core::CorrelationRecord::from(req);
        // store under tenant-b
        client.store_correlation(&record).await.unwrap();

        // lookup as tenant-a should return TenantMismatch
        let res = client.get_correlation("tenant-a", "run", "run-123").await;
        match res {
            Err(dfc_core::DfcError::TenantMismatch {
                expected: _,
                actual,
            }) => {
                assert_eq!(actual, "tenant-b");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }
}
