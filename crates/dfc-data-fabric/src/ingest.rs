use dfc_core::{
    aivcs_pending_correlations, aivcs_ref_snapshot_id, hitl_pending_correlations,
    snapshot_id_from_event, validate_aivcs_event_type, validate_hitl_event_type, CorrelationRecord,
    DfcError, DfcEvent, InboundAivcsEvent, InboundHitlEvent, PendingCorrelation, SourceSystem,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::DataFabricClient;

const DEFAULT_STAGING_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, PartialEq)]
pub enum IngestOutcome {
    Ingested(Box<DfcEvent>),
    Staged {
        event_id: dfc_core::EventId,
        pending: Vec<PendingCorrelation>,
    },
}

#[derive(Debug, Clone)]
struct StagedRecord {
    event: DfcEvent,
    pending: Vec<PendingCorrelation>,
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct StagingStore {
    by_idempotency: HashMap<String, StagedRecord>,
    by_pending: HashMap<(String, String, String), HashSet<String>>,
}

impl StagingStore {
    fn idempotency_key(tenant_id: &str, key: &str) -> String {
        format!("{tenant_id}:{key}")
    }

    fn purge_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .by_idempotency
            .iter()
            .filter(|(_, record)| record.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            self.remove_staged(&key);
        }
    }

    fn remove_staged(&mut self, idempotency_key: &str) {
        if let Some(record) = self.by_idempotency.remove(idempotency_key) {
            for pending in record.pending {
                let index_key = (record.event.tenant_id.clone(), pending.kind, pending.id);
                if let Some(keys) = self.by_pending.get_mut(&index_key) {
                    keys.remove(idempotency_key);
                    if keys.is_empty() {
                        self.by_pending.remove(&index_key);
                    }
                }
            }
        }
    }

    fn stage(
        &mut self,
        event: DfcEvent,
        pending: Vec<PendingCorrelation>,
        ttl: Duration,
    ) -> IngestOutcome {
        let idempotency_key = Self::idempotency_key(&event.tenant_id, &event.idempotency_key);
        for item in &pending {
            self.by_pending
                .entry((event.tenant_id.clone(), item.kind.clone(), item.id.clone()))
                .or_default()
                .insert(idempotency_key.clone());
        }
        let event_id = event.event_id.clone();
        self.by_idempotency.insert(
            idempotency_key,
            StagedRecord {
                pending: pending.clone(),
                event,
                expires_at: Instant::now() + ttl,
            },
        );
        IngestOutcome::Staged { event_id, pending }
    }

    fn get(&self, tenant_id: &str, idempotency_key: &str) -> Option<IngestOutcome> {
        let key = Self::idempotency_key(tenant_id, idempotency_key);
        self.by_idempotency
            .get(&key)
            .map(|record| IngestOutcome::Staged {
                event_id: record.event.event_id.clone(),
                pending: record.pending.clone(),
            })
    }

    fn keys_for(&self, tenant_id: &str, kind: &str, id: &str) -> Vec<String> {
        self.by_pending
            .get(&(tenant_id.to_string(), kind.to_string(), id.to_string()))
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn take(&mut self, idempotency_key: &str) -> Option<StagedRecord> {
        let record = self.by_idempotency.remove(idempotency_key)?;
        for pending in &record.pending {
            let index_key = (
                record.event.tenant_id.clone(),
                pending.kind.clone(),
                pending.id.clone(),
            );
            if let Some(keys) = self.by_pending.get_mut(&index_key) {
                keys.remove(idempotency_key);
                if keys.is_empty() {
                    self.by_pending.remove(&index_key);
                }
            }
        }
        Some(record)
    }
}

pub struct EventIngestService<C: DataFabricClient + ?Sized> {
    client: Arc<C>,
    staging: Arc<RwLock<StagingStore>>,
    staging_ttl: Duration,
}

impl<C: DataFabricClient + ?Sized> EventIngestService<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            staging: Arc::new(RwLock::new(StagingStore::default())),
            staging_ttl: DEFAULT_STAGING_TTL,
        }
    }

    pub async fn ingest_aivcs(
        &self,
        inbound: InboundAivcsEvent,
    ) -> Result<IngestOutcome, DfcError> {
        validate_aivcs_event_type(&inbound.event_type)?;
        if let Some(existing) = self
            .idempotency_lookup(&inbound.tenant_id, &inbound.idempotency_key)
            .await?
        {
            return Ok(existing);
        }

        let pending = aivcs_pending_correlations(&inbound);
        let event = inbound.into_dfc_event();
        event.validate()?;

        if pending.is_empty() {
            return self.forward_aivcs(event).await;
        }

        let unresolved = self.unresolved(&event.tenant_id, &pending).await?;
        if unresolved.is_empty() {
            return self.forward_aivcs(event).await;
        }

        let mut staging = self.staging.write().await;
        staging.purge_expired();
        Ok(staging.stage(event, unresolved, self.staging_ttl))
    }

    pub async fn ingest_hitl(&self, inbound: InboundHitlEvent) -> Result<IngestOutcome, DfcError> {
        validate_hitl_event_type(&inbound.event_type)?;
        if let Some(existing) = self
            .idempotency_lookup(&inbound.tenant_id, &inbound.idempotency_key)
            .await?
        {
            return Ok(existing);
        }

        let pending = hitl_pending_correlations(&inbound);
        let mut event = inbound.into_dfc_event();
        self.enrich_hitl_from_correlation(&mut event).await?;
        event.validate()?;

        if event.run_id.is_some() || pending.is_empty() {
            let stored = self.client.ingest_event(&event).await?;
            return Ok(IngestOutcome::Ingested(Box::new(stored)));
        }

        let unresolved = self.unresolved(&event.tenant_id, &pending).await?;
        if unresolved.is_empty() {
            self.enrich_hitl_from_correlation(&mut event).await?;
            let stored = self.client.ingest_event(&event).await?;
            return Ok(IngestOutcome::Ingested(Box::new(stored)));
        }

        let mut staging = self.staging.write().await;
        staging.purge_expired();
        Ok(staging.stage(event, unresolved, self.staging_ttl))
    }

    pub async fn reconcile_correlation(
        &self,
        tenant_id: &str,
        kind: &str,
        id: &str,
    ) -> Result<Vec<DfcEvent>, DfcError> {
        let keys = {
            let staging = self.staging.read().await;
            staging.keys_for(tenant_id, kind, id)
        };

        let mut ingested = Vec::new();
        for key in keys {
            let record = {
                let mut staging = self.staging.write().await;
                staging.take(&key)
            };
            let Some(record) = record else {
                continue;
            };

            let unresolved = self
                .unresolved(&record.event.tenant_id, &record.pending)
                .await?;
            if !unresolved.is_empty() {
                let mut staging = self.staging.write().await;
                staging.stage(record.event, unresolved, self.staging_ttl);
                continue;
            }

            let outcome = if record.event.source_system == SourceSystem::AivcsApi {
                let stored = self.ingest_aivcs_event(record.event).await?;
                IngestOutcome::Ingested(Box::new(stored))
            } else {
                let mut event = record.event;
                self.enrich_hitl_from_correlation(&mut event).await?;
                let stored = self.client.ingest_event(&event).await?;
                IngestOutcome::Ingested(Box::new(stored))
            };

            if let IngestOutcome::Ingested(stored) = outcome {
                ingested.push(*stored);
            }
        }
        Ok(ingested)
    }

    async fn ingest_aivcs_event(&self, event: DfcEvent) -> Result<DfcEvent, DfcError> {
        if event.event_type == "aivcs.snapshot.created" {
            self.register_snapshot_correlation(&event).await?;
        }
        self.client.ingest_event(&event).await
    }

    async fn forward_aivcs(&self, event: DfcEvent) -> Result<IngestOutcome, DfcError> {
        let stored = self.ingest_aivcs_event(event).await?;
        if let Some(snapshot_id) = snapshot_id_from_event(&stored) {
            self.reconcile_correlation(&stored.tenant_id, "snapshot", &snapshot_id)
                .await?;
        }
        Ok(IngestOutcome::Ingested(Box::new(stored)))
    }

    async fn register_snapshot_correlation(&self, event: &DfcEvent) -> Result<(), DfcError> {
        let Some(snapshot_id) = aivcs_ref_snapshot_id(event.aivcs_ref.as_deref()).or_else(|| {
            event
                .metadata
                .get("snapshot_id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        }) else {
            return Ok(());
        };

        let record = CorrelationRecord {
            correlation_id: dfc_core::CorrelationId::derive(
                &event.tenant_id,
                "snapshot",
                "aivcs-api",
                &snapshot_id,
                "data-fabric",
                event.run_id.as_deref().unwrap_or("pending"),
            ),
            tenant_id: event.tenant_id.clone(),
            repo: event.repo.clone(),
            kind: Some(dfc_core::CorrelationKind::Snapshot),
            source_system: Some("aivcs-api".to_string()),
            source_id: Some(snapshot_id.clone()),
            target_system: Some("data-fabric".to_string()),
            target_id: Some(
                event
                    .run_id
                    .clone()
                    .unwrap_or_else(|| "pending".to_string()),
            ),
            data_fabric_run_id: event.run_id.clone(),
            data_fabric_task_id: event.task_id.clone(),
            aivcs_snapshot_id: Some(snapshot_id),
            aivcs_branch: event
                .metadata
                .get("branch")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            links: event.metadata.clone(),
        };
        self.client.store_correlation(&record).await
    }

    async fn enrich_hitl_from_correlation(&self, event: &mut DfcEvent) -> Result<(), DfcError> {
        if event.run_id.is_some() {
            return Ok(());
        }
        let Some(review_id) = event
            .metadata
            .get("review_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            return Ok(());
        };

        match self
            .client
            .get_correlation(&event.tenant_id, "review", &review_id)
            .await
        {
            Ok(record) => {
                if event.run_id.is_none() {
                    event.run_id = record.data_fabric_run_id.clone();
                }
                if event.task_id.is_none() {
                    event.task_id = record.data_fabric_task_id.clone();
                }
            }
            Err(DfcError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }

    async fn idempotency_lookup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<IngestOutcome>, DfcError> {
        if let Some(event) = self
            .client
            .get_event_by_idempotency(tenant_id, idempotency_key)
            .await?
        {
            return Ok(Some(IngestOutcome::Ingested(Box::new(event))));
        }

        let staging = self.staging.read().await;
        Ok(staging.get(tenant_id, idempotency_key))
    }

    async fn unresolved(
        &self,
        tenant_id: &str,
        pending: &[PendingCorrelation],
    ) -> Result<Vec<PendingCorrelation>, DfcError> {
        let mut unresolved = Vec::new();
        for item in pending {
            match self
                .client
                .get_correlation(tenant_id, &item.kind, &item.id)
                .await
            {
                Ok(_) => {}
                Err(DfcError::NotFound(_)) => unresolved.push(item.clone()),
                Err(err) => return Err(err),
            }
        }
        Ok(unresolved)
    }
}
