use async_trait::async_trait;
use dfc_core::{CorrelationRecord, DfcError, DfcEvent};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::DataFabricConfig;

#[async_trait]
pub trait DataFabricClient: Send + Sync {
    async fn ingest_event(&self, event: &DfcEvent) -> Result<DfcEvent, DfcError>;
    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError>;
    async fn get_correlation(
        &self,
        tenant_id: &str,
        kind: &str,
        id: &str,
    ) -> Result<CorrelationRecord, DfcError>;
}

pub struct HttpDataFabricClient {
    http: Client,
    config: DataFabricConfig,
}

impl HttpDataFabricClient {
    pub fn new(config: DataFabricConfig) -> Self {
        Self {
            http: Client::new(),
            config,
        }
    }
}

#[async_trait]
impl DataFabricClient for HttpDataFabricClient {
    async fn ingest_event(&self, event: &DfcEvent) -> Result<DfcEvent, DfcError> {
        let url = format!("{}/v1/events", self.config.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .header("X-Tenant-Id", &self.config.tenant_id)
            .json(event)
            .send()
            .await
            .map_err(|e| DfcError::Upstream {
                system: "data-fabric".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(DfcError::Upstream {
                system: "data-fabric".into(),
                message: format!("status {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| DfcError::Upstream {
            system: "data-fabric".into(),
            message: e.to_string(),
        })
    }

    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError> {
        let url = format!(
            "{}/v1/correlations",
            self.config.base_url.trim_end_matches('/')
        );
        let resp = self
            .http
            .post(&url)
            .header("X-Tenant-Id", &record.tenant_id)
            .json(record)
            .send()
            .await
            .map_err(|e| DfcError::Upstream {
                system: "data-fabric".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(DfcError::Upstream {
                system: "data-fabric".into(),
                message: format!("status {}", resp.status()),
            });
        }
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
        let resp = self
            .http
            .get(&url)
            .header("X-Tenant-Id", tenant_id)
            .send()
            .await
            .map_err(|e| DfcError::Upstream {
                system: "data-fabric".into(),
                message: e.to_string(),
            })?;

        if resp.status().as_u16() == 404 {
            return Err(DfcError::NotFound(format!("{kind}/{id}")));
        }
        if !resp.status().is_success() {
            return Err(DfcError::Upstream {
                system: "data-fabric".into(),
                message: format!("status {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| DfcError::Upstream {
            system: "data-fabric".into(),
            message: e.to_string(),
        })
    }
}

type CorrelationKey = (String, String, String);

#[derive(Debug, Default)]
pub struct MockDataFabricClient {
    correlations: Arc<RwLock<HashMap<CorrelationKey, CorrelationRecord>>>,
    events: Arc<RwLock<HashMap<String, DfcEvent>>>,
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

    async fn store_correlation(&self, record: &CorrelationRecord) -> Result<(), DfcError> {
        let mut correlations = self.correlations.write().await;
        for (kind, id) in correlation_lookup_keys(record) {
            let key = (record.tenant_id.clone(), kind, id);
            if let Some(existing) = correlations.get(&key) {
                if existing.tenant_id != record.tenant_id {
                    return Err(DfcError::Conflict("tenant mismatch".into()));
                }
            }
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
        correlations
            .get(&(tenant_id.to_string(), kind.to_string(), id.to_string()))
            .cloned()
            .ok_or_else(|| DfcError::NotFound(format!("{kind}/{id}")))
    }
}

fn correlation_lookup_keys(record: &CorrelationRecord) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    if let Some(run_id) = &record.data_fabric_run_id {
        keys.push(("run".into(), run_id.clone()));
    }
    if let Some(task_id) = &record.data_fabric_task_id {
        keys.push(("task".into(), task_id.clone()));
    }
    if let Some(snapshot_id) = &record.aivcs_snapshot_id {
        keys.push(("snapshot".into(), snapshot_id.clone()));
    }
    keys
}
