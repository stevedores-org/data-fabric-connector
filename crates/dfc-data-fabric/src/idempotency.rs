use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dfc_core::{DfcError, DfcEvent};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::DataFabricClient;

/// Minimum idempotency TTL per US-X2 (24 hours).
pub const MIN_IDEMPOTENCY_TTL: Duration = Duration::from_secs(86_400);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyBackendKind {
    DataFabric,
    Memory,
    Redis,
}

#[derive(Debug, Clone)]
pub struct IdempotencyConfig {
    pub backend: IdempotencyBackendKind,
    pub ttl: Duration,
    pub redis_url: Option<String>,
}

impl IdempotencyConfig {
    pub fn from_env() -> Self {
        let backend = match std::env::var("DFC_IDEMPOTENCY_BACKEND")
            .unwrap_or_else(|_| "data-fabric".into())
            .to_lowercase()
            .as_str()
        {
            "memory" => IdempotencyBackendKind::Memory,
            "redis" => IdempotencyBackendKind::Redis,
            _ => IdempotencyBackendKind::DataFabric,
        };

        let ttl_secs = std::env::var("DFC_IDEMPOTENCY_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(MIN_IDEMPOTENCY_TTL.as_secs());

        Self {
            backend,
            ttl: Duration::from_secs(ttl_secs.max(MIN_IDEMPOTENCY_TTL.as_secs())),
            redis_url: std::env::var("DFC_REDIS_URL").ok(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedEvent {
    event: DfcEvent,
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn get(&self, tenant_id: &str, key: &str) -> Result<Option<DfcEvent>, DfcError>;
    async fn put(&self, tenant_id: &str, key: &str, event: &DfcEvent) -> Result<(), DfcError>;
}

struct GenericDataFabricIdempotencyStore<C: DataFabricClient + ?Sized> {
    client: Arc<C>,
}

#[async_trait]
impl<C: DataFabricClient + ?Sized> IdempotencyStore for GenericDataFabricIdempotencyStore<C> {
    async fn get(&self, tenant_id: &str, key: &str) -> Result<Option<DfcEvent>, DfcError> {
        self.client.get_event_by_idempotency(tenant_id, key).await
    }

    async fn put(&self, _tenant_id: &str, _key: &str, _event: &DfcEvent) -> Result<(), DfcError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryIdempotencyStore {
    entries: RwLock<std::collections::HashMap<String, (DfcEvent, Instant)>>,
    ttl: Duration,
}

impl MemoryIdempotencyStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(std::collections::HashMap::new()),
            ttl,
        }
    }

    fn key(tenant_id: &str, idempotency_key: &str) -> String {
        format!("{tenant_id}:{idempotency_key}")
    }

    async fn purge_expired(&self) {
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        entries.retain(|_, (_, expires_at)| *expires_at > now);
    }
}

#[async_trait]
impl IdempotencyStore for MemoryIdempotencyStore {
    async fn get(&self, tenant_id: &str, key: &str) -> Result<Option<DfcEvent>, DfcError> {
        self.purge_expired().await;
        let entries = self.entries.read().await;
        Ok(entries
            .get(&Self::key(tenant_id, key))
            .map(|(event, _)| event.clone()))
    }

    async fn put(&self, tenant_id: &str, key: &str, event: &DfcEvent) -> Result<(), DfcError> {
        let mut entries = self.entries.write().await;
        entries.insert(
            Self::key(tenant_id, key),
            (event.clone(), Instant::now() + self.ttl),
        );
        Ok(())
    }
}

pub struct RedisIdempotencyStore {
    client: redis::Client,
    ttl: Duration,
}

impl RedisIdempotencyStore {
    pub fn new(redis_url: &str, ttl: Duration) -> Result<Self, DfcError> {
        let client = redis::Client::open(redis_url)
            .map_err(|err| DfcError::Validation(format!("invalid DFC_REDIS_URL: {err}")))?;
        Ok(Self { client, ttl })
    }

    fn key(tenant_id: &str, idempotency_key: &str) -> String {
        format!("dfc:idempotency:{tenant_id}:{idempotency_key}")
    }
}

#[async_trait]
impl IdempotencyStore for RedisIdempotencyStore {
    async fn get(&self, tenant_id: &str, key: &str) -> Result<Option<DfcEvent>, DfcError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| DfcError::upstream("redis", err.to_string(), None))?;
        let raw: Option<String> = redis::cmd("GET")
            .arg(Self::key(tenant_id, key))
            .query_async(&mut conn)
            .await
            .map_err(|err| DfcError::upstream("redis", err.to_string(), None))?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let cached: CachedEvent = serde_json::from_str(&raw).map_err(|err| {
            DfcError::Validation(format!("redis idempotency cache decode failed: {err}"))
        })?;
        Ok(Some(cached.event))
    }

    async fn put(&self, tenant_id: &str, key: &str, event: &DfcEvent) -> Result<(), DfcError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| DfcError::upstream("redis", err.to_string(), None))?;
        let payload = serde_json::to_string(&CachedEvent {
            event: event.clone(),
        })
        .map_err(|err| DfcError::Validation(err.to_string()))?;
        redis::cmd("SET")
            .arg(Self::key(tenant_id, key))
            .arg(payload)
            .arg("EX")
            .arg(self.ttl.as_secs())
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| DfcError::upstream("redis", err.to_string(), None))?;
        Ok(())
    }
}

pub fn build_idempotency_store<C: DataFabricClient + 'static>(
    config: &IdempotencyConfig,
    client: Arc<C>,
) -> Result<Arc<dyn IdempotencyStore>, DfcError> {
    match config.backend {
        IdempotencyBackendKind::DataFabric => {
            Ok(Arc::new(GenericDataFabricIdempotencyStore { client }))
        }
        IdempotencyBackendKind::Memory => Ok(Arc::new(MemoryIdempotencyStore::new(config.ttl))),
        IdempotencyBackendKind::Redis => {
            let redis_url = config
                .redis_url
                .as_deref()
                .ok_or_else(|| DfcError::Validation("DFC_REDIS_URL is required".into()))?;
            Ok(Arc::new(RedisIdempotencyStore::new(redis_url, config.ttl)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfc_core::SourceSystem;

    fn sample_event(key: &str) -> DfcEvent {
        DfcEvent::new(
            "aivcs.snapshot.created",
            "tenant-a",
            key,
            SourceSystem::AivcsApi,
        )
    }

    #[tokio::test]
    async fn memory_store_returns_cached_response_on_replay() {
        let store = MemoryIdempotencyStore::new(Duration::from_secs(3600));
        let event = sample_event("snap-key");
        store.put("tenant-a", "snap-key", &event).await.unwrap();

        let replay = store.get("tenant-a", "snap-key").await.unwrap().unwrap();
        assert_eq!(replay.event_id, event.event_id);
        assert_eq!(replay.idempotency_key, "snap-key");
    }

    #[tokio::test]
    async fn config_enforces_minimum_ttl() {
        std::env::set_var("DFC_IDEMPOTENCY_TTL_SECS", "60");
        let config = IdempotencyConfig::from_env();
        assert_eq!(config.ttl, MIN_IDEMPOTENCY_TTL);
        std::env::remove_var("DFC_IDEMPOTENCY_TTL_SECS");
    }
}
