use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identifier for a normalized DFC event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub String);

impl EventId {
    pub fn new() -> Self {
        Self(format!("evt_{}", Uuid::new_v4().simple()))
    }

    pub fn from_idempotency(tenant_id: &str, idempotency_key: &str) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(tenant_id.as_bytes());
        hasher.update(b":");
        hasher.update(idempotency_key.as_bytes());
        let digest = hasher.finalize();
        Self(format!("evt_{:x}", digest))
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic correlation identifier derived from tenant + kind + source/target IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn derive(
        tenant_id: &str,
        kind: &str,
        source_system: &str,
        source_id: &str,
        target_system: &str,
        target_id: &str,
    ) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for part in [
            tenant_id,
            kind,
            source_system,
            source_id,
            target_system,
            target_id,
        ] {
            hasher.update(part.as_bytes());
            hasher.update(b"|");
        }
        let digest = hasher.finalize();
        Self(format!("corr_{}", hex_prefix(&digest[..16])))
    }

    pub fn from_content_hash(tenant_id: &str, content_hash: &str) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(tenant_id.as_bytes());
        hasher.update(b":");
        hasher.update(content_hash.as_bytes());
        let digest = hasher.finalize();
        Self(format!("corr_{}", hex_prefix(&digest[..16])))
    }
}

fn hex_prefix(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
