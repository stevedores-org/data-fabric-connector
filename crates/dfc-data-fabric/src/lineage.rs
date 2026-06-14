use dfc_core::CorrelationId;
use serde::{Deserialize, Serialize};

/// Snapshot chain resolved from data-fabric for replay bridging.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLineage {
    pub correlation_id: Option<CorrelationId>,
    pub snapshot_ids: Vec<String>,
    pub from_snapshot: Option<String>,
    pub to_snapshot: Option<String>,
}
