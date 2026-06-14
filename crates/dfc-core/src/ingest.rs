use crate::correlate::CorrelationKind;
use crate::{DfcError, InboundAivcsEvent, InboundHitlEvent};
use serde::{Deserialize, Serialize};

/// Allowed inbound AIVCS event types (Epic E3 / issue #4).
pub const AIVCS_EVENT_TYPES: &[&str] = &[
    "aivcs.snapshot.created",
    "aivcs.branch.created",
    "aivcs.replay.requested",
];

/// Allowed inbound HITL event types (Epic E3 / issue #4).
pub const HITL_EVENT_TYPES: &[&str] = &[
    "hitl.review.opened",
    "hitl.decision.recorded",
    "hitl.comment.added",
];

/// A correlation lookup that must exist before the event can be forwarded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingCorrelation {
    pub kind: String,
    pub id: String,
}

pub fn validate_aivcs_event_type(event_type: &str) -> Result<(), DfcError> {
    if AIVCS_EVENT_TYPES.contains(&event_type) {
        Ok(())
    } else {
        Err(DfcError::Validation(format!(
            "unsupported aivcs event_type: {event_type}"
        )))
    }
}

pub fn validate_hitl_event_type(event_type: &str) -> Result<(), DfcError> {
    if HITL_EVENT_TYPES.contains(&event_type) {
        Ok(())
    } else {
        Err(DfcError::Validation(format!(
            "unsupported hitl event_type: {event_type}"
        )))
    }
}

/// Snapshot identifiers referenced by an inbound AIVCS event.
pub fn snapshot_ref_from_aivcs(event: &InboundAivcsEvent) -> Option<String> {
    if let Some(id) = metadata_string(&event.metadata, "snapshot_id") {
        return Some(id);
    }
    if let Some(id) = metadata_string(&event.metadata, "parent_snapshot_id") {
        return Some(id);
    }
    aivcs_ref_snapshot_id(event.aivcs_ref.as_deref())
}

/// Parent snapshot a branch event depends on before it can be forwarded.
pub fn aivcs_pending_correlations(event: &InboundAivcsEvent) -> Vec<PendingCorrelation> {
    match event.event_type.as_str() {
        "aivcs.branch.created" => snapshot_ref_from_aivcs(event)
            .map(|id| PendingCorrelation {
                kind: CorrelationKind::Snapshot.as_str().into(),
                id,
            })
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

/// HITL events may wait on a review→run mapping when review_id is present but run_id is not.
pub fn hitl_pending_correlations(event: &InboundHitlEvent) -> Vec<PendingCorrelation> {
    if event.run_id.is_some() {
        return Vec::new();
    }
    event
        .review_id
        .as_ref()
        .map(|review_id| PendingCorrelation {
            kind: CorrelationKind::Review.as_str().into(),
            id: review_id.clone(),
        })
        .into_iter()
        .collect()
}

pub fn snapshot_id_from_event(event: &crate::DfcEvent) -> Option<String> {
    aivcs_ref_snapshot_id(event.aivcs_ref.as_deref())
        .or_else(|| metadata_string(&event.metadata, "snapshot_id"))
}

pub fn aivcs_ref_snapshot_id(aivcs_ref: Option<&str>) -> Option<String> {
    let value = aivcs_ref?;
    let rest = value.strip_prefix("aivcs:snapshot:")?;
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

fn metadata_string(metadata: &serde_json::Value, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_allowed_aivcs_types() {
        assert!(validate_aivcs_event_type("aivcs.snapshot.created").is_ok());
        assert!(validate_aivcs_event_type("aivcs.branch.created").is_ok());
        assert!(validate_aivcs_event_type("aivcs.merge.completed").is_err());
    }

    #[test]
    fn branch_created_waits_on_parent_snapshot() {
        let event = InboundAivcsEvent {
            event_type: "aivcs.branch.created".into(),
            tenant_id: "tenant-a".into(),
            idempotency_key: "k1".into(),
            repo: None,
            run_id: None,
            task_id: None,
            aivcs_ref: None,
            payload_ref: None,
            metadata: serde_json::json!({ "parent_snapshot_id": "snap_parent" }),
        };
        let pending = aivcs_pending_correlations(&event);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, "snapshot");
        assert_eq!(pending[0].id, "snap_parent");
    }

    #[test]
    fn hitl_waits_on_review_mapping_when_run_id_missing() {
        let event = InboundHitlEvent {
            event_type: "hitl.review.opened".into(),
            tenant_id: "tenant-a".into(),
            idempotency_key: "k1".into(),
            review_id: Some("rev-1".into()),
            run_id: None,
            metadata: serde_json::json!({}),
        };
        let pending = hitl_pending_correlations(&event);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, "review");
    }
}
