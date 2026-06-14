use dfc_core::{CorrelationId, DfcError, DfcEvent, SourceSystem};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct AuditContext {
    pub actor: String,
    pub correlation_id: Option<CorrelationId>,
}

impl AuditContext {
    pub fn new(actor: impl Into<String>, correlation_id: Option<CorrelationId>) -> Self {
        Self {
            actor: actor.into(),
            correlation_id,
        }
    }

    pub fn enrich_metadata(&self, mut metadata: Value) -> Value {
        if let Some(obj) = metadata.as_object_mut() {
            obj.insert("actor".into(), json!(self.actor));
            if let Some(correlation_id) = &self.correlation_id {
                obj.insert("correlation_id".into(), json!(correlation_id.0.clone()));
            }
        } else {
            metadata = json!({
                "actor": self.actor,
                "correlation_id": self.correlation_id.as_ref().map(|id| id.0.clone()),
            });
        }
        metadata
    }

    pub fn dlq_metadata(reason: &str) -> Value {
        json!({
            "dlq": {
                "queued": true,
                "reason": reason,
                "epic": "E6"
            }
        })
    }
}

pub fn replay_response_from_event(event: &DfcEvent) -> Result<dfc_core::ReplayResponse, DfcError> {
    let replay_id = event
        .metadata
        .get("replay_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DfcError::Validation("missing replay_id in stored event".into()))?
        .to_string();
    let status = event
        .metadata
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed")
        .to_string();
    let snapshot_ids = event
        .metadata
        .get("snapshot_ids")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    Ok(dfc_core::ReplayResponse {
        replay_id,
        status,
        snapshot_ids,
        data_fabric_event_id: event.data_fabric_event_id.clone(),
        aivcs_operation_id: event
            .metadata
            .get("aivcs_operation_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

pub fn rollback_response_from_event(
    event: &DfcEvent,
) -> Result<dfc_core::RollbackResponse, DfcError> {
    let rollback_id = event
        .metadata
        .get("rollback_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DfcError::Validation("missing rollback_id in stored event".into()))?
        .to_string();
    let status = event
        .metadata
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed")
        .to_string();

    Ok(dfc_core::RollbackResponse {
        rollback_id,
        status,
        data_fabric_event_id: event.data_fabric_event_id.clone(),
        aivcs_operation_id: event
            .metadata
            .get("aivcs_operation_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

pub fn terminal_event(
    event_type: &str,
    tenant_id: &str,
    idempotency_key: &str,
    source: SourceSystem,
    audit: &AuditContext,
    metadata: Value,
) -> DfcEvent {
    let mut event = DfcEvent::new(event_type, tenant_id, idempotency_key, source);
    event.metadata = audit.enrich_metadata(metadata);
    event
}
