use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::EventId;
use crate::SCHEMA_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceSystem {
    AivcsApi,
    AivcsHumanInTheLoop,
    DataFabric,
}

impl SourceSystem {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AivcsApi => "aivcs-api",
            Self::AivcsHumanInTheLoop => "aivcs-human-in-the-loop",
            Self::DataFabric => "data-fabric",
        }
    }
}

/// Canonical event envelope forwarded to data-fabric after normalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DfcEvent {
    pub schema_version: String,
    pub event_id: EventId,
    pub event_type: String,
    pub tenant_id: String,
    pub repo: Option<String>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub aivcs_ref: Option<String>,
    pub data_fabric_event_id: Option<String>,
    pub idempotency_key: String,
    pub source_system: SourceSystem,
    pub payload_ref: Option<String>,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl DfcEvent {
    pub fn new(
        event_type: impl Into<String>,
        tenant_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        source_system: SourceSystem,
    ) -> Self {
        let idempotency_key = idempotency_key.into();
        let tenant_id = tenant_id.into();
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            event_id: EventId::from_idempotency(&tenant_id, &idempotency_key),
            event_type: event_type.into(),
            tenant_id,
            repo: None,
            run_id: None,
            task_id: None,
            aivcs_ref: None,
            data_fabric_event_id: None,
            idempotency_key,
            source_system,
            payload_ref: None,
            created_at: Utc::now(),
            metadata: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn validate(&self) -> Result<(), crate::DfcError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(crate::DfcError::Validation(format!(
                "unsupported schema_version: {}",
                self.schema_version
            )));
        }
        if self.tenant_id.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "tenant_id is required".into(),
            ));
        }
        if self.idempotency_key.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "idempotency_key is required".into(),
            ));
        }
        if self.event_type.trim().is_empty() {
            return Err(crate::DfcError::Validation(
                "event_type is required".into(),
            ));
        }
        Ok(())
    }
}

/// Inbound AIVCS event before normalization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboundAivcsEvent {
    pub event_type: String,
    pub tenant_id: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub aivcs_ref: Option<String>,
    #[serde(default)]
    pub payload_ref: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl InboundAivcsEvent {
    pub fn into_dfc_event(self) -> DfcEvent {
        let mut event = DfcEvent::new(
            self.event_type,
            self.tenant_id,
            self.idempotency_key,
            SourceSystem::AivcsApi,
        );
        event.repo = self.repo;
        event.run_id = self.run_id;
        event.task_id = self.task_id;
        event.aivcs_ref = self.aivcs_ref;
        event.payload_ref = self.payload_ref;
        event.metadata = self.metadata;
        event
    }
}

/// Inbound HITL event before normalization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboundHitlEvent {
    pub event_type: String,
    pub tenant_id: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub review_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl InboundHitlEvent {
    pub fn into_dfc_event(self) -> DfcEvent {
        let mut event = DfcEvent::new(
            self.event_type,
            self.tenant_id,
            self.idempotency_key,
            SourceSystem::AivcsHumanInTheLoop,
        );
        event.run_id = self.run_id;
        if let Some(review_id) = self.review_id {
            event.metadata["review_id"] = serde_json::Value::String(review_id);
        }
        if !self.metadata.is_null() {
            if let (serde_json::Value::Object(base), serde_json::Value::Object(extra)) =
                (&mut event.metadata, self.metadata)
            {
                base.extend(extra);
            }
        }
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_is_stable() {
        let a = EventId::from_idempotency("lornu-ai", "sha256:abc");
        let b = EventId::from_idempotency("lornu-ai", "sha256:abc");
        assert_eq!(a, b);
    }

    #[test]
    fn validates_required_fields() {
        let mut event = DfcEvent::new(
            "aivcs.snapshot.created",
            "lornu-ai",
            "key-1",
            SourceSystem::AivcsApi,
        );
        assert!(event.validate().is_ok());

        event.idempotency_key = "  ".into();
        assert!(event.validate().is_err());
    }
}
