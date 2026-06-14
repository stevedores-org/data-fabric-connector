use serde::{Deserialize, Serialize};

use crate::ids::CorrelationId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationKind {
    Run,
    Task,
    Branch,
    Pr,
    Review,
    Snapshot,
    Session,
}

impl CorrelationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Task => "task",
            Self::Branch => "branch",
            Self::Pr => "pr",
            Self::Review => "review",
            Self::Snapshot => "snapshot",
            Self::Session => "session",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "run" => Some(Self::Run),
            "task" => Some(Self::Task),
            "branch" => Some(Self::Branch),
            "pr" => Some(Self::Pr),
            "review" => Some(Self::Review),
            "snapshot" => Some(Self::Snapshot),
            "session" => Some(Self::Session),
            _ => None,
        }
    }
}

/// Request to register cross-system ID mapping (issue #1 correlate example extended).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CorrelateRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub kind: Option<CorrelationKind>,
    #[serde(default)]
    pub source_system: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub target_system: Option<String>,
    #[serde(default)]
    pub target_id: Option<String>,
    /// Flat mapping fields from issue #1 minimal example.
    #[serde(default)]
    pub data_fabric_run_id: Option<String>,
    #[serde(default)]
    pub data_fabric_task_id: Option<String>,
    #[serde(default)]
    pub aivcs_snapshot_id: Option<String>,
    #[serde(default)]
    pub aivcs_branch: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl CorrelateRequest {
    /// Validates E2 canonical mapping fields or E1 legacy flat fields.
    pub fn validate(&self) -> Result<(), crate::DfcError> {
        if self.tenant_id.trim().is_empty() {
            return Err(crate::DfcError::Validation("tenant_id is required".into()));
        }

        let has_canonical = self.kind.is_some()
            && non_empty(&self.source_system)
            && non_empty(&self.source_id)
            && non_empty(&self.target_system)
            && non_empty(&self.target_id);

        let has_legacy = non_empty(&self.data_fabric_run_id)
            || non_empty(&self.data_fabric_task_id)
            || non_empty(&self.aivcs_snapshot_id)
            || non_empty(&self.aivcs_branch);

        if !has_canonical && !has_legacy {
            return Err(crate::DfcError::Validation(
                "provide kind, source_system, source_id, target_system, target_id \
                 (or legacy data_fabric_run_id / data_fabric_task_id / aivcs_snapshot_id / aivcs_branch)"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn correlation_id(&self) -> CorrelationId {
        if let Some(hash) = self.metadata.get("content_hash").and_then(|v| v.as_str()) {
            return CorrelationId::from_content_hash(&self.tenant_id, hash);
        }

        let kind = self.kind.map(|k| k.as_str()).unwrap_or("run").to_string();
        let source_system = self
            .source_system
            .clone()
            .unwrap_or_else(|| "data-fabric".into());
        let source_id = self
            .data_fabric_run_id
            .clone()
            .or_else(|| self.source_id.clone())
            .unwrap_or_default();
        let target_system = self
            .target_system
            .clone()
            .unwrap_or_else(|| "aivcs-api".into());
        let target_id = self
            .aivcs_snapshot_id
            .clone()
            .or_else(|| self.target_id.clone())
            .unwrap_or_default();

        CorrelationId::derive(
            &self.tenant_id,
            &kind,
            &source_system,
            &source_id,
            &target_system,
            &target_id,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationRecord {
    pub correlation_id: CorrelationId,
    pub tenant_id: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub kind: Option<CorrelationKind>,
    #[serde(default)]
    pub source_system: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub target_system: Option<String>,
    #[serde(default)]
    pub target_id: Option<String>,
    #[serde(default)]
    pub data_fabric_run_id: Option<String>,
    #[serde(default)]
    pub data_fabric_task_id: Option<String>,
    #[serde(default)]
    pub aivcs_snapshot_id: Option<String>,
    #[serde(default)]
    pub aivcs_branch: Option<String>,
    #[serde(default)]
    pub links: serde_json::Value,
}

impl From<CorrelateRequest> for CorrelationRecord {
    fn from(req: CorrelateRequest) -> Self {
        Self {
            correlation_id: req.correlation_id(),
            tenant_id: req.tenant_id,
            repo: req.repo,
            kind: req.kind,
            source_system: req.source_system,
            source_id: req.source_id,
            target_system: req.target_system,
            target_id: req.target_id,
            data_fabric_run_id: req.data_fabric_run_id,
            data_fabric_task_id: req.data_fabric_task_id,
            aivcs_snapshot_id: req.aivcs_snapshot_id,
            aivcs_branch: req.aivcs_branch,
            links: req.metadata,
        }
    }
}

fn non_empty(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|s| !s.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_request() -> CorrelateRequest {
        CorrelateRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            kind: Some(CorrelationKind::Branch),
            source_system: Some("github".into()),
            source_id: Some("feature-abc".into()),
            target_system: Some("aivcs".into()),
            target_id: Some("branch-abc".into()),
            data_fabric_run_id: None,
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({ "content_hash": "hash123" }),
        }
    }

    #[test]
    fn validate_accepts_canonical_mapping() {
        canonical_request()
            .validate()
            .expect("valid canonical request");
    }

    #[test]
    fn validate_accepts_legacy_flat_fields() {
        let req = CorrelateRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            kind: None,
            source_system: None,
            source_id: None,
            target_system: None,
            target_id: None,
            data_fabric_run_id: Some("run-1".into()),
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({}),
        };
        req.validate().expect("valid legacy request");
    }

    #[test]
    fn validate_rejects_empty_mapping() {
        let req = CorrelateRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            kind: None,
            source_system: None,
            source_id: None,
            target_system: None,
            target_id: None,
            data_fabric_run_id: None,
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({}),
        };
        assert!(req.validate().is_err());
    }

    #[test]
    fn content_hash_yields_stable_correlation_id() {
        let req = canonical_request();
        let id1 = req.correlation_id();
        let id2 = req.correlation_id();
        assert_eq!(id1, id2);
        assert!(id1.0.starts_with("corr_"));
    }

    #[test]
    fn derive_is_deterministic_for_same_inputs() {
        let req = CorrelateRequest {
            tenant_id: "tenant-a".into(),
            repo: None,
            kind: Some(CorrelationKind::Run),
            source_system: Some("data-fabric".into()),
            source_id: Some("run-1".into()),
            target_system: Some("aivcs-api".into()),
            target_id: Some("snap-1".into()),
            data_fabric_run_id: None,
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({}),
        };
        assert_eq!(req.correlation_id(), req.correlation_id());
    }
}
