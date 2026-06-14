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
