use thiserror::Error;

#[derive(Debug, Error)]
pub enum DfcError {
    #[error("validation error: {0}")]
    Validation(String),

    #[error("tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: String, actual: String },

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("upstream error ({system}): {message}")]
    Upstream {
        system: String,
        message: String,
        status: Option<u16>,
    },
}

impl DfcError {
    pub fn upstream(
        system: impl Into<String>,
        message: impl Into<String>,
        status: Option<u16>,
    ) -> Self {
        Self::Upstream {
            system: system.into(),
            message: message.into(),
            status,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Upstream {
                status: Some(status),
                ..
            } => *status == 429 || (500..600).contains(status),
            _ => false,
        }
    }
}
