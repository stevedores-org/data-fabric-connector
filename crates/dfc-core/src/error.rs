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
    Upstream { system: String, message: String },
}
