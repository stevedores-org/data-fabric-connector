//! Canonical types for the data-fabric connector (DFC).
//!
//! DFC is a stateless anti-corruption layer between AIVCS, HITL, and data-fabric.
//! This crate owns schemas and IDs only — not persistence or domain semantics.

pub mod correlate;
pub mod error;
pub mod event;
pub mod ids;
pub mod replay;
pub mod tenant;

pub use correlate::{CorrelateRequest, CorrelationKind, CorrelationRecord};
pub use error::DfcError;
pub use event::{
    DfcEvent, InboundAivcsEvent, InboundHitlEvent, SourceSystem,
};
pub use ids::{CorrelationId, EventId};
pub use replay::{ReplayMode, ReplayRequest, ReplayResponse, RollbackRequest, RollbackResponse};
pub use tenant::TenantContext;

/// Current schema version for all DFC envelopes.
pub const SCHEMA_VERSION: &str = "dfc.v1";
