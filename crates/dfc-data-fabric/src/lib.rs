//! data-fabric client adapter for DFC.

mod client;
mod config;
mod http;
mod idempotency;
mod ingest;
mod lineage;
mod retry;
mod review;

pub use client::{DataFabricClient, HttpDataFabricClient, MockDataFabricClient};
pub use config::DataFabricConfig;
pub use idempotency::{
    build_idempotency_store, IdempotencyBackendKind, IdempotencyConfig, IdempotencyStore,
    MemoryIdempotencyStore, MIN_IDEMPOTENCY_TTL,
};
pub use ingest::{EventIngestService, IngestOutcome};
pub use lineage::SnapshotLineage;
pub use review::DataFabricReviewFragment;
