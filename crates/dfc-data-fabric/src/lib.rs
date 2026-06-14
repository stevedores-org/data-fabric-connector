//! data-fabric client adapter for DFC.

mod client;
mod config;
mod ingest;
mod lineage;
mod review;

pub use client::{DataFabricClient, HttpDataFabricClient, MockDataFabricClient};
pub use config::DataFabricConfig;
pub use ingest::{EventIngestService, IngestOutcome};
pub use lineage::SnapshotLineage;
pub use review::DataFabricReviewFragment;
