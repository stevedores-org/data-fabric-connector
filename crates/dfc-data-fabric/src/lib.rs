//! data-fabric client adapter for DFC.

mod client;
mod config;
mod ingest;

pub use client::{DataFabricClient, HttpDataFabricClient, MockDataFabricClient};
pub use config::DataFabricConfig;
pub use ingest::{EventIngestService, IngestOutcome};
