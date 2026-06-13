//! data-fabric client adapter for DFC.

mod client;
mod config;

pub use client::{DataFabricClient, HttpDataFabricClient, MockDataFabricClient};
pub use config::DataFabricConfig;
