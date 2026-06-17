use std::sync::Arc;

use dfc_aivcs::{AivcsClient, AivcsConfig, HttpAivcsClient, MockAivcsClient};
use dfc_core::{DfcError, DfcMetrics};
use dfc_data_fabric::{
    DataFabricClient, DataFabricConfig, HttpDataFabricClient, MockDataFabricClient,
};

use crate::config::UpstreamMode;

pub struct UpstreamClients {
    pub data_fabric: Arc<dyn DataFabricClient>,
    pub aivcs: Arc<dyn AivcsClient>,
}

pub fn build_upstream_clients(
    mode: UpstreamMode,
    metrics: Arc<DfcMetrics>,
) -> Result<UpstreamClients, DfcError> {
    match mode {
        UpstreamMode::Mock => {
            let data_fabric: Arc<dyn DataFabricClient> = Arc::new(MockDataFabricClient::default());
            let aivcs: Arc<dyn AivcsClient> = Arc::new(MockAivcsClient);
            Ok(UpstreamClients { data_fabric, aivcs })
        }
        UpstreamMode::Production => {
            let data_fabric_config = DataFabricConfig::from_env()
                .map_err(|msg| DfcError::Validation(format!("data-fabric config: {msg}")))?;
            let data_fabric: Arc<dyn DataFabricClient> =
                Arc::new(HttpDataFabricClient::new(data_fabric_config).with_metrics(metrics));
            let aivcs: Arc<dyn AivcsClient> =
                Arc::new(HttpAivcsClient::new(AivcsConfig::from_env()));
            Ok(UpstreamClients { data_fabric, aivcs })
        }
    }
}
