#[derive(Debug, Clone)]
pub struct DataFabricConfig {
    pub base_url: String,
    pub tenant_id: String,
}

impl DataFabricConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            base_url: std::env::var("DATA_FABRIC_URL")
                .unwrap_or_else(|_| "http://data-fabric.data-fabric.svc.cluster.local".into()),
            tenant_id: std::env::var("DATA_FABRIC_TENANT_ID")
                .map_err(|_| "DATA_FABRIC_TENANT_ID is required".to_string())?,
        })
    }
}
