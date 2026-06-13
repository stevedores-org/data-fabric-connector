#[derive(Debug, Clone)]
pub struct AivcsConfig {
    pub base_url: String,
}

impl AivcsConfig {
    pub fn from_env() -> Self {
        Self {
            base_url: std::env::var("AIVCS_API_URL")
                .unwrap_or_else(|_| "http://aivcs-api.aivcs.svc.cluster.local".into()),
        }
    }
}
