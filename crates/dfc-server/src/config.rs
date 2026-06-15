pub const DEFAULT_PUBLIC_FQDN: &str = "dfc.aivcs.io";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamMode {
    Mock,
    Production,
}

impl UpstreamMode {
    pub fn from_env() -> Self {
        match std::env::var("DFC_UPSTREAM_MODE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "production" | "prod" => Self::Production,
            "mock" => Self::Mock,
            _ if std::env::var("DATA_FABRIC_TENANT_ID").is_ok() => Self::Production,
            _ => Self::Mock,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Mock => "mock-upstreams",
            Self::Production => "production-upstreams",
        }
    }
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub public_fqdn: String,
    pub upstream_mode: UpstreamMode,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("DFC_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: std::env::var("DFC_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            public_fqdn: std::env::var("DFC_PUBLIC_FQDN")
                .unwrap_or_else(|_| DEFAULT_PUBLIC_FQDN.into()),
            upstream_mode: UpstreamMode::from_env(),
        }
    }

    pub fn public_url(&self) -> String {
        format!("https://{}", self.public_fqdn)
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
