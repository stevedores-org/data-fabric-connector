pub const DEFAULT_PUBLIC_FQDN: &str = "dfc.aivcs.io";

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub public_fqdn: String,
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
        }
    }

    pub fn public_url(&self) -> String {
        format!("https://{}", self.public_fqdn)
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
