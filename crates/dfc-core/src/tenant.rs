use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    pub fn ensure(&self, resource_tenant: &str) -> Result<(), crate::DfcError> {
        if self.tenant_id != resource_tenant {
            // Audit log: record cross-tenant access attempt for observability/forensics
            tracing::warn!(expected = %self.tenant_id, actual = %resource_tenant, "tenant access denied: cross-tenant lookup attempted");
            return Err(crate::DfcError::TenantMismatch {
                expected: self.tenant_id.clone(),
                actual: resource_tenant.to_string(),
            });
        }
        Ok(())
    }
}
