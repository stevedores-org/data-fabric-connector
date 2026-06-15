use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight counters for E6 reliability observability.
#[derive(Debug, Default)]
pub struct DfcMetrics {
    events_ingested_total: AtomicU64,
    dlq_total: AtomicU64,
    retries_total: AtomicU64,
}

impl DfcMetrics {
    pub fn inc_events_ingested(&self) {
        self.events_ingested_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_dlq(&self) {
        self.dlq_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_retries(&self) {
        self.retries_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP dfc_events_ingested_total Events successfully ingested\n\
             # TYPE dfc_events_ingested_total counter\n\
             dfc_events_ingested_total {}\n\
             # HELP dfc_dlq_total Events dead-lettered\n\
             # TYPE dfc_dlq_total counter\n\
             dfc_dlq_total {}\n\
             # HELP dfc_retries_total Upstream HTTP retries\n\
             # TYPE dfc_retries_total counter\n\
             dfc_retries_total {}\n",
            self.events_ingested_total.load(Ordering::Relaxed),
            self.dlq_total.load(Ordering::Relaxed),
            self.retries_total.load(Ordering::Relaxed),
        )
    }
}
