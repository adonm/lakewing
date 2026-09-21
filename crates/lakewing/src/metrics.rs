//! Prometheus text exposition over atomic counters (ported shape from the
//! Go store metrics; Go-runtime gauges replaced by in-flight and dataset
//! metadata).
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    requests: AtomicU64,
    in_flight: AtomicU64,
    ok200: AtomicU64,
    ok204: AtomicU64,
    ok304: AtomicU64,
    err400: AtomicU64,
    err404: AtomicU64,
    err429: AtomicU64,
    err500: AtomicU64,
    pub dataset_version: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            ok200: AtomicU64::new(0),
            ok204: AtomicU64::new(0),
            ok304: AtomicU64::new(0),
            err400: AtomicU64::new(0),
            err404: AtomicU64::new(0),
            err429: AtomicU64::new(0),
            err500: AtomicU64::new(0),
            dataset_version: AtomicU64::new(0),
        }
    }

    pub fn count_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn enter(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn count(&self, status: u16) {
        match status {
            200 => self.ok200.fetch_add(1, Ordering::Relaxed),
            204 => self.ok204.fetch_add(1, Ordering::Relaxed),
            304 => self.ok304.fetch_add(1, Ordering::Relaxed),
            400 => self.err400.fetch_add(1, Ordering::Relaxed),
            404 => self.err404.fetch_add(1, Ordering::Relaxed),
            429 => self.err429.fetch_add(1, Ordering::Relaxed),
            _ => self.err500.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn exposition(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP lakewing_http_responses_total OGC responses served by status.\n# TYPE lakewing_http_responses_total counter\n");
        for (status, value) in [
            ("200", self.ok200.load(Ordering::Relaxed)),
            ("204", self.ok204.load(Ordering::Relaxed)),
            ("304", self.ok304.load(Ordering::Relaxed)),
            ("400", self.err400.load(Ordering::Relaxed)),
            ("404", self.err404.load(Ordering::Relaxed)),
            ("429", self.err429.load(Ordering::Relaxed)),
            ("500", self.err500.load(Ordering::Relaxed)),
        ] {
            out.push_str(&format!(
                "lakewing_http_responses_total{{status=\"{status}\"}} {value}\n"
            ));
        }
        out.push_str("# HELP lakewing_http_requests_total OGC request attempts.\n# TYPE lakewing_http_requests_total counter\n");
        out.push_str(&format!(
            "lakewing_http_requests_total {}\n",
            self.requests.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP lakewing_http_in_flight Requests currently executing.\n# TYPE lakewing_http_in_flight gauge\n");
        out.push_str(&format!(
            "lakewing_http_in_flight {}\n",
            self.in_flight.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP lakewing_lance_dataset_version Pinned serving dataset version.\n# TYPE lakewing_lance_dataset_version gauge\n");
        out.push_str(&format!(
            "lakewing_lance_dataset_version {}\n",
            self.dataset_version.load(Ordering::Relaxed)
        ));
        out
    }
}
