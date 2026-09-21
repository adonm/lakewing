//! Low-cardinality request counters and latency histogram.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const STATUSES: [u16; 11] = [200, 204, 304, 400, 404, 409, 413, 429, 500, 503, 504];
const BUCKETS_MS: [u64; 10] = [5, 10, 25, 50, 100, 200, 500, 1000, 5000, 30000];

pub struct Metrics {
    requests: AtomicU64,
    flight_requests: AtomicU64,
    in_flight: AtomicU64,
    responses: [AtomicU64; 11],
    latency: [AtomicU64; 10],
    duration_us: AtomicU64,
    completed: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    pub dataset_version: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            flight_requests: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            responses: std::array::from_fn(|_| AtomicU64::new(0)),
            latency: std::array::from_fn(|_| AtomicU64::new(0)),
            duration_us: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            dataset_version: AtomicU64::new(0),
        }
    }

    pub fn count_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn count_flight(&self) {
        self.flight_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn count_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn count_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    pub fn enter(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }
    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn count(&self, status: u16) {
        let slot = STATUSES.iter().position(|s| *s == status).unwrap_or(8);
        self.responses[slot].fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe(&self, elapsed: Duration) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.duration_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        for (bound, count) in BUCKETS_MS.iter().zip(&self.latency) {
            if elapsed <= Duration::from_millis(*bound) {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn exposition(&self) -> String {
        let mut out = String::new();
        for (name, kind, value) in [
            ("http_requests_total", "counter", &self.requests),
            ("flight_requests_total", "counter", &self.flight_requests),
            ("in_flight", "gauge", &self.in_flight),
            ("lance_dataset_version", "gauge", &self.dataset_version),
            ("response_cache_hits_total", "counter", &self.cache_hits),
            ("response_cache_misses_total", "counter", &self.cache_misses),
        ] {
            out.push_str(&format!(
                "# TYPE lakewing_{name} {kind}\nlakewing_{name} {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
        out.push_str("# TYPE lakewing_http_responses_total counter\n");
        for (status, value) in STATUSES.iter().zip(&self.responses) {
            out.push_str(&format!(
                "lakewing_http_responses_total{{status=\"{status}\"}} {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
        out.push_str("# TYPE lakewing_http_duration_seconds histogram\n");
        for (bound, count) in BUCKETS_MS.iter().zip(&self.latency) {
            out.push_str(&format!(
                "lakewing_http_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                *bound as f64 / 1000.0,
                count.load(Ordering::Relaxed)
            ));
        }
        let count = self.completed.load(Ordering::Relaxed);
        out.push_str(&format!("lakewing_http_duration_seconds_bucket{{le=\"+Inf\"}} {count}\nlakewing_http_duration_seconds_count {count}\nlakewing_http_duration_seconds_sum {}\n", self.duration_us.load(Ordering::Relaxed) as f64 / 1_000_000.0));
        out
    }
}
