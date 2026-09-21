//! Rendered-response cache: a bounded in-process foyer cache of complete
//! response bodies keyed by (pinned snapshot, canonical selection). A warm
//! repeat skips Lance, DuckDB, JSON assembly, gzip and ETag recompute and
//! answers from memory — the response bytes are deterministic for a pinned
//! dataset version, so entries cannot go stale within a process lifetime
//! (publication restarts the process, and `Cache-Control: public` already
//! invites client/CDN reuse of exactly these bytes).
//!
//! Scope: only successful (`200`) geo+json and MVT bodies. Errors are never
//! cached. Entries above `MAX_ENTRY_BYTES` are not stored so one giant page
//! cannot evict a working set of hot small ones. Cache hits bypass admission
//! (they hold no worker and touch no storage); concurrency for misses is
//! still bounded by the shared semaphore. Concurrent misses may render the
//! same page redundantly (first-touch herd) — the origin-level herd is
//! already single-flighted by the foyer range cache under Lance.
use bytes::Bytes;
use foyer::Cache;

#[derive(Clone)]
pub struct Rendered {
    pub body: Bytes,
    pub content_type: &'static str,
    /// Identity responses only (`gz=false` tiles included): gzip variants
    /// are negotiated per request as before.
    pub gz: bool,
}

pub struct ResponseCache {
    cache: Cache<String, Rendered>,
}

/// Don't store entries larger than this: keep eviction proportional to hot
/// small pages, and the payload budget already caps pages at 64 MiB.
pub const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;

impl ResponseCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let cache = foyer::CacheBuilder::new(capacity_bytes)
            .with_weighter(|key: &String, value: &Rendered| {
                key.len() + value.body.len() + value.etag_len() + 96
            })
            .build();
        Self { cache }
    }

    pub fn get(&self, key: &str) -> Option<Rendered> {
        self.cache.get(key).map(|entry| entry.value().clone())
    }

    pub fn insert(&self, key: String, rendered: Rendered) {
        if rendered.body.len() <= MAX_ENTRY_BYTES {
            self.cache.insert(key, rendered);
        }
    }
}

impl Rendered {
    fn etag_len(&self) -> usize {
        // fnv-1a hex + length suffix; approximate is fine for the weighter.
        32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_above_the_cap_are_not_stored() {
        let cache = ResponseCache::new(1024 * 1024);
        cache.insert(
            "big".into(),
            Rendered {
                body: Bytes::from(vec![0u8; MAX_ENTRY_BYTES + 1]),
                content_type: "application/geo+json",
                gz: true,
            },
        );
        assert!(cache.get("big").is_none());
        cache.insert(
            "small".into(),
            Rendered {
                body: Bytes::from_static(b"{}"),
                content_type: "application/geo+json",
                gz: true,
            },
        );
        assert_eq!(cache.get("small").unwrap().body.as_ref(), b"{}");
    }
}
