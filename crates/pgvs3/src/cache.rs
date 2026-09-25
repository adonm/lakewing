//! Pinned metadata cache: bucket/key -> Meta, count-capped, never evicted by
//! byte pressure. Its only job is removing the lookup round trip per GET.
//!
//! Coherence: a write through this process updates or drops its entry, and a
//! GET that finds stale rows (missing) or a stale size (range clamped into a
//! bogus 416) re-resolves and retries before failing. `HEAD` has no such
//! check, so after an overwrite it can serve the old metadata until the entry
//! is evicted — fine for the design center (objects are immutable), wrong for
//! hot keys.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

/// Object metadata: enough to serve a range without re-reading `s3p.objects`.
#[derive(Debug, Clone)]
pub struct Meta {
    pub size: i64,
    pub etag: Vec<u8>,
    pub created_at: SystemTime,
    pub file_id: i64,
    /// Multipart objects: part file_ids in order and cumulative end offsets.
    /// None = one file (`file_id`) holds all `size` bytes.
    pub parts: Option<Vec<i64>>,
    pub part_ends: Option<Vec<i64>>,
}

impl Meta {
    /// `(file_id, object byte offset, length)` of each stored segment.
    pub fn segments(&self) -> Vec<(i64, i64, i64)> {
        match (&self.parts, &self.part_ends) {
            (Some(ids), Some(ends)) if ids.len() == ends.len() => {
                let mut prev = 0;
                ids.iter()
                    .zip(ends)
                    .map(|(&id, &end)| {
                        let seg = (id, prev, end - prev);
                        prev = end;
                        seg
                    })
                    .collect()
            }
            _ => vec![(self.file_id, 0, self.size)],
        }
    }
}

const META_CAP: usize = 1 << 18;

fn cache() -> &'static Mutex<MetaCache> {
    static C: OnceLock<Mutex<MetaCache>> = OnceLock::new();
    C.get_or_init(|| {
        Mutex::new(MetaCache {
            map: HashMap::new(),
            order: VecDeque::new(),
        })
    })
}

struct MetaCache {
    map: HashMap<(String, String), Meta>,
    order: VecDeque<(String, String)>,
}

pub fn meta_get(bucket: &str, key: &str) -> Option<Meta> {
    cache()
        .lock()
        .unwrap()
        .map
        .get(&(bucket.to_owned(), key.to_owned()))
        .cloned()
}

pub fn meta_put(bucket: &str, key: &str, meta: Meta) {
    let mut c = cache().lock().unwrap();
    let k = (bucket.to_owned(), key.to_owned());
    if c.map.insert(k.clone(), meta).is_none() {
        c.order.push_back(k);
        if c.map.len() > META_CAP {
            if let Some(old) = c.order.pop_front() {
                c.map.remove(&old);
            }
        }
    }
}

pub fn meta_invalidate(bucket: &str, key: &str) {
    cache()
        .lock()
        .unwrap()
        .map
        .remove(&(bucket.to_owned(), key.to_owned()));
}

/// Timestamp from a `EXTRACT(EPOCH FROM ...)` float.
pub fn epoch(secs: f64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs_f64(secs.max(0.0))
}
