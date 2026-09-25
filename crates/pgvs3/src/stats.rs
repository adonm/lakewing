//! GET telemetry: span histogram, latency percentiles, per-class TTFB/total,
//! parts and pool waits. Read via `stage_stats_line` (the stats route and the
//! log timer print it). Kept out of `db` so the read/write paths don't carry
//! observability noise.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Spans up to this size are one query, and "small" in the stats.
pub const SMALL_MAX: usize = 8 << 20;

static SPANS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
// GET latency histogram: quarter-octave buckets of microseconds (~19%
// resolution), read back as p50/p95/p99.
static LAT: [AtomicU64; 128] = [const { AtomicU64::new(0) }; 128];

/// Per span class (0 = small, 1 = larger): GETs, summed time to the first byte
/// handed to the response, summed time to the last.
static GETS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static TTFB_US: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static TOTAL_US: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
// Per part query, both paths: count and pool-acquire wait (summed, so it can
// exceed wall time under concurrency; growth means the pool is the limit).
static PARTS: AtomicU64 = AtomicU64::new(0);
static WAIT_US: AtomicU64 = AtomicU64::new(0);
static SERVED: AtomicU64 = AtomicU64::new(0);

/// Span-size histogram buckets: <64K <512K <2M <8M >=8M (the workload's read
/// shapes; the last boundary is SMALL_MAX).
fn span_bucket(span: usize) -> usize {
    const T: [usize; 4] = [65536, 524_288, 2_097_152, 8_388_608];
    T.iter().position(|&t| span < t).unwrap_or(4)
}

pub fn span_record(len: usize) {
    SPANS[span_bucket(len)].fetch_add(1, Relaxed);
}

pub fn lat_record(us: u64) {
    let idx = ((us.max(1) as f64).log2() * 4.0) as usize;
    LAT[idx.min(127)].fetch_add(1, Relaxed);
}

/// Class 0 = small, 1 = larger: bump TTFB once the response starts.
pub fn ttfb_record(class: usize, us: u64) {
    TTFB_US[class].fetch_add(us, Relaxed);
}

/// Class 0 = small, 1 = larger: bump total once the body is fully handed over.
pub fn get_record(class: usize, us: u64, served: u64) {
    GETS[class].fetch_add(1, Relaxed);
    TOTAL_US[class].fetch_add(us, Relaxed);
    lat_record(us);
    SERVED.fetch_add(served, Relaxed);
}

pub fn part_record(wait_us: u64) {
    PARTS.fetch_add(1, Relaxed);
    WAIT_US.fetch_add(wait_us, Relaxed);
}

/// Upper bound (ms) of the bucket holding the `p` quantile.
fn lat_pct(counts: &[u64], p: f64) -> f64 {
    let total: u64 = counts.iter().sum();
    let target = (total as f64 * p).ceil() as u64;
    let mut seen = 0;
    for (i, c) in counts.iter().enumerate() {
        seen += c;
        if total > 0 && seen >= target {
            return 2f64.powf((i + 1) as f64 / 4.0) / 1e3;
        }
    }
    0.0
}

/// `spans=[..] small n=.. ttfb=.. total=.. | stream n=.. ttfb=.. total=.. | parts=.. wait(sum)=.. | served=..`
pub fn stage_stats_line() -> String {
    let ms = |a: &AtomicU64| a.load(Relaxed) as f64 / 1e3;
    let s: Vec<u64> = SPANS.iter().map(|a| a.load(Relaxed)).collect();
    let lat: Vec<u64> = LAT.iter().map(|a| a.load(Relaxed)).collect();
    format!(
        "perf: pid={} spans=[<64K:{} <512K:{} <2M:{} <8M:{} >=8M:{}] small n={} ttfb={:.0}ms total={:.0}ms | stream n={} ttfb={:.0}ms total={:.0}ms | parts={} wait(sum)={:.0}ms | served={}MiB | get p50={:.1}ms p95={:.1}ms p99={:.1}ms",
        std::process::id(),
        s[0],
        s[1],
        s[2],
        s[3],
        s[4],
        GETS[0].load(Relaxed),
        ms(&TTFB_US[0]),
        ms(&TOTAL_US[0]),
        GETS[1].load(Relaxed),
        ms(&TTFB_US[1]),
        ms(&TOTAL_US[1]),
        PARTS.load(Relaxed),
        ms(&WAIT_US),
        SERVED.load(Relaxed) >> 20,
        lat_pct(&lat, 0.50),
        lat_pct(&lat, 0.95),
        lat_pct(&lat, 0.99),
    )
}
