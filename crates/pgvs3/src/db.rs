//! PostgreSQL storage: objects as fixed-size inline byte rows.
//!
//! Read path: object metadata (bucket,key -> file_id/size/etag) and row slices
//! are cached in-process within a small fixed budget (PGVS3_CACHE_MIB, default
//! 2048): the cache sits between consumers (DuckLake workers own the big byte
//! cache) and Aurora, purely to remove round trips. Objects are immutable by
//! convention, so cache entries are populated at publish and dropped at
//! delete; repeat reads (parquet footers, hot ranges) reach PostgreSQL zero
//! times. Spans above PGVS3_SPLIT_BYTES (default 8 MiB) fetch in parallel
//! parts on separate pool connections: one Aurora connection moves ~420-500
//! MB/s warm / ~300 MB/s cold, which binds a lone large read. At DuckDB's
//! concurrency (many GETs already in flight) smaller parts measured no gain
//! and 512 KiB parts hurt, so only big spans fan out.
//!
//! Write path: deliberately single-threaded per process — one binary COPY
//! stream at a time (a global flush permit), rows cut across multipart part
//! boundaries. Aurora's write ceiling is ~150 MiB/s regardless of stream
//! count, so write parallelism buys latency-per-file at the price of every
//! byte of machinery; instead each flush takes the whole pipe and the rest
//! queue. The object row is published last, so objects appear atomically.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Context;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row, Transaction};

pub const SCHEMA: &str = include_str!("../schema.sql");

/// Row payload: file_id(8) + no(4) + varlena(4) + 8120 = 8136 data bytes,
/// tuple 8160 bytes = one row per 8 KB page.
pub const ROW_BYTES: i64 = 8120;

/// Rows per multi-row statement batch (520 KiB per round trip).
const ROW_BATCH: usize = 64;

const COPY_SQL: &str = "COPY s3p.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)";
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";
const COPY_TRAILER: &[u8] = &[0xFF, 0xFF];
const SEND_BATCH: usize = 4 << 20;

/// Whole rows for a contiguous `no` range (cheaper than `= ANY` on Aurora:
/// 1.74 vs 2.02 ms server time per warm 8 MiB span).
const GET_RANGE_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3";

/// Whole rows for scattered `no`s (cache misses interleaved with hits).
const GET_ROWS_FULL_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no = ANY($2::int4[])";

/// Parts in flight per request, each on its own pool connection.
const SPLIT_INFLIGHT: usize = 8;
/// Pass-through parts never buffer more than ~8 MiB each.
const STREAM_PART_MAX_ROWS: usize = 1024;

/// Rows per parallel part (PGVS3_SPLIT_BYTES, default 8 MiB = only spans above
/// the admission gate split; 0 disables splitting entirely, for A/B runs).
fn split_rows() -> Option<usize> {
    static N: OnceLock<Option<usize>> = OnceLock::new();
    *N.get_or_init(|| {
        let bytes = std::env::var("PGVS3_SPLIT_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8 << 20);
        (bytes > 0).then(|| bytes.div_ceil(ROW_BYTES as usize))
    })
}

pub async fn connect(url: &str) -> Result<PgPool> {
    // Bitmap heap scans hand the row-span TIDs to PostgreSQL 18's async
    // read-stream prefetch: cold >RAM range reads measured 5x faster than
    // one-page-at-a-time index scans (256KiB GET p50 14.1ms -> 2.8ms), with no
    // warm cost. Opt back into index scans with PGVS3_INDEXSCAN=1.
    let indexscan = std::env::var_os("PGVS3_INDEXSCAN").is_some();
    // Aurora round trips dominate small operations; a synchronous_commit per
    // transaction pays one every write. Objects are immutable and
    // sha256-idempotent, so commit-ack loss only ever replays a PUT. Set
    // PGVS3_DURABLE=1 to require synchronous commits.
    let durable = std::env::var_os("PGVS3_DURABLE").is_some();
    Ok(PgPoolOptions::new()
        // Parallel parts multiply connection demand; Aurora allows ~1700.
        .max_connections(256)
        // Warm pool: cold-pool setup (TCP + TLS + SCRAM + session SETs to
        // Aurora) showed up as 10-13 ms average acquire wait per part (~40% of
        // fetch time) in the c7gn A/B; keep DuckDB's in-flight GETs covered.
        .min_connections(32)
        // sqlx pings every connection on acquire by default (sqlx-core 0.8.6
        // pool/options.rs) - a full Aurora round trip per GET. Broken
        // connections still surface on use and get recycled.
        .test_before_acquire(false)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // Whole session setup in one round trip (simple-query batch).
                // work_mem keeps bitmap TID maps exact; effective_io_concurrency
                // deepens bitmap read-stream prefetch (no measured gain past 32
                // on Aurora).
                let sets: &'static str = match (indexscan, durable) {
                    (false, false) => "SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
                                       SET enable_indexscan = off; SET synchronous_commit = off;",
                    (false, true) => "SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
                                      SET enable_indexscan = off;",
                    (true, false) => "SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
                                      SET synchronous_commit = off;",
                    (true, true) => "SET work_mem = '64MB'; SET effective_io_concurrency = 32;",
                };
                // No io_combine_limit: PG18 clamps it to the postmaster-level
                // io_max_combine_limit (commands/variable.c assign hook; 168 kB
                // on Aurora, 128 kB stock), so a session SET above that is a
                // silent no-op. Raising it needs a parameter-group change.
                sqlx::Executor::execute(&mut *conn, sets).await?;
                Ok(())
            })
        })
        .connect(url)
        .await?)
}

pub async fn init(pool: &PgPool) -> Result<()> {
    // PGVS3_UNLOGGED=1: skip WAL on the chunk table (bulk of all bytes).
    // Opt-in: unlogged tables are writer-scoped and their crash semantics are
    // deployment-specific. Objects stay logged (metadata + etags).
    let schema = if std::env::var_os("PGVS3_UNLOGGED").is_some() {
        SCHEMA.replace(
            "CREATE TABLE IF NOT EXISTS s3p.chunks",
            "CREATE UNLOGGED TABLE IF NOT EXISTS s3p.chunks",
        )
    } else {
        SCHEMA.to_owned()
    };
    sqlx::raw_sql(&schema).execute(pool).await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Meta {
    pub size: i64,
    pub etag: Vec<u8>,
    pub created_at: SystemTime,
    pub file_id: i64,
}

/// Everything but the body of a served range: `[start, end]` inclusive.
pub struct SliceMeta {
    pub size: i64,
    pub etag: Vec<u8>,
    pub created_at: SystemTime,
    pub file_id: i64,
    pub start: i64,
    pub end: i64,
}

impl SliceMeta {
    pub fn len(&self) -> i64 {
        (self.end - self.start + 1).max(0)
    }
}

/// Streamed slice pieces, in `no` order. Bounded channel back to the fetching
/// task, so at most a few row batches are ever in flight.
pub struct PieceStream {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>,
}

impl Stream for PieceStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn epoch(secs: f64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs_f64(secs.max(0.0))
}

// ---------------------------------------------------------------------------
// Small in-process caches between consumers (DuckLake workers own the big byte
// cache) and Aurora: their only job is removing round trips. Meta is the
// pinned tier (count-capped, never evicted by byte pressure). Rows use S3-FIFO
// with a span-size gate: the gate only exists to keep scan-flood bytes out of
// the machinery, while frequency-based promotion (second touch, or ghost
// re-arrival) is what pins hot spans. Tuned for analytical workloads
// (ClickBench / SpatialBench on DuckLake): the recurring hot unit there is a
// column chunk (100 KiB–8 MiB), so admission is span-bounded
// (PGVS3_ADMIT_BYTES, default 8 MiB) rather than tiny — hot chunks of any
// reasonable size earn residency on a second read, geometry floods pass
// through. PGVS3_CACHE_MIB=0 disables the row tier entirely.
// ---------------------------------------------------------------------------

const META_CAP: usize = 1 << 18;

fn meta_cache() -> &'static Mutex<MetaCache> {
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

fn meta_put(bucket: &str, key: &str, meta: Meta) {
    let mut c = meta_cache().lock().unwrap();
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

fn meta_invalidate(bucket: &str, key: &str) {
    meta_cache().lock().unwrap().map.remove(&(bucket.to_owned(), key.to_owned()));
}

struct Node {
    data: Bytes,
    freq: u8, // saturating access counter (0..=3)
}

/// S3-FIFO (Yang et al., SOSP'23): small FIFO `s` (10%), main queue `m` (90%)
/// with CLOCK second-chance on the freq counter, and a ghost list `g` of
/// recently evicted keys. New rows enter `s`; only a second touch (freq > 1 at
/// eviction) or a ghost re-arrival earns `m` residency. One-timers — including
/// any medium reads that slip the admission gate — die in `s`/`g` without
/// displacing pinned footers. All operations O(1); no recency bookkeeping.
struct RowCache {
    s: HashMap<(i64, i32), Node>,
    s_order: VecDeque<(i64, i32)>,
    s_used: usize,
    m: HashMap<(i64, i32), Node>,
    m_order: VecDeque<(i64, i32)>,
    m_used: usize,
    g: std::collections::HashSet<(i64, i32)>,
    g_order: VecDeque<(i64, i32)>,
    budget: usize,
    enabled: bool,
    // per-span-size counters for gate tuning (printed by the stats line)
    pub spans: [u64; 5],
    pub hits: u64,
    pub admits: u64,
    pub promotes: u64,
    pub admits_ghost: u64,
}

const FREQ_MAX: u8 = 3;

fn row_cache() -> &'static Mutex<RowCache> {
    static C: OnceLock<Mutex<RowCache>> = OnceLock::new();
    C.get_or_init(|| {
        let mib = std::env::var("PGVS3_CACHE_MIB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2048);
        let budget = mib * 1024 * 1024;
        Mutex::new(RowCache {
            s: HashMap::new(),
            s_order: VecDeque::new(),
            s_used: 0,
            m: HashMap::new(),
            m_order: VecDeque::new(),
            m_used: 0,
            g: std::collections::HashSet::new(),
            g_order: VecDeque::new(),
            budget,
            enabled: budget > 0,
            spans: [0; 5],
            hits: 0,
            promotes: 0,
            admits: 0,
            admits_ghost: 0,
        })
    })
}

fn ghost_put(c: &mut RowCache, k: (i64, i32)) {
    if c.g.insert(k) {
        c.g_order.push_back(k);
    }
    let cap = (c.budget / ROW_BYTES as usize / 10) * 9; // ~90% of slot count
    while c.g_order.len() > cap {
        match c.g_order.pop_front() {
            Some(old) => {
                c.g.remove(&old);
            }
            None => break,
        }
    }
}

/// Evict/promote until bytes fit. `s` drains first above its 10% quota;
/// second touches promote to `m`, one-timers ghost out; `m` uses CLOCK.
fn evict_to_fit(c: &mut RowCache) {
    while c.s_used + c.m_used > c.budget {
        let s_quota = c.budget / 10;
        if !c.s_order.is_empty() && (c.s_used > s_quota || c.m_order.is_empty()) {
            let front = c.s_order.pop_front().unwrap();
            if let Some(node) = c.s.remove(&front) {
                c.s_used -= node.data.len();
                if node.freq > 1 {
                    c.promotes += 1;
                    c.m_used += node.data.len();
                    c.m.insert(front, node);
                    c.m_order.push_back(front);
                } else {
                    ghost_put(c, front);
                }
            }
            continue;
        }
        let Some(front) = c.m_order.front().copied() else { break };
        c.m_order.pop_front();
        let Some(node) = c.m.get_mut(&front) else { continue };
        if node.freq == 0 {
            let Some(gone) = c.m.remove(&front) else { continue };
            c.m_used -= gone.data.len();
            ghost_put(c, front);
        } else {
            node.freq -= 1; // second chance
            c.m_order.push_back(front);
        }
    }
}

fn row_put(file_id: i64, no: i32, bytes: Bytes) {
    let mut c = row_cache().lock().unwrap();
    if !c.enabled || bytes.len() > c.budget {
        return;
    }
    let k = (file_id, no);
    if c.s.contains_key(&k) || c.m.contains_key(&k) {
        return;
    }
    c.admits += 1;
    let ghost = c.g.remove(&k);
    if ghost {
        // Seen recently and evicted: it earned main-queue residency.
        c.admits_ghost += 1;
        c.m_used += bytes.len();
        c.m.insert(k, Node { data: bytes, freq: 1 });
        c.m_order.push_back(k);
    } else {
        c.s_used += bytes.len();
        c.s.insert(k, Node { data: bytes, freq: 0 });
        c.s_order.push_back(k);
    }
    evict_to_fit(&mut c);
}

fn row_get(file_id: i64, no: i32) -> Option<Bytes> {
    let mut c = row_cache().lock().unwrap();
    if !c.enabled {
        return None;
    }
    let k = (file_id, no);
    let hit = if let Some(node) = c.s.get_mut(&k) {
        node.freq = node.freq.saturating_add(1).min(FREQ_MAX);
        Some(node.data.clone())
    } else if let Some(node) = c.m.get_mut(&k) {
        node.freq = node.freq.saturating_add(1).min(FREQ_MAX);
        Some(node.data.clone())
    } else {
        None
    };
    if hit.is_some() {
        c.hits += 1;
    }
    hit
}

// ---------------------------------------------------------------------------
// GET stage attribution for bottleneck hunting: time awaiting PostgreSQL
// versus local cache/decode/assembly, and pass-through stream time. Read via
// `stage_stats_line` (the stats route and the log timer print it).
// ---------------------------------------------------------------------------
// Small path: wall time per request and of its (parallel) fetch phase.
static SMALL_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static FETCH_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SMALL_N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static STREAM_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static STREAM_N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
// Per part query, both paths: count and pool-acquire wait (summed, so it can
// exceed wall time under concurrency; growth means the pool is the limit).
static PARTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SERVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `small total=.. fetch=.. local=.. | stream=.. | parts=.. wait(sum)=.. | served=..`
pub fn stage_stats_line() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let ms = |us: u64| us as f64 / 1e3;
    let total = SMALL_US.load(Relaxed);
    let fetch = FETCH_US.load(Relaxed);
    format!(
        "perf: small total={:.0}ms fetch={:.0}ms local={:.0}ms n={} | stream={:.0}ms n={} | parts={} wait(sum)={:.0}ms | served={}MiB",
        ms(total),
        ms(fetch),
        ms(total.saturating_sub(fetch)),
        SMALL_N.load(Relaxed),
        ms(STREAM_US.load(Relaxed)),
        STREAM_N.load(Relaxed),
        PARTS.load(Relaxed),
        ms(WAIT_US.load(Relaxed)),
        SERVED.load(Relaxed) / 1024 / 1024,
    )
}

/// Span-size buckets for the tuning counters: the gate lives at the last
/// boundary.
fn span_bucket(span: usize) -> usize {
    const T: [usize; 4] = [65536, 524_288, 2_097_152, 8_388_608];
    T.iter().position(|&t| span < t).unwrap_or(4)
}

fn admit_bytes() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGVS3_ADMIT_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8 * 1024 * 1024)
    })
}

/// One-line cache telemetry for tuning PGVS3_ADMIT_BYTES / PGVS3_CACHE_MIB.
pub fn cache_stats_line() -> String {
    let c = row_cache().lock().unwrap();
    format!(
        "cache: spans=[<64K:{} <512K:{} <2M:{} <8M:{} >=8M:{}] hits={} admits={} (ghost={} promotes={}) tiers=s:{}B/m:{}B",
        c.spans[0], c.spans[1], c.spans[2], c.spans[3], c.spans[4],
        c.hits, c.admits, c.admits_ghost, c.promotes, c.s_used, c.m_used
    )
}

// ---------------------------------------------------------------------------

/// Metadata lookup, cached (a repeat open costs no round trip).
pub async fn meta(pool: &PgPool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    if let Some(m) = meta_cache().lock().unwrap().map.get(&(bucket.to_owned(), key.to_owned())) {
        return Ok(Some(m.clone()));
    }
    let row = sqlx::query(
        "SELECT file_id, size, etag, EXTRACT(EPOCH FROM created_at)::float8 AS created_epoch \
         FROM s3p.objects WHERE bucket = $1 AND key = $2",
    )
    .bind(bucket)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    let meta = row.map(|r| Meta {
        file_id: r.get("file_id"),
        size: r.get("size"),
        etag: r.get("etag"),
        created_at: epoch(r.get::<f64, _>("created_epoch")),
    });
    if let Some(m) = &meta {
        meta_put(bucket, key, m.clone());
    }
    Ok(meta)
}

/// One object row (no caching of the row itself: callers manage that).
async fn object_row(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    bucket: &str,
    key: &str,
) -> Result<Option<i64>> {
    Ok(sqlx::query("SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2")
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?
        .map(|r| r.get("file_id")))
}

/// Serve `[start, end]` of an object. Spans above the admission gate stream
/// straight through (no cache reads or writes — their bytes live in DuckLake's
/// buffer); small spans take the fast path: rows gathered in memory (2Q cache
/// first, one query for misses) and concatenated — no channel hop, no task.
pub async fn get_body(
    pool: PgPool,
    bucket: String,
    key: String,
    first: i64,
    last: i64,
    suffix: i64,
) -> Result<Option<(SliceMeta, PieceBody)>> {
    let Some(m) = meta(&pool, &bucket, &key).await? else { return Ok(None) };
    let (start, end) = eff_range(m.size, first, last, suffix);
    let smeta = SliceMeta {
        size: m.size,
        etag: m.etag.clone(),
        created_at: m.created_at,
        file_id: m.file_id,
        start,
        end,
    };
    let first_row = (start / ROW_BYTES) as i32;
    let last_row = (end / ROW_BYTES) as i32;
    let nrows = (last_row - first_row + 1) as usize;
    let bkt = span_bucket(smeta.len() as usize);
    row_cache().lock().unwrap().spans[bkt] += 1;

    if (smeta.len() as usize) <= admit_bytes() {
        use std::sync::atomic::Ordering::Relaxed;
        let t0 = std::time::Instant::now();
        let rows = gather_rows(&pool, &m, first_row, nrows).await?;
        let mut out = BytesMut::with_capacity(smeta.len() as usize);
        for (i, data) in rows.iter().enumerate() {
            let no = first_row + i as i32;
            let row_start = i64::from(no) * ROW_BYTES;
            let lo = start.max(row_start) - row_start;
            let hi = end.min(row_start + data.len() as i64 - 1) - row_start;
            if hi >= lo {
                out.extend_from_slice(&data[lo as usize..=hi as usize]);
            }
        }
        SMALL_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
        SMALL_N.fetch_add(1, Relaxed);
        SERVED.fetch_add(smeta.len() as u64, Relaxed);
        Ok(Some((smeta, PieceBody::OneShot(out.freeze()))))
    } else {
        // Room for SPLIT_INFLIGHT parts of row pieces, so part fetches keep
        // flowing while the body drains.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1024);
        tokio::spawn(async move {
            if let Err(e) = stream_pass_through(&pool, &m, start, end, first_row, last_row, &tx).await {
                let _ = tx.send(Err(io_err(e))).await;
            }
        });
        Ok(Some((smeta, PieceBody::Streamed(PieceStream { rx }))))
    }
}

/// Response body for a served range: one buffer for small spans, a stream for
/// pass-through spans.
pub enum PieceBody {
    OneShot(Bytes),
    Streamed(PieceStream),
}

/// Whole-row gather for small spans: cache first, then the misses as parallel
/// parts (split_rows each, SPLIT_INFLIGHT at a time).
async fn gather_rows(pool: &PgPool, m: &Meta, first_row: i32, nrows: usize) -> Result<Vec<Bytes>> {
    let mut rows: Vec<Option<Bytes>> = vec![None; nrows];
    let mut missing: Vec<i32> = Vec::new();
    for (i, slot) in rows.iter_mut().enumerate() {
        let no = first_row + i as i32;
        match row_get(m.file_id, no) {
            Some(r) => *slot = Some(r),
            None => missing.push(no),
        }
    }
    if !missing.is_empty() {
        let t0 = std::time::Instant::now();
        let file_id = m.file_id;
        // Owned per-part `no` lists: futures over borrowed slices are
        // higher-ranked and trip the Send check of spawned callers.
        let chunks = missing.chunks(split_rows().unwrap_or(usize::MAX)).map(<[i32]>::to_vec);
        let mut parts = futures::stream::iter(chunks)
            .map(|nos| async move { fetch_part(pool, file_id, &nos).await })
            .buffer_unordered(SPLIT_INFLIGHT);
        while let Some(part) = parts.next().await {
            for (no, data) in part? {
                row_put(m.file_id, no, data.clone());
                if let Some(slot) = rows.get_mut((no - first_row) as usize) {
                    *slot = Some(data);
                }
            }
        }
        FETCH_US.fetch_add(t0.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(rows.into_iter().map(|p| p.unwrap_or_default()).collect())
}

/// One part query on its own pool connection; `nos` ascending. Contiguous
/// runs use the range predicate, scattered misses `= ANY`.
async fn fetch_part(pool: &PgPool, file_id: i64, nos: &[i32]) -> Result<Vec<(i32, Bytes)>> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = std::time::Instant::now();
    let mut conn = pool.acquire().await?;
    WAIT_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    PARTS.fetch_add(1, Relaxed);
    let (lo, hi) = (nos[0], nos[nos.len() - 1]);
    let got = if (hi - lo + 1) as usize == nos.len() {
        sqlx::query(GET_RANGE_SQL).bind(file_id).bind(lo).bind(hi).fetch_all(&mut *conn).await?
    } else {
        sqlx::query(GET_ROWS_FULL_SQL).bind(file_id).bind(nos).fetch_all(&mut *conn).await?
    };
    let mut out = Vec::with_capacity(got.len());
    for r in got {
        let raw = r.try_get_raw("data")?;
        out.push((r.get("no"), Bytes::copy_from_slice(raw.as_bytes().unwrap_or(&[]))));
    }
    Ok(out)
}

/// Effective `[start, end]` inclusive for a request (mirrors the old SQL clamp).
fn eff_range(size: i64, first: i64, last: i64, suffix: i64) -> (i64, i64) {
    if suffix >= 0 {
        ((size - suffix).max(0), size - 1)
    } else {
        (first.max(0), if last >= 0 { last.min(size - 1) } else { size - 1 })
    }
}

/// Large-span path (never touches the cache): the span splits into parts
/// fetched SPLIT_INFLIGHT at a time on separate connections and emitted in
/// submission order (`buffered`); each part is sorted locally (bitmap heap
/// scans do not guarantee `no` order) and its edge rows sliced to the range.
async fn stream_pass_through(
    pool: &PgPool,
    m: &Meta,
    start: i64,
    end: i64,
    first_row: i32,
    last_row: i32,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let t0 = std::time::Instant::now();
    let r = stream_pass_inner(pool, m, start, end, first_row, last_row, tx).await;
    use std::sync::atomic::Ordering::Relaxed;
    STREAM_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    STREAM_N.fetch_add(1, Relaxed);
    SERVED.fetch_add((end - start + 1) as u64, Relaxed);
    r
}

#[allow(clippy::too_many_arguments)]
async fn stream_pass_inner(
    pool: &PgPool,
    m: &Meta,
    start: i64,
    end: i64,
    first_row: i32,
    last_row: i32,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let (step, inflight) = match split_rows() {
        Some(n) => (n.min(STREAM_PART_MAX_ROWS), SPLIT_INFLIGHT),
        None => (STREAM_PART_MAX_ROWS, 1), // A/B baseline: one query at a time
    };
    let file_id = m.file_id;
    let ranges = (first_row..=last_row)
        .step_by(step)
        .map(move |lo| (lo..=(lo + step as i32 - 1).min(last_row)).collect::<Vec<i32>>());
    let mut parts = futures::stream::iter(ranges)
        .map(|nos| async move { fetch_part(pool, file_id, &nos).await })
        .buffered(inflight);
    while let Some(part) = parts.next().await {
        let mut part = part?;
        part.sort_unstable_by_key(|(no, _)| *no);
        for (no, data) in part {
            let row_start = i64::from(no) * ROW_BYTES;
            let lo = start.max(row_start) - row_start;
            let hi = end.min(row_start + data.len() as i64 - 1) - row_start;
            if hi >= lo && tx.send(Ok(data.slice(lo as usize..=hi as usize))).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Buffered variant (tests, bench floor).
pub async fn get(pool: &PgPool, bucket: &str, key: &str, first: i64, last: i64, suffix: i64) -> Result<Option<Slice>> {
    let Some((meta, body)) =
        get_body(pool.clone(), bucket.to_owned(), key.to_owned(), first, last, suffix).await?
    else {
        return Ok(None);
    };
    let bytes = match body {
        PieceBody::OneShot(b) => b,
        PieceBody::Streamed(mut s) => {
            let mut out = BytesMut::with_capacity(meta.len() as usize);
            while let Some(piece) = s.next().await {
                let piece = piece?;
                out.extend_from_slice(&piece);
            }
            out.freeze()
        }
    };
    Ok(Some(Slice {
        size: meta.size,
        etag: meta.etag,
        created_at: meta.created_at,
        start: meta.start,
        end: meta.end,
        bytes,
    }))
}

/// A served byte range, fully buffered.
pub struct Slice {
    pub size: i64,
    pub etag: Vec<u8>,
    pub created_at: SystemTime,
    pub start: i64,
    pub end: i64,
    pub bytes: Bytes,
}

/// Overwrite-or-create through the ingest pipeline — the single write path
/// for buffered and multipart writes alike. The ETag is the caller's sha256
/// (idempotent retries); visibility is atomic via `publish`.
pub async fn put(pool: &PgPool, bucket: &str, key: &str, data: &[u8], etag: &[u8]) -> Result<()> {
    let writer = ChunkWriter::start(pool.clone()).await?;
    let file_id = writer.file_id;
    for chunk in data.chunks(SEND_BATCH) {
        writer.push(Bytes::copy_from_slice(chunk)).await?;
    }
    let (size, _sum) = writer.finish().await?;
    publish(pool, bucket, key, file_id, size, etag).await
}

/// Streaming ingest: bytes flow into one open binary COPY stream through
/// `push` (rows cut across pushes through a single cursor). One ingest at a
/// time holds the global write permit — writes stay single-threaded per
/// process by design.
pub struct ChunkWriter {
    pub file_id: i64,
    tx: Option<tokio::sync::mpsc::Sender<IngestMsg>>,
    done: Option<tokio::task::JoinHandle<Result<(i64, Vec<u8>)>>>,
}

pub enum IngestMsg {
    Data(Bytes),
    Abort,
}

impl ChunkWriter {
    pub async fn start(pool: PgPool) -> Result<Self> {
        let file_id: i64 =
            sqlx::query("SELECT nextval(pg_get_serial_sequence('s3p.objects', 'file_id'))")
                .fetch_one(&pool)
                .await?
                .get(0);
        let (tx, rx) = tokio::sync::mpsc::channel::<IngestMsg>(8);
        let done = tokio::spawn(ingest_writer(pool, file_id, rx));
        Ok(Self {
            file_id,
            tx: Some(tx),
            done: Some(done),
        })
    }

    pub async fn push(&self, chunk: Bytes) -> Result<()> {
        self.tx
            .as_ref()
            .expect("writer open")
            .send(IngestMsg::Data(chunk))
            .await
            .map_err(|_| anyhow::anyhow!("ingest writer gone"))
    }

    /// Feed staged part files through the stream in the given order.
    pub async fn feed_files(&self, paths: &[PathBuf]) -> Result<()> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 1 << 20];
        for p in paths {
            let mut f = tokio::fs::File::open(p).await?;
            loop {
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                self.push(Bytes::copy_from_slice(&buf[..n])).await?;
            }
        }
        Ok(())
    }

    /// Close the stream and wait for the COPY to land. Returns `(size, sha256)`.
    pub async fn finish(mut self) -> Result<(i64, Vec<u8>)> {
        self.tx.take();
        self.done.take().expect("writer joined").await?
    }

    /// Roll the ingest back (drops the COPY transaction; no rows are visible
    /// since the object row never published).
    pub async fn abort(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(IngestMsg::Abort).await;
        }
        if let Some(h) = self.done.take() {
            let _ = h.await;
        }
    }
}

async fn ingest_writer(
    pool: PgPool,
    file_id: i64,
    mut rx: tokio::sync::mpsc::Receiver<IngestMsg>,
) -> Result<(i64, Vec<u8>)> {
    // Serialized writes: one COPY stream at a time across the process. The
    // permit is held for the ingest lifetime; part pushes backpressure the
    // client instead of competing for Aurora's write ceiling.
    static FLUSH_SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    let _permit = FLUSH_SLOTS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let t0 = std::time::Instant::now();

    let mut tx = pool.begin().await?;
    let mut sink = tx.copy_in_raw(COPY_SQL).await?;
    sink.send(&COPY_HEADER[..]).await?;

    let mut hasher = Sha256::new();
    let mut pending: Vec<u8> = Vec::with_capacity(ROW_BYTES as usize * (ROW_BATCH + 1));
    let mut frame: Vec<u8> = Vec::with_capacity(SEND_BATCH);
    let mut next_no = 0i32;
    let mut total = 0i64;
    while let Some(msg) = rx.recv().await {
        let chunk = match msg {
            IngestMsg::Data(b) => b,
            IngestMsg::Abort => anyhow::bail!("ingest aborted"),
        };
        hasher.update(&chunk);
        total += chunk.len() as i64;
        pending.extend_from_slice(&chunk);
        let whole = pending.len() - pending.len() % ROW_BYTES as usize;
        if whole > 0 {
            frame_rows(file_id, next_no, &pending[..whole], &mut frame);
            next_no += (whole / ROW_BYTES as usize) as i32;
            pending.drain(..whole);
            if frame.len() >= SEND_BATCH {
                sink.send(&frame[..]).await?;
                frame.clear();
            }
        }
    }
    if !pending.is_empty() {
        frame_rows(file_id, next_no, &pending, &mut frame);
    }
    frame.extend_from_slice(&COPY_TRAILER);
    sink.send(&frame[..]).await?;
    sink.finish().await?;
    let sum = hasher.finalize().to_vec();
    // Chunk rows land here; the object row publishes at Complete
    // (`publish`), so objects appear atomically and aborts leave nothing.
    tx.commit().await?;
    eprintln!(
        "pgvs3: ingest {:.1} MiB at {:.0} MiB/s",
        total as f64 / 1024.0 / 1024.0,
        total as f64 / 1024.0 / 1024.0 / t0.elapsed().as_secs_f64().max(1e-9)
    );
    Ok((total, sum))
}

/// Publish an object row: pointer-swap on overwrite (old rows reap after the
/// swap), then refresh the meta cache. This is the visibility boundary.
pub async fn publish(
    pool: &PgPool,
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let old = object_row(&mut tx, bucket, key).await?;
    sqlx::query(
        "INSERT INTO s3p.objects (bucket, key, file_id, size, etag) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size, \
           etag = EXCLUDED.etag, created_at = now()",
    )
    .bind(bucket)
    .bind(key)
    .bind(file_id)
    .bind(size)
    .bind(etag)
    .execute(&mut *tx)
    .await?;
    if let Some(old_id) = old {
        sqlx::query("DELETE FROM s3p.chunks WHERE file_id = $1")
            .bind(old_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    publish_cache(bucket, key, file_id, size, etag);
    Ok(())
}

fn publish_cache(bucket: &str, key: &str, file_id: i64, size: i64, etag: &[u8]) {
    meta_put(
        bucket,
        key,
        Meta {
            size,
            etag: etag.to_vec(),
            created_at: SystemTime::now(),
            file_id,
        },
    );
}

/// Binary COPY framing: stream header once per stream, then row tuples.
fn frame_rows(file_id: i64, first_no: i32, data: &[u8], out: &mut Vec<u8>) {
    let nrows = data.len().div_ceil(ROW_BYTES as usize);
    for i in 0..nrows {
        let start = i * ROW_BYTES as usize;
        let end = ((i + 1) * ROW_BYTES as usize).min(data.len());
        out.extend_from_slice(&3i16.to_be_bytes()); // field count
        out.extend_from_slice(&8i32.to_be_bytes());
        out.extend_from_slice(&file_id.to_be_bytes());
        out.extend_from_slice(&4i32.to_be_bytes());
        out.extend_from_slice(&(first_no + i as i32).to_be_bytes());
        out.extend_from_slice(&((end - start) as i32).to_be_bytes());
        out.extend_from_slice(&data[start..end]);
    }
}

pub async fn delete(pool: &PgPool, bucket: &str, key: &str) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let old: Option<i64> =
        sqlx::query("DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id")
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.get("file_id"));
    if let Some(file_id) = old {
        sqlx::query("DELETE FROM s3p.chunks WHERE file_id = $1")
            .bind(file_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    meta_invalidate(bucket, key);
    Ok(old.is_some())
}

#[derive(Debug, Clone)]
pub struct Listed {
    pub key: String,
    pub size: i64,
    pub etag: Vec<u8>,
    pub created_at: SystemTime,
}

/// Key-ordered listing in `[prefix, prefix_end)` (byte order); `after` is
/// exclusive.
pub async fn list(
    pool: &PgPool,
    bucket: &str,
    prefix: &str,
    prefix_end: &str,
    after: &str,
    limit: i64,
) -> Result<Vec<Listed>> {
    let rows = sqlx::query(
        "SELECT key, size, etag, EXTRACT(EPOCH FROM created_at)::float8 AS created_epoch \
         FROM s3p.objects \
         WHERE bucket = $1 AND key COLLATE \"C\" >= $2 AND key COLLATE \"C\" < $3 \
           AND key COLLATE \"C\" > $4 \
         ORDER BY key COLLATE \"C\" LIMIT $5",
    )
    .bind(bucket)
    .bind(prefix)
    .bind(prefix_end)
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| Listed {
            key: r.get("key"),
            size: r.get("size"),
            etag: r.get("etag"),
            created_at: epoch(r.get::<f64, _>("created_epoch")),
        })
        .collect())
}

pub async fn buckets(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT DISTINCT bucket FROM s3p.objects ORDER BY bucket")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<String, _>("bucket")).collect())
}

/// `(logical_bytes, physical_bytes)` over objects + chunks (tables + indexes).
pub async fn sizes(pool: &PgPool) -> Result<(i64, i64)> {
    let row = sqlx::query(
        "SELECT (SELECT COALESCE(sum(size), 0)::int8 FROM s3p.objects) AS logical, \
                pg_total_relation_size('s3p.objects') + pg_total_relation_size('s3p.chunks') AS physical, \
                (SELECT count(*) FROM s3p.objects) AS n_objects, \
                (SELECT count(*) FROM s3p.chunks) AS n_chunks, \
                pg_database_size(current_database()) AS db_size",
    )
    .fetch_one(pool)
    .await?;
    let logical: i64 = row.get("logical");
    let physical: i64 = row.get("physical");
    println!(
        "objects={} chunks={} logical={} MiB physical={} MiB db={} MiB overhead={:.2}%",
        row.get::<i64, _>("n_objects"),
        row.get::<i64, _>("n_chunks"),
        logical / 1024 / 1024,
        physical / 1024 / 1024,
        row.get::<i64, _>("db_size") / 1024 / 1024,
        (physical as f64 / logical.max(1) as f64 - 1.0) * 100.0,
    );
    Ok((logical, physical))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
