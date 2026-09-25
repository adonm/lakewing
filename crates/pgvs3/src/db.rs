//! PostgreSQL storage: an object is numbered 8120-byte rows in `s3p.chunks`
//! plus one row in `s3p.objects` (layout: schema.sql).
//!
//! Reads: object metadata is cached in-process (filled at publish and first
//! lookup, dropped at delete), so a GET costs one query per 8 MiB of range.
//! Rows go to the response as they arrive; a range over 8 MiB fetches up to 8
//! parts at once on separate connections, since one connection tops out near
//! 600 MiB/s. Row bytes are not cached: DuckDB caches what it reads, and a
//! proxy cache behind it hit ~0% of the time.
//!
//! Writes: every PUT and every multipart part streams into its own binary
//! COPY, with no staging and no global lock. A multipart object is the ordered
//! list of its part files; upload state lives in PostgreSQL, so any gateway
//! can take any part, and Complete only checks and publishes. An object
//! appears atomically when its `objects` row is written.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::task::Context;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Row, Transaction};

pub use crate::pg::Pool;

pub const SCHEMA: &str = include_str!("../schema.sql");

/// Row payload: file_id(8) + no(4) + varlena(4) + 8120 = 8136 data bytes,
/// tuple 8160 bytes = one row per 8 KB page.
pub const ROW_BYTES: i64 = 8120;

const COPY_SQL: &str = "COPY s3p.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)";
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";
const COPY_TRAILER: &[u8] = &[0xFF, 0xFF];
const SEND_BATCH: usize = 4 << 20;

/// Whole rows for a contiguous `no` range (cheaper than `= ANY` on Aurora:
/// 1.74 vs 2.02 ms server time per warm 8 MiB span).
const GET_RANGE_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3";

/// Spans up to this size are one query, and "small" in the stats.
const SMALL_MAX: usize = 8 << 20;
/// Rows per part: any span up to SMALL_MAX (it may start mid-row) fits one.
const PART_ROWS: usize = SMALL_MAX / ROW_BYTES as usize + 2;
/// Parts in flight per GET, each on its own pool connection. Smaller parts
/// measured slower at DuckDB's concurrency (2 MiB: +5% pass time, 1 MiB:
/// +12-21%), so a span is only split above 8 MiB.
const PARTS_INFLIGHT: usize = 8;
/// Rows go to the response in chunks of about this size.
const CHUNK: usize = 256 << 10;

pub async fn connect(url: &str) -> Result<Pool> {
    // Index scans off: bitmap heap scans hand the whole row span to
    // PostgreSQL 18's read-ahead, ~5x faster on cold ranges at no warm cost.
    // work_mem keeps the bitmaps exact; effective_io_concurrency deepens the
    // read-ahead (no gain past 32 on Aurora). The keepalive and
    // idle-transaction limits bound what a dead gateway can leave holding
    // locks.
    let mut session = String::from(
        "SET enable_indexscan = off; SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
         SET tcp_keepalives_idle = 30; SET tcp_keepalives_interval = 10; \
         SET tcp_keepalives_count = 3; SET idle_in_transaction_session_timeout = '5min';",
    );
    // A synchronous commit costs every write an Aurora round trip, while
    // objects are immutable and sha256-idempotent: a lost commit only means a
    // retried PUT. PGVS3_DURABLE=1 turns synchronous commits back on.
    if std::env::var_os("PGVS3_DURABLE").is_none() {
        session.push_str(" SET synchronous_commit = off;");
    }
    // (io_combine_limit cannot go here: PostgreSQL clamps it to the server's
    // io_max_combine_limit, which only a parameter-group change raises.)
    Pool::connect(
        url,
        crate::pg::Options {
            // Opening a connection (TCP, TLS, SCRAM, the SETs) costs ~10 ms,
            // so keep enough open for DuckDB's bursts.
            min: pool_min(),
            // Parallel parts multiply connection demand; Aurora allows ~1700.
            max: 256,
            session,
            range_sql: GET_RANGE_SQL,
            range_types: &[Type::INT8, Type::INT4, Type::INT4],
        },
    )
    .await
}

/// Storage layout this binary reads and writes (schema.sql); bumped only by
/// breaking layout changes.
pub const LAYOUT_VERSION: i32 = 2;

/// Create the layout if absent, and fail closed on any other version rather
/// than misread it. Serialized across gateways by an advisory lock, so
/// concurrent first starts do not race the DDL.
pub async fn init(pool: &Pool) -> Result<()> {
    const LOCK: i64 = 0x7067_7673; // "pgvs"
    let conn = pool.get().await?;
    conn.query_typed("SELECT pg_advisory_lock($1)", &[(&LOCK, Type::INT8)])
        .await?;
    let result = init_locked(&conn).await;
    let _ = conn
        .query_typed("SELECT pg_advisory_unlock($1)", &[(&LOCK, Type::INT8)])
        .await;
    result
}

async fn init_locked(client: &Client) -> Result<()> {
    let chunks: bool = client
        .query_typed_one("SELECT to_regclass('s3p.chunks') IS NOT NULL", &[])
        .await?
        .try_get(0)?;
    if chunks {
        let marked: bool = client
            .query_typed_one("SELECT to_regclass('s3p.layout') IS NOT NULL", &[])
            .await?
            .try_get(0)?;
        let found: Option<i32> = if marked {
            client
                .query_typed_one("SELECT max(version) FROM s3p.layout", &[])
                .await?
                .try_get(0)?
        } else {
            Some(1) // v1 predates the marker (unpartitioned s3p.chunks)
        };
        match found {
            Some(v) if v == LAYOUT_VERSION => {}
            Some(v) => anyhow::bail!(
                "s3p holds storage layout v{v}; this pgvs3 reads v{LAYOUT_VERSION} \
                 (migrate the data, or point it at a fresh database)"
            ),
            None => anyhow::bail!("s3p.layout is empty: refusing to guess the storage layout"),
        }
    }
    client.batch_execute(SCHEMA).await?;
    client
        .query_typed(
            "INSERT INTO s3p.layout (version) SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM s3p.layout)",
            &[(&LAYOUT_VERSION, Type::INT4)],
        )
        .await?;
    Ok(())
}

/// Warm connections per gateway (PGVS3_POOL_MIN, default 64: measured
/// wait-free for one DuckDB worker's bursts). Every gateway holds this many
/// Aurora backends open, so large fleets should lower it.
fn pool_min() -> usize {
    std::env::var("PGVS3_POOL_MIN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

/// Load the chunk primary-key indexes into shared_buffers when they are small
/// beside it (a cold btree leaf was ~1/3 of a cold small GET on Aurora).
/// Past ~10% of shared_buffers (the index is ~0.4% of the data, so multi-TB)
/// they are left to the buffer manager: prewarming on every gateway start
/// would evict the working set. Returns the blocks loaded, or None if skipped.
pub async fn prewarm_index(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let _ = conn
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
        .await;
    let mut idx: Vec<(String, i64)> = Vec::new();
    for r in conn
        .query_typed(
            "SELECT i.indexrelid::regclass::text, pg_relation_size(i.indexrelid) \
             FROM pg_partition_tree('s3p.chunks') p JOIN pg_index i ON i.indrelid = p.relid \
             WHERE p.isleaf AND i.indisprimary",
            &[],
        )
        .await?
    {
        idx.push((r.try_get(0)?, r.try_get(1)?));
    }
    let budget: i64 = conn
        .query_typed_one(
            "SELECT setting::int8 * 8192 / 10 FROM pg_settings WHERE name = 'shared_buffers'",
            &[],
        )
        .await?
        .try_get(0)?;
    if idx.iter().map(|(_, bytes)| bytes).sum::<i64>() > budget {
        return Ok(None);
    }
    let mut blocks = 0;
    for (name, _) in &idx {
        blocks += conn
            .query_typed_one("SELECT pg_prewarm($1::regclass)", &[(name, Type::TEXT)])
            .await?
            .try_get::<_, i64>(0)?;
    }
    Ok(Some(blocks))
}

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
    fn segments(&self) -> Vec<(i64, i64, i64)> {
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A cut-through response body: the first chunk (fetched before the response
/// started), then the rest in `no` order as the span task forwards it.
pub struct PieceStream {
    first: Option<Bytes>,
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>,
}

impl Stream for PieceStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.first.take() {
            Some(head) => std::task::Poll::Ready(Some(Ok(head))),
            None => this.rx.poll_recv(cx),
        }
    }
}

fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn epoch(secs: f64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs_f64(secs.max(0.0))
}

// ---------------------------------------------------------------------------
// Pinned metadata cache: bucket/key -> Meta, count-capped, never evicted by
// byte pressure. Its only job is removing the lookup round trip per GET.
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
    meta_cache()
        .lock()
        .unwrap()
        .map
        .remove(&(bucket.to_owned(), key.to_owned()));
}

// ---------------------------------------------------------------------------
// GET telemetry for bottleneck hunting: span-size histogram, time to the first
// and the last byte handed to the response, parts and pool waits. Read via
// `stage_stats_line` (the stats route and the log timer print it).
// ---------------------------------------------------------------------------
static SPANS: [std::sync::atomic::AtomicU64; 5] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 5];
// GET latency histogram: quarter-octave buckets of microseconds (~19%
// resolution), read back as p50/p95/p99.
static LAT: [std::sync::atomic::AtomicU64; 128] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 128];

fn lat_record(us: u64) {
    let idx = ((us.max(1) as f64).log2() * 4.0) as usize;
    LAT[idx.min(127)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
// Per span class (0 = small, 1 = larger): GETs, summed time to the first byte
// handed to the response, summed time to the last.
static GETS: [std::sync::atomic::AtomicU64; 2] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 2];
static TTFB_US: [std::sync::atomic::AtomicU64; 2] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 2];
static TOTAL_US: [std::sync::atomic::AtomicU64; 2] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 2];
// Per part query, both paths: count and pool-acquire wait (summed, so it can
// exceed wall time under concurrency; growth means the pool is the limit).
static PARTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SERVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `spans=[..] small n=.. ttfb=.. total=.. | stream n=.. ttfb=.. total=.. | parts=.. wait(sum)=.. | served=..`
pub fn stage_stats_line() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let ms = |a: &std::sync::atomic::AtomicU64| a.load(Relaxed) as f64 / 1e3;
    let s: Vec<u64> = SPANS.iter().map(|a| a.load(Relaxed)).collect();
    let lat: Vec<u64> = LAT.iter().map(|a| a.load(Relaxed)).collect();
    format!(
        "perf: spans=[<64K:{} <512K:{} <2M:{} <8M:{} >=8M:{}] small n={} ttfb={:.0}ms total={:.0}ms | stream n={} ttfb={:.0}ms total={:.0}ms | parts={} wait(sum)={:.0}ms | served={}MiB | get p50={:.1}ms p95={:.1}ms p99={:.1}ms",
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

/// Span-size histogram buckets: <64K <512K <2M <8M >=8M (the workload's read
/// shapes; the last boundary is SMALL_MAX).
fn span_bucket(span: usize) -> usize {
    const T: [usize; 4] = [65536, 524_288, 2_097_152, 8_388_608];
    T.iter().position(|&t| span < t).unwrap_or(4)
}

// ---------------------------------------------------------------------------

/// Metadata lookup, cached (a repeat open costs no round trip).
pub async fn meta(pool: &Pool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    if let Some(m) = meta_cache()
        .lock()
        .unwrap()
        .map
        .get(&(bucket.to_owned(), key.to_owned()))
    {
        return Ok(Some(m.clone()));
    }
    let row = pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT file_id, size, etag, EXTRACT(EPOCH FROM created_at)::float8, parts, part_ends \
             FROM s3p.objects WHERE bucket = $1 AND key = $2",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let Some(r) = row else { return Ok(None) };
    let meta = Meta {
        file_id: r.try_get(0)?,
        size: r.try_get(1)?,
        etag: r.try_get(2)?,
        created_at: epoch(r.try_get(3)?),
        parts: r.try_get(4)?,
        part_ends: r.try_get(5)?,
    };
    meta_put(bucket, key, meta.clone());
    Ok(Some(meta))
}

/// Serve `[start, end]` of an object, cut through: rows go to the response as
/// they arrive from PostgreSQL (`stream_span`), not after the whole span (on
/// an 8 MiB GET that was a serial ~3 ms, a 0.5 ms assembly copy plus the
/// loopback send, after a 12 ms fetch). The response starts only once the
/// first chunk exists, so a failure before any byte is a clean S3 error; a
/// span that fits in the first chunk goes out as one buffer.
pub async fn get_body(
    pool: Pool,
    bucket: String,
    key: String,
    first: i64,
    last: i64,
    suffix: i64,
) -> Result<Option<(SliceMeta, PieceBody)>> {
    use std::sync::atomic::Ordering::Relaxed;
    let Some(m) = meta(&pool, &bucket, &key).await? else {
        return Ok(None);
    };
    let (start, end) = eff_range(m.size, first, last, suffix);
    let smeta = SliceMeta {
        size: m.size,
        etag: m.etag.clone(),
        created_at: m.created_at,
        file_id: m.file_id,
        start,
        end,
    };
    let len = smeta.len() as usize;
    if len == 0 {
        return Ok(Some((smeta, PieceBody::OneShot(Bytes::new()))));
    }
    SPANS[span_bucket(len)].fetch_add(1, Relaxed);
    let class = usize::from(len > SMALL_MAX);
    let t0 = std::time::Instant::now();
    // ~8 MiB of chunks queue for the response; parts buffer their own rows.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    tokio::spawn(async move {
        if let Err(e) = stream_span(&pool, &m, start, end, &tx).await {
            let _ = tx.send(Err(io_err(e))).await;
        }
        let us = t0.elapsed().as_micros() as u64;
        GETS[class].fetch_add(1, Relaxed);
        TOTAL_US[class].fetch_add(us, Relaxed);
        lat_record(us);
        SERVED.fetch_add(len as u64, Relaxed);
    });
    let head = rx.recv().await;
    TTFB_US[class].fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    match head {
        Some(Ok(head)) if head.len() == len => Ok(Some((smeta, PieceBody::OneShot(head)))),
        Some(Ok(head)) => Ok(Some((
            smeta,
            PieceBody::Streamed(PieceStream {
                first: Some(head),
                rx,
            }),
        ))),
        Some(Err(e)) => {
            // Perhaps a stale cache entry (the object replaced via another
            // gateway): the retry looks the metadata up again.
            meta_invalidate(&bucket, &key);
            Err(e.into())
        }
        None => Err(anyhow::anyhow!(
            "GET of {bucket}/{key} ended before any data"
        )),
    }
}

/// Response body for a served range: one buffer when the span fits in the
/// first chunk, else the cut-through stream.
pub enum PieceBody {
    OneShot(Bytes),
    Streamed(PieceStream),
}

/// One fetch unit: rows `[lo, hi]` of segment file `file_id`, which starts
/// at object byte offset `base`.
#[derive(Clone, Copy)]
struct Piece {
    file_id: i64,
    base: i64,
    lo: i32,
    hi: i32,
}

/// Pieces covering object bytes `[start, end]` across the object's segments
/// (one file, or its part files), in order, at most `step` rows each.
fn plan(m: &Meta, start: i64, end: i64, step: usize) -> Vec<Piece> {
    let mut out = Vec::new();
    for (file_id, base, len) in m.segments() {
        if len <= 0 || base + len - 1 < start || base > end {
            continue;
        }
        let lo = ((start.max(base) - base) / ROW_BYTES) as i32;
        let hi = ((end.min(base + len - 1) - base) / ROW_BYTES) as i32;
        out.extend(part_ranges(lo, hi, step).map(|(lo, hi)| Piece {
            file_id,
            base,
            lo,
            hi,
        }));
    }
    out
}

/// The part of row `no` of a segment at `base` inside object bytes `[start, end]`.
fn row_slice(base: i64, no: i32, data: &[u8], start: i64, end: i64) -> Option<&[u8]> {
    let row_start = base + i64::from(no) * ROW_BYTES;
    let lo = start.max(row_start) - row_start;
    let hi = end.min(row_start + data.len() as i64 - 1) - row_start;
    (hi >= lo).then(|| &data[lo as usize..=hi as usize])
}

/// Contiguous `[lo, hi]` ranges of at most `step` rows covering the span.
fn part_ranges(first_row: i32, last_row: i32, step: usize) -> impl Iterator<Item = (i32, i32)> {
    let step = step.clamp(1, i32::MAX as usize);
    (first_row..=last_row)
        .step_by(step)
        .map(move |lo| (lo, lo.saturating_add(step as i32 - 1).min(last_row)))
}

/// Rows of `[start, end]` in `no` order, forwarded in ~CHUNK pieces as they
/// arrive. Parts fetch concurrently (PARTS_INFLIGHT at a time), each in its
/// own task (`spawn_part`); the head part is cut through while later parts
/// queue their rows. A missing row (e.g. the object was replaced under a
/// stale meta-cache entry) is an error, never a short body.
async fn stream_span(
    pool: &Pool,
    m: &Meta,
    start: i64,
    end: i64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let mut todo = plan(m, start, end, PART_ROWS).into_iter();
    let mut running = VecDeque::with_capacity(PARTS_INFLIGHT);
    let mut chunk = BytesMut::with_capacity(CHUNK + ROW_BYTES as usize);
    let mut sent = 0i64;
    loop {
        while running.len() < PARTS_INFLIGHT {
            let Some(p) = todo.next() else { break };
            running.push_back((p, spawn_part(pool, p)));
        }
        let Some((p, mut rows)) = running.pop_front() else {
            break;
        };
        // Bitmap heap scans return TID order: hold any row that runs ahead.
        let mut early: BTreeMap<i32, Row> = BTreeMap::new();
        let mut next = p.lo;
        while let Some(row) = rows.recv().await {
            let row = row?;
            let no: i32 = row.try_get(0)?;
            if no != next {
                early.insert(no, row);
                continue;
            }
            put_row(&mut chunk, &p, no, &row, start, end)?;
            next += 1;
            while let Some(row) = early.remove(&next) {
                put_row(&mut chunk, &p, next, &row, start, end)?;
                next += 1;
            }
            if chunk.len() >= CHUNK {
                sent += chunk.len() as i64;
                if tx.send(Ok(chunk.split().freeze())).await.is_err() {
                    return Ok(()); // the client went away
                }
                chunk.reserve(CHUNK + ROW_BYTES as usize);
            }
        }
        anyhow::ensure!(
            next > p.hi,
            "file {} rows {}..={}: row {next} missing",
            p.file_id,
            p.lo,
            p.hi
        );
    }
    anyhow::ensure!(
        sent + chunk.len() as i64 == end - start + 1,
        "short read: {} of {} bytes",
        sent + chunk.len() as i64,
        end - start + 1
    );
    if !chunk.is_empty() {
        let _ = tx.send(Ok(chunk.freeze())).await;
    }
    Ok(())
}

/// Append row `no`'s part of `[start, end]` to `chunk`.
fn put_row(
    chunk: &mut BytesMut,
    p: &Piece,
    no: i32,
    row: &Row,
    start: i64,
    end: i64,
) -> Result<()> {
    if let Some(s) = row_slice(p.base, no, row.try_get(1)?, start, end) {
        chunk.extend_from_slice(s);
    }
    Ok(())
}

/// Fetch one part in its own task, into a channel with room for all its
/// rows: the fetch never waits on the consumer (which drains parts in order),
/// so every in-flight part streams from PostgreSQL at full speed.
fn spawn_part(pool: &Pool, p: Piece) -> tokio::sync::mpsc::Receiver<Result<Row>> {
    let (tx, rx) = tokio::sync::mpsc::channel((p.hi - p.lo + 2) as usize);
    let pool = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = fetch_part(&pool, p, &tx).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

/// One contiguous row-range query (the prepared GET_RANGE_SQL) on its own
/// pool connection; rows go to `tx` as they arrive. Each keeps its bytes in
/// the connection's receive buffer until copied into a response chunk.
async fn fetch_part(
    pool: &Pool,
    p: Piece,
    tx: &tokio::sync::mpsc::Sender<Result<Row>>,
) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = std::time::Instant::now();
    let mut conn = pool.get().await?;
    WAIT_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    PARTS.fetch_add(1, Relaxed);
    let params: [&(dyn ToSql + Sync); 3] = [&p.file_id, &p.lo, &p.hi];
    let range = conn.range().await?.clone();
    let mut rows = std::pin::pin!(conn.query_raw(&range, params).await?);
    while let Some(row) = rows.try_next().await? {
        if tx.send(Ok(row)).await.is_err() {
            break; // the span was abandoned
        }
    }
    Ok(())
}

/// Effective `[start, end]` inclusive for a request (mirrors the old SQL clamp).
fn eff_range(size: i64, first: i64, last: i64, suffix: i64) -> (i64, i64) {
    if suffix >= 0 {
        ((size - suffix).max(0), size - 1)
    } else {
        (
            first.max(0),
            if last >= 0 {
                last.min(size - 1)
            } else {
                size - 1
            },
        )
    }
}

/// Buffered variant (tests, bench floor).
pub async fn get(
    pool: &Pool,
    bucket: &str,
    key: &str,
    first: i64,
    last: i64,
    suffix: i64,
) -> Result<Option<Slice>> {
    let Some((meta, body)) = get_body(
        pool.clone(),
        bucket.to_owned(),
        key.to_owned(),
        first,
        last,
        suffix,
    )
    .await?
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

/// Overwrite-or-create a buffered object through the ingest pipeline (seed and
/// bench; the server streams request bodies through the same writer). The
/// ETag is the caller's sha256 (idempotent retries); visibility is atomic via
/// `publish`.
pub async fn put(pool: &Pool, bucket: &str, key: &str, data: &[u8], etag: &[u8]) -> Result<()> {
    let writer = ChunkWriter::start(pool.clone()).await?;
    let file_id = writer.file_id;
    for chunk in data.chunks(SEND_BATCH) {
        writer.push(Bytes::copy_from_slice(chunk)).await?;
    }
    let (size, _sum) = writer.finish().await?;
    publish(pool, bucket, key, file_id, size, etag).await
}

/// Streaming ingest: bytes flow into one open binary COPY stream through
/// `push` (rows cut across pushes through a single cursor). Every ingest has
/// its own connection and COPY, so PUTs and multipart parts write in parallel.
pub struct ChunkWriter {
    pub file_id: i64,
    tx: Option<tokio::sync::mpsc::Sender<IngestMsg>>,
    done: Option<tokio::task::JoinHandle<IngestResult>>,
}

/// `(size, sha256)` of the bytes an ingest streamed.
type IngestResult = Result<(i64, Vec<u8>)>;

pub enum IngestMsg {
    Data(Bytes),
    Abort,
}

impl ChunkWriter {
    pub async fn start(pool: Pool) -> Result<Self> {
        Self::begin(pool, None).await
    }

    /// A multipart part: its rows and its `upload_parts` record commit in one
    /// transaction, so a part is either fully recorded or absent.
    pub async fn start_part(pool: Pool, upload_id: String, part_no: i32) -> Result<Self> {
        Self::begin(pool, Some((upload_id, part_no))).await
    }

    async fn begin(pool: Pool, part: Option<(String, i32)>) -> Result<Self> {
        let file_id: i64 = pool
            .get()
            .await?
            .query_typed_one(
                "SELECT nextval(pg_get_serial_sequence('s3p.objects', 'file_id'))",
                &[],
            )
            .await?
            .try_get(0)?;
        let (tx, rx) = tokio::sync::mpsc::channel::<IngestMsg>(8);
        let done = tokio::spawn(ingest_writer(pool, file_id, part, rx));
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

/// Stream a request body into a writer and finish it. A body error rolls the
/// ingest back; a writer failure surfaces through `finish`.
pub async fn ingest_body<S, E>(writer: ChunkWriter, mut body: S) -> Result<(i64, Vec<u8>)>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(c) => {
                if writer.push(c).await.is_err() {
                    break; // the writer failed: finish() reports why
                }
            }
            Err(e) => {
                writer.abort().await;
                anyhow::bail!("request body: {e}");
            }
        }
    }
    writer.finish().await
}

async fn ingest_writer(
    pool: Pool,
    file_id: i64,
    part: Option<(String, i32)>,
    mut rx: tokio::sync::mpsc::Receiver<IngestMsg>,
) -> Result<(i64, Vec<u8>)> {
    // No global write lock: each ingest owns a connection and a COPY, and the
    // bounded channel backpressures the request body at COPY speed.
    let t0 = std::time::Instant::now();

    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let mut sink = std::pin::pin!(tx.copy_in::<_, Bytes>(COPY_SQL).await?);
    sink.send(Bytes::from_static(COPY_HEADER)).await?;

    let mut hasher = Sha256::new();
    // Bytes not yet framed: always less than one row between pushes.
    let mut pending: Vec<u8> = Vec::new();
    let mut frame = BytesMut::with_capacity(SEND_BATCH);
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
                sink.send(frame.split().freeze()).await?;
            }
        }
    }
    if !pending.is_empty() {
        frame_rows(file_id, next_no, &pending, &mut frame);
    }
    frame.extend_from_slice(COPY_TRAILER);
    sink.send(frame.split().freeze()).await?;
    sink.as_mut().finish().await?;
    let sum = hasher.finalize().to_vec();
    // A multipart part records itself in the same transaction as its rows; a
    // re-sent part replaces the earlier attempt, rows included. Unpublished
    // rows are invisible: objects appear atomically at publish / Complete.
    if let Some((upload_id, part_no)) = &part {
        let old = tx
            .query_typed_opt(
                "DELETE FROM s3p.upload_parts WHERE upload_id = $1 AND part_no = $2 RETURNING file_id",
                &[(upload_id, Type::TEXT), (part_no, Type::INT4)],
            )
            .await?;
        if let Some(old) = old {
            let old: i64 = old.try_get(0)?;
            tx.query_typed(
                "DELETE FROM s3p.chunks WHERE file_id = $1",
                &[(&old, Type::INT8)],
            )
            .await?;
        }
        tx.query_typed(
            "INSERT INTO s3p.upload_parts (upload_id, part_no, file_id, size, sha256) VALUES ($1, $2, $3, $4, $5)",
            &[
                (upload_id, Type::TEXT),
                (part_no, Type::INT4),
                (&file_id, Type::INT8),
                (&total, Type::INT8),
                (&sum, Type::BYTEA),
            ],
        )
        .await?;
    }
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
    pool: &Pool,
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
) -> Result<()> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    swap_object(&tx, bucket, key, file_id, size, etag, None).await?;
    tx.commit().await?;
    publish_cache(bucket, key, file_id, size, etag, None);
    Ok(())
}

/// Point (bucket, key) at new storage inside `tx` and reap the storage of any
/// object it replaces (all of its files: the single file or every part).
async fn swap_object(
    tx: &Transaction<'_>,
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
    parts: Option<(&[i64], &[i64])>,
) -> Result<()> {
    let old = tx
        .query_typed_opt(
            "SELECT file_id, parts FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let (ids, ends) = parts.unzip();
    tx.query_typed(
        "INSERT INTO s3p.objects (bucket, key, file_id, size, etag, parts, part_ends) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size, \
           etag = EXCLUDED.etag, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends, created_at = now()",
        &[
            (&bucket, Type::TEXT),
            (&key, Type::TEXT),
            (&file_id, Type::INT8),
            (&size, Type::INT8),
            (&etag, Type::BYTEA),
            (&ids, Type::INT8_ARRAY),
            (&ends, Type::INT8_ARRAY),
        ],
    )
    .await?;
    if let Some(r) = old {
        reap(tx, r.try_get(0)?, r.try_get(1)?).await?;
    }
    Ok(())
}

/// Delete every chunk row of an unpublished object's files.
async fn reap(tx: &Transaction<'_>, file_id: i64, parts: Option<Vec<i64>>) -> Result<()> {
    let mut dead = parts.unwrap_or_default();
    dead.push(file_id);
    tx.query_typed(
        "DELETE FROM s3p.chunks WHERE file_id = ANY($1)",
        &[(&dead, Type::INT8_ARRAY)],
    )
    .await?;
    Ok(())
}

fn publish_cache(
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
    parts: Option<(Vec<i64>, Vec<i64>)>,
) {
    let (parts, part_ends) = parts.unzip();
    meta_put(
        bucket,
        key,
        Meta {
            size,
            etag: etag.to_vec(),
            created_at: SystemTime::now(),
            file_id,
            parts,
            part_ends,
        },
    );
}

/// Binary COPY framing: stream header once per stream, then row tuples.
fn frame_rows(file_id: i64, first_no: i32, data: &[u8], out: &mut BytesMut) {
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

pub async fn delete(pool: &Pool, bucket: &str, key: &str) -> Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let old = tx
        .query_typed_opt(
            "DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id, parts",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let found = old.is_some();
    if let Some(r) = old {
        reap(&tx, r.try_get(0)?, r.try_get(1)?).await?;
    }
    tx.commit().await?;
    meta_invalidate(bucket, key);
    Ok(found)
}

/// Start (or re-attach, for a retried Create) the multipart upload of a key.
pub async fn create_upload(pool: &Pool, bucket: &str, key: &str) -> Result<String> {
    Ok(pool
        .get()
        .await?
        .query_typed_one(
            "INSERT INTO s3p.uploads (upload_id, bucket, key) VALUES (gen_random_uuid()::text, $1, $2) \
             ON CONFLICT (bucket, key) DO UPDATE SET bucket = EXCLUDED.bucket RETURNING upload_id",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?
        .try_get(0)?)
}

pub async fn upload_exists(pool: &Pool, upload_id: &str) -> Result<bool> {
    Ok(pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT 1 FROM s3p.uploads WHERE upload_id = $1",
            &[(&upload_id, Type::TEXT)],
        )
        .await?
        .is_some())
}

pub enum Completed {
    Done {
        bucket: String,
        key: String,
        etag: Vec<u8>,
        size: i64,
    },
    InvalidPart,
    NoSuchUpload,
}

/// Complete: every recorded part listed exactly once with a matching ETag (any
/// listing order), then the object publishes as its ordered part files. No
/// data moves. ETag = sha256 over the part sha256s (S3's hash-of-part-hashes).
pub async fn complete_upload(
    pool: &Pool,
    upload_id: &str,
    listed: &[(i32, String)],
) -> Result<Completed> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let Some(up) = tx
        .query_typed_opt(
            "SELECT bucket, key FROM s3p.uploads WHERE upload_id = $1 FOR UPDATE",
            &[(&upload_id, Type::TEXT)],
        )
        .await?
    else {
        return Ok(Completed::NoSuchUpload);
    };
    let (bucket, key): (String, String) = (up.try_get(0)?, up.try_get(1)?);
    let rows = tx
        .query_typed(
            "SELECT part_no, file_id, size, sha256 FROM s3p.upload_parts WHERE upload_id = $1 ORDER BY part_no",
            &[(&upload_id, Type::TEXT)],
        )
        .await?;
    let mut want: Vec<&(i32, String)> = listed.iter().collect();
    want.sort_by_key(|(no, _)| *no);
    if want.is_empty() || want.len() != rows.len() {
        return Ok(Completed::InvalidPart);
    }
    let (mut ids, mut ends, mut size) = (
        Vec::with_capacity(rows.len()),
        Vec::with_capacity(rows.len()),
        0i64,
    );
    let mut hasher = Sha256::new();
    for (p, r) in want.iter().zip(&rows) {
        let sha: Vec<u8> = r.try_get(3)?;
        if p.0 != r.try_get::<_, i32>(0)? || p.1 != hex(&sha) {
            return Ok(Completed::InvalidPart);
        }
        hasher.update(&sha);
        size += r.try_get::<_, i64>(2)?;
        ids.push(r.try_get::<_, i64>(1)?);
        ends.push(size);
    }
    let etag = hasher.finalize().to_vec();
    swap_object(&tx, &bucket, &key, ids[0], size, &etag, Some((&ids, &ends))).await?;
    tx.query_typed(
        "DELETE FROM s3p.uploads WHERE upload_id = $1",
        &[(&upload_id, Type::TEXT)],
    )
    .await?;
    tx.commit().await?;
    publish_cache(&bucket, &key, ids[0], size, &etag, Some((ids, ends)));
    Ok(Completed::Done {
        bucket,
        key,
        etag,
        size,
    })
}

/// Drop a multipart upload and the rows of every part it recorded.
pub async fn abort_upload(pool: &Pool, upload_id: &str) -> Result<()> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let mut ids: Vec<i64> = Vec::new();
    for r in tx
        .query_typed(
            "SELECT file_id FROM s3p.upload_parts WHERE upload_id = $1",
            &[(&upload_id, Type::TEXT)],
        )
        .await?
    {
        ids.push(r.try_get(0)?);
    }
    tx.query_typed(
        "DELETE FROM s3p.uploads WHERE upload_id = $1",
        &[(&upload_id, Type::TEXT)],
    )
    .await?;
    tx.query_typed(
        "DELETE FROM s3p.chunks WHERE file_id = ANY($1)",
        &[(&ids, Type::INT8_ARRAY)],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Unreferenced chunk files of one partition with ids in [$1, $2): a loose
/// index scan (one probe of that partition's primary key per distinct file,
/// not per row; per partition because a scan of the parent would probe all
/// 32 per step) anti-joined, all by index, against everything that can own a
/// file: published objects (single file, or parts via GIN) and upload parts.
fn orphans_sql(partition: &str) -> String {
    format!(
        "WITH RECURSIVE f(file_id) AS ( \
             SELECT min(file_id) FROM {partition} WHERE file_id >= $1 AND file_id < $2 \
           UNION ALL \
             SELECT (SELECT min(c.file_id) FROM {partition} c WHERE c.file_id > f.file_id AND c.file_id < $2) \
             FROM f WHERE f.file_id IS NOT NULL \
         ) \
         SELECT f.file_id FROM f \
         WHERE f.file_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = f.file_id) \
           AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.parts @> ARRAY[f.file_id]) \
           AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = f.file_id)"
    )
}

/// Reap chunk files nothing references once provably older than `grace`: a
/// PUT whose publish never ran (its rows commit before the object row), or a
/// pre-part-file multipart flush cut short by a killed gateway. file_ids come
/// from one sequence (cache 1), so every id below the newest object published
/// before now-grace was allocated before then: no per-row timestamp needed.
/// Scans ids in [from, horizon); returns (files, rows, horizon) so the caller
/// resumes there. Interrupted writes of every other kind are single
/// transactions and leave nothing behind.
pub async fn sweep_orphans(pool: &Pool, grace: Duration, from: i64) -> Result<(usize, u64, i64)> {
    let mut conn = pool.get().await?;
    let horizon: Option<i64> = conn
        .query_typed_one(
            "SELECT max(file_id) FROM s3p.objects WHERE created_at < now() - make_interval(secs => $1)",
            &[(&grace.as_secs_f64(), Type::FLOAT8)],
        )
        .await?
        .try_get(0)?;
    let horizon = match horizon {
        Some(h) if h > from => h,
        _ => return Ok((0, 0, from)),
    };
    // Partition names come from the catalog (regclass text, already quoted).
    let mut partitions: Vec<String> = Vec::new();
    for r in conn
        .query_typed(
            "SELECT relid::regclass::text FROM pg_partition_tree('s3p.chunks') WHERE isleaf",
            &[],
        )
        .await?
    {
        partitions.push(r.try_get(0)?);
    }
    let tx = conn.transaction().await?;
    // The loose scan wants index probes; gateway sessions default to bitmaps.
    tx.batch_execute("SET LOCAL enable_indexscan = on").await?;
    let mut orphans: Vec<i64> = Vec::new();
    for partition in &partitions {
        for r in tx
            .query_typed(
                &orphans_sql(partition),
                &[(&from, Type::INT8), (&horizon, Type::INT8)],
            )
            .await?
        {
            orphans.push(r.try_get(0)?);
        }
    }
    tx.commit().await?;
    let mut rows = 0;
    for id in &orphans {
        rows += conn
            .execute("DELETE FROM s3p.chunks WHERE file_id = $1", &[id])
            .await?;
    }
    Ok((orphans.len(), rows, horizon))
}

/// Abort uploads abandoned for longer than `age` (S3's incomplete-upload
/// lifecycle): their parts are committed rows, so something must reap them.
pub async fn expire_uploads(pool: &Pool, age: Duration) -> Result<usize> {
    let mut ids: Vec<String> = Vec::new();
    for r in pool
        .get()
        .await?
        .query_typed(
            "SELECT upload_id FROM s3p.uploads WHERE created_at < now() - make_interval(secs => $1)",
            &[(&age.as_secs_f64(), Type::FLOAT8)],
        )
        .await?
    {
        ids.push(r.try_get(0)?);
    }
    for id in &ids {
        abort_upload(pool, id).await?;
    }
    Ok(ids.len())
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
    pool: &Pool,
    bucket: &str,
    prefix: &str,
    prefix_end: &str,
    after: &str,
    limit: i64,
) -> Result<Vec<Listed>> {
    let rows = pool
        .get()
        .await?
        .query_typed(
            "SELECT key, size, etag, EXTRACT(EPOCH FROM created_at)::float8 \
             FROM s3p.objects \
             WHERE bucket = $1 AND key COLLATE \"C\" >= $2 AND key COLLATE \"C\" < $3 \
               AND key COLLATE \"C\" > $4 \
             ORDER BY key COLLATE \"C\" LIMIT $5",
            &[
                (&bucket, Type::TEXT),
                (&prefix, Type::TEXT),
                (&prefix_end, Type::TEXT),
                (&after, Type::TEXT),
                (&limit, Type::INT8),
            ],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        out.push(Listed {
            key: r.try_get(0)?,
            size: r.try_get(1)?,
            etag: r.try_get(2)?,
            created_at: epoch(r.try_get(3)?),
        });
    }
    Ok(out)
}

pub async fn buckets(pool: &Pool) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for r in pool
        .get()
        .await?
        .query_typed(
            "SELECT DISTINCT bucket FROM s3p.objects ORDER BY bucket",
            &[],
        )
        .await?
    {
        out.push(r.try_get(0)?);
    }
    Ok(out)
}

/// `(logical_bytes, physical_bytes)` over objects + chunks (tables + indexes;
/// the chunk partitions, as the partitioned parent has no storage).
pub async fn sizes(pool: &Pool) -> Result<(i64, i64)> {
    let row = pool
        .get()
        .await?
        .query_typed_one(
            "SELECT (SELECT COALESCE(sum(size), 0)::int8 FROM s3p.objects), \
                    pg_total_relation_size('s3p.objects') \
                      + (SELECT COALESCE(sum(pg_total_relation_size(relid)), 0)::int8 \
                         FROM pg_partition_tree('s3p.chunks')), \
                    (SELECT count(*) FROM s3p.objects), \
                    (SELECT count(*) FROM s3p.chunks), \
                    pg_database_size(current_database())",
            &[],
        )
        .await?;
    let logical: i64 = row.try_get(0)?;
    let physical: i64 = row.try_get(1)?;
    println!(
        "objects={} chunks={} logical={} MiB physical={} MiB db={} MiB overhead={:.2}%",
        row.try_get::<_, i64>(2)?,
        row.try_get::<_, i64>(3)?,
        logical / 1024 / 1024,
        physical / 1024 / 1024,
        row.try_get::<_, i64>(4)? / 1024 / 1024,
        (physical as f64 / logical.max(1) as f64 - 1.0) * 100.0,
    );
    Ok((logical, physical))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
