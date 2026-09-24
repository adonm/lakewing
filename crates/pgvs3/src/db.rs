//! PostgreSQL storage: objects as fixed-size inline byte rows.
//!
//! Read path: object metadata (bucket,key -> file_id/size/etag) is cached
//! in-process (pinned, count-capped; populated at publish, dropped at delete),
//! so opening an object costs no round trip. Row bytes are not cached: DuckDB
//! caches what it reads, and a proxy row cache measured ~0% hits on
//! ClickBench/SpatialBench for ~0.5 ms of CPU per request. Each GET is one
//! contiguous row-range query; spans above PGVS3_SPLIT_BYTES (default 8 MiB)
//! fetch in parallel
//! parts on separate pool connections: one Aurora connection moves ~420-500
//! MB/s warm / ~300 MB/s cold, which binds a lone large read. At DuckDB's
//! concurrency (many GETs already in flight) smaller parts measured no gain
//! and 512 KiB parts hurt, so only big spans fan out.
//!
//! Write path: every PUT and every multipart part streams straight into its
//! own binary COPY on its own connection as it arrives: no staging, no global
//! write lock (Performance Insights showed the old serialized design leaving
//! Aurora idle, its one COPY session 100% Client:ClientRead, and its RAM
//! staging deadlocked at full scale). A multipart object is the ordered list
//! of its part files (objects.parts/part_ends); upload state lives in Aurora
//! (s3p.uploads/upload_parts), so any gateway can take any part and Complete
//! only validates and publishes. Objects appear atomically at publish.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
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

/// Spans up to this size are fetched into one buffer (no task, no channel);
/// larger spans stream through in ordered parts.
const ONESHOT_MAX: usize = 8 << 20;

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
        // fetch time) in the c7gn A/B; at 32 SpatialBench's burst still waited
        // 5-8 ms per part, so keep DuckDB's in-flight GETs covered.
        .min_connections(64)
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
    meta_cache().lock().unwrap().map.remove(&(bucket.to_owned(), key.to_owned()));
}

// ---------------------------------------------------------------------------
// GET telemetry for bottleneck hunting: span-size histogram, time awaiting
// PostgreSQL versus local decode/assembly, and pass-through stream time. Read
// via `stage_stats_line` (the stats route and the log timer print it).
// ---------------------------------------------------------------------------
static SPANS: [std::sync::atomic::AtomicU64; 5] = [const { std::sync::atomic::AtomicU64::new(0) }; 5];
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

/// `spans=[..] small total=.. fetch=.. local=.. | stream=.. | parts=.. wait(sum)=.. | served=..`
pub fn stage_stats_line() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let ms = |us: u64| us as f64 / 1e3;
    let total = SMALL_US.load(Relaxed);
    let fetch = FETCH_US.load(Relaxed);
    let s: Vec<u64> = SPANS.iter().map(|a| a.load(Relaxed)).collect();
    format!(
        "perf: spans=[<64K:{} <512K:{} <2M:{} <8M:{} >=8M:{}] small total={:.0}ms fetch={:.0}ms local={:.0}ms n={} | stream={:.0}ms n={} | parts={} wait(sum)={:.0}ms | served={}MiB",
        s[0],
        s[1],
        s[2],
        s[3],
        s[4],
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

/// Span-size histogram buckets: <64K <512K <2M <8M >=8M (the workload's read
/// shapes; the last boundary is ONESHOT_MAX).
fn span_bucket(span: usize) -> usize {
    const T: [usize; 4] = [65536, 524_288, 2_097_152, 8_388_608];
    T.iter().position(|&t| span < t).unwrap_or(4)
}

// ---------------------------------------------------------------------------

/// Metadata lookup, cached (a repeat open costs no round trip).
pub async fn meta(pool: &PgPool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    if let Some(m) = meta_cache().lock().unwrap().map.get(&(bucket.to_owned(), key.to_owned())) {
        return Ok(Some(m.clone()));
    }
    let row = sqlx::query(
        "SELECT file_id, size, etag, EXTRACT(EPOCH FROM created_at)::float8 AS created_epoch, \
                parts, part_ends \
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
        parts: r.get("parts"),
        part_ends: r.get("part_ends"),
    });
    if let Some(m) = &meta {
        meta_put(bucket, key, m.clone());
    }
    Ok(meta)
}

/// Serve `[start, end]` of an object. Spans up to ONESHOT_MAX take the fast
/// path: rows fetched (as parallel parts above PGVS3_SPLIT_BYTES) into one
/// buffer, no channel hop, no task; larger spans stream through in ordered
/// parts.
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
    SPANS[span_bucket(smeta.len() as usize)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if (smeta.len() as usize) <= ONESHOT_MAX {
        use std::sync::atomic::Ordering::Relaxed;
        let t0 = std::time::Instant::now();
        let pieces = plan(&m, start, end, split_rows().unwrap_or(usize::MAX));
        let fetched = fetch_pieces(&pool, &pieces).await?;
        FETCH_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
        let mut out = BytesMut::with_capacity(smeta.len() as usize);
        for (p, mut rows) in pieces.iter().zip(fetched) {
            rows.sort_unstable_by_key(|(no, _)| *no);
            for (no, data) in &rows {
                if let Some(s) = row_slice(p.base, *no, data, start, end) {
                    out.extend_from_slice(&s);
                }
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
            if let Err(e) = stream_pass_through(&pool, &m, start, end, &tx).await {
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
        out.extend(part_ranges(lo, hi, step).map(|(lo, hi)| Piece { file_id, base, lo, hi }));
    }
    out
}

/// The part of row `no` of a segment at `base` inside object bytes `[start, end]`.
fn row_slice(base: i64, no: i32, data: &Bytes, start: i64, end: i64) -> Option<Bytes> {
    let row_start = base + i64::from(no) * ROW_BYTES;
    let lo = start.max(row_start) - row_start;
    let hi = end.min(row_start + data.len() as i64 - 1) - row_start;
    (hi >= lo).then(|| data.slice(lo as usize..=hi as usize))
}

/// All pieces' rows, fetched SPLIT_INFLIGHT at a time, returned in piece order.
async fn fetch_pieces(pool: &PgPool, pieces: &[Piece]) -> Result<Vec<Vec<(i32, Bytes)>>> {
    let mut out = vec![Vec::new(); pieces.len()];
    let mut fetched = futures::stream::iter(pieces.iter().copied().enumerate())
        .map(|(i, p)| async move { fetch_part(pool, p.file_id, p.lo, p.hi).await.map(|rows| (i, rows)) })
        .buffer_unordered(SPLIT_INFLIGHT);
    while let Some(r) = fetched.next().await {
        let (i, rows) = r?;
        out[i] = rows;
    }
    Ok(out)
}

/// Contiguous `[lo, hi]` ranges of at most `step` rows covering the span.
fn part_ranges(first_row: i32, last_row: i32, step: usize) -> impl Iterator<Item = (i32, i32)> {
    let step = step.clamp(1, i32::MAX as usize);
    (first_row..=last_row)
        .step_by(step)
        .map(move |lo| (lo, lo.saturating_add(step as i32 - 1).min(last_row)))
}

/// One contiguous row-range query on its own pool connection.
async fn fetch_part(pool: &PgPool, file_id: i64, lo: i32, hi: i32) -> Result<Vec<(i32, Bytes)>> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = std::time::Instant::now();
    let mut conn = pool.acquire().await?;
    WAIT_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    PARTS.fetch_add(1, Relaxed);
    let got = sqlx::query(GET_RANGE_SQL).bind(file_id).bind(lo).bind(hi).fetch_all(&mut *conn).await?;
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

/// Large-span path: the span splits into pieces (across part files too),
/// fetched SPLIT_INFLIGHT at a time on separate connections and emitted in
/// submission order (`buffered`); each piece is sorted locally (bitmap heap
/// scans do not guarantee `no` order) and its edge rows sliced to the range.
async fn stream_pass_through(
    pool: &PgPool,
    m: &Meta,
    start: i64,
    end: i64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let t0 = std::time::Instant::now();
    let r = stream_pass_inner(pool, m, start, end, tx).await;
    use std::sync::atomic::Ordering::Relaxed;
    STREAM_US.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    STREAM_N.fetch_add(1, Relaxed);
    SERVED.fetch_add((end - start + 1) as u64, Relaxed);
    r
}

async fn stream_pass_inner(
    pool: &PgPool,
    m: &Meta,
    start: i64,
    end: i64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let (step, inflight) = match split_rows() {
        Some(n) => (n.min(STREAM_PART_MAX_ROWS), SPLIT_INFLIGHT),
        None => (STREAM_PART_MAX_ROWS, 1), // A/B baseline: one query at a time
    };
    let mut parts = futures::stream::iter(plan(m, start, end, step))
        .map(|p| async move { fetch_part(pool, p.file_id, p.lo, p.hi).await.map(|rows| (p, rows)) })
        .buffered(inflight);
    while let Some(r) = parts.next().await {
        let (p, mut rows) = r?;
        rows.sort_unstable_by_key(|(no, _)| *no);
        for (no, data) in rows {
            if let Some(s) = row_slice(p.base, no, &data, start, end) {
                if tx.send(Ok(s)).await.is_err() {
                    return Ok(());
                }
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

/// Overwrite-or-create a buffered object through the ingest pipeline (seed and
/// bench; the server streams request bodies through the same writer). The
/// ETag is the caller's sha256 (idempotent retries); visibility is atomic via
/// `publish`.
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
/// `push` (rows cut across pushes through a single cursor). Every ingest has
/// its own connection and COPY, so PUTs and multipart parts write in parallel.
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
        Self::begin(pool, None).await
    }

    /// A multipart part: its rows and its `upload_parts` record commit in one
    /// transaction, so a part is either fully recorded or absent.
    pub async fn start_part(pool: PgPool, upload_id: String, part_no: i32) -> Result<Self> {
        Self::begin(pool, Some((upload_id, part_no))).await
    }

    async fn begin(pool: PgPool, part: Option<(String, i32)>) -> Result<Self> {
        let file_id: i64 =
            sqlx::query("SELECT nextval(pg_get_serial_sequence('s3p.objects', 'file_id'))")
                .fetch_one(&pool)
                .await?
                .get(0);
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
    pool: PgPool,
    file_id: i64,
    part: Option<(String, i32)>,
    mut rx: tokio::sync::mpsc::Receiver<IngestMsg>,
) -> Result<(i64, Vec<u8>)> {
    // No global write lock: each ingest owns a connection and a COPY, and the
    // bounded channel backpressures the request body at COPY speed.
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
    // A multipart part records itself in the same transaction as its rows; a
    // re-sent part replaces the earlier attempt, rows included. Unpublished
    // rows are invisible: objects appear atomically at publish / Complete.
    if let Some((upload_id, part_no)) = &part {
        let old: Option<i64> = sqlx::query_scalar(
            "DELETE FROM s3p.upload_parts WHERE upload_id = $1 AND part_no = $2 RETURNING file_id",
        )
        .bind(upload_id)
        .bind(part_no)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(old) = old {
            sqlx::query("DELETE FROM s3p.chunks WHERE file_id = $1")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "INSERT INTO s3p.upload_parts (upload_id, part_no, file_id, size, sha256) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(upload_id)
        .bind(part_no)
        .bind(file_id)
        .bind(total)
        .bind(&sum)
        .execute(&mut *tx)
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
    pool: &PgPool,
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    swap_object(&mut tx, bucket, key, file_id, size, etag, None).await?;
    tx.commit().await?;
    publish_cache(bucket, key, file_id, size, etag, None);
    Ok(())
}

/// Point (bucket, key) at new storage inside `tx` and reap the storage of any
/// object it replaces (all of its files: the single file or every part).
async fn swap_object(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    bucket: &str,
    key: &str,
    file_id: i64,
    size: i64,
    etag: &[u8],
    parts: Option<(&[i64], &[i64])>,
) -> Result<()> {
    let old = sqlx::query("SELECT file_id, parts FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE")
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    sqlx::query(
        "INSERT INTO s3p.objects (bucket, key, file_id, size, etag, parts, part_ends) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size, \
           etag = EXCLUDED.etag, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends, created_at = now()",
    )
    .bind(bucket)
    .bind(key)
    .bind(file_id)
    .bind(size)
    .bind(etag)
    .bind(parts.map(|(ids, _)| ids))
    .bind(parts.map(|(_, ends)| ends))
    .execute(&mut **tx)
    .await?;
    if let Some(r) = old {
        reap(tx, r.get("file_id"), r.get("parts")).await?;
    }
    Ok(())
}

/// Delete every chunk row of an unpublished object's files.
async fn reap(tx: &mut Transaction<'_, sqlx::Postgres>, file_id: i64, parts: Option<Vec<i64>>) -> Result<()> {
    let mut dead = parts.unwrap_or_default();
    dead.push(file_id);
    sqlx::query("DELETE FROM s3p.chunks WHERE file_id = ANY($1)")
        .bind(&dead)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn publish_cache(bucket: &str, key: &str, file_id: i64, size: i64, etag: &[u8], parts: Option<(Vec<i64>, Vec<i64>)>) {
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
    let old = sqlx::query("DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id, parts")
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
    let found = old.is_some();
    if let Some(r) = old {
        reap(&mut tx, r.get("file_id"), r.get("parts")).await?;
    }
    tx.commit().await?;
    meta_invalidate(bucket, key);
    Ok(found)
}

/// Start (or re-attach, for a retried Create) the multipart upload of a key.
pub async fn create_upload(pool: &PgPool, bucket: &str, key: &str) -> Result<String> {
    Ok(sqlx::query_scalar(
        "INSERT INTO s3p.uploads (upload_id, bucket, key) VALUES (gen_random_uuid()::text, $1, $2) \
         ON CONFLICT (bucket, key) DO UPDATE SET bucket = EXCLUDED.bucket RETURNING upload_id",
    )
    .bind(bucket)
    .bind(key)
    .fetch_one(pool)
    .await?)
}

pub async fn upload_exists(pool: &PgPool, upload_id: &str) -> Result<bool> {
    Ok(sqlx::query("SELECT 1 FROM s3p.uploads WHERE upload_id = $1")
        .bind(upload_id)
        .fetch_optional(pool)
        .await?
        .is_some())
}

pub enum Completed {
    Done { bucket: String, key: String, etag: Vec<u8>, size: i64 },
    InvalidPart,
    NoSuchUpload,
}

/// Complete: every recorded part listed exactly once with a matching ETag (any
/// listing order), then the object publishes as its ordered part files. No
/// data moves. ETag = sha256 over the part sha256s (S3's hash-of-part-hashes).
pub async fn complete_upload(pool: &PgPool, upload_id: &str, listed: &[(i32, String)]) -> Result<Completed> {
    let mut tx = pool.begin().await?;
    let Some(up) = sqlx::query("SELECT bucket, key FROM s3p.uploads WHERE upload_id = $1 FOR UPDATE")
        .bind(upload_id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(Completed::NoSuchUpload);
    };
    let (bucket, key): (String, String) = (up.get("bucket"), up.get("key"));
    let rows = sqlx::query(
        "SELECT part_no, file_id, size, sha256 FROM s3p.upload_parts WHERE upload_id = $1 ORDER BY part_no",
    )
    .bind(upload_id)
    .fetch_all(&mut *tx)
    .await?;
    let mut want: Vec<&(i32, String)> = listed.iter().collect();
    want.sort_by_key(|(no, _)| *no);
    if want.is_empty() || want.len() != rows.len() {
        return Ok(Completed::InvalidPart);
    }
    let (mut ids, mut ends, mut size) = (Vec::with_capacity(rows.len()), Vec::with_capacity(rows.len()), 0i64);
    let mut hasher = Sha256::new();
    for (p, r) in want.iter().zip(&rows) {
        let sha: Vec<u8> = r.get("sha256");
        if p.0 != r.get::<i32, _>("part_no") || p.1 != hex(&sha) {
            return Ok(Completed::InvalidPart);
        }
        hasher.update(&sha);
        size += r.get::<i64, _>("size");
        ids.push(r.get::<i64, _>("file_id"));
        ends.push(size);
    }
    let etag = hasher.finalize().to_vec();
    swap_object(&mut tx, &bucket, &key, ids[0], size, &etag, Some((&ids, &ends))).await?;
    sqlx::query("DELETE FROM s3p.uploads WHERE upload_id = $1")
        .bind(upload_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    publish_cache(&bucket, &key, ids[0], size, &etag, Some((ids, ends)));
    Ok(Completed::Done { bucket, key, etag, size })
}

/// Drop a multipart upload and the rows of every part it recorded.
pub async fn abort_upload(pool: &PgPool, upload_id: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let ids: Vec<i64> = sqlx::query_scalar("SELECT file_id FROM s3p.upload_parts WHERE upload_id = $1")
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM s3p.uploads WHERE upload_id = $1")
        .bind(upload_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM s3p.chunks WHERE file_id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Abort uploads abandoned for longer than `age` (S3's incomplete-upload
/// lifecycle): their parts are committed rows, so something must reap them.
pub async fn expire_uploads(pool: &PgPool, age: Duration) -> Result<usize> {
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT upload_id FROM s3p.uploads WHERE created_at < now() - make_interval(secs => $1)")
            .bind(age.as_secs_f64())
            .fetch_all(pool)
            .await?;
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
