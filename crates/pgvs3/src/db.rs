//! PostgreSQL storage: objects as fixed-size inline byte rows.
//!
//! Read path: object metadata (bucket,key -> file_id/size/etag) and row slices
//! are cached in-process within a small fixed budget (PGVS3_CACHE_MIB, default
//! 2048): the cache sits between consumers (DuckLake workers own the big byte
//! cache) and Aurora, purely to remove round trips. Objects are immutable by
//! convention, so cache entries are populated at publish and dropped at
//! delete; repeat reads (parquet footers, hot ranges) reach PostgreSQL zero
//! times. Uncached rows fetch in ONE query (`no = ANY(...)`), served via a
//! bounded channel into the response body.
//!
//! Write path: deliberately single-threaded per process — one binary COPY
//! stream at a time (a global flush permit), rows cut across multipart part
//! boundaries. Aurora's write ceiling is ~150 MiB/s regardless of stream
//! count, so write parallelism buys latency-per-file at the price of every
//! byte of machinery; instead each flush takes the whole pipe and the rest
//! queue. The object row is published last, so objects appear atomically.

use std::collections::{HashMap, VecDeque};
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

/// Sliced pieces for the pass-through path (span form; rows may arrive in any
/// order — the forwarder reorders).
const GET_ROWS_SQL: &str = "SELECT c.no, \
       CASE WHEN $2::int8 <= c.no::int8 * $3 \
                 AND $2::int8 + $4::int8 - 1 >= (c.no::int8 + 1) * $3 - 1 \
            THEN c.data \
            ELSE substring(c.data \
              FROM (GREATEST($2::int8, c.no::int8 * $3) - c.no::int8 * $3)::int4 + 1 \
              FOR GREATEST(LEAST($2::int8 + $4::int8 - 1, (c.no::int8 + 1) * $3 - 1) \
                           - GREATEST($2::int8, c.no::int8 * $3) + 1, 0)::int4) \
       END AS piece \
FROM s3p.chunks c WHERE c.file_id = $1 AND c.no >= $5 AND c.no <= $6";

/// Whole rows for the admitted path (cached in full; slices computed in Rust).
const GET_ROWS_FULL_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no = ANY($2::int4[])";

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
        .max_connections(64)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // Sorts of slice rows carry the payload; never let them spill
                // (the generic plan badly misestimates range row counts).
                sqlx::query("SET work_mem = '64MB'").execute(&mut *conn).await?;
                // Deeper bitmap read-stream prefetch on network storage.
                let _ = sqlx::query("SET effective_io_concurrency = 32").execute(&mut *conn).await;
                if !indexscan {
                    sqlx::query("SET enable_indexscan = off").execute(&mut *conn).await?;
                }
                if !durable {
                    let _ = sqlx::query("SET synchronous_commit = off").execute(&mut *conn).await;
                }
                // PG18 read-stream combine width (pages per async op): wider
                // suiting network storage. io_method/io_workers are
                // postmaster-scoped and absent from Aurora's parameter-group
                // surface entirely — io_combine_limit is the live knob.
                let _ = sqlx::query("SET io_combine_limit = 64").execute(&mut *conn).await;
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
// pinned tier (count-capped, never evicted by byte pressure). Rows use 2Q:
// admission is gated on span size so scan floods never enter, probation is
// FIFO (25%), and a row earns protected-tier LRU residency (75%) only on its
// second distinct read. Coherence is structural: publish populates, delete
// drops. PGVS3_CACHE_MIB=0 disables the row tier; PGVS3_ADMIT_ROWS tunes the
// gate (default 8 rows = 64 KiB) against the stats log.
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
    // counters for gate/policy tuning (printed by the server's stats line)
    pub small_reqs: u64,
    pub big_reqs: u64,
    pub hits: u64,
    pub promotes: u64,
    pub admits: u64,
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
            small_reqs: 0,
            big_reqs: 0,
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

/// One-line cache telemetry for tuning PGVS3_ADMIT_ROWS / PGVS3_CACHE_MIB.
pub fn cache_stats_line() -> String {
    let c = row_cache().lock().unwrap();
    let reqs = c.small_reqs + c.big_reqs;
    format!(
        "cache: reqs={} (small={} big={}) hits={} admits={} (ghost={} promotes={}) tiers=s:{}B/m:{}B",
        reqs, c.small_reqs, c.big_reqs, c.hits, c.admits, c.admits_ghost, c.promotes, c.s_used, c.m_used
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

    if nrows <= admit_rows() {
        row_cache().lock().unwrap().small_reqs += 1;
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
        Ok(Some((smeta, PieceBody::OneShot(out.freeze()))))
    } else {
        row_cache().lock().unwrap().big_reqs += 1;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
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

/// Whole-row gather for small spans: 2Q cache first, one query for misses.
async fn gather_rows(pool: &PgPool, m: &Meta, first_row: i32, nrows: usize) -> Result<Vec<Bytes>> {
    let mut rows: Vec<Option<Bytes>> = vec![None; nrows];
    let mut missing: Vec<i32> = Vec::new();
    for i in 0..nrows {
        let no = first_row + i as i32;
        if let Some(r) = row_get(m.file_id, no) {
            rows[i] = Some(r);
        } else {
            missing.push(no);
        }
    }
    if !missing.is_empty() {
        let got = sqlx::query(GET_ROWS_FULL_SQL)
            .bind(m.file_id)
            .bind(&missing[..])
            .fetch_all(pool)
            .await?;
        for r in got {
            let no: i32 = r.get("no");
            let raw = r.try_get_raw("data")?;
            let data = Bytes::copy_from_slice(raw.as_bytes().unwrap_or(&[]));
            row_put(m.file_id, no, data.clone());
            if let Some(slot) = rows.get_mut((no - first_row) as usize) {
                *slot = Some(data);
            }
        }
    }
    Ok(rows.into_iter().map(|p| p.unwrap_or_default()).collect())
}

/// Effective `[start, end]` inclusive for a request (mirrors the old SQL clamp).
fn eff_range(size: i64, first: i64, last: i64, suffix: i64) -> (i64, i64) {
    if suffix >= 0 {
        ((size - suffix).max(0), size - 1)
    } else {
        (first.max(0), if last >= 0 { last.min(size - 1) } else { size - 1 })
    }
}

/// Large-span path: stream sliced pieces straight from the database (never
/// touches the cache). Rows arrive in `no` order only by plan luck (bitmap
/// scans do not preserve it), so a tiny reorder buffer restores byte order.
async fn stream_pass_through(
    pool: &PgPool,
    m: &Meta,
    start: i64,
    end: i64,
    first_row: i32,
    last_row: i32,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let mut stream = sqlx::query(GET_ROWS_SQL)
        .bind(m.file_id)
        .bind(start)
        .bind(ROW_BYTES)
        .bind(end - start + 1)
        .bind(first_row)
        .bind(last_row)
        .fetch(pool);
    let mut pending: HashMap<i32, Bytes> = HashMap::new();
    let mut next = first_row;
    while let Some(row) = stream.next().await {
        let row = row?;
        let no: i32 = row.get("no");
        let raw = row.try_get_raw("piece")?;
        pending.insert(no, Bytes::copy_from_slice(raw.as_bytes().unwrap_or(&[])));
        while let Some(p) = pending.remove(&next) {
            if !p.is_empty() && tx.send(Ok(p)).await.is_err() {
                return Ok(());
            }
            next += 1;
        }
    }
    while let Some(p) = pending.remove(&next) {
        if !p.is_empty() && tx.send(Ok(p)).await.is_err() {
            return Ok(());
        }
        next += 1;
    }
    Ok(())
}

fn admit_rows() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGVS3_ADMIT_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8)
    })
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

/// Overwrite-or-create in one transaction. The ETag is the sha256 of the body,
/// so retried identical PUTs are idempotent.
pub async fn put(pool: &PgPool, bucket: &str, key: &str, data: &[u8], etag: &[u8]) -> Result<()> {
    let mut tx = pool.begin().await?;
    let file_id = replace_object_row(&mut tx, bucket, key, data.len() as i64, etag).await?;

    let mut sink = tx.copy_in_raw(COPY_SQL).await?;
    let mut buf = Vec::with_capacity(SEND_BATCH);
    buf.extend_from_slice(&COPY_HEADER);
    for (b, batch) in data.chunks(ROW_BYTES as usize * ROW_BATCH).enumerate() {
        frame_rows(file_id, (b * ROW_BATCH) as i32, batch, &mut buf);
        if buf.len() >= SEND_BATCH {
            sink.send(&buf[..]).await?;
            buf.clear();
        }
    }
    buf.extend_from_slice(&COPY_TRAILER);
    sink.send(&buf[..]).await?;
    sink.finish().await?;

    tx.commit().await?;
    publish_cache(bucket, key, file_id, data.len() as i64, etag);
    Ok(())
}

/// Streaming ingest for multipart uploads: part bytes flow straight into one
/// open binary COPY stream as they arrive (rows cut across part boundaries
/// through a single cursor), so `Complete` only validates and publishes.
/// One ingest at a time holds the global write permit — writes stay
/// single-threaded per process by design.
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

/// Delete any previous object with this key and insert its row. Plain
/// statements on purpose: a single-statement CTE here (delete + scrub + insert)
/// is snapshot-hazardous — the insert's conflict handling can observe the
/// pre-delete row while the scrub's deletions are not yet visible to the chunk
/// insert, colliding on chunks_pkey on overwrite.
async fn replace_object_row(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    bucket: &str,
    key: &str,
    size: i64,
    etag: &[u8],
) -> Result<i64> {
    let old: Option<i64> =
        sqlx::query("DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id")
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut **tx)
            .await?
            .map(|r| r.get("file_id"));
    if let Some(old_id) = old {
        sqlx::query("DELETE FROM s3p.chunks WHERE file_id = $1")
            .bind(old_id)
            .execute(&mut **tx)
            .await?;
    }
    let file_id: i64 = sqlx::query(
        "INSERT INTO s3p.objects (bucket, key, size, etag) VALUES ($1, $2, $3, $4) RETURNING file_id",
    )
    .bind(bucket)
    .bind(key)
    .bind(size)
    .bind(etag)
    .fetch_one(&mut **tx)
    .await?
    .get("file_id");
    Ok(file_id)
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
