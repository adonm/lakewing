//! PostgreSQL storage: an object is numbered 8120-byte rows in `s3p.chunks`
//! plus one row in `s3p.objects` (layout: schema.sql).
//!
//! Reads cache metadata, never row bytes. Ranges stream rows in 8 MiB spans;
//! larger ranges use up to eight connections concurrently.
//!
//! Writes use a binary COPY per PUT or multipart part. Object publication
//! shares the COPY transaction; multipart Complete publishes existing parts.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::Context;
use std::time::SystemTime;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Row, Transaction};

use crate::cache::{epoch, meta_get, meta_invalidate, meta_put, Meta};
use crate::ingest::{IngestMsg, IngestResult, RowFramer, COPY_HEADER, COPY_SQL, SEND_BATCH};
pub use crate::pg::Pool;
use crate::stats::SMALL_MAX;

pub const SCHEMA: &str = include_str!("../schema.sql");

/// Row payload: file_id(8) + no(4) + varlena(4) + 8120 = 8136 data bytes,
/// tuple 8160 bytes = one row per 8 KB page.
pub const ROW_BYTES: i64 = 8120;

/// Whole rows for a contiguous `no` range (cheaper than `= ANY` on Aurora:
/// 1.74 vs 2.02 ms server time per warm 8 MiB span).
const GET_RANGE_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3";

/// Rows per part: any span up to SMALL_MAX (it may start mid-row) fits one.
const PART_ROWS: usize = SMALL_MAX / ROW_BYTES as usize + 2;
/// Parts in flight per GET, each on its own pool connection. Smaller parts
/// measured slower at DuckDB's concurrency (2 MiB: +5% pass time, 1 MiB:
/// +12-21%), so a span is only split above 8 MiB.
const PARTS_INFLIGHT: usize = 8;
/// Rows go to the response in chunks of about this size.
const CHUNK: usize = 256 << 10;

pub async fn connect(url: &str) -> Result<Pool> {
    // Bitmap scans use PostgreSQL 18 read-ahead for cold ranges; the session
    // timeouts prevent dead gateways from holding locks indefinitely.
    let session = String::from(
        "SET enable_indexscan = off; SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
         SET tcp_keepalives_idle = 30; SET tcp_keepalives_interval = 10; \
         SET tcp_keepalives_count = 3; SET idle_in_transaction_session_timeout = '5min'; \
         SET synchronous_commit = on;",
    );
    Pool::connect(
        url,
        crate::pg::Options {
            // Opening a connection (TCP, TLS, SCRAM, the SETs) costs ~10 ms,
            // so keep enough open for DuckDB's bursts.
            min: pool_min().min(pool_max()),
            // The budget the cluster really has, not its ceiling: 64 covers a
            // DuckDB worker's burst (measured wait-free), and the same default
            // must fit a small shared Postgres alongside Quickwit and
            // DuckLake. `PGVS3_POOL_MAX=256` where the cluster allows it.
            max: pool_max(),
            session,
            range_sql: GET_RANGE_SQL,
            range_types: &[Type::INT8, Type::INT4, Type::INT4],
        },
    )
    .await
}

/// Storage layout this binary reads and writes (schema.sql); bumped only by
/// breaking layout changes.
pub const LAYOUT_VERSION: i32 = 3;

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
/// backends open, so large fleets should lower it. Clamped to the max.
fn pool_min() -> usize {
    std::env::var("PGVS3_POOL_MIN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

/// Connections open at once (PGVS3_POOL_MAX, default 64). Sized to the
/// cluster's shared budget — the pool is one of several clients, and a fleet
/// of gateways multiplies this — not to the server's ceiling.
fn pool_max() -> usize {
    std::env::var("PGVS3_POOL_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
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

// ---------------------------------------------------------------------------

/// Metadata lookup, cached (a repeat open costs no round trip).
pub async fn meta(pool: &Pool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    if let Some(m) = meta_get(bucket, key) {
        return Ok(Some(m));
    }
    meta_fresh(pool, bucket, key).await
}

/// Current metadata for operations such as HEAD that cannot serve a stale
/// size, ETag or existence result after another gateway modifies the key.
pub async fn meta_fresh(pool: &Pool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    let row = pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT file_id, size, etag, EXTRACT(EPOCH FROM created_at)::float8, parts, part_ends \
             FROM s3p.objects WHERE bucket = $1 AND key = $2",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let Some(r) = row else {
        meta_invalidate(bucket, key);
        return Ok(None);
    };
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
    // A stale meta-cache entry (the object replaced through another gateway,
    // or behind all of them) shows up as missing rows — an overwrite reaps
    // the old file's rows in the same transaction, so a stale entry can never
    // quietly serve old bytes — or as a bogus 416, because range clamping
    // uses the old size. Before any byte has gone out, retry once with the
    // metadata looked up again: the client should never see either.
    for attempt in 0..2 {
        let Some(m) = meta(&pool, &bucket, &key).await? else {
            return Ok(None);
        };
        let (start, end) = eff_range(m.size, first, last, suffix);
        if m.size > 0 && (start > end || start >= m.size) && attempt == 0 {
            meta_invalidate(&bucket, &key);
            continue;
        }
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
        crate::stats::span_record(len);
        let class = usize::from(len > SMALL_MAX);
        let t0 = std::time::Instant::now();
        // ~8 MiB of chunks queue for the response; parts buffer their own rows.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
        let pool_c = pool.clone();
        tokio::spawn(async move {
            if let Err(e) = stream_span(&pool_c, &m, start, end, &tx).await {
                let _ = tx.send(Err(io_err(e))).await;
            }
            let us = t0.elapsed().as_micros() as u64;
            crate::stats::get_record(class, us, len as u64);
        });
        let head = rx.recv().await;
        crate::stats::ttfb_record(class, t0.elapsed().as_micros() as u64);
        match head {
            Some(Ok(head)) if head.len() == len => {
                return Ok(Some((smeta, PieceBody::OneShot(head))))
            }
            Some(Ok(head)) => {
                return Ok(Some((
                    smeta,
                    PieceBody::Streamed(PieceStream {
                        first: Some(head),
                        rx,
                    }),
                )))
            }
            Some(Err(e)) => {
                meta_invalidate(&bucket, &key);
                if attempt == 1 {
                    return Err(e.into());
                }
            }
            None => {
                return Err(anyhow::anyhow!(
                    "GET of {bucket}/{key} ended before any data"
                ))
            }
        }
    }
    unreachable!("get_body returns on every attempt")
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
    let t0 = std::time::Instant::now();
    let mut conn = pool.get().await?;
    crate::stats::part_record(t0.elapsed().as_micros() as u64);
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

/// Overwrite-or-create a buffered object through the same atomic write path
/// as the S3 PUT handler. The writer hashes and publishes in its COPY
/// transaction; failed writes leave no committed chunk rows behind.
pub async fn put(pool: &Pool, bucket: &str, key: &str, data: &[u8]) -> Result<()> {
    let writer = ChunkWriter::start_object(pool.clone(), bucket.to_owned(), key.to_owned()).await?;
    for chunk in data.chunks(SEND_BATCH) {
        writer.push(Bytes::copy_from_slice(chunk)).await?;
    }
    writer.finish().await?;
    Ok(())
}

enum WriterTarget {
    Object { bucket: String, key: String },
    Part { upload_id: String, part_no: i32 },
}

/// Streaming ingest: bytes flow into one open binary COPY stream through
/// `push` (rows cut across pushes through a single cursor). Every ingest has
/// its own connection and COPY, so PUTs and multipart parts write in parallel.
pub struct ChunkWriter {
    tx: Option<tokio::sync::mpsc::Sender<IngestMsg>>,
    done: Option<tokio::task::JoinHandle<IngestResult>>,
}

impl ChunkWriter {
    pub async fn start_object(pool: Pool, bucket: String, key: String) -> Result<Self> {
        Self::begin(pool, WriterTarget::Object { bucket, key }).await
    }

    /// A multipart part: its rows and its `upload_parts` record commit in one
    /// transaction, so a part is either fully recorded or absent.
    pub async fn start_part(pool: Pool, upload_id: String, part_no: i32) -> Result<Self> {
        Self::begin(pool, WriterTarget::Part { upload_id, part_no }).await
    }

    async fn begin(pool: Pool, target: WriterTarget) -> Result<Self> {
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
        let done = tokio::spawn(ingest_writer(pool, file_id, target, rx));
        Ok(Self {
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
    target: WriterTarget,
    mut rx: tokio::sync::mpsc::Receiver<IngestMsg>,
) -> Result<(i64, Vec<u8>)> {
    // No global write lock: each ingest owns a connection and a COPY, and the
    // bounded channel backpressures the request body at COPY speed.
    let t0 = std::time::Instant::now();

    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if let WriterTarget::Object { bucket, .. } = &target {
        anyhow::ensure!(
            tx.query_typed_opt(
                "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR KEY SHARE",
                &[(&bucket, Type::TEXT)],
            )
            .await?
            .is_some(),
            "bucket does not exist"
        );
    }
    let mut sink = std::pin::pin!(tx.copy_in::<_, Bytes>(COPY_SQL).await?);
    sink.send(Bytes::from_static(COPY_HEADER)).await?;

    let mut framer = RowFramer::new(file_id);
    while let Some(msg) = rx.recv().await {
        match msg {
            IngestMsg::Data(b) => {
                if let Some(buf) = framer.push(&b) {
                    sink.send(buf).await?;
                }
            }
            IngestMsg::Abort => anyhow::bail!("ingest aborted"),
        }
    }
    let (last, (total, sum)) = framer.finish();
    sink.send(last).await?;
    sink.as_mut().finish().await?;
    match &target {
        WriterTarget::Object { bucket, key } => {
            swap_object(&tx, bucket, key, file_id, total, &sum, None).await?;
        }
        WriterTarget::Part { upload_id, part_no } => {
            commit_part(&tx, upload_id, *part_no, file_id, total, &sum).await?;
        }
    }
    tx.commit().await?;
    if let WriterTarget::Object { bucket, key } = target {
        publish_cache(&bucket, &key, file_id, total, &sum, None);
    }
    eprintln!(
        "pgvs3: ingest {:.1} MiB at {:.0} MiB/s",
        total as f64 / 1024.0 / 1024.0,
        total as f64 / 1024.0 / 1024.0 / t0.elapsed().as_secs_f64().max(1e-9)
    );
    Ok((total, sum))
}

/// Record a multipart part in the same transaction as its rows; a re-sent
/// part replaces the earlier attempt, rows included. Unpublished rows are
/// invisible: objects appear atomically at publish / Complete.
async fn commit_part(
    tx: &Transaction<'_>,
    upload_id: &str,
    part_no: i32,
    file_id: i64,
    total: i64,
    sum: &[u8],
) -> Result<()> {
    let old = tx
        .query_typed_opt(
            "DELETE FROM s3p.upload_parts WHERE upload_id = $1 AND part_no = $2 RETURNING file_id",
            &[(&upload_id, Type::TEXT), (&part_no, Type::INT4)],
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
            (&upload_id, Type::TEXT),
            (&part_no, Type::INT4),
            (&file_id, Type::INT8),
            (&total, Type::INT8),
            (&sum, Type::BYTEA),
        ],
    )
    .await?;
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

pub async fn create_bucket(pool: &Pool, name: &str) -> Result<()> {
    pool.get()
        .await?
        .query_typed(
            "INSERT INTO s3p.buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[(&name, Type::TEXT)],
        )
        .await?;
    Ok(())
}

pub async fn bucket_exists(pool: &Pool, name: &str) -> Result<bool> {
    Ok(pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT 1 FROM s3p.buckets WHERE name = $1",
            &[(&name, Type::TEXT)],
        )
        .await?
        .is_some())
}

pub enum BucketDeletion {
    Deleted,
    NotEmpty,
    NotFound,
}

pub async fn delete_bucket(pool: &Pool, name: &str) -> Result<BucketDeletion> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if tx
        .query_typed_opt(
            "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR UPDATE",
            &[(&name, Type::TEXT)],
        )
        .await?
        .is_none()
    {
        return Ok(BucketDeletion::NotFound);
    }
    let occupied: bool = tx
        .query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM s3p.objects WHERE bucket = $1) \
                    OR EXISTS (SELECT 1 FROM s3p.uploads WHERE bucket = $1)",
            &[(&name, Type::TEXT)],
        )
        .await?
        .try_get(0)?;
    if occupied {
        return Ok(BucketDeletion::NotEmpty);
    }
    tx.query_typed(
        "DELETE FROM s3p.buckets WHERE name = $1",
        &[(&name, Type::TEXT)],
    )
    .await?;
    tx.commit().await?;
    Ok(BucketDeletion::Deleted)
}

/// Start (or re-attach, for a retried Create) the multipart upload of a key.
pub async fn create_upload(pool: &Pool, bucket: &str, key: &str) -> Result<Option<String>> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if tx
        .query_typed_opt(
            "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR KEY SHARE",
            &[(&bucket, Type::TEXT)],
        )
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let id = tx
        .query_typed_one(
            "INSERT INTO s3p.uploads (upload_id, bucket, key) VALUES (gen_random_uuid()::text, $1, $2) \
              ON CONFLICT (bucket, key) DO UPDATE SET bucket = EXCLUDED.bucket RETURNING upload_id",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?
        .try_get(0)?;
    tx.commit().await?;
    Ok(Some(id))
}

pub async fn upload_exists(pool: &Pool, upload_id: &str, bucket: &str, key: &str) -> Result<bool> {
    Ok(pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT 1 FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3",
            &[
                (&upload_id, Type::TEXT),
                (&bucket, Type::TEXT),
                (&key, Type::TEXT),
            ],
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
    request_bucket: &str,
    request_key: &str,
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
    if bucket != request_bucket || key != request_key {
        return Ok(Completed::NoSuchUpload);
    }
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
pub async fn abort_upload(pool: &Pool, upload_id: &str, bucket: &str, key: &str) -> Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if tx
        .query_typed_opt(
            "SELECT 1 FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR UPDATE",
            &[
                (&upload_id, Type::TEXT),
                (&bucket, Type::TEXT),
                (&key, Type::TEXT),
            ],
        )
        .await?
        .is_none()
    {
        return Ok(false);
    }
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
    Ok(true)
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

pub async fn buckets(pool: &Pool) -> Result<Vec<(String, SystemTime)>> {
    let mut out = Vec::new();
    for r in pool
        .get()
        .await?
        .query_typed(
            "SELECT name, EXTRACT(EPOCH FROM created_at)::float8 \
             FROM s3p.buckets ORDER BY name",
            &[],
        )
        .await?
    {
        out.push((r.try_get(0)?, epoch(r.try_get(1)?)));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn a_failed_publish_rolls_back_its_copy_rows() -> Result<()> {
        let url = std::env::var("PGVS3_TEST_DB_URL")?;
        let pool = connect(&url).await?;
        let file_id: i64 = pool
            .get()
            .await?
            .query_typed_one(
                "SELECT nextval(pg_get_serial_sequence('s3p.objects', 'file_id'))",
                &[],
            )
            .await?
            .try_get(0)?;
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(IngestMsg::Data(Bytes::from(vec![
            42;
            ROW_BYTES as usize + 19
        ])))
        .await?;
        drop(tx);

        // PostgreSQL rejects NUL in the object key at the publish step,
        // *after* COPY has received its rows. Neither action may commit.
        let result = ingest_writer(
            pool.clone(),
            file_id,
            WriterTarget::Object {
                bucket: "pgvs3-contract".to_owned(),
                key: "invalid\0key".to_owned(),
            },
            rx,
        )
        .await;
        anyhow::ensure!(result.is_err(), "invalid key unexpectedly published");
        let count: i64 = pool
            .get()
            .await?
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        anyhow::ensure!(count == 0, "failed publish left orphan chunk rows");
        Ok(())
    }
}
