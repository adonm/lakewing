//! The S3 service: `s3s` REST/SigV4 layer over `db::` PostgreSQL storage.
//! Only HEAD, GET(+Range), PUT, DELETE, multipart and bucket operations are
//! implemented; everything else stays `NotImplemented` (s3s trait defaults).

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use bytes::Bytes;
use hyper::service::Service as HyperService;
use hyper::{Request, Response};
use s3s::auth::SimpleAuth;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketOutput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag,
    GetObjectInput, GetObjectOutput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput,
    PutObjectOutput, Range, StreamingBlob, Timestamp, UploadPartInput, UploadPartOutput,
};
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::db;

/// Stateless over Aurora: multipart upload state lives in `s3p.uploads`, so
/// any gateway instance can serve any request of any upload.
#[derive(Clone)]
pub struct PgS3 {
    pool: db::Pool,
}

fn etag(raw: &[u8]) -> Option<ETag> {
    format!("\"{}\"", db::hex(raw)).parse().ok()
}

fn internal(e: impl std::fmt::Display) -> s3s::S3Error {
    eprintln!("pgvs3 internal error: {e}");
    s3_error!(InternalError)
}

/// Upper bound (exclusive) for `key LIKE prefix%` as a range scan bound.
fn prefix_end(prefix: &str) -> String {
    let mut s = prefix.to_string();
    while let Some(last) = s.pop() {
        if let Some(next) = char::from_u32(last as u32 + 1) {
            s.push(next);
            return s;
        }
    }
    "\u{10FFFF}".to_owned()
}

/// Map the S3 Range header to `get()`'s (first, last, suffix) parameters.
fn range_params(range: Option<Range>) -> (i64, i64, i64) {
    match range {
        None => (0, -1, -1),
        Some(Range::Int { first, last }) => {
            (first as i64, last.map(|v| v as i64).unwrap_or(-1), -1)
        }
        Some(Range::Suffix { length }) => (0, -1, length as i64),
    }
}

/// One row of a `list_objects_v2` scan: push it onto `contents` or, past a
/// `delimiter`, onto `common`. Returns the new cursor and whether the listing
/// is full.
fn listing_step(
    r: &db::Listed,
    prefix: &str,
    delimiter: &str,
    max: usize,
    contents: &mut Vec<Object>,
    common: &mut Vec<CommonPrefix>,
    seen: &mut BTreeSet<String>,
) -> (String, bool) {
    let rest = &r.key[prefix.len()..];
    if !delimiter.is_empty() {
        if let Some(idx) = rest.find(delimiter) {
            let cp = format!("{prefix}{}", &rest[..idx + delimiter.len()]);
            if seen.insert(cp.clone()) {
                if contents.len() + common.len() >= max {
                    return (r.key.clone(), true);
                }
                common.push(CommonPrefix { prefix: Some(cp) });
            }
            return (r.key.clone(), false);
        }
    }
    if contents.len() + common.len() >= max {
        return (r.key.clone(), true);
    }
    contents.push(Object {
        key: Some(r.key.clone()),
        size: Some(r.size),
        e_tag: etag(&r.etag),
        last_modified: Some(Timestamp::from(r.created_at)),
        ..Default::default()
    });
    (r.key.clone(), false)
}

#[async_trait::async_trait]
impl S3 for PgS3 {
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let Some(meta) = db::meta_fresh(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?
        else {
            return Err(
                if db::bucket_exists(&self.pool, &input.bucket)
                    .await
                    .map_err(internal)?
                {
                    s3_error!(NoSuchKey)
                } else {
                    s3_error!(NoSuchBucket)
                },
            );
        };
        let out = HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(meta.size),
            e_tag: etag(&meta.etag),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let ranged = input.range.is_some();
        let (first, last, suffix) = range_params(input.range);
        // One round trip: metadata + a stream of exactly the requested bytes.
        let Some((meta, body)) = db::get_body(
            self.pool.clone(),
            input.bucket.clone(),
            input.key,
            first,
            last,
            suffix,
        )
        .await
        .map_err(internal)?
        else {
            return Err(
                if db::bucket_exists(&self.pool, &input.bucket)
                    .await
                    .map_err(internal)?
                {
                    s3_error!(NoSuchKey)
                } else {
                    s3_error!(NoSuchBucket)
                },
            );
        };

        if meta.size > 0 && (meta.start > meta.end || meta.start >= meta.size) {
            return Err(s3_error!(InvalidRange));
        }
        let body_len = meta.len();
        let out = GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            body: Some(match body {
                db::PieceBody::OneShot(b) => StreamingBlob::from_bytes(b),
                db::PieceBody::Streamed(s) => StreamingBlob::wrap(s),
            }),
            content_length: Some(body_len),
            content_range: ranged
                .then(|| format!("bytes {}-{}/{}", meta.start, meta.end, meta.size)),
            content_type: Some("application/octet-stream".to_owned()),
            e_tag: etag(&meta.etag),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let mut input = req.input;
        if !db::bucket_exists(&self.pool, &input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        let body = input
            .body
            .take()
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        // Streams into its own COPY (never buffered whole); the writer hashes.
        let writer =
            db::ChunkWriter::start_object(self.pool.clone(), input.bucket.clone(), input.key)
                .await
                .map_err(internal)?;
        let (_size, sum) = match db::ingest_body(writer, body).await {
            Ok(done) => done,
            Err(e) => {
                return Err(
                    if db::bucket_exists(&self.pool, &input.bucket)
                        .await
                        .map_err(internal)?
                    {
                        internal(e)
                    } else {
                        s3_error!(NoSuchBucket)
                    },
                );
            }
        };
        Ok(S3Response::new(PutObjectOutput {
            e_tag: etag(&sum),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<s3s::dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        if !db::bucket_exists(&self.pool, &input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        db::delete(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        if !db::bucket_exists(&self.pool, &input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        if input.delete.objects.len() > 1000 {
            return Err(s3_error!(InvalidRequest));
        }
        // This store is unversioned. Validate the whole batch before any
        // deletion so an unsupported conditional/versioned request cannot
        // partially delete it.
        for obj in &input.delete.objects {
            if obj.version_id.is_some()
                || obj.e_tag.is_some()
                || obj.last_modified_time.is_some()
                || obj.size.is_some()
            {
                return Err(s3_error!(NotImplemented));
            }
        }
        let mut deleted = Vec::new();
        for obj in input.delete.objects {
            db::delete(&self.pool, &input.bucket, &obj.key)
                .await
                .map_err(internal)?;
            if input.delete.quiet != Some(true) {
                deleted.push(DeletedObject {
                    key: Some(obj.key),
                    ..Default::default()
                });
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        // Idempotent per (bucket, key): a client retry of Create re-attaches
        // to the in-progress upload instead of forking a second id.
        let id = db::create_upload(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchBucket))?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let mut input = req.input;
        if !db::upload_exists(&self.pool, &input.upload_id, &input.bucket, &input.key)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchUpload));
        }
        // Each part streams straight into its own COPY, in any arrival order
        // and in parallel with its siblings; a re-sent part replaces the
        // earlier attempt atomically.
        let writer = db::ChunkWriter::start_part(
            self.pool.clone(),
            input.upload_id.clone(),
            input.part_number,
        )
        .await
        .map_err(internal)?;
        let body = input
            .body
            .take()
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        let (size, sum) = db::ingest_body(writer, body).await.map_err(internal)?;
        eprintln!(
            "pgvs3: upload_part {} no={} {size} bytes",
            input.upload_id, input.part_number
        );
        Ok(S3Response::new(UploadPartOutput {
            e_tag: etag(&sum),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let listed = input
            .multipart_upload
            .as_ref()
            .and_then(|m| m.parts.clone())
            .unwrap_or_default();
        let mut parts = Vec::with_capacity(listed.len());
        for cp in &listed {
            let no = cp.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
            parts.push((
                no,
                cp.e_tag
                    .as_ref()
                    .map(|e| e.value().to_owned())
                    .unwrap_or_default(),
            ));
        }
        // Parts are already rows in Aurora: Complete validates and publishes,
        // moving no data (so no long response window to lose).
        match db::complete_upload(
            &self.pool,
            &input.upload_id,
            &input.bucket,
            &input.key,
            &parts,
        )
        .await
        .map_err(internal)?
        {
            db::Completed::Done {
                bucket,
                key,
                etag: sum,
                size,
            } => {
                eprintln!(
                    "pgvs3: multipart {bucket}/{key} = {size} bytes in {} parts",
                    parts.len()
                );
                Ok(S3Response::new(CompleteMultipartUploadOutput {
                    location: Some(format!("/{bucket}/{key}")),
                    bucket: Some(bucket),
                    key: Some(key),
                    e_tag: etag(&sum),
                    ..Default::default()
                }))
            }
            db::Completed::InvalidPart => Err(s3_error!(InvalidPart)),
            db::Completed::NoSuchUpload => {
                // Retried after a lost success response: the object is
                // already published (S3 clients treat identical
                // re-completion as success).
                match db::meta_fresh(&self.pool, &input.bucket, &input.key)
                    .await
                    .map_err(internal)?
                {
                    Some(meta) => Ok(S3Response::new(CompleteMultipartUploadOutput {
                        bucket: Some(input.bucket),
                        key: Some(input.key),
                        e_tag: etag(&meta.etag),
                        ..Default::default()
                    })),
                    None => Err(s3_error!(NoSuchUpload)),
                }
            }
        }
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        if !db::abort_upload(&self.pool, &input.upload_id, &input.bucket, &input.key)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchUpload));
        }
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = &input.bucket;
        if !db::bucket_exists(&self.pool, bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter.unwrap_or_default();
        let max = input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;
        let echo_token = input.continuation_token.clone();
        let mut after = input
            .continuation_token
            .or(input.start_after)
            .unwrap_or_default();
        let bound = prefix_end(&prefix);

        let mut contents: Vec<Object> = Vec::new();
        let mut common: Vec<CommonPrefix> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;

        'outer: loop {
            let rows = db::list(&self.pool, bucket, &prefix, &bound, &after, 1024)
                .await
                .map_err(internal)?;
            if rows.is_empty() {
                break;
            }
            let last_row = rows.len() < 1024;
            for r in rows {
                let (cursor, full) = listing_step(
                    &r,
                    &prefix,
                    &delimiter,
                    max,
                    &mut contents,
                    &mut common,
                    &mut seen,
                );
                after = cursor;
                if full {
                    truncated = true;
                    break 'outer;
                }
            }
            if last_row {
                break;
            }
        }

        let out = ListObjectsV2Output {
            name: Some(bucket.clone()),
            prefix: Some(prefix),
            max_keys: Some(max as i32),
            key_count: Some((contents.len() + common.len()) as i32),
            continuation_token: echo_token,
            is_truncated: Some(truncated),
            next_continuation_token: truncated.then(|| after.clone()),
            contents: Some(contents),
            common_prefixes: (!common.is_empty()).then_some(common),
            delimiter: (!delimiter.is_empty()).then_some(delimiter),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn create_bucket(
        &self,
        req: S3Request<s3s::dto::CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        db::create_bucket(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{}", req.input.bucket)),
            ..Default::default()
        }))
    }

    async fn head_bucket(
        &self,
        req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if !db::bucket_exists(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        match db::delete_bucket(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?
        {
            db::BucketDeletion::Deleted => Ok(S3Response::new(DeleteBucketOutput {})),
            db::BucketDeletion::NotEmpty => Err(s3_error!(BucketNotEmpty)),
            db::BucketDeletion::NotFound => Err(s3_error!(NoSuchBucket)),
        }
    }

    async fn list_buckets(
        &self,
        _req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let names = db::buckets(&self.pool).await.map_err(internal)?;
        let out = ListBucketsOutput {
            buckets: Some(
                names
                    .into_iter()
                    .map(|(name, created_at)| Bucket {
                        name: Some(name),
                        creation_date: Some(Timestamp::from(created_at)),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }
}

/// Debug endpoint: `GET /_pgvs3/stats` returns the cache telemetry line so
/// harnesses snapshot it on demand (the 60s log cadence misses short runs).
struct StatsRoute;

#[async_trait::async_trait]
impl s3s::route::S3Route for StatsRoute {
    fn is_match(
        &self,
        method: &hyper::http::Method,
        uri: &hyper::http::Uri,
        _: &hyper::http::HeaderMap,
        _: &mut hyper::http::Extensions,
    ) -> bool {
        *method == hyper::http::Method::GET && uri.path() == "/_pgvs3/stats"
    }
    async fn call(&self, _req: S3Request<s3s::Body>) -> S3Result<S3Response<s3s::Body>> {
        let line = format!("{}\n", crate::stats::stage_stats_line());
        Ok(S3Response::new(s3s::Body::from(bytes::Bytes::from(line))))
    }
}

/// Dispatch layer: an unauthenticated health path for probes, everything else
/// to the SigV4-protected S3 service. Kubernetes probes cannot sign requests,
/// and `s3s` rejects them with 403 (which used to make liveness kill the pod).
#[derive(Clone)]
struct Gateway {
    s3: S3Service,
}

impl HyperService<Request<hyper::body::Incoming>> for Gateway {
    type Response = Response<s3s::Body>;
    type Error = s3s::HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        if req.uri().path() == "/healthz" {
            let body = s3s::Body::from(Bytes::from_static(b"ok\n"));
            return Box::pin(async move { Ok(Response::new(body)) });
        }
        let svc = self.s3.clone();
        Box::pin(async move { HyperService::call(&svc, req).await })
    }
}

pub struct ServeConfig {
    pub addr: String,
    pub access_key: String,
    pub secret_key: String,
}

pub async fn serve(pool: db::Pool, cfg: ServeConfig) -> Result<()> {
    // Best-effort performance warmup; no gateway-owned cleanup or cursor.
    let warm = pool.clone();
    tokio::spawn(async move {
        match crate::warmup::prewarm_index(&warm).await {
            Ok(Some(blocks)) => eprintln!("pgvs3: prewarmed chunk index ({blocks} blocks)"),
            Ok(None) => eprintln!("pgvs3: chunk index exceeds 10% of shared_buffers: left cold"),
            Err(e) => eprintln!("pgvs3: prewarm skipped: {e}"),
        }
    });
    let s3 = PgS3 { pool };
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(SimpleAuth::from_single(
        cfg.access_key.as_str(),
        cfg.secret_key.as_str(),
    ));
    builder.set_route(StatsRoute);
    let service = Gateway {
        s3: builder.build(),
    };

    let listener = tokio::net::TcpListener::bind(&cfg.addr).await?;
    println!(
        "pgvs3 serving on http://{} (sigv4 key: {})",
        listener.local_addr()?,
        cfg.access_key
    );
    tokio::spawn(async {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            eprintln!("{}", crate::stats::stage_stats_line());
        }
    });
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true).ok(); // no Nagle: request/response latency matters
        let io = hyper_util::rt::TokioIo::new(stream);
        let svc = service.clone();
        tokio::spawn(async move {
            let builder =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            if let Err(e) = builder.serve_connection(io, svc).await {
                eprintln!("connection {peer}: {e}");
            }
        });
    }
}
