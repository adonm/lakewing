//! The S3 service: `s3s` REST/SigV4 layer over `db::` PostgreSQL storage.
//! Only HEAD, GET(+Range), PUT, DELETE and ListObjectsV2/Buckets are
//! implemented; everything else stays `NotImplemented` (s3s trait defaults).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use futures::StreamExt;
use s3s::auth::SimpleAuth;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketOutput,
    DeleteObjectOutput, ETag, GetObjectInput, GetObjectOutput, HeadBucketOutput,
    HeadObjectInput, HeadObjectOutput, ListBucketsOutput, ListObjectsV2Input,
    ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput, Range, StreamingBlob,
    Timestamp, UploadPartInput, UploadPartOutput,
};
use s3s::service::S3ServiceBuilder;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::io::AsyncWriteExt;

use crate::db;

/// One in-flight multipart upload: parts staged as temp files, re-chunked into
/// rows on CompleteMultipartUpload.
#[derive(Clone)]
struct Mpu {
    bucket: String,
    key: String,
    dir: PathBuf,
    parts: BTreeMap<i32, (PathBuf, Vec<u8>)>, // part_no -> (file, sha256)
}

#[derive(Clone)]
pub struct PgS3 {
    pool: PgPool,
    mpus: Arc<tokio::sync::Mutex<HashMap<String, Mpu>>>,
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
        Some(Range::Int { first, last }) => (first as i64, last.map(|v| v as i64).unwrap_or(-1), -1),
        Some(Range::Suffix { length }) => (0, -1, length as i64),
    }
}

#[async_trait::async_trait]
impl S3 for PgS3 {
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let meta = db::meta(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let out = HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(meta.size),
            e_tag: etag(&meta.etag),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let ranged = input.range.is_some();
        let (first, last, suffix) = range_params(input.range);
        // One round trip: metadata + a stream of exactly the requested bytes.
        let (meta, body) = db::get_stream(self.pool.clone(), input.bucket, input.key, first, last, suffix)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        if meta.size > 0 && (meta.start > meta.end || meta.start >= meta.size) {
            return Err(s3_error!(InvalidRange));
        }
        let body_len = meta.len();
        let out = GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            body: Some(StreamingBlob::wrap(body)),
            content_length: Some(body_len),
            content_range: ranged.then(|| format!("bytes {}-{}/{}", meta.start, meta.end, meta.size)),
            content_type: Some("application/octet-stream".to_owned()),
            e_tag: etag(&meta.etag),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let mut input = req.input;
        let mut body = input.body.take().unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        let mut buf = BytesMut::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.map_err(internal)?);
        }
        let data = buf.freeze();
        let sum = Sha256::digest(&data).to_vec();
        db::put(&self.pool, &input.bucket, &input.key, &data, &sum)
            .await
            .map_err(internal)?;
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
        db::delete(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        // Idempotent per (bucket, key): a client retry of Create must not fork
        // the upload into two ids.
        let mut mpus = self.mpus.lock().await;
        let existing = mpus
            .iter()
            .find(|(_, m)| m.bucket == input.bucket && m.key == input.key)
            .map(|(id, _)| id.clone());
        let id = match existing {
            Some(id) => id,
            None => {
                let id = format!("{:016x}", std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0));
                // Staging lives on the NVMe scratch tree, not tmpfs: parts can be GBs.
                let dir = std::path::PathBuf::from(".tmp/pgvs3/mpu").join(&id);
                tokio::fs::create_dir_all(&dir).await.map_err(internal)?;
                mpus.insert(
                    id.clone(),
                    Mpu {
                        bucket: input.bucket.clone(),
                        key: input.key.clone(),
                        dir,
                        parts: BTreeMap::new(),
                    },
                );
                id
            }
        };
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(id),
            ..Default::default()
        }))
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let mut input = req.input;
        let id = input.upload_id;
        let part_no = input.part_number;
        let path = {
            let mut mpus = self.mpus.lock().await;
            let mpu = mpus.get_mut(&id).ok_or_else(|| s3_error!(NoSuchUpload))?;
            let path = mpu.dir.join(format!("{part_no:06}"));
            mpu.parts.insert(part_no, (path.clone(), Vec::new()));
            path
        };
        let mut f = tokio::fs::File::create(&path).await.map_err(internal)?;
        let mut hasher = Sha256::new();
        let mut body = input.body.take().unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(internal)?;
            hasher.update(&chunk);
            f.write_all(&chunk).await.map_err(internal)?;
        }
        f.flush().await.map_err(internal)?;
        let sum = hasher.finalize().to_vec();
        {
            let mut mpus = self.mpus.lock().await;
            if let Some(slot) = mpus.get_mut(&id).and_then(|m| m.parts.get_mut(&part_no)) {
                slot.1 = sum.clone();
            }
        }
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
        // State survives until success: Complete flushes gigabytes to the
        // database and clients retry on timeout — a re-run must be able to
        // find (or redo) the work, never see NoSuchUpload.
        let mpu = {
            let mpus = self.mpus.lock().await;
            match mpus.get(&input.upload_id) {
                Some(m) => m.clone(),
                None => {
                    // Retried after a lost success response: the object is
                    // already there. Report it (S3 clients treat identical
                    // re-completion as success).
                    if let Some(meta) = db::meta(&self.pool, &input.bucket, &input.key)
                        .await
                        .map_err(internal)?
                    {
                        return Ok(S3Response::new(CompleteMultipartUploadOutput {
                            bucket: Some(input.bucket),
                            key: Some(input.key),
                            e_tag: etag(&meta.etag),
                            ..Default::default()
                        }));
                    }
                    return Err(s3_error!(NoSuchUpload));
                }
            }
        };

        let listed = input.multipart_upload.and_then(|m| m.parts).unwrap_or_default();
        if listed.is_empty() {
            return Err(s3_error!(InvalidPart));
        }
        let mut paths = Vec::with_capacity(listed.len());
        let mut last_no = 0i32;
        for cp in listed {
            let no = cp.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
            if no <= last_no {
                return Err(s3_error!(InvalidPartOrder));
            }
            last_no = no;
            let (path, sha) = mpu.parts.get(&no).ok_or_else(|| s3_error!(InvalidPart))?;
            let want = cp.e_tag.map(|e| e.value().to_owned()).unwrap_or_default();
            if want != db::hex(sha) {
                return Err(s3_error!(InvalidPart));
            }
            paths.push(path.clone());
        }

        let (size, sum) = db::put_files(&self.pool, &mpu.bucket, &mpu.key, &paths)
            .await
            .map_err(internal)?;
        // Only now is the upload finished with: drop state and staging.
        self.mpus.lock().await.remove(&input.upload_id);
        let _ = tokio::fs::remove_dir_all(&mpu.dir).await;
        eprintln!("pgvs3: multipart {}/{} = {} bytes", mpu.bucket, mpu.key, size);
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(mpu.bucket.clone()),
            key: Some(mpu.key.clone()),
            e_tag: etag(&sum),
            location: Some(format!("/{}/{}", mpu.bucket, mpu.key)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        if let Some(mpu) = self.mpus.lock().await.remove(&req.input.upload_id) {
            let _ = tokio::fs::remove_dir_all(&mpu.dir).await;
        }
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = &input.bucket;
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
                let rest = &r.key[prefix.len()..];
                if !delimiter.is_empty() {
                    if let Some(idx) = rest.find(&delimiter) {
                        let cp = format!("{prefix}{}", &rest[..idx + delimiter.len()]);
                        if seen.insert(cp.clone()) {
                            if contents.len() + common.len() >= max {
                                truncated = true;
                                break 'outer;
                            }
                            common.push(CommonPrefix {
                                prefix: Some(cp),
                                ..Default::default()
                            });
                        }
                        after = r.key;
                        continue;
                    }
                }
                if contents.len() + common.len() >= max {
                    truncated = true;
                    break 'outer;
                }
                contents.push(Object {
                    key: Some(r.key.clone()),
                    size: Some(r.size),
                    e_tag: etag(&r.etag),
                    last_modified: Some(Timestamp::from(r.created_at)),
                    ..Default::default()
                });
                after = r.key;
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
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{}", req.input.bucket)),
            ..Default::default()
        }))
    }

    async fn head_bucket(
        &self,
        _req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        _req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        Ok(S3Response::new(DeleteBucketOutput {}))
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
                    .map(|name| Bucket {
                        name: Some(name),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }
}

pub struct ServeConfig {
    pub addr: String,
    pub access_key: String,
    pub secret_key: String,
}

pub async fn serve(pool: PgPool, cfg: ServeConfig) -> Result<()> {
    let s3 = PgS3 {
        pool,
        mpus: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(SimpleAuth::from_single(cfg.access_key.as_str(), cfg.secret_key.as_str()));
    let service = builder.build();

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
            eprintln!("{}", crate::db::cache_stats_line());
        }
    });
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true).ok(); // no Nagle: request/response latency matters
        let io = hyper_util::rt::TokioIo::new(stream);
        let svc = service.clone();
        tokio::spawn(async move {
            let builder = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            if let Err(e) = builder.serve_connection(io, svc).await {
                eprintln!("connection {peer}: {e}");
            }
        });
    }
}
