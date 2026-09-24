//! Latency/throughput bench: signed S3 client (object_store, SigV4) against the
//! gateway, plus a raw-PG floor for the same read sizes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::db;
use crate::seed::Filler;

pub struct BenchConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub sizes: Vec<usize>,
    pub concurrency: Vec<usize>,
    pub requests: usize,
    pub pg_url: String,
    pub sample: usize,
}

impl BenchConfig {
    fn clone_fields(&self) -> BenchConfig {
        BenchConfig {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            sizes: vec![],
            concurrency: vec![],
            requests: 0,
            pg_url: self.pg_url.clone(),
            sample: self.sample,
        }
    }
}

fn store(cfg: &BenchConfig) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region("us-east-1")
            .with_endpoint(&cfg.endpoint)
            .with_access_key_id(&cfg.access_key)
            .with_secret_access_key(&cfg.secret_key)
            .with_allow_http(true)
            .build()?,
    ))
}

fn stats(label: &str, mut v: Vec<Duration>, bytes: usize) {
    v.sort_unstable();
    let at = |p: f64| v[(((v.len() - 1) as f64) * p) as usize].as_secs_f64() * 1e3;
    let total_s: f64 = v.iter().map(|d| d.as_secs_f64()).sum();
    let mib = (bytes * v.len()) as f64 / 1024.0 / 1024.0;
    println!(
        "{label:<28} n={:<5} p50={:>7.2}ms p95={:>7.2}ms p99={:>7.2}ms max={:>7.2}ms thr={:>8.0} MiB/s",
        v.len(),
        at(0.50),
        at(0.95),
        at(0.99),
        at(1.0),
        mib / total_s.max(1e-9)
    );
}

type Op = Arc<dyn Fn(Arc<dyn ObjectStore>, String, u64, usize) -> BoxFuture<'static, Result<Duration>> + Send + Sync>;

/// Spawn `concurrency` tasks, each running `per_task` sequential ops against its
/// own store handle. `op(store, key, offset, size)` performs one measured call;
/// key/offset are drawn deterministically per task.
async fn load(
    concurrency: usize,
    per_task: usize,
    size: usize,
    cfg: BenchConfig,
    keys: Vec<(String, u64)>,
    op: Op,
) -> Result<Vec<Duration>> {
    let mut handles = Vec::new();
    for t in 0..concurrency.max(1) {
        let keys = keys.clone();
        let op = op.clone();
        let cfg = cfg.clone_fields();
        handles.push(tokio::spawn(async move {
            let store = store(&cfg)?;
            let mut filler = Filler::new(0xC0FFEE + t as u64);
            let mut out = Vec::with_capacity(per_task);
            for _ in 0..per_task {
                let (key, obj_size) = keys[filler.next_u64() as usize % keys.len()].clone();
                let off = if obj_size <= size as u64 {
                    0
                } else {
                    let slots = (obj_size - size as u64) / 4096 + 1;
                    (filler.next_u64() % slots) * 4096
                };
                // S3 clamps ranges at EOF: expect at most what the object has.
                let want = ((obj_size - off) as usize).min(size);
                out.push(op(store.clone(), key, off, want).await?);
            }
            anyhow::Ok(out)
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await??);
    }
    Ok(all)
}

fn get_op(_size: usize) -> Op {
    Arc::new(move |store, key, off, size| {
        Box::pin(async move {
            let path = Path::from(key.clone());
            let t0 = Instant::now();
            let bytes = store.get_range(&path, off..off + size as u64).await?;
            if bytes.len() != size {
                anyhow::bail!("short read: {} != {} at {key} off={off}", bytes.len(), size);
            }
            Ok(t0.elapsed())
        })
    })
}

pub async fn run(cfg: BenchConfig) -> Result<()> {
    // Discover up to `sample` objects to spread reads across (cold tests need
    // a sample larger than any cache).
    let mut keys: Vec<(String, u64)> = Vec::new();
    let mut stream = store(&cfg)?.list(None);
    while let Some(meta) = stream.try_next().await? {
        keys.push((meta.location.to_string(), meta.size));
        if keys.len() >= cfg.sample.max(1) {
            break;
        }
    }
    assert!(!keys.is_empty(), "no objects found; run `pgvs3 seed` first");
    println!(
        "bench against {} (bucket {}, {} objects sampled)",
        cfg.endpoint, cfg.bucket, keys.len()
    );

    let head_op: Op = Arc::new(|store, key, _off, _size| {
        Box::pin(async move {
            let path = Path::from(key);
            let t0 = Instant::now();
            store.head(&path).await?;
            Ok(t0.elapsed())
        })
    });

    for conc in &cfg.concurrency {
        let conc = *conc;
        let per_task = cfg.requests.div_ceil(conc.max(1));
        let lat = load(conc, per_task, 0, cfg.clone_fields(), keys.clone(), head_op.clone()).await?;
        stats(&format!("HEAD conc={conc}"), lat, 0);
    }

    for size in &cfg.sizes {
        let size = *size;
        for conc in &cfg.concurrency {
            let conc = *conc;
            let n = if size >= 4 * 1024 * 1024 { cfg.requests / 5 } else { cfg.requests };
            let per_task = n.div_ceil(conc.max(1));
            let lat = load(conc, per_task, size, cfg.clone_fields(), keys.clone(), get_op(size)).await?;
            stats(&format!("GET {size}B conc={conc}"), lat, size);
        }
    }

    // LIST first page (~1000 keys per ListObjectsV2 round trip).
    let list_op: Op = Arc::new(|store, _key, _off, _size| {
        Box::pin(async move {
            let t0 = Instant::now();
            let mut s = store.list(None);
            let mut n = 0usize;
            while let Some(_meta) = s.try_next().await? {
                n += 1;
                if n >= 1000 {
                    break;
                }
            }
            Ok(t0.elapsed())
        })
    });
    let lat = load(1, 100, 0, cfg.clone_fields(), keys.clone(), list_op).await?;
    stats("LIST page<=1000", lat, 0);

    // Raw PostgreSQL floor: the gateway's single-trip query minus HTTP/SigV4.
    let pool = db::connect(&cfg.pg_url).await?;
    let (key0, size0) = keys[0].clone();
    let mut filler = Filler::new(42);
    let mut lat = Vec::with_capacity(2000);
    for _ in 0..2000 {
        let len = 262144i64.min(size0 as i64);
        let max_off = (size0 as i64 - len).max(0) / 4096;
        let off = ((filler.next_u64() % (max_off as u64 + 1)) as i64) * 4096;
        let t0 = Instant::now();
        let slice = db::get(&pool, &cfg.bucket, &key0, off, off + len - 1, -1).await?;
        if slice.map(|s| s.bytes.is_empty()).unwrap_or(true) {
            anyhow::bail!("empty PG read for {key0} @ {off}");
        }
        lat.push(t0.elapsed());
    }
    stats("PG floor 256KiB", lat, 262144);

    Ok(())
}
