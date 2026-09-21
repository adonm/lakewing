//! Node-local NVMe range cache under Lance's object store: a
//! WrappingObjectStore that answers `get_ranges` from a foyer HybridCache
//! (memory + disk tiers) and delegates everything else. Modeled on
//! lance-io's own throttle wrapper (object_store 0.14 trait surface).
//!
//! This replaces the Go s3cache HTTP proxy for the per-pod case: no SigV4
//! proxying, no HTTP hop, cached bytes are handed to Lance as `Bytes`
//! directly. Sharing across pods on one node is the known tradeoff (see
//! docs/rust-architecture.md); a shared foyer proxy can slot in later
//! behind the same trait if measurements demand it.
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use foyer::HybridCache;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result as StoreResult,
};

/// CachingStore wraps the real object store and serves range reads from a
/// foyer cache keyed by (path, start, end). Range alignment is Lance's
/// concern (it batches column-page-aligned reads); we cache exactly what
/// is asked for and let foyer's LRU handle the working set.
pub struct CachingStore {
    inner: Arc<dyn OSStore>,
    cache: HybridCache<String, Bytes>,
    metadata: foyer::Cache<String, (ObjectMeta, Attributes)>,
    scheme: String,
    pub metrics: Arc<CacheMetrics>,
    _lock: Arc<std::fs::File>,
}

#[derive(Debug, Default)]
pub struct CacheMetrics {
    lookups: AtomicU64,
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    origin_ranges: AtomicU64,
    origin_bytes: AtomicU64,
    origin_heads: AtomicU64,
    bypass_get_opts: AtomicU64,
}

impl CacheMetrics {
    pub fn exposition(&self) -> String {
        let mut out = String::new();
        for (name, value) in [
            ("lookups", &self.lookups),
            ("memory_hits", &self.memory_hits),
            ("disk_hits", &self.disk_hits),
            ("origin_ranges", &self.origin_ranges),
            ("origin_bytes", &self.origin_bytes),
            ("origin_heads", &self.origin_heads),
            ("bypass_get_opts", &self.bypass_get_opts),
        ] {
            out.push_str(&format!(
                "# TYPE lakewing_cache_{name}_total counter\nlakewing_cache_{name}_total {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
        out
    }
}

use object_store::ObjectStore as OSStore;

impl Debug for CachingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingStore")
            .field("scheme", &self.scheme)
            .finish()
    }
}

impl std::fmt::Display for CachingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingStore({})", self.scheme)
    }
}

impl CachingStore {
    pub async fn open(
        dir: &str,
        disk_bytes: usize,
        memory_bytes: usize,
        identity: &str,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            disk_bytes >= 64 * 1024 * 1024 && memory_bytes > 0,
            "cache needs >=64 MiB disk and positive memory capacity"
        );
        std::fs::create_dir_all(dir)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(std::path::Path::new(dir).join("lakewing.lock"))?;
        lock.try_lock()
            .map_err(|e| anyhow::anyhow!("cache directory already in use ({dir}): {e}"))?;
        let cache = build_cache(dir, disk_bytes, memory_bytes).await?;
        let metadata = foyer::CacheBuilder::new(2 * 1024 * 1024)
            .with_weighter(|key: &String, value: &(ObjectMeta, Attributes)| {
                key.len()
                    + value.0.location.as_ref().len()
                    + 256
                    + value
                        .1
                        .iter()
                        .map(|(_, v)| v.as_ref().len() + 64)
                        .sum::<usize>()
            })
            .build();
        Ok(Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            cache,
            metadata,
            scheme: identity.to_string(),
            metrics: Arc::default(),
            _lock: Arc::new(lock),
        })
    }

    fn key(&self, location: &Path, range: &Range<u64>) -> String {
        serde_json::to_string(&(&self.scheme, location.as_ref(), range.start, range.end))
            .expect("range key")
    }

    async fn range(&self, location: &Path, range: Range<u64>) -> StoreResult<Bytes> {
        // Lance data, index, deletion and numbered manifest objects are immutable.
        // Tags, namespace metadata and arbitrary objects always retain origin semantics.
        if !immutable(location) || range.start >= range.end {
            return self.inner.get_range(location, range).await;
        }
        self.metrics.lookups.fetch_add(1, Ordering::Relaxed);
        let key = self.key(location, &range);
        let store = self.inner.clone();
        let location = location.clone();
        let metrics = self.metrics.clone();
        let entry = self
            .cache
            .get_or_fetch(&key, move || async move {
                metrics.origin_ranges.fetch_add(1, Ordering::Relaxed);
                let bytes = store.get_range(&location, range).await?;
                metrics
                    .origin_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Ok::<_, object_store::Error>(bytes)
            })
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "foyer",
                source: Box::new(error),
            })?;
        match entry.source() {
            foyer::Source::Memory => {
                self.metrics.memory_hits.fetch_add(1, Ordering::Relaxed);
            }
            foyer::Source::Disk => {
                self.metrics.disk_hits.fetch_add(1, Ordering::Relaxed);
            }
            foyer::Source::Outer => {}
        }
        Ok(entry.value().clone())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.cache.close().await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for CachingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let eligible = immutable(location)
            && !options.head
            && options.version.is_none()
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.extensions.is_empty();
        if eligible {
            if let Some(get_range) = options.range.as_ref() {
                let key = serde_json::to_string(&(&self.scheme, location.as_ref()))
                    .expect("metadata key");
                let store = self.inner.clone();
                let path = location.clone();
                let metrics = self.metrics.clone();
                let meta = self
                    .metadata
                    .get_or_fetch(&key, move || async move {
                        metrics.origin_heads.fetch_add(1, Ordering::Relaxed);
                        let result = store
                            .get_opts(
                                &path,
                                GetOptions {
                                    head: true,
                                    ..Default::default()
                                },
                            )
                            .await?;
                        Ok::<_, object_store::Error>((result.meta, result.attributes))
                    })
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: "foyer",
                        source: Box::new(e),
                    })?;
                let (meta, attributes) = meta.value();
                let range =
                    get_range
                        .as_range(meta.size)
                        .map_err(|e| object_store::Error::Generic {
                            store: "lakewing",
                            source: Box::new(e),
                        })?;
                if range.end - range.start <= 64 * 1024 * 1024 {
                    let bytes = self.range(location, range.clone()).await?;
                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(
                            futures::stream::once(async move { Ok(bytes) }).boxed(),
                        ),
                        meta: meta.clone(),
                        range,
                        attributes: attributes.clone(),
                        extensions: Default::default(),
                    });
                }
            }
        }
        self.metrics.bypass_get_opts.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        futures::stream::iter(
            ranges
                .iter()
                .cloned()
                .map(|range| self.range(location, range)),
        )
        .buffered(16)
        .try_collect()
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> StoreResult<()> {
        self.inner.rename_opts(from, to, opts).await
    }
}

/// lance-io hook: wrap the constructed store with our cache.
impl WrappingObjectStore for CachingStore {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn OSStore>) -> Arc<dyn OSStore> {
        Arc::new(CachingStore {
            inner: original,
            cache: self.cache.clone(),
            metadata: self.metadata.clone(),
            scheme: serde_json::to_string(&(&self.scheme, store_prefix)).expect("store identity"),
            metrics: self.metrics.clone(),
            _lock: self._lock.clone(),
        })
    }

    fn wrap_paginated(
        &self,
        _store_prefix: &str,
        original: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        Some(original)
    }
}

fn immutable(location: &Path) -> bool {
    location
        .parts()
        .any(|p| matches!(p.as_ref(), "data" | "_indices" | "_deletions" | "_versions"))
}

/// Build the foyer hybrid cache (memory + NVMe disk tiers).
async fn build_cache(
    dir: &str,
    disk_bytes: usize,
    memory_bytes: usize,
) -> anyhow::Result<HybridCache<String, Bytes>> {
    let cache = foyer::HybridCacheBuilder::new()
        .memory(memory_bytes)
        .with_weighter(|k: &String, v: &Bytes| k.len() + v.len() + 64)
        .storage()
        .with_engine_config(foyer::BlockEngineConfig::new(
            <foyer::FsDeviceBuilder as foyer::DeviceBuilder>::build(
                foyer::FsDeviceBuilder::new(dir).with_capacity(disk_bytes),
            )?,
        ))
        .build()
        .await?;
    Ok(cache)
}
