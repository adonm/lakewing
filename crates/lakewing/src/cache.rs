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
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as StoreResult,
};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, clap::Args)]
pub struct CacheConfig {
    /// Foyer disk budget (requires --cache-dir).
    #[arg(long = "cache-bytes", default_value_t = 512 * 1024 * 1024)]
    pub disk_bytes: usize,
    /// Foyer data/index byte-cache RAM budget, excluding Lance's decoded caches.
    #[arg(long = "cache-memory-bytes", default_value_t = 64 * 1024 * 1024)]
    pub memory_bytes: usize,
    /// Bounded in-memory immutable-object HEAD cache.
    #[arg(long = "cache-metadata-bytes", default_value_t = 2 * 1024 * 1024)]
    pub metadata_bytes: usize,
    /// Data block grid in bytes; 0 uses exact ranges.
    #[arg(long = "cache-block-bytes", default_value_t = 256 * 1024)]
    pub block_bytes: u64,
    /// Index block grid in bytes; 0 uses exact ranges.
    #[arg(long = "cache-index-block-bytes", default_value_t = 64 * 1024)]
    pub index_block_bytes: u64,
    /// Reads at least this many blocks wide use one exact-range cache entry.
    #[arg(long = "cache-align-max-blocks", default_value_t = 4)]
    pub align_max_blocks: u64,
    /// Larger ranges bypass cache admission to limit sequential-scan pollution.
    #[arg(long = "cache-max-range-bytes", default_value_t = 8 * 1024 * 1024)]
    pub max_range_bytes: u64,
    /// Concurrent cache-fill GETs/HEADs across all requests and wrapped stores.
    #[arg(long = "cache-fetch-concurrency", default_value_t = 16)]
    pub fetch_concurrency: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            disk_bytes: 512 * 1024 * 1024,
            memory_bytes: 64 * 1024 * 1024,
            metadata_bytes: 2 * 1024 * 1024,
            block_bytes: 256 * 1024,
            index_block_bytes: 64 * 1024,
            align_max_blocks: 4,
            max_range_bytes: 8 * 1024 * 1024,
            fetch_concurrency: 16,
        }
    }
}

impl CacheConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.disk_bytes >= 64 * 1024 * 1024,
            "cache-bytes must be >=64 MiB"
        );
        anyhow::ensure!(
            self.memory_bytes > 0 && self.metadata_bytes > 0,
            "cache RAM budgets must be positive"
        );
        anyhow::ensure!(
            (4096..=64 * 1024 * 1024).contains(&self.max_range_bytes),
            "cache-max-range-bytes must be 4 KiB..64 MiB"
        );
        for block in [self.block_bytes, self.index_block_bytes] {
            anyhow::ensure!(
                block == 0 || (4096..=4 * 1024 * 1024).contains(&block),
                "cache blocks must be 0 or 4 KiB..4 MiB"
            );
            anyhow::ensure!(
                block <= self.max_range_bytes,
                "cache blocks must fit cache-max-range-bytes"
            );
        }
        anyhow::ensure!(
            (1..=64).contains(&self.align_max_blocks),
            "cache-align-max-blocks must be 1..64"
        );
        anyhow::ensure!(
            (1..=256).contains(&self.fetch_concurrency),
            "cache-fetch-concurrency must be 1..256"
        );
        Ok(())
    }
}

/// CachingStore wraps the real object store and serves range reads from a
/// foyer cache. Immutable-object ranges are cached **block-aligned**
/// (default 256 KiB grid): a read is served from the fixed blocks covering
/// it, so overlapping-but-different queries (e.g. take-scattered payload
/// rows, adjacent column pages) reuse cached bytes instead of missing on
/// exact-range key mismatches. Ranges at least 4 blocks wide are fetched
/// as one exact GET (large sequential reads must not shard into per-block
/// origin requests). Lance's concern stays on coalescing; block reuse and
/// LRU eviction are ours.
pub struct CachingStore {
    inner: Arc<dyn OSStore>,
    cache: HybridCache<String, Bytes>,
    metadata: foyer::Cache<String, (ObjectMeta, Attributes)>,
    scheme: String,
    config: CacheConfig,
    fetches: Arc<Semaphore>,
    pub metrics: Arc<CacheMetrics>,
    _lock: Arc<std::fs::File>,
}

#[derive(Debug, Default)]
pub struct CacheMetrics {
    requests: AtomicU64,
    lookups: AtomicU64,
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    origin_ranges: AtomicU64,
    origin_bytes: AtomicU64,
    origin_heads: AtomicU64,
    requested_bytes: AtomicU64,
    bypass_get_opts: AtomicU64,
    bypass_ranges: AtomicU64,
}

impl CacheMetrics {
    pub fn exposition(&self) -> String {
        let mut out = String::new();
        for (name, value) in [
            ("requests", &self.requests),
            ("lookups", &self.lookups),
            ("memory_hits", &self.memory_hits),
            ("disk_hits", &self.disk_hits),
            ("origin_ranges", &self.origin_ranges),
            ("origin_bytes", &self.origin_bytes),
            ("origin_heads", &self.origin_heads),
            ("requested_bytes", &self.requested_bytes),
            ("bypass_get_opts", &self.bypass_get_opts),
            ("bypass_ranges", &self.bypass_ranges),
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
    pub async fn open(dir: &str, config: CacheConfig, identity: &str) -> anyhow::Result<Self> {
        config.validate()?;
        std::fs::create_dir_all(dir)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(std::path::Path::new(dir).join("lakewing.lock"))?;
        lock.try_lock()
            .map_err(|e| anyhow::anyhow!("cache directory already in use ({dir}): {e}"))?;
        let cache = build_cache(dir, config.disk_bytes, config.memory_bytes).await?;
        let metadata = foyer::CacheBuilder::new(config.metadata_bytes)
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
            fetches: Arc::new(Semaphore::new(config.fetch_concurrency)),
            config,
            metrics: Arc::default(),
            _lock: Arc::new(lock),
        })
    }

    fn key(&self, location: &Path, range: &Range<u64>) -> String {
        serde_json::to_string(&(&self.scheme, location.as_ref(), range.start, range.end))
            .expect("range key")
    }

    /// Cached HEAD (ObjectMeta + attributes): one origin HEAD per object,
    /// then memory-resident. Shared by the ranged-`get_opts` path (envelope
    /// construction) and block alignment (object-size clamping).
    async fn object_meta(
        &self,
        location: &Path,
    ) -> StoreResult<foyer::CacheEntry<String, (ObjectMeta, Attributes)>> {
        let key = serde_json::to_string(&(&self.scheme, location.as_ref())).expect("metadata key");
        let store = self.inner.clone();
        let path = location.clone();
        let metrics = self.metrics.clone();
        let fetches = self.fetches.clone();
        self.metadata
            .get_or_fetch(&key, move || async move {
                let _permit = fetches.acquire().await.expect("cache semaphore stays open");
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
            .map_err(|error| object_store::Error::Generic {
                store: "foyer",
                source: Box::new(error),
            })
    }

    /// Serve one immutable range: block-aligned when blocks are enabled and
    /// the range is under 4 blocks (larger reads fetch as one exact GET so
    /// sequential scans don't shard into per-block origin requests).
    ///
    /// Block keys live on a fixed grid clamped to the object size
    /// (`block_start .. min(block_start + block, size)`), so any request
    /// touching a block shares one entry regardless of its own edges.
    async fn range(&self, location: &Path, range: Range<u64>) -> StoreResult<Bytes> {
        // Lance data, index, deletion and numbered manifest objects are immutable.
        // Tags, namespace metadata and arbitrary objects always retain origin semantics.
        if !immutable(location) || range.start >= range.end {
            return self.inner.get_range(location, range).await;
        }
        let size = self.object_meta(location).await?.value().0.size;
        let range =
            GetRange::Bounded(range)
                .as_range(size)
                .map_err(|e| object_store::Error::Generic {
                    store: "lakewing",
                    source: Box::new(e),
                })?;
        self.cached_range(location, range, size).await
    }

    /// The caller normalizes the range against cached metadata before alignment.
    async fn cached_range(
        &self,
        location: &Path,
        range: Range<u64>,
        size: u64,
    ) -> StoreResult<Bytes> {
        let len = range.end - range.start;
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .requested_bytes
            .fetch_add(len, Ordering::Relaxed);
        if len == 0 {
            return Ok(Bytes::new());
        }
        if len > self.config.max_range_bytes {
            self.metrics.bypass_ranges.fetch_add(1, Ordering::Relaxed);
            return self.origin_range(location, range).await;
        }
        let block_size = if location.parts().any(|part| part.as_ref() == "_indices") {
            self.config.index_block_bytes
        } else {
            self.config.block_bytes
        };
        if block_size == 0 || len >= block_size * self.config.align_max_blocks {
            return self.exact(location, range).await;
        }
        let first = range.start - range.start % block_size;
        let first_end = first.saturating_add(block_size).min(size);
        if range.end <= first_end {
            let block = self.exact(location, first..first_end).await?;
            return Ok(block.slice((range.start - first) as usize..(range.end - first) as usize));
        }
        // Copy only requested bytes; padding stays in the cached blocks.
        let mut span = bytes::BytesMut::with_capacity(len as usize);
        let mut offset = first;
        while offset < range.end {
            let end = offset.saturating_add(block_size).min(size);
            let block = self.exact(location, offset..end).await?;
            span.extend_from_slice(
                &block[(range.start.max(offset) - offset) as usize
                    ..(range.end.min(end) - offset) as usize],
            );
            offset = end;
        }
        Ok(span.freeze())
    }

    async fn origin_range(&self, location: &Path, range: Range<u64>) -> StoreResult<Bytes> {
        let _permit = self
            .fetches
            .acquire()
            .await
            .expect("cache semaphore stays open");
        self.metrics.origin_ranges.fetch_add(1, Ordering::Relaxed);
        let bytes = self.inner.get_range(location, range).await?;
        self.metrics
            .origin_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    /// One cache entry per exact range, single-flighted via get_or_fetch.
    async fn exact(&self, location: &Path, range: Range<u64>) -> StoreResult<Bytes> {
        self.metrics.lookups.fetch_add(1, Ordering::Relaxed);
        let key = self.key(location, &range);
        let store = self.inner.clone();
        let location = location.clone();
        let metrics = self.metrics.clone();
        let fetches = self.fetches.clone();
        let entry = self
            .cache
            .get_or_fetch(&key, move || async move {
                let _permit = fetches.acquire().await.expect("cache semaphore stays open");
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
                let meta = self.object_meta(location).await?;
                let (meta, attributes) = meta.value();
                let range =
                    get_range
                        .as_range(meta.size)
                        .map_err(|e| object_store::Error::Generic {
                            store: "lakewing",
                            source: Box::new(e),
                        })?;
                if range.end - range.start <= self.config.max_range_bytes {
                    let bytes = self
                        .cached_range(location, range.clone(), meta.size)
                        .await?;
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
        .buffered(self.config.fetch_concurrency)
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
            config: self.config.clone(),
            fetches: self.fetches.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn block_alignment_reuses_overlapping_ranges() {
        let dir = tempfile::TempDir::new().unwrap();
        let inner = Arc::new(object_store::memory::InMemory::new());
        // 32 KiB pattern object under an immutable prefix.
        let payload: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
        inner
            .put(&Path::from("data/blocks.bin"), payload.clone().into())
            .await
            .unwrap();
        let mut store = CachingStore::open(dir.path().to_str().unwrap(), test_config(4096), "test")
            .await
            .unwrap();
        store.inner = inner.clone();
        let path = Path::from("data/blocks.bin");
        let expect = |r: Range<u64>| payload[r.start as usize..r.end as usize].to_vec();
        let origin_ranges = || store.metrics.origin_ranges.load(Ordering::Relaxed);

        // First read spans two blocks: 2 origin fetches, amplification visible.
        let got = store.get_range(&path, 1000..5000).await.unwrap();
        assert_eq!(got.to_vec(), expect(1000..5000));
        assert_eq!(origin_ranges(), 2);

        // Overlapping-but-different range: the same blocks serve it — no new
        // origin fetch. Exact-range caching misses here.
        let got = store.get_range(&path, 3000..7000).await.unwrap();
        assert_eq!(got.to_vec(), expect(3000..7000));
        assert_eq!(origin_ranges(), 2);

        // A third block: exactly one new fetch.
        let got = store.get_range(&path, 8192..9000).await.unwrap();
        assert_eq!(got.to_vec(), expect(8192..9000));
        assert_eq!(origin_ranges(), 3);

        // Ranges >= 4 blocks fetch as one exact GET (no per-block sharding of
        // large sequential reads), and repeat exactly.
        let got = store.get_range(&path, 0..16_384).await.unwrap();
        assert_eq!(got.to_vec(), expect(0..16_384));
        assert_eq!(origin_ranges(), 4);
        let got = store.get_range(&path, 0..16_384).await.unwrap();
        assert_eq!(got.to_vec(), expect(0..16_384));
        assert_eq!(origin_ranges(), 4);

        // A finer/coarser index grid is independent of the data grid.
        store.config.index_block_bytes = 8192;
        let index_path = Path::from("_indices/id/leaf.lance");
        store
            .inner
            .put(&index_path, payload.clone().into())
            .await
            .unwrap();
        let before = store.metrics.origin_bytes.load(Ordering::Relaxed);
        assert_eq!(
            store
                .get_range(&index_path, 100..200)
                .await
                .unwrap()
                .to_vec(),
            expect(100..200)
        );
        assert_eq!(
            store.metrics.origin_bytes.load(Ordering::Relaxed) - before,
            8192
        );
        let permits = store
            .fetches
            .acquire_many(store.config.fetch_concurrency as u32)
            .await
            .unwrap();
        let hit = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            store.get_range(&index_path, 110..210),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            hit.to_vec(),
            expect(110..210),
            "hits must not acquire an origin slot"
        );
        drop(permits);

        // Exact-range mode (block == 0) keeps the previous behavior: an
        // overlapping-but-different range is a fresh origin fetch.
        store.close().await.unwrap();
        let exact_dir = tempfile::TempDir::new().unwrap();
        let mut store =
            CachingStore::open(exact_dir.path().to_str().unwrap(), test_config(0), "test")
                .await
                .unwrap();
        store.inner = inner;
        let got = store.get_range(&path, 1000..5000).await.unwrap();
        assert_eq!(got.to_vec(), expect(1000..5000));
        let got = store.get_range(&path, 3000..7000).await.unwrap();
        assert_eq!(got.to_vec(), expect(3000..7000));
        assert_eq!(
            store.metrics.origin_ranges.load(Ordering::Relaxed),
            2,
            "exact-range mode must not reuse overlapping reads"
        );
        store.close().await.unwrap();
    }

    fn test_config(block: u64) -> CacheConfig {
        CacheConfig {
            disk_bytes: 64 * 1024 * 1024,
            memory_bytes: 1024 * 1024,
            block_bytes: block,
            index_block_bytes: block,
            max_range_bytes: 16 * 1024,
            fetch_concurrency: 2,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn range_edges_bypass_and_zero_copy() {
        for block in [0, 4096, 5000] {
            let dir = tempfile::tempdir().unwrap();
            let store =
                CachingStore::open(dir.path().to_str().unwrap(), test_config(block), "edges")
                    .await
                    .unwrap();
            let path = Path::from("data/edges.lance");
            let payload: Vec<u8> = (0..32 * 1024 + 123).map(|i| (i % 251) as u8).collect();
            store
                .inner
                .put(&path, payload.clone().into())
                .await
                .unwrap();
            for range in [0..1, 4090..4100, 8192..9000, 32760..u64::MAX] {
                let expected = store.inner.get_range(&path, range.clone()).await.unwrap();
                assert_eq!(
                    store.get_range(&path, range.clone()).await.unwrap(),
                    expected
                );
                assert_eq!(
                    store.get_ranges(&path, &[range]).await.unwrap(),
                    vec![expected]
                );
            }
            for range in [0..0, Range { start: 10, end: 9 }, u64::MAX - 1..u64::MAX] {
                assert!(store.get_range(&path, range.clone()).await.is_err());
                assert!(store.get_ranges(&path, &[range]).await.is_err());
            }
            for range in [
                GetRange::Suffix(73),
                GetRange::Suffix(0),
                GetRange::Offset(32760),
            ] {
                let options = GetOptions {
                    range: Some(range),
                    ..Default::default()
                };
                let expected = store
                    .inner
                    .get_opts(&path, options.clone())
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(
                    store
                        .get_opts(&path, options)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                    expected
                );
            }
            if block != 0 {
                let full = store.exact(&path, 0..block).await.unwrap();
                let slice = store.get_range(&path, 5..15).await.unwrap();
                assert_eq!(
                    slice.as_ptr(),
                    full.slice(5..15).as_ptr(),
                    "single-block reads share storage"
                );
            }
            let before = store.metrics.origin_ranges.load(Ordering::Relaxed);
            for _ in 0..2 {
                store
                    .get_ranges(&path, std::slice::from_ref(&(0..20000)))
                    .await
                    .unwrap();
            }
            assert_eq!(
                store.metrics.origin_ranges.load(Ordering::Relaxed) - before,
                2
            );
            assert_eq!(store.metrics.bypass_ranges.load(Ordering::Relaxed), 2);
            let mutable = Path::from("_refs/tags/prod.json");
            store
                .put(&mutable, Bytes::from_static(b"old").into())
                .await
                .unwrap();
            assert_eq!(store.get_range(&mutable, 0..3).await.unwrap(), "old");
            store
                .put(&mutable, Bytes::from_static(b"new").into())
                .await
                .unwrap();
            assert_eq!(store.get_range(&mutable, 0..3).await.unwrap(), "new");
            store.close().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn herd_isolation_lock_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(4096);
        let inner = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("_indices/index/data.lance");
        inner
            .put(&path, Bytes::from(vec![7; 8192]).into())
            .await
            .unwrap();
        let store = CachingStore::open(dir.path().to_str().unwrap(), config.clone(), "endpoint")
            .await
            .unwrap();
        assert!(
            CachingStore::open(dir.path().to_str().unwrap(), config.clone(), "endpoint")
                .await
                .is_err()
        );
        let wrapped = store.wrap("s3://one", inner.clone());
        let barrier = Arc::new(tokio::sync::Barrier::new(32));
        let tasks = (0..32)
            .map(|i| {
                let (wrapped, path, barrier) = (wrapped.clone(), path.clone(), barrier.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    assert_eq!(
                        wrapped.get_range(&path, i..i + 100).await.unwrap(),
                        Bytes::from(vec![7; 100])
                    );
                })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(
            store.metrics.origin_ranges.load(Ordering::Relaxed),
            1,
            "overlapping herd coalesces on a block"
        );
        assert_eq!(store.metrics.origin_heads.load(Ordering::Relaxed), 1);
        assert_eq!(store.metrics.requests.load(Ordering::Relaxed), 32);
        assert_eq!(store.metrics.lookups.load(Ordering::Relaxed), 32);
        let other = Arc::new(object_store::memory::InMemory::new());
        other
            .put(&path, Bytes::from(vec![9; 8192]).into())
            .await
            .unwrap();
        let isolated = store.wrap("s3://two", other);
        assert_eq!(
            isolated.get_range(&path, 0..100).await.unwrap(),
            Bytes::from(vec![9; 100])
        );
        drop(isolated);
        drop(wrapped);
        store.close().await.unwrap();
        drop(store);
        let reopened = CachingStore::open(dir.path().to_str().unwrap(), config, "endpoint")
            .await
            .unwrap();
        let wrapped = reopened.wrap("s3://one", inner);
        assert_eq!(
            wrapped.get_range(&path, 50..150).await.unwrap(),
            Bytes::from(vec![7; 100])
        );
        assert_eq!(
            reopened.metrics.origin_ranges.load(Ordering::Relaxed),
            0,
            "restart must reuse disk entries"
        );
        assert_eq!(reopened.metrics.disk_hits.load(Ordering::Relaxed), 1);
        drop(wrapped);
        reopened.close().await.unwrap();
    }

    #[test]
    fn rejects_unbounded_cache_settings() {
        for config in [
            CacheConfig {
                block_bytes: u64::MAX,
                ..Default::default()
            },
            CacheConfig {
                index_block_bytes: 1,
                ..Default::default()
            },
            CacheConfig {
                align_max_blocks: u64::MAX,
                ..Default::default()
            },
            CacheConfig {
                max_range_bytes: u64::MAX,
                ..Default::default()
            },
            CacheConfig {
                memory_bytes: 0,
                ..Default::default()
            },
            CacheConfig {
                fetch_concurrency: 0,
                ..Default::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
    }
}
