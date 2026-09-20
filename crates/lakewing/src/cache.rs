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
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::HybridCache;
use futures::stream::BoxStream;
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as StoreResult,
};

/// CachingStore wraps the real object store and serves range reads from a
/// foyer cache keyed by (path, start, end). Range alignment is Lance's
/// concern (it batches column-page-aligned reads); we cache exactly what
/// is asked for and let foyer's LRU handle the working set.
pub struct CachingStore {
    inner: Arc<dyn OSStore>,
    cache: HybridCache<String, Bytes>,
    scheme: String,
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
    pub fn new(inner: Arc<dyn OSStore>, cache: HybridCache<String, Bytes>, scheme: &str) -> Self {
        Self {
            inner,
            cache,
            scheme: scheme.to_string(),
        }
    }

    fn key(&self, location: &Path, range: &Range<u64>) -> String {
        format!("{}{}:{}:{}", self.scheme, location, range.start, range.end)
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
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        let mut out: Vec<Bytes> = Vec::with_capacity(ranges.len());
        let mut missing: Vec<usize> = Vec::new();
        for (i, range) in ranges.iter().enumerate() {
            if let Ok(Some(hit)) = self.cache.get(&self.key(location, range)).await {
                out.push(hit.value().clone());
            } else {
                out.push(Bytes::new());
                missing.push(i);
            }
        }
        if !missing.is_empty() {
            let fetch: Vec<Range<u64>> = missing.iter().map(|i| ranges[*i].clone()).collect();
            let fetched = self.inner.get_ranges(location, &fetch).await?;
            for (slot, bytes) in missing.iter().zip(fetched) {
                let range = &ranges[*slot];
                self.cache.insert(self.key(location, range), bytes.clone());
                out[*slot] = bytes;
            }
        }
        Ok(out)
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
    fn wrap(&self, _store_prefix: &str, original: Arc<dyn OSStore>) -> Arc<dyn OSStore> {
        Arc::new(CachingStore {
            inner: original,
            cache: self.cache.clone(),
            scheme: self.scheme.clone(),
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

/// Build the foyer hybrid cache (memory + NVMe disk tiers).
pub async fn build_cache(
    dir: &str,
    disk_bytes: usize,
) -> anyhow::Result<HybridCache<String, Bytes>> {
    let cache = foyer::HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .with_weighter(|_k: &String, v: &Bytes| v.len())
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
