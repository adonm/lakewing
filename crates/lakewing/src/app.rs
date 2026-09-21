//! Shared HTTP/Flight selection and bounded local execution.
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::catalog::Catalog;
use crate::duck::{key_filter, quote, Duck, FeatureRow};
use crate::lake::LanceSource;
use crate::query::{urlencode, QueryError, RowKey, Selection};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub concurrency: usize,
    pub duck_threads: usize,
    pub duck_memory_mb: usize,
    /// Rendered-response cache budget in bytes; 0 disables the cache.
    pub response_cache_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            concurrency: 4,
            duck_threads: 1,
            duck_memory_mb: 512,
            response_cache_bytes: 256 * 1024 * 1024,
        }
    }
}

pub struct Admission {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<crate::metrics::Metrics>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.metrics.leave();
    }
}

pub struct App {
    pub lance: Arc<LanceSource>,
    pub duck: Arc<Duck>,
    pub collections: Vec<String>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub cache_metrics: Option<Arc<crate::cache::CacheMetrics>>,
    responses: Option<crate::response_cache::ResponseCache>,
    sem: Arc<Semaphore>,
}

impl App {
    pub async fn open_via_catalog(
        root: &str,
        table: &str,
        tag: Option<String>,
        version: Option<u64>,
        storage_options: std::collections::HashMap<String, String>,
        cache: Option<Arc<crate::cache::CachingStore>>,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        let catalog = Catalog::open(root, &storage_options).await?;
        let uri = catalog.resolve(table).await?;
        Self::open_at(uri, tag, version, storage_options, cache, limits).await
    }

    pub async fn open_at(
        uri: String,
        tag: Option<String>,
        version: Option<u64>,
        storage_options: std::collections::HashMap<String, String>,
        cache: Option<Arc<crate::cache::CachingStore>>,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            limits.concurrency > 0 && limits.concurrency <= 256,
            "concurrency must be 1..256"
        );
        let cache_metrics = cache.as_ref().map(|cache| cache.metrics.clone());
        let lance = Arc::new(
            LanceSource::open(
                crate::lake::SourceConfig {
                    uri,
                    tag,
                    version,
                    storage_options,
                },
                cache,
            )
            .await?,
        );
        let duck = Arc::new(Duck::open(
            limits.concurrency,
            limits.duck_threads,
            limits.duck_memory_mb,
        )?);
        let collections = lance.collections().await?;
        let metrics = Arc::new(crate::metrics::Metrics::new());
        metrics
            .dataset_version
            .store(lance.version, std::sync::atomic::Ordering::Relaxed);
        Ok(Self {
            lance,
            duck,
            collections,
            metrics,
            cache_metrics,
            responses: if limits.response_cache_bytes > 0 {
                Some(crate::response_cache::ResponseCache::new(
                    limits.response_cache_bytes,
                ))
            } else {
                None
            },
            sem: Arc::new(Semaphore::new(limits.concurrency)),
        })
    }

    /// Cached rendered response for a canonical request key, if enabled and
    /// present. Counts hit/miss metrics so warm behavior is observable.
    pub fn cached_response(&self, key: &str) -> Option<crate::response_cache::Rendered> {
        match &self.responses {
            Some(cache) => {
                let hit = cache.get(key);
                if hit.is_some() {
                    self.metrics.count_cache_hit();
                } else {
                    self.metrics.count_cache_miss();
                }
                hit
            }
            None => None,
        }
    }

    /// Store a successful render. Only called for 200 responses; errors are
    /// never stored.
    pub fn store_response(
        &self,
        key: String,
        body: bytes::Bytes,
        content_type: &'static str,
        gz: bool,
    ) {
        if let Some(cache) = &self.responses {
            cache.insert(
                key,
                crate::response_cache::Rendered {
                    body,
                    content_type,
                    gz,
                },
            );
        }
    }

    pub fn admit(&self, flight: bool) -> Result<Admission, QueryError> {
        if flight {
            self.metrics.count_flight();
        }
        let permit = self
            .sem
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::new(429, "server overloaded"))?;
        self.metrics.enter();
        Ok(Admission {
            _permit: permit,
            metrics: self.metrics.clone(),
        })
    }

    pub fn validate_collection(&self, collection: &str) -> anyhow::Result<()> {
        if self.collections.iter().any(|c| c == collection) {
            Ok(())
        } else {
            Err(QueryError::new(404, "unknown collection").into())
        }
    }

    pub async fn selected_keys(
        &self,
        selection: &Selection,
        extra: usize,
    ) -> anyhow::Result<Vec<RowKey>> {
        self.validate_collection(&selection.collection)?;
        if selection.limit == 0 || selection.sources.is_empty() {
            return Ok(Vec::new());
        }
        let filter = selection.filter(self.lance.geo_geom, self.lance.spatial)?;
        let keys = self
            .lance
            .scan_keys_topk(&filter, selection.offset + selection.limit + extra)
            .await?;
        Ok(keys
            .into_iter()
            .skip(selection.offset)
            .take(selection.limit + extra)
            .collect())
    }

    pub fn payload_filter(collection: &str, keys: &[RowKey]) -> String {
        format!("layer = {} AND {}", quote(collection), key_filter(keys))
    }

    #[tracing::instrument(name = "items", skip_all, fields(collection = selection.collection, dataset_version = self.lance.version))]
    pub async fn items_page(&self, selection: &Selection) -> anyhow::Result<String> {
        let mut keys = self.selected_keys(selection, 1).await?;
        let has_next = keys.len() > selection.limit;
        keys.truncate(selection.limit);
        let rows = if keys.is_empty() {
            Vec::new()
        } else {
            let batches = self
                .lance
                .scan_page(&Self::payload_filter(&selection.collection, &keys), None)
                .await?;
            self.duck.render_page(batches).await?
        };
        let features = rows
            .iter()
            .map(|row| render_feature(&selection.collection, row, self.lance.version))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut links = vec![
            json!({"rel": "self", "href": selection.href(self.lance.version, None), "type": "application/geo+json"}),
            json!({"rel": "collection", "href": format!("/collections/{}", urlencode(&selection.collection)), "type": "application/json"}),
        ];
        if has_next {
            if let Some(last) = keys.last() {
                links.push(json!({"rel": "next", "href": selection.href(self.lance.version, Some(last)), "type": "application/geo+json"}));
            }
        }
        Ok(serde_json::to_string(
            &json!({"type": "FeatureCollection", "numberReturned": features.len(), "features": features, "links": links}),
        )?)
    }

    #[tracing::instrument(name = "item", skip_all, fields(collection, dataset_version = self.lance.version))]
    pub async fn item(
        &self,
        collection: &str,
        feature_id: &str,
        sources: &[i64],
    ) -> anyhow::Result<Option<String>> {
        self.validate_collection(collection)?;
        if sources.is_empty() {
            return Ok(None);
        }
        let filter = format!(
            "{} AND id = {}",
            crate::duck::pushed_filter(collection, None, sources, self.lance.geo_geom),
            quote(feature_id)
        );
        let batches = self.lance.scan_page(&filter, Some(1)).await?;
        let rows = self.duck.render_page(batches).await?;
        rows.first()
            .map(|row| {
                Ok(serde_json::to_string(&render_feature(
                    collection,
                    row,
                    self.lance.version,
                )?)?)
            })
            .transpose()
    }

    #[tracing::instrument(name = "tile", skip_all, fields(collection, dataset_version = self.lance.version))]
    pub async fn tile(
        &self,
        collection: &str,
        z: u8,
        x: u32,
        y: u32,
        sources: &[i64],
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let selection = Selection::new(
            collection.into(),
            Some(crate::tiles::xyz_to_bbox(z, x, y)),
            sources.to_vec(),
            crate::tiles::TILE_LIMIT,
            0,
            None,
            self.lance.version,
        )?;
        let keys = self.selected_keys(&selection, 0).await?;
        if keys.is_empty() {
            return Ok(None);
        }
        let batches = self
            .lance
            .scan_page(&Self::payload_filter(collection, &keys), None)
            .await?;
        let sql = crate::tiles::mvt_sql(collection, crate::tiles::mercator_extent(z, x, y));
        self.duck.mvt(batches, sql).await
    }
}

fn render_feature(collection: &str, row: &FeatureRow, version: u64) -> anyhow::Result<Value> {
    let geometry: Value = if row.geom_json.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&row.geom_json)?
    };
    let props: Value = if row.properties.is_empty() {
        json!({})
    } else {
        serde_json::from_str(&row.properties)?
    };
    Ok(json!({
        "type": "Feature", "id": row.id, "geometry": geometry, "properties": props,
        "links": [
            {"rel": "self", "href": format!("/collections/{}/items/{}?sources={}&snapshot={version}", urlencode(collection), urlencode(&row.id), row.source_id), "type": "application/geo+json"},
            {"rel": "collection", "href": format!("/collections/{}", urlencode(collection)), "type": "application/json"}
        ]
    }))
}
