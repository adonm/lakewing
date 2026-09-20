//! Lance feature source: a tag/version-pinned dataset with SQL-filtered
//! scans. "Basic retrieval/lookup done by lance": point lookups ride the
//! BTREE on id, bbox pages push the column-overlap predicates, and exact
//! predicates run in DuckDB over the candidate rows (duck.rs).
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use lance::dataset::refs::Ref;
use lance::dataset::scanner::ColumnOrdering;
use lance::Dataset;

use crate::cache::CachingStore;

/// Options for opening the serving dataset. `tag` wins over `version`;
/// exactly one must be set (serving pins, never floats on latest).
pub struct SourceConfig {
    pub uri: String,
    pub tag: Option<String>,
    pub version: Option<u64>,
}

pub struct LanceSource {
    dataset: Arc<Dataset>,
    pub version: u64,
}

impl LanceSource {
    pub async fn open(cfg: SourceConfig, cache: Option<Arc<CachingStore>>) -> anyhow::Result<Self> {
        let tag = cfg.tag.as_deref();
        let version = cfg.version;
        if tag.is_none() && version.is_none() {
            anyhow::bail!("pin a tag or version (latest is not a snapshot)");
        }
        let dataset = Dataset::open(&cfg.uri).await?;
        let dataset = match (tag, version) {
            (Some(tag), _) => dataset.checkout_version(Ref::Tag(tag.to_string())).await?,
            (None, Some(version)) => {
                dataset
                    .checkout_version(Ref::VersionNumber(version))
                    .await?
            }
            _ => unreachable!(),
        };
        let dataset = match cache {
            Some(wrapper) => dataset.with_object_store_wrappers([
                wrapper as Arc<dyn lance_io::object_store::WrappingObjectStore>
            ]),
            None => dataset,
        };
        let version = dataset.version().version;
        Ok(Self {
            dataset: Arc::new(dataset),
            version,
        })
    }

    fn scanner(&self, filter: &str) -> anyhow::Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.dataset.scan();
        scanner.filter(filter)?;
        scanner.batch_size(8192);
        Ok(scanner)
    }

    /// Scan payload columns (geom as stored: WKB-blob datasets).
    pub async fn scan_page(
        &self,
        filter: &str,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        let _ = scanner
            .project(&[
                "id",
                "geom",
                "properties",
                "layer",
                "source_id",
                "xmin",
                "ymin",
                "xmax",
                "ymax",
            ])?
            .limit(limit.map(|l| l as i64), offset.map(|o| o as i64));
        let batches = scanner.try_into_stream().await?.try_collect().await?;
        Ok(batches)
    }

    /// Scan the narrow id/layer/source/bbox projection for ids-first
    /// pagination and layer discovery. Bbox columns ride along so pushed
    /// range filters prune fragments without touching payloads.
    pub async fn scan_ids(&self, filter: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        scanner.project(&["id", "layer", "source_id", "xmin", "ymin", "xmax", "ymax"])?;
        let batches = scanner.try_into_stream().await?.try_collect().await?;
        Ok(batches)
    }

    /// Ordered id window with the top-N pushed into Lance
    /// (`order_by` id asc plus `limit`), returning at most `fetch` ids
    /// in id order. The caller applies cursor/offset slicing; Lance
    /// never materializes more than the window.
    pub async fn scan_ids_window(&self, filter: &str, fetch: u64) -> anyhow::Result<Vec<String>> {
        let mut scanner = self.scanner(filter)?;
        scanner.project(&["id"])?;
        scanner.order_by(Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]))?;
        let _ = scanner.limit(Some(fetch as i64), None);
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        batch_ids(&batches)
    }
}

/// Collect the `id` column (VARCHAR or LargeVARCHAR) into Strings.
pub fn batch_ids(batches: &[RecordBatch]) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of("id")
            .map_err(|_| anyhow::anyhow!("id column missing"))?;
        let col = batch.column(idx);
        if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..a.len() {
                ids.push(a.value(i).to_string());
            }
        } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
            for i in 0..a.len() {
                ids.push(a.value(i).to_string());
            }
        } else {
            anyhow::bail!("id column is {}", col.data_type());
        }
    }
    Ok(ids)
}
