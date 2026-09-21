//! Lance feature source: a tag/version-pinned dataset with SQL-filtered
//! scans. "Basic retrieval/lookup done by lance": point lookups ride the
//! BTREE on id, bbox pages push `ST_Intersects` on the GeoArrow geometry
//! (driving the RTREE) or bbox-column overlap on WKB datasets, and exact
//! predicates run in DuckDB over the candidate rows (duck.rs).
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::ColumnOrdering;
use lance::Dataset;

use crate::cache::CachingStore;

/// Options for opening the serving dataset. `tag` wins over `version`;
/// exactly one must be set (serving pins, never floats on latest).
pub struct SourceConfig {
    pub uri: String,
    pub tag: Option<String>,
    pub version: Option<u64>,
    /// Object-store options (endpoint/allow_http/skip_signature/region).
    pub storage_options: HashMap<String, String>,
}

pub struct LanceSource {
    dataset: Arc<Dataset>,
    pub version: u64,
    /// geom is GeoArrow (list-of-rings) — payloads project WKB via
    /// ST_AsBinary and the pushed filter uses ST_Intersects (RTREE).
    pub geo_geom: bool,
    /// geo datasets carry the polygon-vs-multipolygon promotion bit.
    pub has_was_polygon: bool,
}

impl LanceSource {
    pub async fn open(cfg: SourceConfig, cache: Option<Arc<CachingStore>>) -> anyhow::Result<Self> {
        let tag = cfg.tag.as_deref();
        let version = cfg.version;
        if tag.is_none() && version.is_none() {
            anyhow::bail!("pin a tag or version (latest is not a snapshot)");
        }
        let mut builder = DatasetBuilder::from_uri(&cfg.uri);
        if !cfg.storage_options.is_empty() {
            builder = builder.with_storage_options(cfg.storage_options);
        }
        let dataset = match (tag, version) {
            (Some(tag), _) => builder.with_tag(tag).load().await?,
            (None, Some(version)) => builder.with_version(version).load().await?,
            _ => unreachable!(),
        };
        let dataset = match cache {
            Some(wrapper) => dataset.with_object_store_wrappers([
                wrapper as Arc<dyn lance_io::object_store::WrappingObjectStore>
            ]),
            None => dataset,
        };

        let schema = dataset.schema();
        let geom_type = schema
            .field("geom")
            .map(|f| f.data_type().clone())
            .ok_or_else(|| anyhow::anyhow!("dataset has no geom column"))?;
        let geo_geom = !matches!(geom_type, DataType::Binary | DataType::LargeBinary);
        let has_was_polygon = schema.field("was_polygon").is_some();
        let version = dataset.version().version;
        Ok(Self {
            dataset: Arc::new(dataset),
            version,
            geo_geom,
            has_was_polygon,
        })
    }

    fn scanner(&self, filter: &str) -> anyhow::Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.dataset.scan();
        scanner.filter(filter)?;
        scanner.batch_size(8192);
        Ok(scanner)
    }

    /// Scan payload columns. GeoArrow geometry projects as WKB
    /// (`ST_AsBinary(geom)`); WKB datasets pass the blob through.
    pub async fn scan_page(
        &self,
        filter: &str,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        let mut projection: Vec<(&str, String)> = vec![
            ("id", "id".to_string()),
            ("geom", "geom".to_string()),
            ("properties", "properties".to_string()),
            ("layer", "layer".to_string()),
            ("source_id", "source_id".to_string()),
            ("xmin", "xmin".to_string()),
            ("ymin", "ymin".to_string()),
            ("xmax", "xmax".to_string()),
            ("ymax", "ymax".to_string()),
        ];
        if self.geo_geom {
            projection[1] = ("geom", "ST_AsBinary(geom)".to_string());
        }
        if self.has_was_polygon {
            projection.push(("was_polygon", "was_polygon".to_string()));
        }
        let _ = scanner
            .project_with_transform(&projection)?
            .limit(limit.map(|l| l as i64), offset.map(|o| o as i64));
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        Ok(batches)
    }

    /// Ordered id window: unsorted narrow id scan (parallel, index-free)
    /// with a bounded max-heap selecting the `fetch` smallest ids — no
    /// full sort of the candidate set, no more than `fetch` ids held.
    pub async fn scan_ids_topk(&self, filter: &str, fetch: usize) -> anyhow::Result<Vec<String>> {
        let mut scanner = self.scanner(filter)?;
        scanner.project(&["id"])?;
        let stream = scanner.try_into_stream().await?;
        tokio::pin!(stream);
        let mut heap: BinaryHeap<String> = BinaryHeap::with_capacity(fetch + 1);
        while let Some(batch) = stream.try_next().await? {
            let idx = batch
                .schema()
                .index_of("id")
                .map_err(|_| anyhow::anyhow!("id column missing"))?;
            let col = batch.column(idx);
            let push = |heap: &mut BinaryHeap<String>, value: &str| {
                if heap.len() < fetch {
                    heap.push(value.to_string());
                } else if let Some(max) = heap.peek() {
                    if value < max.as_str() {
                        // Replace the current max.
                        let mut slot = heap.pop().unwrap();
                        slot.clear();
                        slot.push_str(value);
                        heap.push(slot);
                    }
                }
            };
            if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..a.len() {
                    push(&mut heap, a.value(i));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..a.len() {
                    push(&mut heap, a.value(i));
                }
            } else {
                anyhow::bail!("id column is {}", col.data_type());
            }
        }
        let mut ids = heap.into_vec();
        ids.sort_unstable();
        Ok(ids)
    }

    /// Flight projection: ticket columns mapped onto the dataset
    /// (`geometry` = WKB via ST_AsBinary on GeoArrow datasets, `x`/`y` =
    /// centroid columns).
    pub async fn scan_flight(
        &self,
        filter: &str,
        columns: &[String],
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        scanner.order_by(Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]))?;
        let projection: Vec<(&str, String)> = columns
            .iter()
            .map(|c| {
                let expr = match c.as_str() {
                    "geometry" if self.geo_geom => "ST_AsBinary(geom)".to_string(),
                    "geometry" => "geom".to_string(),
                    "x" => "cx".to_string(),
                    "y" => "cy".to_string(),
                    other => other.to_string(),
                };
                (c.as_str(), expr)
            })
            .collect();
        scanner.project_with_transform(&projection)?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        Ok(batches)
    }

    /// Scan the narrow id/layer projection for layer discovery.
    pub async fn scan_ids(&self, filter: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        scanner.project(&["id", "layer", "source_id"])?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        Ok(batches)
    }
}
