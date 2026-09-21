//! Snapshot-pinned Lance selection, late payload materialization and Flight projection.
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, BinaryArray, BooleanArray, Int64Array, LargeStringArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::ColumnOrdering;
use lance::Dataset;

use crate::cache::CachingStore;
use crate::query::{QueryError, RowKey, MAX_PAGE_BYTES};

pub struct SourceConfig {
    pub uri: String,
    pub tag: Option<String>,
    pub version: Option<u64>,
    pub storage_options: HashMap<String, String>,
}

pub struct LanceSource {
    dataset: Arc<Dataset>,
    pub version: u64,
    pub spatial: bool,
    pub geo_geom: bool,
    pub has_was_polygon: bool,
    /// Coarse-bbox split plan requires the bbox columns.
    pub has_bbox_columns: bool,
}

impl LanceSource {
    pub async fn open(cfg: SourceConfig, cache: Option<Arc<CachingStore>>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            cfg.tag.is_some() ^ cfg.version.is_some(),
            "pin exactly one tag or version"
        );
        let builder = DatasetBuilder::from_uri(&cfg.uri).with_storage_options(cfg.storage_options);
        let dataset = match (cfg.tag, cfg.version) {
            (Some(tag), None) => builder.with_tag(&tag).load().await?,
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
        for (name, expected) in [
            ("id", DataType::Utf8),
            ("layer", DataType::Utf8),
            ("properties", DataType::Utf8),
            ("source_id", DataType::Int64),
        ] {
            let field = schema
                .field(name)
                .ok_or_else(|| anyhow::anyhow!("missing {name} column"))?;
            anyhow::ensure!(
                field.data_type() == expected
                    || (expected == DataType::Utf8 && field.data_type() == DataType::LargeUtf8),
                "unsupported {name} type: {}",
                field.data_type()
            );
        }
        let spatial = schema.field("geom").is_some();
        let geo_geom = schema
            .field("geom")
            .is_some_and(|f| !matches!(f.data_type(), DataType::Binary | DataType::LargeBinary));
        let has_was_polygon = schema.field("was_polygon").is_some();
        let has_bbox_columns = ["xmin", "ymin", "xmax", "ymax"]
            .iter()
            .all(|name| schema.field(name).is_some());
        let version = dataset.version().version;
        Ok(Self {
            dataset: Arc::new(dataset),
            version,
            spatial,
            geo_geom,
            has_was_polygon,
            has_bbox_columns,
        })
    }

    fn scanner(&self, filter: &str) -> anyhow::Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.dataset.scan();
        scanner.filter(filter)?;
        // 8192 measured ~2.5x faster than 1024 on the 25M-row fixture for
        // whole-collection key scans (per-batch scheduling dominates); Lance
        // chunks fragment I/O independently of this, and Flight still
        // streams per batch.
        scanner.batch_size(8192);
        Ok(scanner)
    }

    /// Physical plan for a filter (index prefilter vs refine vs scan) —
    /// used by tests to pin index usage for tile/bbox/id queries instead
    /// of assuming it.
    #[cfg(test)]
    pub async fn explain(&self, filter: &str, columns: &[&str]) -> anyhow::Result<String> {
        let mut scanner = self.scanner(filter)?;
        scanner.project(columns)?;
        Ok(scanner.explain_plan(true).await?)
    }

    pub async fn collections(&self) -> anyhow::Result<Vec<String>> {
        if let Some(json) = self.dataset.metadata().get("lakewing.collections") {
            return Ok(serde_json::from_str(json)?);
        }
        // Older datasets have no published collection metadata. Stream only the layer column.
        let mut scanner = self.scanner("true")?;
        scanner.project(&["layer"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut layers = BTreeSet::new();
        while let Some(batch) = stream.try_next().await? {
            for i in 0..batch.num_rows() {
                layers.insert(string_at(batch.column(0).as_ref(), i)?.to_string());
            }
        }
        Ok(layers.into_iter().collect())
    }

    /// Materialize the payload for an already-selected key window.
    ///
    /// This is the one deliberate materialization in the read path, and it is
    /// fail-closed: the stream is consumed batch-by-batch and rejected with
    /// 413 as soon as the accumulated Arrow bytes exceed `MAX_PAGE_BYTES`.
    /// Callers pass `limit` ≤ the selected window (limit+1 / tile cap), so
    /// memory stays proportional to the *page*, never the collection. The
    /// key-selection scan itself (scan_keys_topk) is lazy and heap-bounded.
    #[tracing::instrument(name = "lance.payload", skip_all)]
    pub async fn scan_page(
        &self,
        filter: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut scanner = self.scanner(filter)?;
        let names = [
            "id",
            "geom",
            "properties",
            "layer",
            "source_id",
            "xmin",
            "ymin",
            "xmax",
            "ymax",
            "was_polygon",
        ];
        let projection = names
            .iter()
            .filter(|name| self.dataset.schema().field(name).is_some())
            .map(|name| {
                // Bare names: Lance's SQL parser reads double-quoted strings as literals.
                (
                    *name,
                    if *name == "geom" && self.geo_geom {
                        "ST_AsBinary(geom)".to_string()
                    } else {
                        name.to_string()
                    },
                )
            })
            .collect::<Vec<_>>();
        scanner.project_with_transform(&projection)?;
        // No order_by here: the selected key window is already sorted
        // (scan_keys_topk), and DuckDB's render SQL re-orders the page —
        // an ordered payload scan would disable Lance's limit pushdown and
        // force a second random-access take of payload columns per fragment
        // (measured ~2.5x on the 25M-row fixture). Flight keeps its order_by:
        // ordering is part of that contract.
        scanner.limit(limit.map(|v| v as i64), None)?;
        let mut stream = scanner.try_into_stream().await?;
        let mut batches = Vec::new();
        let mut bytes = 0;
        while let Some(batch) = stream.try_next().await? {
            bytes += batch.get_array_memory_size();
            if bytes > MAX_PAGE_BYTES {
                return Err(QueryError::new(
                    413,
                    "page exceeds the 64 MiB payload budget; reduce limit",
                )
                .into());
            }
            let mut fields = batch
                .schema()
                .fields()
                .iter()
                .map(|f| (**f).clone())
                .collect::<Vec<_>>();
            let mut arrays = batch.columns().to_vec();
            for (name, ty) in [
                ("geom", DataType::Binary),
                ("xmin", DataType::Float64),
                ("ymin", DataType::Float64),
                ("xmax", DataType::Float64),
                ("ymax", DataType::Float64),
            ] {
                if batch.schema().index_of(name).is_err() {
                    arrays.push(new_null_array(&ty, batch.num_rows()));
                    fields.push(Field::new(name, ty, true));
                }
            }
            batches.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?);
        }
        Ok(batches)
    }

    /// Select the ordered `(id, source_id)` window over an exact filter.
    ///
    /// Laziness contract: the id/source scan streams over the *entire*
    /// matching set (this is the part that scales with the lake, not the
    /// page), but only `fetch + 1` keys are ever held — a bounded max-heap
    /// replaces its current max as smaller keys stream past. `examined`
    /// records how many rows the scan touched, so profiles can attribute
    /// selection cost separately from payload cost.
    #[tracing::instrument(name = "lance.select", skip_all, fields(fetch, examined = tracing::field::Empty))]
    pub async fn scan_keys_topk(&self, filter: &str, fetch: usize) -> anyhow::Result<Vec<RowKey>> {
        if fetch == 0 || filter == "false" {
            return Ok(Vec::new());
        }
        let mut scanner = self.scanner(filter)?;
        scanner.project(&["id", "source_id"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut heap: BinaryHeap<RowKey> = BinaryHeap::with_capacity(fetch + 1);
        let mut examined = 0usize;
        while let Some(batch) = stream.try_next().await? {
            let sources = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("source_id must be int64"))?;
            examined += batch.num_rows();
            for i in 0..batch.num_rows() {
                anyhow::ensure!(!sources.is_null(i), "source_id must not be null");
                let id = string_at(batch.column(0).as_ref(), i)?;
                let source_id = sources.value(i);
                if heap.len() < fetch
                    || heap
                        .peek()
                        .is_some_and(|key| (id, source_id) < (key.id.as_str(), key.source_id))
                {
                    if heap.len() == fetch {
                        heap.pop();
                    }
                    heap.push(RowKey {
                        id: id.to_string(),
                        source_id,
                    });
                }
            }
        }
        tracing::Span::current().record("examined", examined);
        Ok(heap.into_sorted_vec())
    }

    pub fn flight_schema(&self, columns: &[String]) -> anyhow::Result<SchemaRef> {
        let mut fields = Vec::new();
        for name in columns {
            let actual = match name.as_str() {
                "geometry" => "geom",
                "x" => "cx",
                "y" => "cy",
                other => other,
            };
            let ty = if name == "geometry" {
                DataType::Binary
            } else {
                let field = self
                    .dataset
                    .schema()
                    .field(actual)
                    .ok_or_else(|| QueryError::new(400, format!("unknown column {name:?}")))?;
                match field.data_type() {
                    DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
                    other => other,
                }
            };
            fields.push(Field::new(name, ty, true));
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    pub async fn scan_flight(
        &self,
        filter: &str,
        columns: &[String],
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<RecordBatch>>> {
        let schema = self.flight_schema(columns)?;
        let mut scanner = self.scanner(filter)?;
        scanner.order_by(Some(vec![
            ColumnOrdering::asc_nulls_last("id".into()),
            ColumnOrdering::asc_nulls_last("source_id".into()),
        ]))?;
        let mut projection = columns
            .iter()
            .filter(|c| c.as_str() != "geometry" || self.spatial)
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
            .collect::<Vec<_>>();
        if projection.is_empty() {
            projection.push(("id", "id".into()));
        }
        if self.has_was_polygon && columns.iter().any(|c| c == "geometry") {
            projection.push(("was_polygon", "was_polygon".into()));
        }
        scanner.project_with_transform(&projection)?;
        let stream = scanner.try_into_stream().await?;
        Ok(stream
            .map(move |result| normalize_flight(result?, schema.clone()))
            .boxed())
    }
}

pub fn string_at(array: &dyn Array, i: usize) -> anyhow::Result<&str> {
    anyhow::ensure!(!array.is_null(i), "key/collection must not be null");
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        Ok(a.value(i))
    } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        Ok(a.value(i))
    } else {
        anyhow::bail!("expected string, got {}", array.data_type())
    }
}

fn normalize_flight(batch: RecordBatch, schema: SchemaRef) -> anyhow::Result<RecordBatch> {
    if batch.get_array_memory_size() > MAX_PAGE_BYTES {
        return Err(QueryError::new(413, "Flight batch exceeds 64 MiB").into());
    }
    let mut arrays = Vec::new();
    for field in schema.fields() {
        let Ok(idx) = batch.schema().index_of(field.name()) else {
            arrays.push(new_null_array(field.data_type(), batch.num_rows()));
            continue;
        };
        let mut array = arrow::compute::cast(batch.column(idx), field.data_type())?;
        if field.name() == "geometry" {
            if let Ok(wp) = batch.schema().index_of("was_polygon") {
                let flags = batch
                    .column(wp)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| anyhow::anyhow!("was_polygon must be boolean"))?;
                let wkb = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let mut restored = Vec::with_capacity(batch.num_rows());
                for i in 0..batch.num_rows() {
                    if wkb.is_null(i) {
                        restored.push(None);
                        continue;
                    }
                    let bytes = wkb.value(i);
                    let polygon = !flags.is_null(i) && flags.value(i);
                    if polygon {
                        anyhow::ensure!(bytes.len() >= 9, "truncated multipolygon WKB");
                        let word = |offset| {
                            let b = <[u8; 4]>::try_from(&bytes[offset..offset + 4]).unwrap();
                            if bytes[0] == 1 {
                                u32::from_le_bytes(b)
                            } else {
                                u32::from_be_bytes(b)
                            }
                        };
                        anyhow::ensure!(
                            word(1) == 6 && word(5) == 1,
                            "was_polygon requires a single-part multipolygon"
                        );
                    }
                    restored.push(Some(if polygon { &bytes[9..] } else { bytes }));
                }
                array = Arc::new(BinaryArray::from(restored));
            }
        }
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}
