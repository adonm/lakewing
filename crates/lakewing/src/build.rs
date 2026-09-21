//! Build/materialize pipeline: source parquet (WKB geometry + bbox
//! columns) → GeoArrow Lance dataset with BTREE(id) + RTREE(geom) and a
//! release tag. Replaces the pylance harness as the dataset producer, on
//! the pinned release train (lance 12.0.0) the serve reads.
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use geoarrow_array::cast::from_wkb;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::{Dimension, MultiPolygonType};
use lance::dataset::refs::Ref;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

pub struct BuildConfig {
    /// Source parquet file or directory of parquet files.
    pub source: String,
    /// Output dataset directory.
    pub out: String,
    /// Release tag to publish (empty = no tag).
    pub tag: String,
    pub max_rows_per_file: usize,
    pub max_bytes_per_file: usize,
    pub indexes: crate::indexes::IndexConfig,
}

/// Build the dataset. Mirrors the layout the parity gates verified:
/// data_storage_version 2.2, GeoArrow multipolygon geometry (polygons
/// promoted, `was_polygon` records the original form), BTREE on id,
/// RTREE on geom.
pub async fn build(cfg: BuildConfig) -> anyhow::Result<()> {
    cfg.indexes.validate()?;
    anyhow::ensure!(
        cfg.max_rows_per_file > 0 && cfg.max_bytes_per_file > 0,
        "fragment limits must be positive"
    );
    let started = std::time::Instant::now();
    let files = discover(&cfg.source)?;
    if files.is_empty() {
        anyhow::bail!("no parquet files under {}", cfg.source);
    }

    let handle = std::fs::File::open(&files[0])?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(handle)?;
    let source_schema = builder.schema().clone();
    validate_source(&source_schema)?;
    let _target_schema = target_schema(&source_schema);

    let write_params = WriteParams {
        max_rows_per_file: cfg.max_rows_per_file,
        max_bytes_per_file: cfg.max_bytes_per_file,
        data_storage_version: Some(parse_version("2.2")?),
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        SourceChain::new(files, source_schema),
        cfg.out.as_str(),
        Some(write_params),
    )
    .await?;
    let version = dataset.version().version;
    tracing::info!(
        rows = dataset.count_rows(None).await?,
        version,
        elapsed = ?started.elapsed(),
        "wrote dataset"
    );

    crate::indexes::install(&mut dataset, &cfg.indexes, false).await?;

    if !cfg.tag.is_empty() {
        let version = dataset.version().version;
        dataset
            .tags()
            .create(cfg.tag.as_str(), Ref::VersionNumber(version))
            .await?;
        tracing::info!(tag = %cfg.tag, version, "tagged dataset");
    }
    Ok(())
}

/// GeoArrow multipolygon type (XY, no CRS — matches the harness-built
/// datasets the parity gates verified against).
fn multi_polygon_type() -> GeoArrowType {
    GeoArrowType::MultiPolygon(MultiPolygonType::new(Dimension::XY, Default::default()))
}

fn parse_version(v: &str) -> anyhow::Result<LanceFileVersion> {
    // LanceFileVersion implements FromStr; surfaced through lance::Dataset's
    // public re-export path.
    v.parse()
        .map_err(|e| anyhow::anyhow!("bad data_storage_version {v}: {e:?}"))
}

fn discover(source: &str) -> anyhow::Result<Vec<String>> {
    let path = std::path::Path::new(source);
    let mut files = Vec::new();
    if path.is_dir() {
        collect_parquet(path, &mut files)?;
        files.sort();
    } else {
        files.push(source.to_string());
    }
    Ok(files)
}

pub(crate) fn discover_parquet(source: &str) -> anyhow::Result<Vec<String>> {
    discover(source)
}

fn collect_parquet(dir: &std::path::Path, out: &mut Vec<String>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_parquet(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path.to_string_lossy().to_string());
        }
    }
    Ok(())
}

pub(crate) fn validate_source(schema: &Schema) -> anyhow::Result<()> {
    for name in [
        "id",
        "layer",
        "source_id",
        "geom",
        "properties",
        "xmin",
        "ymin",
        "xmax",
        "ymax",
    ] {
        schema
            .field_with_name(name)
            .map_err(|_| anyhow::anyhow!("source is missing column {name}"))?;
    }
    let geom = schema.field_with_name("geom")?;
    if !matches!(geom.data_type(), DataType::Binary | DataType::LargeBinary) {
        anyhow::bail!("source geom must be WKB binary, got {}", geom.data_type());
    }
    Ok(())
}

fn target_schema(source: &Schema) -> SchemaRef {
    let geom_type = multi_polygon_type();
    let fields: Vec<Field> = source
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "geom" => geom_type.to_field("geom", true),
            other => Field::new(other, field.data_type().clone(), field.is_nullable()),
        })
        .chain(std::iter::once(Field::new(
            "was_polygon",
            DataType::Boolean,
            true,
        )))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Lazily chains the source parquet files, converting each batch's WKB
/// geom to GeoArrow multipolygon storage plus the was_polygon bit.
struct SourceChain {
    files: Vec<String>,
    file_idx: usize,
    current: Option<ParquetRecordBatchReader>,
    source_schema: SchemaRef,
    schema: SchemaRef,
    done: bool,
}

impl SourceChain {
    fn new(files: Vec<String>, source_schema: SchemaRef) -> Self {
        let schema = target_schema(&source_schema);
        Self {
            files,
            file_idx: 0,
            current: None,
            source_schema,
            schema,
            done: false,
        }
    }

    fn advance(&mut self) -> Option<parquet::errors::Result<ParquetRecordBatchReader>> {
        if self.file_idx >= self.files.len() {
            return None;
        }
        let path = self.files[self.file_idx].clone();
        self.file_idx += 1;
        Some(
            std::fs::File::open(&path)
                .map_err(parquet::errors::ParquetError::from)
                .and_then(|handle| {
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(handle)?
                        .with_batch_size(65_536)
                        .build()
                }),
        )
    }

    fn convert(&self, batch: &RecordBatch) -> arrow::error::Result<RecordBatch> {
        let schema = batch.schema();
        let geom_idx = schema
            .index_of("geom")
            .map_err(|e| arrow::error::ArrowError::SchemaError(e.to_string()))?;
        let geom = batch.column(geom_idx);
        let wkb = downcast_wkb(geom)?;

        let was_polygon = was_polygon_bits(wkb);
        // geoarrow 0.8's WKB -> MultiPolygon builder mishandles null offsets.
        // Convert valid values, then use Arrow's null-aware take to restore positions.
        let valid = arrow::compute::filter(wkb, &arrow::compute::is_not_null(wkb)?)?;
        let valid = valid
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let multi = if valid.is_empty() {
            arrow::array::new_null_array(
                multi_polygon_type().to_field("geom", true).data_type(),
                wkb.len(),
            )
        } else {
            let wkb_typed =
                geoarrow_array::array::WkbArray::new(valid.clone(), std::sync::Arc::default());
            let multi = from_wkb(&wkb_typed, multi_polygon_type())
                .map_err(|e| arrow::error::ArrowError::InvalidArgumentError(format!("{e}")))?
                .into_array_ref();
            if wkb.null_count() == 0 {
                multi
            } else {
                let mut next = 0u32;
                let indices = UInt32Array::from(
                    (0..wkb.len())
                        .map(|i| {
                            if wkb.is_null(i) {
                                None
                            } else {
                                let value = next;
                                next += 1;
                                Some(value)
                            }
                        })
                        .collect::<Vec<_>>(),
                );
                arrow::compute::take(multi.as_ref(), &indices, None)?
            }
        };

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns() + 1);
        let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns() + 1);
        for (i, field) in schema.fields().iter().enumerate() {
            if i == geom_idx {
                fields.push(multi_polygon_type().to_field("geom", true));
                columns.push(multi.clone());
            } else {
                fields.push((**field).clone());
                columns.push(batch.column(i).clone());
            }
        }
        fields.push(Field::new("was_polygon", DataType::Boolean, true));
        columns.push(Arc::new(was_polygon));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

fn downcast_wkb(geom: &dyn Array) -> arrow::error::Result<&arrow::array::BinaryArray> {
    use arrow::array::LargeBinaryArray;
    if let Some(a) = geom.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        Ok(a)
    } else if let Some(large) = geom.as_any().downcast_ref::<LargeBinaryArray>() {
        let _ = large;
        Err(arrow::error::ArrowError::InvalidArgumentError(
            "large-binary WKB sources: re-encode as binary WKB".to_string(),
        ))
    } else {
        Err(arrow::error::ArrowError::InvalidArgumentError(format!(
            "geom column is {} (expected binary WKB)",
            geom.data_type()
        )))
    }
}

fn was_polygon_bits(wkb: &arrow::array::BinaryArray) -> BooleanArray {
    let mut bits = Vec::with_capacity(wkb.len());
    for i in 0..wkb.len() {
        let flag = if wkb.is_null(i) {
            None
        } else {
            let bytes = wkb.value(i);
            if bytes.len() < 5 {
                Some(false)
            } else {
                let word = if bytes[0] == 1 {
                    u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
                } else {
                    u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
                };
                Some(word == 3)
            }
        };
        bits.push(flag);
    }
    BooleanArray::from(bits)
}

impl Iterator for SourceChain {
    type Item = arrow::error::Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if self.current.is_none() {
                match self.advance() {
                    Some(Ok(reader)) => self.current = Some(reader),
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e))));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }
            let reader = self.current.as_mut().unwrap();
            match reader.next() {
                Some(Ok(batch)) => match self.convert(&batch) {
                    Ok(converted) => return Some(Ok(converted)),
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                },
                Some(Err(e)) => {
                    self.current = None;
                    return Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e))));
                }
                None => self.current = None,
            }
        }
    }
}

impl RecordBatchReader for SourceChain {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// Silence unused warnings for schema field kept for API symmetry.
impl SourceChain {
    #[allow(dead_code)]
    fn source_schema(&self) -> SchemaRef {
        self.source_schema.clone()
    }
}
