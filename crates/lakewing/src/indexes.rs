//! Serving indexes, installed on new or existing datasets without rewriting data.
use std::collections::HashSet;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use futures::TryStreamExt;
use lance::index::DatasetIndexExt;
use lance::Dataset;
use lance_index::scalar::ScalarIndexParams;
use lance_index::IndexType;
use serde_json::json;

#[derive(Debug, Clone, clap::Args)]
pub struct IndexConfig {
    /// Rows per ID BTREE leaf page.
    #[arg(long, default_value_t = 4096)]
    pub btree_page_rows: u64,
    /// Entries per spatial RTREE page (fanout).
    #[arg(long, default_value_t = 4096)]
    pub rtree_page_rows: u32,
    /// Rows per bbox zonemap zone; smaller zones improve spatial pruning.
    #[arg(long, default_value_t = 2048)]
    pub zonemap_rows: u64,
    /// Build layer/source bitmaps only for 2..this many distinct values; 0 disables.
    #[arg(long, default_value_t = 4096)]
    pub bitmap_max_values: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            btree_page_rows: 4096,
            rtree_page_rows: 4096,
            zonemap_rows: 2048,
            bitmap_max_values: 4096,
        }
    }
}

impl IndexConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (64..=65536).contains(&self.btree_page_rows),
            "btree-page-rows must be 64..65536"
        );
        anyhow::ensure!(
            (16..=65536).contains(&self.rtree_page_rows),
            "rtree-page-rows must be 16..65536"
        );
        anyhow::ensure!(
            (64..=1_048_576).contains(&self.zonemap_rows),
            "zonemap-rows must be 64..1048576"
        );
        anyhow::ensure!(
            self.bitmap_max_values == 0 || (2..=65536).contains(&self.bitmap_max_values),
            "bitmap-max-values must be 0 or 2..65536"
        );
        Ok(())
    }
}

pub async fn install(
    dataset: &mut Dataset,
    config: &IndexConfig,
    replace: bool,
) -> anyhow::Result<()> {
    config.validate()?;
    let existing: HashSet<String> = dataset
        .load_indices()
        .await?
        .iter()
        .map(|i| i.name.clone())
        .collect();
    let mut specs = vec![(
        "id",
        "id_idx".to_string(),
        IndexType::BTree,
        json!({"zone_size": config.btree_page_rows}),
    )];
    let schema = dataset.schema();
    if schema
        .field("geom")
        .is_some_and(|f| !matches!(f.data_type(), DataType::Binary | DataType::LargeBinary))
    {
        specs.push((
            "geom",
            "geom_idx".into(),
            IndexType::RTree,
            json!({"page_size": config.rtree_page_rows}),
        ));
    }
    for column in ["xmin", "ymin", "xmax", "ymax"] {
        if schema.field(column).is_some() {
            specs.push((
                column,
                format!("{column}_zonemap"),
                IndexType::ZoneMap,
                json!({"rows_per_zone": config.zonemap_rows}),
            ));
        }
    }
    for column in ["layer", "source_id"] {
        let name = format!("{column}_bitmap");
        if config.bitmap_max_values > 0 && (!existing.contains(&name) || replace) {
            let cardinality = cardinality_up_to(dataset, column, config.bitmap_max_values).await?;
            if (2..=config.bitmap_max_values).contains(&cardinality) {
                specs.push((column, name, IndexType::Bitmap, json!({})));
            } else {
                tracing::info!(
                    column,
                    cardinality,
                    "skipping unselective or high-cardinality bitmap"
                );
            }
        }
    }
    for (column, name, ty, params) in specs {
        if existing.contains(&name) && !replace {
            tracing::info!(
                index = name,
                "keeping existing index (use --replace to retune)"
            );
            continue;
        }
        let started = std::time::Instant::now();
        let params = ScalarIndexParams::new(ty.to_string()).with_params(&params);
        dataset
            .create_index_builder(&[column], ty, &params)
            .name(name.clone())
            .replace(replace)
            .await?;
        tracing::info!(index = name, elapsed = ?started.elapsed(), statistics = %dataset.index_statistics(&name).await?, "installed serving index");
    }
    Ok(())
}

/// Stop as soon as the bitmap would be too large. Singleton columns cannot
/// prune a matching request and can force expensive index-address takes.
async fn cardinality_up_to(dataset: &Dataset, column: &str, max: usize) -> anyhow::Result<usize> {
    let mut scanner = dataset.scan();
    scanner.project(&[column])?;
    scanner.batch_size(8192);
    let mut stream = scanner.try_into_stream().await?;
    let mut values = HashSet::new();
    while let Some(batch) = stream.try_next().await? {
        let array = batch.column(0);
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let value = if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                array.value(i).to_string()
            } else {
                crate::lake::string_at(array.as_ref(), i)?.to_string()
            };
            values.insert(value);
            if values.len() > max {
                return Ok(values.len());
            }
        }
    }
    Ok(values.len())
}
