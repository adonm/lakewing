//! Dev/benchmark dataset replication: materialize N longitude-shifted
//! GeoParquet copies of a real source through DuckDB spatial, so scale
//! testing (index size, selection, cache working set) runs against
//! realistic geometry spread over a nation-scale extent without inventing
//! shapes. Copy 0 keeps the original ids unchanged; copy k >= 1 prefixes
//! `k:` so `(id, source_id)` stays unique.
pub struct ReplicateConfig {
    pub source: String,
    pub out_dir: String,
    pub copies: usize,
    /// Longitude offset between consecutive copies (degrees).
    pub offset_degrees: f64,
    /// Cap rows read per copy (0 = all) — verification aid.
    pub limit: usize,
}

pub async fn replicate(cfg: ReplicateConfig) -> anyhow::Result<()> {
    anyhow::ensure!(cfg.copies >= 1 && cfg.copies <= 18, "copies must be 1..18");
    anyhow::ensure!(
        cfg.offset_degrees > 0.0 && (cfg.copies as f64 - 1.0) * cfg.offset_degrees < 170.0,
        "offset must keep every copy inside CRS84 lon bounds"
    );
    let files = crate::build::discover_parquet(&cfg.source)?;
    anyhow::ensure!(!files.is_empty(), "no parquet files under {}", cfg.source);
    let handle = std::fs::File::open(&files[0])?;
    let schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(handle)?
        .schema()
        .clone();
    crate::build::validate_source(&schema)?;
    std::fs::create_dir_all(&cfg.out_dir)?;

    let db = duckdb::Connection::open_in_memory()?;
    db.execute_batch("INSTALL spatial; LOAD spatial")?;
    let mut wrote = 0usize;
    for copy in 0..cfg.copies {
        let shift = copy as f64 * cfg.offset_degrees;
        let id_expr = if copy == 0 {
            "id".to_string()
        } else {
            format!("'{copy}:' || id")
        };
        let limit = if cfg.limit > 0 {
            format!("LIMIT {}", cfg.limit)
        } else {
            String::new()
        };
        let part = format!("{}/part-copy-{copy}.parquet", cfg.out_dir);
        // One part per copy keeps file ordering deterministic for the build.
        let sql = format!(
            "COPY (
                 SELECT {id_expr} AS id, layer, source_id,
                        ST_AsWKB(ST_Affine(ST_GeomFromWKB(geom),
                                           1, 0, 0,
                                           0, 1, 0,
                                           0, 0, 1,
                                           {shift}, 0, 0)) AS geom,
                        properties,
                        xmin + {shift} AS xmin, ymin,
                        xmax + {shift} AS xmax, ymax
                 FROM read_parquet('{src}', union_by_name=true)
                 {limit}
             ) TO '{out}' (FORMAT PARQUET)",
            src = files[0].replace('\'', "''"),
            out = part.replace('\'', "''"),
        );
        db.execute_batch(&sql)?;
        let rows = parquet_rows(&part)?;
        tracing::info!(copy, shift, rows, part = %part, "materialized copy");
        wrote += rows;
    }
    tracing::info!(parts = cfg.copies, rows = wrote, "replication complete");
    Ok(())
}

fn parquet_rows(path: &str) -> anyhow::Result<usize> {
    let handle = std::fs::File::open(path)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(handle)?
        .with_batch_size(65_536)
        .build()?;
    Ok(reader
        .into_iter()
        .map(|batch| batch.map(|b| b.num_rows()))
        .sum::<Result<usize, _>>()?)
}
