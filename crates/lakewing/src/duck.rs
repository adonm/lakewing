//! DuckDB: complex local work. Candidate rows from Lance enter through an
//! appender (row-wise over page-sized sets, which also sidesteps any
//! arrow-version alignment between the lance and duckdb crates), then the
//! exact predicate, ordering, pagination and GeoJSON rendering run in SQL.
use std::sync::Mutex;

use arrow::array::{Array, Float64Array, Int64Array, LargeStringArray, StringArray};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;

pub struct Duck {
    conn: Mutex<Connection>,
}

/// One rendered feature row.
pub struct FeatureRow {
    pub id: String,
    pub geom_json: String,
    pub properties: String,
}

impl Duck {
    pub fn open() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "INSTALL spatial; LOAD spatial; INSTALL json; LOAD json; SET threads=4;",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn load_page(&self, batches: &[RecordBatch]) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS lw_page; CREATE TEMP TABLE lw_page(
                id VARCHAR, geom BLOB, properties VARCHAR,
                layer VARCHAR, source_id BIGINT,
                xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)",
        )?;
        let mut appender = conn.appender("lw_page")?;
        for batch in batches {
            let schema = batch.schema();
            let id = batch.column(schema.index_of("id").map_err(|_| anyhow::anyhow!("id"))?);
            let geom = batch.column(
                schema
                    .index_of("geom")
                    .map_err(|_| anyhow::anyhow!("geom"))?,
            );
            let props = batch.column(
                schema
                    .index_of("properties")
                    .map_err(|_| anyhow::anyhow!("props"))?,
            );
            let layer = batch.column(
                schema
                    .index_of("layer")
                    .map_err(|_| anyhow::anyhow!("layer"))?,
            );
            let source = batch.column(
                schema
                    .index_of("source_id")
                    .map_err(|_| anyhow::anyhow!("source"))?,
            );
            let xmin = batch.column(
                schema
                    .index_of("xmin")
                    .map_err(|_| anyhow::anyhow!("xmin"))?,
            );
            let ymin = batch.column(
                schema
                    .index_of("ymin")
                    .map_err(|_| anyhow::anyhow!("ymin"))?,
            );
            let xmax = batch.column(
                schema
                    .index_of("xmax")
                    .map_err(|_| anyhow::anyhow!("xmax"))?,
            );
            let ymax = batch.column(
                schema
                    .index_of("ymax")
                    .map_err(|_| anyhow::anyhow!("ymax"))?,
            );
            let n = batch.num_rows();
            for i in 0..n {
                appender.append_rows([(
                    str_at(id, i)?,
                    blob_at(geom, i)?,
                    str_at(props, i)?,
                    str_at(layer, i)?,
                    int_at(source, i)?,
                    f64_at(xmin, i)?,
                    f64_at(ymin, i)?,
                    f64_at(xmax, i)?,
                    f64_at(ymax, i)?,
                )])?;
            }
        }
        Ok(())
    }

    /// Render the page: exact predicate + ORDER BY id + LIMIT/OFFSET over
    /// the loaded candidates. `fetch_limit` is limit+1 when the caller
    /// needs hasNext detection.
    pub fn render_page(
        &self,
        batches: &[RecordBatch],
        exact: &str,
        fetch_limit: Option<usize>,
        offset: usize,
    ) -> anyhow::Result<Vec<FeatureRow>> {
        self.load_page(batches)?;
        let conn = self.conn.lock().unwrap();
        let tail = match fetch_limit {
            Some(limit) => format!("LIMIT {} OFFSET {}", limit, offset),
            None => format!("OFFSET {}", offset),
        };
        let sql = format!(
            "SELECT id, ST_AsGeoJSON(ST_GeomFromWKB(geom)), properties FROM lw_page \
             WHERE {} ORDER BY id {}",
            exact, tail
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(FeatureRow {
                id: row.get(0)?,
                geom_json: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                properties: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Distinct layer values (collections) over narrow batches.
    pub fn distinct_layers(&self, batches: &[RecordBatch]) -> anyhow::Result<Vec<String>> {
        let layers: std::collections::BTreeSet<String> = batches
            .iter()
            .flat_map(|b| {
                let schema = b.schema();
                schema
                    .index_of("layer")
                    .map(|idx| {
                        let col = b.column(idx);
                        (0..b.num_rows()).filter_map(move |i| str_at(col, i).ok())
                    })
                    .into_iter()
                    .flatten()
            })
            .collect();
        Ok(layers.into_iter().collect())
    }
}

fn str_at(col: &dyn Array, i: usize) -> anyhow::Result<String> {
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        Ok(a.value(i).to_string())
    } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        Ok(a.value(i).to_string())
    } else {
        anyhow::bail!("expected string column, got {}", col.data_type())
    }
}

fn blob_at(col: &dyn Array, i: usize) -> anyhow::Result<Vec<u8>> {
    use arrow::array::BinaryArray;
    if let Some(a) = col.as_any().downcast_ref::<BinaryArray>() {
        Ok(a.value(i).to_vec())
    } else {
        use arrow::array::LargeBinaryArray;
        if let Some(a) = col.as_any().downcast_ref::<LargeBinaryArray>() {
            Ok(a.value(i).to_vec())
        } else {
            anyhow::bail!("expected binary geometry column, got {}", col.data_type())
        }
    }
}

fn int_at(col: &dyn Array, i: usize) -> anyhow::Result<i64> {
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Ok(a.value(i))
    } else {
        anyhow::bail!("expected int64 column, got {}", col.data_type())
    }
}

fn f64_at(col: &dyn Array, i: usize) -> anyhow::Result<f64> {
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        Ok(a.value(i))
    } else {
        anyhow::bail!("expected double column, got {}", col.data_type())
    }
}

/// SQL-quote a string literal.
pub fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn id_in_list(ids: &[String]) -> String {
    ids.iter().map(|id| quote(id)).collect::<Vec<_>>().join(",")
}

/// The exact predicate re-checked by DuckDB over candidates: the same
/// semantics as the Go store (bbox overlap + contained-or-intersects).
pub fn exact_predicate(collection: &str, bounds: Option<[f64; 4]>, sources: &[i64]) -> String {
    let srcs: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
    let mut base = format!(
        "layer = {} AND source_id IN ({})",
        quote(collection),
        srcs.join(",")
    );
    if let Some([w, s, e, n]) = bounds {
        base.push_str(&format!(
            " AND xmax >= {w} AND xmin <= {e} AND ymax >= {s} AND ymin <= {n} \
             AND ((xmin >= {w} AND xmax <= {e} AND ymin >= {s} AND ymax <= {n}) \
             OR ST_Intersects(ST_GeomFromWKB(geom), ST_MakeEnvelope({w}, {s}, {e}, {n})))"
        ));
    }
    base
}

/// The candidate-superset filter pushed into Lance (same contract as
/// Parquet zonemap pruning: DuckDB re-checks everything).
pub fn pushed_filter(collection: &str, bounds: Option<[f64; 4]>, sources: &[i64]) -> String {
    let srcs: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
    let mut base = format!(
        "layer = {} AND source_id IN ({})",
        quote(collection),
        srcs.join(",")
    );
    if let Some([w, s, e, n]) = bounds {
        base.push_str(&format!(
            " AND xmax >= {w} AND xmin <= {e} AND ymax >= {s} AND ymin <= {n}"
        ));
    }
    base
}
