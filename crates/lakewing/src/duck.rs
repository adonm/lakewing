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

    fn load_page(&self, batches: &[RecordBatch], was_polygon: bool) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS lw_page; CREATE TEMP TABLE lw_page(
                id VARCHAR, geom BLOB, properties VARCHAR,
                layer VARCHAR, source_id BIGINT,
                xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE);
             DROP TABLE IF EXISTS lw_wp; CREATE TEMP TABLE lw_wp(id VARCHAR, was_polygon BOOLEAN);",
        )?;
        let mut appender = conn.appender("lw_page")?;
        for batch in batches {
            let schema = batch.schema();
            let col = |name: &str| {
                schema
                    .index_of(name)
                    .map_err(|_| anyhow::anyhow!("{name} column missing"))
            };
            let id = batch.column(col("id")?);
            let geom = batch.column(col("geom")?);
            let props = batch.column(col("properties")?);
            let layer = batch.column(col("layer")?);
            let source = batch.column(col("source_id")?);
            let xmin = batch.column(col("xmin")?);
            let ymin = batch.column(col("ymin")?);
            let xmax = batch.column(col("xmax")?);
            let ymax = batch.column(col("ymax")?);
            let wp_col = if was_polygon {
                Some(batch.column(col("was_polygon")?))
            } else {
                None
            };
            let n = batch.num_rows();
            let mut wp_appender = if was_polygon {
                Some(conn.appender("lw_wp")?)
            } else {
                None
            };
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
                if let (Some(wp), Some(app)) = (wp_col, wp_appender.as_mut()) {
                    app.append_rows([(str_at(id, i)?, bool_at(wp, i)?)])?;
                }
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
        was_polygon: bool,
    ) -> anyhow::Result<Vec<FeatureRow>> {
        self.load_page(batches, was_polygon)?;
        let conn = self.conn.lock().unwrap();
        let tail = match fetch_limit {
            Some(limit) => format!("LIMIT {} OFFSET {}", limit, offset),
            None => format!("OFFSET {}", offset),
        };
        // GeoArrow datasets store every polygon promoted to multipolygon;
        // was_polygon restores the original single-part form so GeoJSON
        // matches the source features exactly.
        let geom_expr = if was_polygon {
            "CASE WHEN lw_wp.was_polygon THEN (ST_Dump(ST_GeomFromWKB(geom)))[1].geom ELSE ST_GeomFromWKB(geom) END"
        } else {
            "ST_GeomFromWKB(geom)"
        };
        let from_join = if was_polygon {
            "lw_page JOIN lw_wp USING (id)"
        } else {
            "lw_page"
        };
        let sql = format!(
            "SELECT lw_page.id, ST_AsGeoJSON({geom_expr}), lw_page.properties FROM {from_join} \
             WHERE lw_page.id IS NOT NULL AND {} ORDER BY lw_page.id {}",
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

fn bool_at(col: &dyn Array, i: usize) -> anyhow::Result<bool> {
    use arrow::array::BooleanArray;
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        Ok(a.value(i))
    } else {
        anyhow::bail!("expected boolean column, got {}", col.data_type())
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
/// Parquet zonemap pruning: DuckDB re-checks everything). GeoArrow
/// geometries push `ST_Intersects` (drives the RTREE); WKB datasets push
/// bbox-column overlap.
pub fn pushed_filter(
    collection: &str,
    bounds: Option<[f64; 4]>,
    sources: &[i64],
    geo_geom: bool,
) -> String {
    let srcs: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
    let mut base = format!(
        "layer = {} AND source_id IN ({})",
        quote(collection),
        srcs.join(",")
    );
    if let Some([w, s, e, n]) = bounds {
        if geo_geom {
            base.push_str(&format!(
                " AND ST_Intersects(geom, ST_GeomFromText('POLYGON (({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))'))"
            ));
        } else {
            base.push_str(&format!(
                " AND xmax >= {w} AND xmin <= {e} AND ymax >= {s} AND ymin <= {n}"
            ));
        }
    }
    base
}
