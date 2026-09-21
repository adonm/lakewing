//! Bounded, connection-local DuckDB work over Arrow page batches.
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use tokio::sync::Semaphore;

pub struct Duck {
    connections: Arc<Mutex<Vec<Connection>>>,
    permits: Arc<Semaphore>,
}

pub struct FeatureRow {
    pub id: String,
    pub source_id: i64,
    pub geom_json: String,
    pub properties: String,
}

pub const GEOMETRY_SQL: &str = "CASE WHEN was_polygon THEN (ST_Dump(ST_GeomFromWKB(geom)))[1].geom ELSE ST_GeomFromWKB(geom) END";

impl Duck {
    pub fn open(workers: usize, threads: usize, memory_mb: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            workers > 0 && threads > 0 && memory_mb > 0,
            "DuckDB budgets must be positive"
        );
        let first = Connection::open_in_memory()?;
        first.execute_batch(&format!(
            "INSTALL spatial; LOAD spatial; SET threads={threads}; SET memory_limit='{memory_mb}MB';"
        ))?;
        let mut connections = Vec::with_capacity(workers);
        for _ in 1..workers {
            connections.push(first.try_clone()?);
        }
        connections.push(first);
        for conn in &connections {
            conn.execute_batch(
                "CREATE TEMP TABLE lw_page(
                id VARCHAR, geom BLOB, properties VARCHAR, layer VARCHAR, source_id BIGINT,
                xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, was_polygon BOOLEAN)",
            )?;
        }
        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
            permits: Arc::new(Semaphore::new(workers)),
        })
    }

    async fn run<T, F>(&self, work: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> anyhow::Result<T> + Send + 'static,
    {
        let permit = self.permits.clone().acquire_owned().await?;
        let connections = self.connections.clone();
        let conn = connections
            .lock()
            .unwrap()
            .pop()
            .expect("connection permit");
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            let result = work(&conn);
            connections.lock().unwrap().push(conn);
            // A cancelled caller cannot release this worker while native SQL is running.
            drop(permit);
            result
        })
        .await?
    }

    /// Load one page of candidate batches into this connection's temp table.
    ///
    /// Memory contract: this is a *push* pipeline — each RecordBatch is
    /// converted to DuckDB data chunks (vector-size slices) and appended
    /// immediately (duckdb `appender-arrow`); nothing registers a reader or
    /// virtual table, and DuckDB never pulls. The caller (lake::scan_page)
    /// bounds the total batch set to `MAX_PAGE_BYTES` and the selected key
    /// window, so `lw_page` can never hold a page proportional to the
    /// collection.
    fn load_page(conn: &Connection, batches: &[RecordBatch]) -> anyhow::Result<()> {
        conn.execute_batch("DELETE FROM lw_page")?;
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
        ];
        let mut appender = conn.appender("lw_page")?;
        for batch in batches {
            let schema = batch.schema();
            let mut fields = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();
            for name in names {
                let idx = schema.index_of(name)?;
                fields.push(schema.field(idx).clone());
                columns.push(batch.column(idx).clone());
            }
            fields.push(Field::new("was_polygon", DataType::Boolean, true));
            columns.push(match schema.index_of("was_polygon") {
                Ok(idx) => batch.column(idx).clone(),
                Err(_) => Arc::new(BooleanArray::from(vec![false; batch.num_rows()])),
            });
            appender.append_record_batch(RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                columns,
            )?)?;
        }
        appender.flush()?;
        Ok(())
    }

    #[tracing::instrument(name = "duck.render", skip_all, fields(rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>()))]
    pub async fn render_page(&self, batches: Vec<RecordBatch>) -> anyhow::Result<Vec<FeatureRow>> {
        self.run(move |conn| {
            Self::load_page(conn, &batches)?;
            let sql = format!("SELECT id, source_id, ST_AsGeoJSON({GEOMETRY_SQL}), properties FROM lw_page ORDER BY id, source_id");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| Ok(FeatureRow {
                id: row.get(0)?,
                source_id: row.get(1)?,
                geom_json: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                properties: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            }))?;
            Ok(rows.collect::<Result<_, _>>()?)
        }).await
    }

    #[tracing::instrument(name = "duck.tile", skip_all)]
    pub async fn mvt(
        &self,
        batches: Vec<RecordBatch>,
        sql: String,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        self.run(move |conn| {
            Self::load_page(conn, &batches)?;
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            match rows.next()? {
                Some(row) => Ok(row.get(0)?),
                None => Ok(None),
            }
        })
        .await
    }
}

pub fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn key_filter(keys: &[crate::query::RowKey]) -> String {
    if keys.is_empty() {
        return "false".into();
    }
    let mut sources = std::collections::BTreeMap::<i64, Vec<String>>::new();
    for key in keys {
        sources
            .entry(key.source_id)
            .or_default()
            .push(quote(&key.id));
    }
    format!(
        "({})",
        sources
            .into_iter()
            .map(|(source, ids)| format!("(source_id = {source} AND id IN ({}))", ids.join(",")))
            .collect::<Vec<_>>()
            .join(" OR ")
    )
}

/// Exact selection precedes ordering and pagination for both storage representations.
pub fn pushed_filter(
    collection: &str,
    bounds: Option<[f64; 4]>,
    sources: &[i64],
    geo: bool,
) -> String {
    if sources.is_empty() {
        return "false".into();
    }
    let sources = sources
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut filter = format!("layer = {} AND source_id IN ({sources})", quote(collection));
    if let Some([w, s, e, n]) = bounds {
        let geom = if geo { "geom" } else { "ST_GeomFromWKB(geom)" };
        if !geo {
            filter.push_str(&format!(
                " AND xmax >= {w} AND xmin <= {e} AND ymax >= {s} AND ymin <= {n}"
            ));
        }
        filter.push_str(&format!(" AND ST_Intersects({geom}, ST_GeomFromText('POLYGON (({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))'))"));
    }
    filter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_native_work_keeps_its_worker_permit() {
        let duck = Arc::new(Duck::open(1, 1, 128).unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = duck.clone();
        let task = tokio::spawn(async move {
            worker
                .run(move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), duck.run(|_| Ok(())))
                .await
                .is_err()
        );
        release_tx.send(()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            duck.run(|conn| Ok(conn.query_row("SELECT 42", [], |row| row.get::<_, i64>(0))?)),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
