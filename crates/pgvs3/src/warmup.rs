use anyhow::Result;
use tokio_postgres::types::Type;

use crate::db::Pool;

/// Warm a small chunk index at startup without evicting the data working set.
pub async fn prewarm_index(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let _ = conn
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
        .await;
    let mut idx: Vec<(String, i64)> = Vec::new();
    for r in conn
        .query_typed(
            "SELECT i.indexrelid::regclass::text, pg_relation_size(i.indexrelid) \
             FROM pg_partition_tree('s3p.chunks') p JOIN pg_index i ON i.indrelid = p.relid \
             WHERE p.isleaf AND i.indisprimary",
            &[],
        )
        .await?
    {
        idx.push((r.try_get(0)?, r.try_get(1)?));
    }
    let budget: i64 = conn
        .query_typed_one(
            "SELECT setting::int8 * 8192 / 10 FROM pg_settings WHERE name = 'shared_buffers'",
            &[],
        )
        .await?
        .try_get(0)?;
    if idx.iter().map(|(_, bytes)| bytes).sum::<i64>() > budget {
        return Ok(None);
    }
    let mut blocks = 0;
    for (name, _) in &idx {
        blocks += conn
            .query_typed_one("SELECT pg_prewarm($1::regclass)", &[(name, Type::TEXT)])
            .await?
            .try_get::<_, i64>(0)?;
    }
    Ok(Some(blocks))
}
