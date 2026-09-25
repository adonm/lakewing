//! Housekeeping: reap state that interrupted writes leave behind (orphaned
//! chunk files, abandoned multipart uploads) and warm what a cold start
//! should not have to re-read. Idempotent: any gateway can run any of it.

use std::time::Duration;

use anyhow::Result;
use tokio_postgres::types::Type;

use crate::db::{abort_upload, Pool};

/// Unreferenced chunk files of one partition with ids in [$1, $2): a loose
/// index scan (one probe of that partition's primary key per distinct file,
/// not per row; per partition because a scan of the parent would probe all
/// 32 per step) anti-joined, all by index, against everything that can own a
/// file: published objects (single file, or parts via GIN) and upload parts.
fn orphans_sql(partition: &str) -> String {
    format!(
        "WITH RECURSIVE f(file_id) AS ( \
             SELECT min(file_id) FROM {partition} WHERE file_id >= $1 AND file_id < $2 \
           UNION ALL \
             SELECT (SELECT min(c.file_id) FROM {partition} c WHERE c.file_id > f.file_id AND c.file_id < $2) \
             FROM f WHERE f.file_id IS NOT NULL \
         ) \
         SELECT f.file_id FROM f \
         WHERE f.file_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = f.file_id) \
           AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.parts @> ARRAY[f.file_id]) \
           AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = f.file_id)"
    )
}

/// Reap chunk files nothing references once provably older than `grace`: a
/// PUT whose publish never ran (its rows commit before the object row), or a
/// multipart flush cut short by a killed gateway. file_ids come from one
/// sequence (cache 1), so every id below the newest object published before
/// now-grace was allocated before then: no per-row timestamp needed. Scans
/// ids in [from, horizon); returns (files, rows, horizon) so the caller
/// resumes there. Interrupted writes of every other kind are single
/// transactions and leave nothing behind.
pub async fn sweep_orphans(pool: &Pool, grace: Duration, from: i64) -> Result<(usize, u64, i64)> {
    let mut conn = pool.get().await?;
    let horizon: Option<i64> = conn
        .query_typed_one(
            "SELECT max(file_id) FROM s3p.objects WHERE created_at < now() - make_interval(secs => $1)",
            &[(&grace.as_secs_f64(), Type::FLOAT8)],
        )
        .await?
        .try_get(0)?;
    let horizon = match horizon {
        Some(h) if h > from => h,
        _ => return Ok((0, 0, from)),
    };
    // Partition names come from the catalog (regclass text, already quoted).
    let mut partitions: Vec<String> = Vec::new();
    for r in conn
        .query_typed(
            "SELECT relid::regclass::text FROM pg_partition_tree('s3p.chunks') WHERE isleaf",
            &[],
        )
        .await?
    {
        partitions.push(r.try_get(0)?);
    }
    let tx = conn.transaction().await?;
    // The loose scan wants index probes; gateway sessions default to bitmaps.
    tx.batch_execute("SET LOCAL enable_indexscan = on").await?;
    let mut orphans: Vec<i64> = Vec::new();
    for partition in &partitions {
        for r in tx
            .query_typed(
                &orphans_sql(partition),
                &[(&from, Type::INT8), (&horizon, Type::INT8)],
            )
            .await?
        {
            orphans.push(r.try_get(0)?);
        }
    }
    tx.commit().await?;
    let mut rows = 0;
    for id in &orphans {
        rows += conn
            .execute("DELETE FROM s3p.chunks WHERE file_id = $1", &[id])
            .await?;
    }
    Ok((orphans.len(), rows, horizon))
}

/// Abort uploads abandoned for longer than `age` (S3's incomplete-upload
/// lifecycle): their parts are committed rows, so something must reap them.
pub async fn expire_uploads(pool: &Pool, age: Duration) -> Result<usize> {
    let mut ids: Vec<String> = Vec::new();
    for r in pool
        .get()
        .await?
        .query_typed(
            "SELECT upload_id FROM s3p.uploads WHERE created_at < now() - make_interval(secs => $1)",
            &[(&age.as_secs_f64(), Type::FLOAT8)],
        )
        .await?
    {
        ids.push(r.try_get(0)?);
    }
    for id in &ids {
        abort_upload(pool, id).await?;
    }
    Ok(ids.len())
}

/// Load the chunk primary-key indexes into shared_buffers when they are small
/// beside it (a cold btree leaf was ~1/3 of a cold small GET on Aurora).
/// Past ~10% of shared_buffers (the index is ~0.4% of the data, so multi-TB)
/// they are left to the buffer manager: prewarming on every gateway start
/// would evict the working set. Returns the blocks loaded, or None if skipped.
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
