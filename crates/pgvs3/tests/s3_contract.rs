//! S3 contract against the running kind deployment. Run with `just kind-contract`;
//! ordinary `cargo test` builds this test but needs no network or database.

use anyhow::{ensure, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio_postgres::types::Type;

fn bucket_request(endpoint: &str, method: &str, bucket: &str) -> Result<(u16, String)> {
    let mut cmd = std::process::Command::new("curl");
    cmd.args([
        "-sS",
        "--aws-sigv4",
        "aws:amz:us-east-1:s3",
        "--user",
        "cachebench:cachebench-local-only",
        "-X",
        method,
        "-w",
        "\n%{http_code}",
    ]);
    if method == "HEAD" {
        cmd.arg("--head");
    }
    let output = cmd.arg(format!("{endpoint}/{bucket}")).output()?;
    ensure!(output.status.success(), "curl failed: {:?}", output.stderr);
    let text = String::from_utf8(output.stdout)?;
    let (body, code) = text
        .rsplit_once('\n')
        .ok_or_else(|| anyhow::anyhow!("no HTTP status"))?;
    Ok((code.parse()?, body.to_owned()))
}

fn store(endpoint: &str, bucket: &str) -> Result<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_access_key_id("cachebench")
        .with_secret_access_key("cachebench-local-only")
        .with_allow_http(true)
        .build()?)
}

fn prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after 1970")
        .as_nanos();
    format!("contract/{}-{nanos}", std::process::id())
}

fn endpoints() -> Result<(AmazonS3, AmazonS3)> {
    let bucket = "pgvs3-contract";
    Ok((
        store(&std::env::var("PGVS3_TEST_ENDPOINT_A")?, bucket)?,
        store(&std::env::var("PGVS3_TEST_ENDPOINT_B")?, bucket)?,
    ))
}

async fn pool() -> Result<pgvs3::db::Pool> {
    let url = std::env::var("PGVS3_TEST_DB_URL")?;
    pgvs3::db::connect(&url).await
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn bucket_lifecycle_is_visible_on_both_gateways() -> Result<()> {
    let a = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let b = std::env::var("PGVS3_TEST_ENDPOINT_B")?;
    let bucket = format!(
        "pgvs3-{:x}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let store = store(&b, &bucket)?;
    let path = Path::from("contract/bucket-lifecycle");
    let result: Result<()> = async {
        ensure!(bucket_request(&a, "HEAD", &bucket)?.0 == 404);
        ensure!(bucket_request(&a, "PUT", &bucket)?.0 == 200);
        ensure!(bucket_request(&b, "HEAD", &bucket)?.0 == 200);
        let (status, list) = bucket_request(&b, "GET", "")?;
        ensure!(status == 200 && list.contains(&format!("<Name>{bucket}</Name>")));
        ensure!(list.contains("<CreationDate>"));
        ensure!(bucket_request(&b, "PUT", &bucket)?.0 == 200);

        store
            .put(&path, Bytes::from_static(b"bucket-test").into())
            .await?;
        let (status, body) = bucket_request(&a, "DELETE", &bucket)?;
        ensure!(status == 409 && body.contains("BucketNotEmpty"));
        store.delete(&path).await?;

        let mut upload = store.put_multipart(&path).await?;
        let result: Result<()> = async {
            let conn = pool().await?.get().await?;
            let upload_id: String = conn
                .query_typed_one(
                    "SELECT upload_id FROM s3p.uploads WHERE bucket = $1 AND key = $2",
                    &[
                        (&bucket, Type::TEXT),
                        (&"contract/bucket-lifecycle", Type::TEXT),
                    ],
                )
                .await?
                .try_get(0)?;
            let (status, body) = bucket_request(
                &a,
                "DELETE",
                &format!("{bucket}/wrong-key?uploadId={upload_id}"),
            )?;
            ensure!(status == 404 && body.contains("NoSuchUpload"));
            let (status, body) = bucket_request(&a, "DELETE", &bucket)?;
            ensure!(status == 409 && body.contains("BucketNotEmpty"));
            Ok(())
        }
        .await;
        let aborted = upload.abort().await;
        result?;
        aborted?;

        ensure!(bucket_request(&a, "DELETE", &bucket)?.0 == 204);
        ensure!(bucket_request(&b, "HEAD", &bucket)?.0 == 404);
        let (_, list) = bucket_request(&b, "GET", "")?;
        ensure!(!list.contains(&format!("<Name>{bucket}</Name>")));
        let (status, body) = bucket_request(&a, "GET", &format!("{bucket}?list-type=2"))?;
        ensure!(status == 404 && body.contains("NoSuchBucket"));
        let (status, body) = bucket_request(&b, "PUT", &format!("{bucket}/missing"))?;
        ensure!(status == 404 && body.contains("NoSuchBucket"));
        Ok(())
    }
    .await;

    let _ = store.delete(&path).await;
    let _ = bucket_request(&a, "DELETE", &bucket);
    result
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn clean_failed_prior_contract_objects() -> Result<()> {
    let (store, _) = endpoints()?;
    let objects = store
        .list(Some(&Path::from("contract")))
        .try_collect::<Vec<_>>()
        .await?;
    for object in objects {
        store.delete(&object.location).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn database_maintenance_is_automatic() -> Result<()> {
    let pool = pool().await?;
    let conn = pool.get().await?;
    let cron_jobs: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM cron.job \
             WHERE jobname = 'pgvs3-expire-uploads' AND database = current_database() AND active",
            &[],
        )
        .await?
        .try_get(0)?;
    ensure!(
        cron_jobs == 1,
        "multipart expiry is not scheduled in PostgreSQL"
    );
    let configured_partitions: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM pg_partition_tree('s3p.chunks') p \
             JOIN pg_class c ON c.oid = p.relid \
             WHERE p.isleaf AND c.reloptions @> \
               ARRAY['autovacuum_vacuum_scale_factor=0.01', \
                     'autovacuum_analyze_scale_factor=0.02', \
                     'autovacuum_vacuum_threshold=1000']",
            &[],
        )
        .await?
        .try_get(0)?;
    ensure!(
        configured_partitions == 32,
        "chunk partitions lack autovacuum tuning"
    );
    let upload_age_index: bool = conn
        .query_typed_one("SELECT to_regclass('s3p.uploads_by_age') IS NOT NULL", &[])
        .await?
        .try_get(0)?;
    ensure!(upload_age_index, "multipart expiry lacks its age index");
    Ok(())
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn overwrite_and_delete_are_visible_on_both_gateways() -> Result<()> {
    let (a, b) = endpoints()?;
    let key = format!("{}/object", prefix());
    let path = Path::from(key.clone());
    let old = Bytes::from(vec![0x41; 8120 * 2 + 23]);
    let new = Bytes::from(vec![0x42; 8120 * 3 + 31]);

    let result: Result<()> = async {
        a.put(&path, old.clone().into()).await?;
        let first = b.head(&path).await?;
        ensure!(
            first.size == old.len() as u64,
            "initial HEAD returned wrong size"
        );
        ensure!(
            b.get_range(&path, 8117..8140).await?.as_ref() == &old[8117..8140],
            "cross-row range disagrees with PUT"
        );

        // Gateway B has cached the old file ID. An overwrite through A must
        // not leave a stale HEAD or return old bytes/416 on a new range.
        a.put(&path, new.clone().into()).await?;
        let second = b.head(&path).await?;
        ensure!(second.size == new.len() as u64, "HEAD kept the old size");
        ensure!(second.e_tag != first.e_tag, "HEAD kept the old ETag");
        let off = old.len() + 1;
        ensure!(
            b.get_range(&path, off as u64..(off + 17) as u64)
                .await?
                .as_ref()
                == &new[off..off + 17],
            "GET range past the old EOF disagrees with PUT"
        );

        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let file_id: i64 = row.try_get(0)?;
        a.delete(&path).await?;
        ensure!(
            matches!(
                b.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "HEAD still exposes a deleted object"
        );
        ensure!(
            a.list(Some(&Path::from(key.clone())))
                .try_collect::<Vec<_>>()
                .await?
                .is_empty(),
            "LIST still exposes a deleted object"
        );
        let count: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        ensure!(count == 0, "DELETE left chunk rows for the object");
        Ok(())
    }
    .await;

    // Even when an assertion fails, leave no published test object behind.
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn multipart_completion_and_abort_manage_staged_rows() -> Result<()> {
    let (a, b) = endpoints()?;
    let key = format!("{}/multipart", prefix());
    let path = Path::from(key.clone());
    let part1 = Bytes::from(vec![0x33; 5 * 1024 * 1024 + 1]);
    let part2 = Bytes::from(vec![0x44; 8120 * 2 + 7]);

    let result: Result<()> = async {
        let mut upload = a.put_multipart(&path).await?;
        let first = upload.put_part(part1.clone().into());
        let second = upload.put_part(part2.clone().into());
        futures::future::try_join(first, second).await?;
        ensure!(
            matches!(
                b.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "multipart parts appeared before Complete"
        );
        upload.complete().await?;
        let meta = b.head(&path).await?;
        ensure!(meta.size == (part1.len() + part2.len()) as u64);
        let off = part1.len() - 10;
        let got = b.get_range(&path, off as u64..(off + 20) as u64).await?;
        ensure!(got[..10] == part1[off..] && got[10..] == part2[..10]);

        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT parts FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let parts: Vec<i64> = row.try_get(0)?;
        ensure!(
            parts.len() == 2,
            "multipart object does not reference both parts"
        );
        a.delete(&path).await?;
        for id in parts {
            let count: i64 = conn
                .query_typed_one(
                    "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                    &[(&id, Type::INT8)],
                )
                .await?
                .try_get(0)?;
            ensure!(count == 0, "DELETE left chunk rows for a multipart part");
        }

        let mut abandoned = a.put_multipart(&path).await?;
        abandoned.put_part(part1.into()).await?;
        let row = conn
            .query_typed_one(
                "SELECT p.file_id FROM s3p.uploads u JOIN s3p.upload_parts p USING (upload_id) \
                 WHERE u.bucket = $1 AND u.key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let staged_id: i64 = row.try_get(0)?;
        abandoned.abort().await?;
        let count: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&staged_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        ensure!(count == 0, "Abort left staged chunk rows");
        Ok(())
    }
    .await;

    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn expired_upload_cleanup_is_bounded_and_s3_visible() -> Result<()> {
    let (store, other_gateway) = endpoints()?;
    let key = format!("{}/expired", prefix());
    let path = Path::from(key.clone());
    let mut upload = store.put_multipart(&path).await?;

    let result: Result<()> = async {
        upload
            .put_part(Bytes::from(vec![0x45; 5 * 1024 * 1024 + 17]).into())
            .await?;
        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT u.upload_id, p.file_id FROM s3p.uploads u \
                 JOIN s3p.upload_parts p USING (upload_id) \
                 WHERE u.bucket = $1 AND u.key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let upload_id: String = row.try_get(0)?;
        let file_id: i64 = row.try_get(1)?;
        let original_rows: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        ensure!(
            original_rows > 32,
            "the test needs more than one cleanup batch"
        );

        let mut completed = false;
        for _ in 0..40 {
            let removed: i32 = conn
                .query_typed_one(
                    "SELECT s3p.expire_uploads(interval '0 seconds', $1, $2)",
                    &[(&32i32, Type::INT4), (&"pgvs3-contract", Type::TEXT)],
                )
                .await?
                .try_get(0)?;
            ensure!(
                (0..=32).contains(&removed),
                "cleanup exceeded its row budget"
            );
            let remaining: i64 = conn
                .query_typed_one(
                    "SELECT count(*) FROM s3p.uploads WHERE upload_id = $1",
                    &[(&upload_id, Type::TEXT)],
                )
                .await?
                .try_get(0)?;
            if remaining == 0 {
                completed = true;
                break;
            }
        }
        ensure!(completed, "the abandoned upload never expired");
        let chunks: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        ensure!(chunks == 0, "expired upload left hidden chunk rows");
        ensure!(
            matches!(
                other_gateway.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "uncompleted upload became visible through S3"
        );
        Ok(())
    }
    .await;

    let _ = upload.abort().await;
    result
}

#[tokio::test]
#[ignore = "requires kind; run just kind-contract"]
async fn scheduled_expiry_removes_old_empty_uploads() -> Result<()> {
    let (store, _) = endpoints()?;
    let key = format!("{}/cron-expiry", prefix());
    let path = Path::from(key.clone());
    let mut upload = store.put_multipart(&path).await?;
    let result: Result<()> = async {
        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "UPDATE s3p.uploads SET created_at = now() - interval '25 hours' \
                 WHERE bucket = $1 AND key = $2 RETURNING upload_id",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let upload_id: String = row.try_get(0)?;
        for _ in 0..45 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let exists = conn
                .query_typed_opt(
                    "SELECT 1 FROM s3p.uploads WHERE upload_id = $1",
                    &[(&upload_id, Type::TEXT)],
                )
                .await?
                .is_some();
            if !exists {
                return Ok(());
            }
        }
        anyhow::bail!("pg_cron did not expire the old empty upload")
    }
    .await;
    let _ = upload.abort().await;
    result
}
