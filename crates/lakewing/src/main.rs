//! lakewing (rust): poem OGC API over a catalog-resolved, tag-pinned Lance
//! dataset on S3, DuckDB for exact predicates/rendering, and a foyer NVMe
//! range cache under Lance's object store. `lakewing build` materializes
//! indexed GeoArrow datasets from WKB parquet sources. See
//! docs/rust-architecture.md.
mod api;
mod app;
mod build;
mod cache;
mod catalog;
mod duck;
mod flight;
mod http_cache;
mod lake;
mod metrics;
mod otel;
mod query;
mod response_cache;
#[cfg(test)]
mod tests;
mod tiles;

use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("build") {
        return run_build(args[1..].to_vec()).await;
    }
    serve_main(args.into_iter()).await
}

async fn serve_main(mut args: std::vec::IntoIter<String>) -> anyhow::Result<()> {
    let mut catalog_root: Option<String> = None;
    let mut table = "features".to_string();
    let mut uri: Option<String> = None;
    let mut endpoint: Option<String> = None;
    let mut s3_key: Option<String> = None;
    let mut s3_secret: Option<String> = None;
    let mut tag: Option<String> = None;
    let mut version: Option<u64> = None;
    let mut listen = "127.0.0.1:3000".to_string();
    let mut flight_listen: Option<String> = None;
    let mut otel_endpoint: Option<String> = None;
    let mut cache_dir: Option<String> = None;
    let mut cache_bytes: usize = 512 * 1024 * 1024;
    let mut cache_block_bytes: u64 = 256 * 1024;
    let mut concurrency: usize = 4;
    let mut duck_threads: usize = 1;
    let mut duck_memory_mb: usize = 512;
    // Response caching is opt-in: the default posture optimizes the serving
    // path over cached *data* (block-aligned foyer ranges under Lance).
    let mut response_cache_bytes: usize = 0;
    while let Some(arg) = args.next() {
        // Accept both `--flag value` and `--flag=value`.
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        let value = || {
            inline
                .or_else(|| args.next())
                .unwrap_or_else(|| panic!("flag {flag} needs a value"))
        };
        match flag.as_str() {
            "--catalog-uri" => catalog_root = Some(value()),
            "--table" => table = value(),
            "--uri" => uri = Some(value()),
            "--endpoint" => endpoint = Some(value()),
            "--s3-key" => s3_key = Some(value()),
            "--s3-secret" => s3_secret = Some(value()),
            "--tag" => tag = Some(value()),
            "--version" => version = value().parse().ok(),
            "--listen" => listen = value(),
            "--flight-listen" => flight_listen = Some(value()),
            "--otel-endpoint" => otel_endpoint = Some(value()),
            "--cache-dir" => cache_dir = Some(value()),
            "--cache-bytes" => cache_bytes = value().parse().unwrap_or(cache_bytes),
            "--cache-block-bytes" => {
                cache_block_bytes = value().parse().unwrap_or(cache_block_bytes)
            }
            "--concurrency" => concurrency = value().parse().unwrap_or(concurrency),
            "--duck-threads" => duck_threads = value().parse().unwrap_or(duck_threads),
            "--duck-memory-mb" => duck_memory_mb = value().parse().unwrap_or(duck_memory_mb),
            "--response-cache-bytes" => {
                response_cache_bytes = value().parse().unwrap_or(response_cache_bytes)
            }
            other => anyhow::bail!("unknown flag {other}"),
        }
    }
    let mut storage_options: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    if let Some(endpoint) = &endpoint {
        // Endpoint for the node-local cache/S3 gateway or direct S3.
        storage_options.insert("endpoint".into(), endpoint.clone());
        storage_options.insert("allow_http".into(), "true".into());
        storage_options.insert("virtual_hosted_style_request".into(), "false".into());
        storage_options.insert("region".into(), "us-east-1".into());
        match (&s3_key, &s3_secret) {
            (Some(key), Some(secret)) => {
                storage_options.insert("aws_access_key_id".into(), key.clone());
                storage_options.insert("aws_secret_access_key".into(), secret.clone());
            }
            _ => {
                storage_options.insert("skip_signature".into(), "true".into());
            }
        }
    }
    let _telemetry = otel::init(otel_endpoint.as_deref())?;
    let cache = match &cache_dir {
        Some(dir) => Some(Arc::new(
            cache::CachingStore::open(
                dir,
                cache_bytes,
                64 * 1024 * 1024,
                cache_block_bytes,
                endpoint.as_deref().unwrap_or("default-endpoint"),
            )
            .await?,
        )),
        None => None,
    };
    let cache_handle = cache.clone();
    let app = Arc::new(match (catalog_root, uri) {
        (Some(root), _) => {
            app::App::open_via_catalog(
                &root,
                &table,
                tag,
                version,
                storage_options,
                cache,
                app::Limits {
                    concurrency,
                    duck_threads,
                    duck_memory_mb,
                    response_cache_bytes,
                },
            )
            .await?
        }
        (None, Some(uri)) => {
            tracing::warn!("direct --uri open bypasses the catalog; prefer --catalog-uri");
            app::App::open_at(
                uri,
                tag,
                version,
                storage_options,
                cache,
                app::Limits {
                    concurrency,
                    duck_threads,
                    duck_memory_mb,
                    response_cache_bytes,
                },
            )
            .await?
        }
        (None, None) => anyhow::bail!("--catalog-uri (or dev --uri) is required"),
    });
    tracing::info!(
        version = app.lance.version,
        collections = app.collections.join(","),
        listen = %listen,
        flight = ?flight_listen,
        "serving lance dataset"
    );
    if let Some(addr) = flight_listen {
        let flight_app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = flight::serve(flight_app, &addr).await {
                tracing::error!(%e, "flight server failed");
            }
        });
    }
    let result = api::serve(app, &listen).await;
    // Flush the disk tier before exit so restarts reuse warm bytes.
    if let Some(cache) = cache_handle {
        let _ = cache.close().await;
    }
    result
}

async fn run_build(args: Vec<String>) -> anyhow::Result<()> {
    let _telemetry = otel::init(None)?;
    let mut source = String::new();
    let mut out = String::new();
    let mut tag = "prod".to_string();
    let mut max_rows_per_file = 10_000_000usize;
    let mut max_bytes_per_file = 512 * 1024 * 1024usize;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        let mut value = || args.next().expect("flag needs a value");
        match flag.as_str() {
            "--source" => source = value(),
            "--out" => out = value(),
            "--tag" => tag = value(),
            "--max-rows-per-file" => {
                max_rows_per_file = value().parse().unwrap_or(max_rows_per_file)
            }
            "--max-bytes-per-file" => {
                max_bytes_per_file = value().parse().unwrap_or(max_bytes_per_file)
            }
            other => anyhow::bail!("unknown flag {other}"),
        }
    }
    if source.is_empty() || out.is_empty() {
        anyhow::bail!("build needs --source and --out");
    }
    build::build(build::BuildConfig {
        source,
        out,
        tag,
        max_rows_per_file,
        max_bytes_per_file,
    })
    .await
}
