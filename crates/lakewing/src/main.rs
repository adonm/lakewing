//! lakewing (rust): poem OGC API over a catalog-resolved, tag-pinned Lance
//! dataset on S3, DuckDB for exact predicates/rendering, and a foyer NVMe
//! range cache under Lance's object store. See docs/rust-architecture.md.
mod api;
mod app;
mod cache;
mod catalog;
mod duck;
mod http_cache;
mod lake;
mod metrics;
mod tiles;

use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let mut catalog_root: Option<String> = None;
    let mut table = "features".to_string();
    let mut uri: Option<String> = None;
    let mut endpoint: Option<String> = None;
    let mut s3_key: Option<String> = None;
    let mut s3_secret: Option<String> = None;
    let mut tag: Option<String> = None;
    let mut version: Option<u64> = None;
    let mut listen = "127.0.0.1:3000".to_string();
    let mut cache_dir: Option<String> = None;
    let mut cache_bytes: usize = 512 * 1024 * 1024;
    while let Some(flag) = args.next() {
        let mut value = || args.next().expect("flag needs a value");
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
            "--cache-dir" => cache_dir = Some(value()),
            "--cache-bytes" => cache_bytes = value().parse().unwrap_or(cache_bytes),
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
    let cache = match &cache_dir {
        Some(dir) => Some(Arc::new(cache::CachingStore::new(
            // Placeholder inner store: lance replaces it through the
            // WrappingObjectStore hook when it constructs the real store.
            Arc::new(object_store::memory::InMemory::new()),
            cache::build_cache(dir, cache_bytes).await?,
            "lakewing-cache",
        ))),
        None => None,
    };
    let app = Arc::new(match (catalog_root, uri) {
        (Some(root), _) => {
            app::App::open_via_catalog(&root, &table, tag, version, storage_options, cache).await?
        }
        (None, Some(uri)) => {
            tracing::warn!("direct --uri open bypasses the catalog; prefer --catalog-uri");
            app::App::open_at(uri, tag, version, storage_options, cache).await?
        }
        (None, None) => anyhow::bail!("--catalog-uri (or dev --uri) is required"),
    });
    tracing::info!(
        version = app.lance.version,
        collections = app.collections.join(","),
        listen = %listen,
        "serving lance dataset"
    );
    api::serve(app, &listen).await
}
