//! Poem/Flight serving over pinned Lance snapshots; DuckDB rendering and foyer data caching.
mod api;
mod app;
mod build;
mod cache;
mod catalog;
mod duck;
mod flight;
mod http_cache;
mod indexes;
mod lake;
mod metrics;
mod otel;
mod query;
mod response_cache;
#[cfg(test)]
mod tests;
mod tiles;

use std::sync::Arc;

use clap::Parser;

#[derive(Debug, clap::Args)]
struct StorageArgs {
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long, requires = "s3_secret")]
    s3_key: Option<String>,
    #[arg(long, requires = "s3_key")]
    s3_secret: Option<String>,
}

impl StorageArgs {
    fn options(&self) -> std::collections::HashMap<String, String> {
        let mut options = std::collections::HashMap::new();
        if let Some(endpoint) = &self.endpoint {
            options.insert("endpoint".into(), endpoint.clone());
            options.insert("allow_http".into(), "true".into());
            options.insert("virtual_hosted_style_request".into(), "false".into());
            options.insert("region".into(), "us-east-1".into());
            if self.s3_key.is_none() {
                options.insert("skip_signature".into(), "true".into());
            }
        }
        if let (Some(key), Some(secret)) = (&self.s3_key, &self.s3_secret) {
            options.insert("aws_access_key_id".into(), key.clone());
            options.insert("aws_secret_access_key".into(), secret.clone());
        }
        options
    }
}

#[derive(Parser)]
#[command(
    name = "lakewing",
    about = "Serve a pinned Lance dataset. Also: lakewing build --help; lakewing index --help"
)]
struct ServeArgs {
    #[arg(long, required_unless_present = "uri", conflicts_with = "uri")]
    catalog_uri: Option<String>,
    #[arg(long, default_value = "features")]
    table: String,
    /// Direct dataset open (development bypass of the namespace).
    #[arg(long)]
    uri: Option<String>,
    #[arg(long, required_unless_present = "version", conflicts_with = "version")]
    tag: Option<String>,
    #[arg(long)]
    version: Option<u64>,
    #[arg(long, default_value = "127.0.0.1:3000")]
    listen: String,
    #[arg(long)]
    flight_listen: Option<String>,
    #[arg(long)]
    otel_endpoint: Option<String>,
    #[arg(long)]
    cache_dir: Option<String>,
    #[command(flatten)]
    cache: cache::CacheConfig,
    /// RAM for decoded Lance index pages; 0 disables. Separate from foyer.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    lance_index_cache_bytes: usize,
    /// RAM for Lance file metadata; 0 disables. Separate from foyer HEADs.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    lance_metadata_cache_bytes: usize,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long, default_value_t = 1)]
    duck_threads: usize,
    #[arg(long, default_value_t = 512)]
    duck_memory_mb: usize,
    /// Opt-in rendered-body RAM cache; 0 disables.
    #[arg(long, default_value_t = 0)]
    response_cache_bytes: usize,
    #[command(flatten)]
    storage: StorageArgs,
}

#[derive(Parser)]
#[command(
    name = "lakewing build",
    about = "Build indexed GeoArrow Lance data from WKB parquet"
)]
struct BuildArgs {
    #[arg(long)]
    source: String,
    #[arg(long)]
    out: String,
    #[arg(long, default_value = "prod")]
    tag: String,
    #[arg(long, default_value_t = 10_000_000)]
    max_rows_per_file: usize,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    max_bytes_per_file: usize,
    #[command(flatten)]
    indexes: indexes::IndexConfig,
}

#[derive(Parser)]
#[command(
    name = "lakewing index",
    about = "Install serving indexes on the latest snapshot (single writer); create a new tag after success"
)]
struct IndexArgs {
    #[arg(long)]
    uri: String,
    /// New release tag; existing tags are never moved.
    #[arg(long)]
    tag: String,
    /// Rebuild lakewing's named indexes with the requested parameters.
    #[arg(long)]
    replace: bool,
    #[command(flatten)]
    indexes: indexes::IndexConfig,
    #[command(flatten)]
    storage: StorageArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("build") => {
            args.remove(1);
            let args = BuildArgs::parse_from(args);
            let _telemetry = otel::init(None)?;
            build::build(build::BuildConfig {
                source: args.source,
                out: args.out,
                tag: args.tag,
                max_rows_per_file: args.max_rows_per_file,
                max_bytes_per_file: args.max_bytes_per_file,
                indexes: args.indexes,
            })
            .await
        }
        Some("index") => {
            args.remove(1);
            let args = IndexArgs::parse_from(args);
            args.indexes.validate()?;
            anyhow::ensure!(!args.tag.is_empty(), "index needs a nonempty new tag");
            let _telemetry = otel::init(None)?;
            let mut dataset = lance::dataset::builder::DatasetBuilder::from_uri(&args.uri)
                .with_storage_options(args.storage.options())
                .load()
                .await?;
            anyhow::ensure!(
                !dataset.tags().list().await?.contains_key(&args.tag),
                "tag {} already exists",
                args.tag
            );
            indexes::install(&mut dataset, &args.indexes, args.replace).await?;
            let version = dataset.version().version;
            dataset
                .tags()
                .create(&args.tag, lance::dataset::refs::Ref::VersionNumber(version))
                .await?;
            tracing::info!(tag = args.tag, version, "published indexed snapshot");
            Ok(())
        }
        _ => serve_main(ServeArgs::parse_from(args)).await,
    }
}

async fn serve_main(args: ServeArgs) -> anyhow::Result<()> {
    args.cache.validate()?;
    let _telemetry = otel::init(args.otel_endpoint.as_deref())?;
    let cache = match &args.cache_dir {
        Some(dir) => Some(Arc::new(
            cache::CachingStore::open(
                dir,
                args.cache.clone(),
                args.storage
                    .endpoint
                    .as_deref()
                    .unwrap_or("default-endpoint"),
            )
            .await?,
        )),
        None => None,
    };
    let limits = app::Limits {
        concurrency: args.concurrency,
        duck_threads: args.duck_threads,
        duck_memory_mb: args.duck_memory_mb,
        response_cache_bytes: args.response_cache_bytes,
        lance_index_cache_bytes: args.lance_index_cache_bytes,
        lance_metadata_cache_bytes: args.lance_metadata_cache_bytes,
    };
    let options = args.storage.options();
    let app = Arc::new(match args.catalog_uri {
        Some(root) => {
            app::App::open_via_catalog(
                &root,
                &args.table,
                args.tag,
                args.version,
                options,
                cache.clone(),
                limits,
            )
            .await?
        }
        None => {
            tracing::warn!("direct --uri open bypasses the catalog; prefer --catalog-uri");
            app::App::open_at(
                args.uri.expect("validated uri"),
                args.tag,
                args.version,
                options,
                cache.clone(),
                limits,
            )
            .await?
        }
    });
    tracing::info!(version = app.lance.version, collections = app.collections.join(","), listen = args.listen,
        cache = ?args.cache, lance_index_cache_bytes = args.lance_index_cache_bytes,
        lance_metadata_cache_bytes = args.lance_metadata_cache_bytes, "serving lance dataset");
    if let Some(addr) = args.flight_listen {
        let flight_app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = flight::serve(flight_app, &addr).await {
                tracing::error!(%e, "flight server failed");
            }
        });
    }
    let result = api::serve(app, &args.listen).await;
    if let Some(cache) = cache {
        cache.close().await?;
    }
    result
}
