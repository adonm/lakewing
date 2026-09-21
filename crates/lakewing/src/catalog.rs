//! Catalog of record: a Lance Namespace over the storage root
//! (`DirectoryNamespace`, V1 layout: `<table>.lance` directories — the
//! same shape the S3 lake uses). Table discovery and location resolution
//! go through the namespace API, never ad-hoc prefix listing; snapshot
//! pinning stays on the dataset itself via tags/versions (lake.rs).
use std::sync::Arc;

use lance_namespace::models::{DescribeTableRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;

pub struct Catalog {
    ns: Arc<dyn LanceNamespace + Send + Sync + 'static>,
}

impl Catalog {
    /// Open a directory namespace rooted at `root` (local path or
    /// `s3://bucket/prefix`), applying object-store options (endpoint,
    /// allow_http, skip_signature, region, ...).
    pub async fn open(
        root: &str,
        storage_options: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let mut builder = lance_namespace_impls::dir::DirectoryNamespaceBuilder::new(root);
        if !storage_options.is_empty() {
            builder = builder.storage_options(storage_options.clone());
        }
        let ns = builder
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("directory namespace: {e}"))?;
        Ok(Self { ns: Arc::new(ns) })
    }

    /// Resolve a table name to its dataset location via DescribeTable —
    /// the proper namespace round-trip (existence + location).
    pub async fn resolve(&self, table: &str) -> anyhow::Result<String> {
        let request = DescribeTableRequest {
            id: Some(vec![table.to_string()]),
            with_table_uri: Some(true),
            ..Default::default()
        };
        let result = self
            .ns
            .describe_table(request)
            .await
            .map_err(|e| anyhow::anyhow!("describe table {table}: {e}"))?;
        let location = result
            .table_uri
            .or(result.location)
            .ok_or_else(|| anyhow::anyhow!("table {table} has no location"))?;
        Ok(location)
    }

    /// List table names in the root namespace.
    #[allow(dead_code)]
    pub async fn tables(&self) -> anyhow::Result<Vec<String>> {
        let result = self
            .ns
            .list_tables(ListTablesRequest::default())
            .await
            .map_err(|e| anyhow::anyhow!("list tables: {e}"))?;
        Ok(result.tables)
    }
}
