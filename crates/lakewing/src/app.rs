//! Application core: catalog-resolved dataset, one items-page plan executed
//! as lance scan (pushed candidate filter) -> duckdb exact predicate ->
//! order/pagination/render, with the ids-first two-phase shape for deep
//! offsets (port of the proven plan.rs/HeavyItemsSQL semantics from the
//! archived Go serve).
use std::sync::Arc;

use serde_json::{json, Value};

use crate::catalog::Catalog;
use crate::duck::{exact_predicate, id_in_list, pushed_filter, Duck, FeatureRow};
use crate::lake::LanceSource;

pub struct App {
    pub lance: Arc<LanceSource>,
    pub duck: Arc<Duck>,
    pub collections: Vec<String>,
}

impl App {
    /// Open via the catalog of record: `root` is a Lance Namespace root
    /// (directory namespace), `table` resolves to the dataset location.
    pub async fn open_via_catalog(
        root: &str,
        table: &str,
        tag: Option<String>,
        version: Option<u64>,
        cache: Option<Arc<crate::cache::CachingStore>>,
    ) -> anyhow::Result<Self> {
        let catalog = Catalog::open(root).await?;
        let uri = catalog.resolve(table).await?;
        tracing::info!(%root, table, %uri, "catalog resolved table");
        Self::open_at(uri, tag, version, cache).await
    }

    /// Open directly at a dataset URI (development/benchmark bypass of the
    /// catalog; serving should use open_via_catalog).
    pub async fn open_at(
        uri: String,
        tag: Option<String>,
        version: Option<u64>,
        cache: Option<Arc<crate::cache::CachingStore>>,
    ) -> anyhow::Result<Self> {
        let lance = Arc::new(
            LanceSource::open(crate::lake::SourceConfig { uri, tag, version }, cache).await?,
        );
        let duck = Arc::new(Duck::open()?);
        // Collections: distinct layer values over a narrow scan.
        let layer_batches = lance.scan_ids("true").await?;
        let collections = duck.distinct_layers(&layer_batches)?;
        if collections.is_empty() {
            anyhow::bail!("dataset has no layers");
        }
        Ok(Self {
            lance,
            duck,
            collections,
        })
    }

    fn validate_collection(&self, collection: &str) -> anyhow::Result<()> {
        if self.collections.iter().any(|c| c == collection) {
            Ok(())
        } else {
            anyhow::bail!("unknown collection")
        }
    }

    pub async fn items_page(
        &self,
        collection: &str,
        bounds: Option<[f64; 4]>,
        sources: &[i64],
        limit: usize,
        offset: u32,
        cursor: Option<&str>,
    ) -> anyhow::Result<String> {
        self.validate_collection(collection)?;
        let exact = exact_predicate(collection, bounds, sources);
        let pushed = pushed_filter(collection, bounds, sources);

        // Always ids-first: the ordered id window pushes top-N into Lance
        // (order_by + limit), the payload scan is by id IN, and DuckDB
        // only ever sees the page.
        let pushed = match cursor {
            Some(cursor) => format!("{pushed} AND id > {}", crate::duck::quote(cursor)),
            None => pushed,
        };
        let fetch = limit as u64 + 1 + offset as u64;
        let ids = self.lance.scan_ids_window(&pushed, fetch).await?;
        let ids: Vec<String> = ids
            .into_iter()
            .skip(offset as usize)
            .take(limit + 1)
            .collect();
        let rows: Vec<FeatureRow> = if ids.is_empty() {
            Vec::new()
        } else {
            let payload_filter = format!("{pushed} AND id IN ({})", id_in_list(&ids));
            let batches = self.lance.scan_page(&payload_filter, None, None).await?;
            self.duck.render_page(&batches, &exact, None, 0)?
        };

        let has_next = rows.len() > limit;
        let rows: Vec<_> = rows.into_iter().take(limit).collect();
        let features: Vec<Value> = rows
            .iter()
            .map(|r| render_feature(collection, &r.id, &r.geom_json, &r.properties))
            .collect();
        let mut links = vec![
            json!({"rel": "self", "href": self_href(collection, bounds, limit, offset, cursor), "type": "application/geo+json"}),
            json!({"rel": "collection", "href": format!("/collections/{collection}"), "type": "application/json"}),
        ];
        if has_next {
            if let Some(last) = rows.last() {
                links.push(json!({
                    "rel": "next",
                    "href": format!(
                        "/collections/{}/items?{bbox}cursor={}&limit={}&offset=0&sources=1",
                        collection, urlencode(&last.id), limit,
                        bbox = bounds
                            .map(|[w, s, e, n]| format!("bbox={w},{s},{e},{n}&"))
                            .unwrap_or_default()
                    ),
                    "type": "application/geo+json"
                }));
            }
        }
        Ok(serde_json::to_string(&json!({
            "type": "FeatureCollection",
            "numberReturned": features.len(),
            "features": features,
            "links": links,
        }))?)
    }

    pub async fn item(
        &self,
        collection: &str,
        feature_id: &str,
        sources: &[i64],
    ) -> anyhow::Result<Option<String>> {
        self.validate_collection(collection)?;
        let pushed = format!(
            "{} AND id = {}",
            pushed_filter(collection, None, sources),
            crate::duck::quote(feature_id)
        );
        let batches = self.lance.scan_page(&pushed, Some(2), None).await?;
        let rows = self.duck.render_page(&batches, "TRUE", Some(1), 0)?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(serde_json::to_string(&render_feature(
                collection,
                &row.id,
                &row.geom_json,
                &row.properties,
            ))?)),
            None => Ok(None),
        }
    }
}

fn render_feature(collection: &str, id: &str, geom_json: &str, properties: &str) -> Value {
    let geometry: Value = if geom_json.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(geom_json).unwrap_or(Value::Null)
    };
    let props: Value = if properties.is_empty() {
        json!({})
    } else {
        serde_json::from_str(properties).unwrap_or(json!({}))
    };
    json!({
        "type": "Feature",
        "id": id,
        "geometry": geometry,
        "properties": props,
        "links": [
            {"rel": "self", "href": format!("/collections/{collection}/items/{id}?sources=1"), "type": "application/geo+json"},
            {"rel": "collection", "href": format!("/collections/{collection}"), "type": "application/json"}
        ]
    })
}

fn self_href(
    collection: &str,
    bounds: Option<[f64; 4]>,
    limit: usize,
    offset: u32,
    cursor: Option<&str>,
) -> String {
    let bbox = bounds
        .map(|[w, s, e, n]| format!("bbox={w},{s},{e},{n}&"))
        .unwrap_or_default();
    match cursor {
        Some(c) => format!(
            "/collections/{}/items?{bbox}cursor={}&limit={}&offset={}&sources=1",
            collection,
            urlencode(c),
            limit,
            offset
        ),
        None => format!(
            "/collections/{}/items?{bbox}limit={}&offset={}&sources=1",
            collection, limit, offset
        ),
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
