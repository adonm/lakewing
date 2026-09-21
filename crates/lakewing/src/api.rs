//! Poem HTTP contract over the shared snapshot-bound query path.
use std::sync::Arc;
use std::time::{Duration, Instant};

use poem::http::{header, StatusCode};
use poem::web::{Data, Json, Path, Query};
use poem::{
    get, handler, listener::TcpListener, Endpoint, EndpointExt, Request, Response, Route, Server,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::Instrument;

use crate::app::App;
use crate::http_cache;
use crate::query::{check_snapshot, parse_bbox, parse_sources, QueryError, Selection};

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ItemsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub bbox: Option<String>,
    pub cursor: Option<String>,
    pub sources: Option<String>,
    pub snapshot: Option<u64>,
}

fn sources(query: &ItemsQuery, req: &Request) -> anyhow::Result<Vec<i64>> {
    let header = req
        .headers()
        .get("x-source-ids")
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| QueryError::new(400, "invalid X-Source-Ids"))?;
    match (query.sources.as_deref(), header) {
        (Some(q), Some(h)) => {
            let selected = parse_sources(Some(h))?;
            Ok(parse_sources(Some(q))?
                .into_iter()
                .filter(|s| selected.contains(s))
                .collect())
        }
        (q, h) => parse_sources(q.or(h)),
    }
}

#[handler]
async fn healthz() -> &'static str {
    "ok"
}

#[handler]
async fn metrics_endpoint(app: Data<&Arc<App>>) -> Response {
    let mut body = app.metrics.exposition();
    if let Some(cache) = &app.cache_metrics {
        body.push_str(&cache.exposition());
    }
    Response::builder()
        .content_type("text/plain; version=0.0.4")
        .header(header::CACHE_CONTROL, "no-store")
        .body(body)
}

#[handler]
async fn collections(app: Data<&Arc<App>>) -> Json<Value> {
    Json(
        json!({"collections": app.collections.iter().map(|c| json!({"id": c, "links": [{"rel": "items", "href": format!("/collections/{}/items", crate::query::urlencode(c)), "type": "application/geo+json"}]})).collect::<Vec<_>>()}),
    )
}

#[handler]
async fn collection_detail(Path(name): Path<String>, app: Data<&Arc<App>>) -> Response {
    match app.validate_collection(&name) {
        Ok(()) => Response::builder()
            .content_type("application/json")
            .body(json!({"id": name, "snapshot": app.lance.version}).to_string()),
        Err(err) => error_response(err),
    }
}

#[handler]
async fn items(
    Path(collection): Path<String>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let parsed = async {
        check_snapshot(q.snapshot, app.lance.version)?;
        let limit = q.limit.unwrap_or(101);
        if !(1..=10_000).contains(&limit) {
            return Err(QueryError::new(400, "limit must be 1..10000").into());
        }
        Selection::new(
            collection,
            q.bbox.as_deref().map(parse_bbox).transpose()?,
            sources(&q, req)?,
            limit,
            q.offset.unwrap_or(0),
            q.cursor.as_deref(),
            app.lance.version,
        )
    };
    let selection = match parsed.await {
        Ok(selection) => selection,
        Err(err) => return error_response(err),
    };
    // Canonical key: pinned version + the selection's own canonical href
    // (sorted sources, normalized bbox, cursor). Equivalent requests share
    // one entry; different effective sources never collide.
    let key = format!("items|{}", selection.href(app.lance.version, None));
    if let Some(rendered) = app.cached_response(&key) {
        return http_cache::success(
            req.headers(),
            &app.metrics,
            rendered.body,
            rendered.content_type,
            rendered.gz,
        );
    }
    let run = async {
        let _permit = app.admit(false)?;
        app.items_page(&selection).await
    };
    match run.await {
        Ok(body) => {
            let body = bytes::Bytes::from(body.into_bytes());
            app.store_response(key, body.clone(), "application/geo+json", true);
            http_cache::success(
                req.headers(),
                &app.metrics,
                body,
                "application/geo+json",
                true,
            )
        }
        Err(err) => error_response(err),
    }
}

#[handler]
async fn item(
    Path((collection, feature_id)): Path<(String, String)>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let parsed = async {
        check_snapshot(q.snapshot, app.lance.version)?;
        sources(&q, req)
    };
    let sources = match parsed.await {
        Ok(sources) => sources,
        Err(err) => return error_response(err),
    };
    let key = format!(
        "item|{collection}|{feature_id}|{}|v{}",
        sources
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        app.lance.version
    );
    if let Some(rendered) = app.cached_response(&key) {
        return http_cache::success(
            req.headers(),
            &app.metrics,
            rendered.body,
            rendered.content_type,
            rendered.gz,
        );
    }
    let run = async {
        let _permit = app.admit(false)?;
        app.item(&collection, &feature_id, &sources).await
    };
    match run.await {
        Ok(Some(body)) => {
            let body = bytes::Bytes::from(body.into_bytes());
            app.store_response(key, body.clone(), "application/geo+json", true);
            http_cache::success(
                req.headers(),
                &app.metrics,
                body,
                "application/geo+json",
                true,
            )
        }
        Ok(None) => error_response(QueryError::new(404, "not found").into()),
        Err(err) => error_response(err),
    }
}

#[handler]
async fn tile(
    Path((collection, z, x, y)): Path<(String, u8, u32, u32)>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let parsed = async {
        check_snapshot(q.snapshot, app.lance.version)?;
        crate::tiles::validate(z, x, y).map_err(|e| QueryError::new(400, e.to_string()))?;
        sources(&q, req)
    };
    let sources = match parsed.await {
        Ok(sources) => sources,
        Err(err) => return error_response(err),
    };
    let key = format!(
        "tile|{collection}|{z}/{x}/{y}|{}|v{}",
        sources
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        app.lance.version
    );
    if let Some(rendered) = app.cached_response(&key) {
        return http_cache::success(
            req.headers(),
            &app.metrics,
            rendered.body,
            rendered.content_type,
            rendered.gz,
        );
    }
    let run = async {
        let _permit = app.admit(false)?;
        app.tile(&collection, z, x, y, &sources).await
    };
    match run.await {
        Ok(Some(body)) => {
            let body = bytes::Bytes::from(body);
            app.store_response(
                key,
                body.clone(),
                "application/vnd.mapbox-vector-tile",
                false,
            );
            http_cache::success(
                req.headers(),
                &app.metrics,
                body,
                "application/vnd.mapbox-vector-tile",
                false,
            )
        }
        Ok(None) => http_cache::no_content(&app.metrics),
        Err(err) => error_response(err),
    }
}

fn error_response(err: anyhow::Error) -> Response {
    let (status, message) = if let Some(err) = err.downcast_ref::<QueryError>() {
        (err.status, err.message.clone())
    } else {
        tracing::error!(error = %err, "request failed");
        (500, "internal query error".into())
    };
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .content_type("application/json")
        .header(header::CACHE_CONTROL, "no-store");
    if status == 429 {
        builder = builder.header(header::RETRY_AFTER, "1");
    }
    builder.body(json!({"code": status, "description": message}).to_string())
}

pub fn routes(app: Arc<App>) -> impl Endpoint<Output = Response> {
    let version = app.lance.version;
    let metrics = app.metrics.clone();
    Route::new()
        .at("/healthz", get(healthz))
        .at("/metrics", get(metrics_endpoint))
        .at("/collections", get(collections))
        .at("/collections/:collection", get(collection_detail))
        .at("/collections/:collection/items", get(items))
        .at("/collections/:collection/items/:feature_id", get(item))
        .at("/collections/:collection/tiles/:z/:x/:y", get(tile))
        .data(app)
        .around(move |ep, req| {
            let metrics = metrics.clone();
            let measured = req.uri().path().starts_with("/collections");
            let span = tracing::info_span!(
                "http.request",
                path = req.uri().path(),
                dataset_version = version
            );
            async move {
                let started = Instant::now();
                if measured {
                    metrics.count_request();
                }
                let mut response =
                    match tokio::time::timeout(Duration::from_secs(30), ep.get_response(req)).await
                    {
                        Ok(response) => response,
                        Err(_) => {
                            error_response(QueryError::new(504, "query deadline exceeded").into())
                        }
                    };
                response
                    .headers_mut()
                    .insert("x-lakewing-snapshot", version.to_string().parse().unwrap());
                if response.status().is_client_error() || response.status().is_server_error() {
                    response
                        .headers_mut()
                        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
                }
                if measured {
                    metrics.count(response.status().as_u16());
                    metrics.observe(started.elapsed());
                }
                Ok(response)
            }
            .instrument(span)
        })
}

pub async fn serve(app: Arc<App>, listen: &str) -> anyhow::Result<()> {
    Server::new(TcpListener::bind(listen))
        .run(routes(app))
        .await?;
    Ok(())
}
