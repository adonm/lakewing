//! poem OGC API: collections, items pages (bbox/limit/offset/cursor),
//! single-item lookup, XYZ MVT tiles, metrics. Renders the same HTTP
//! contract as the archived Go serve (ETags, gzip variants, conditional
//! requests, admission) — see docs/rust-architecture.md.
use std::sync::Arc;

use poem::http::{header, HeaderMap, StatusCode};
use poem::web::{Data, Json, Path, Query};
use poem::{get, handler, listener::TcpListener, EndpointExt, Request, Response, Route, Server};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::app::App;
use crate::http_cache;

#[derive(Deserialize)]
pub struct ItemsQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub bbox: Option<String>,
    pub cursor: Option<String>,
    pub sources: Option<String>,
}

fn parse_bbox(raw: &str) -> Option<[f64; 4]> {
    let parts: Vec<f64> = raw
        .split(',')
        .filter_map(|p| p.trim().parse().ok())
        .collect();
    match parts.as_slice() {
        [w, s, e, n] if w.is_finite() && s.is_finite() && e.is_finite() && n.is_finite() => {
            Some([*w, *s, *e, *n])
        }
        _ => None,
    }
}

fn parse_sources(raw: Option<&str>) -> Vec<i64> {
    match raw {
        None => vec![1],
        Some(s) => s.split(',').filter_map(|p| p.trim().parse().ok()).collect(),
    }
}

#[handler]
async fn healthz() -> Response {
    Response::builder().content_type("text/plain").body("ok")
}

#[handler]
async fn metrics_endpoint(app: Data<&Arc<App>>) -> Response {
    Response::builder()
        .content_type("text/plain; version=0.0.4")
        .body(app.metrics.exposition())
}

#[handler]
async fn collections(app: Data<&Arc<App>>) -> Json<Value> {
    Json(json!({
        "collections": app.collections.iter().map(|c| json!({
            "id": c,
            "links": [{"rel": "items", "href": format!("/collections/{}/items", c), "type": "application/geo+json"}]
        })).collect::<Vec<_>>()
    }))
}

/// Admission wrapper: counts the request, holds the in-flight gauge, and
/// enforces the concurrency semaphore (429 + Retry-After when saturated).
#[allow(clippy::result_large_err)]
async fn admit<'a>(
    app: &'a App,
    _headers: &HeaderMap,
) -> Result<tokio::sync::SemaphorePermit<'a>, Response> {
    app.metrics.count_request();
    app.metrics.enter();
    match app.acquire().await {
        Ok(permit) => Ok(permit),
        Err(_) => {
            app.metrics.leave();
            app.metrics.count(429);
            Err(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header(header::RETRY_AFTER, "1")
                .content_type("application/json")
                .body("{\"code\":429,\"description\":\"server overloaded\"}"))
        }
    }
}

struct InFlight {
    app: Arc<App>,
}

impl InFlight {
    fn new(app: Arc<App>) -> Self {
        app.metrics.enter();
        Self { app }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.app.metrics.leave();
    }
}

#[handler]
async fn items(
    Path(collection): Path<String>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let limit = q.limit.unwrap_or(101).clamp(1, 10000) as usize;
    let offset = q.offset.unwrap_or(0);
    let bounds = q.bbox.as_deref().and_then(parse_bbox);
    let sources = parse_sources(q.sources.as_deref());
    let _permit = match admit(&app, req.headers()).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let _in_flight = InFlight::new((*app).clone());
    match app
        .items_page(
            &collection,
            bounds,
            &sources,
            limit,
            offset,
            q.cursor.as_deref(),
        )
        .await
    {
        Ok(body) => http_cache::success(
            req.headers(),
            &app.metrics,
            body.into_bytes(),
            "application/geo+json",
            true,
        ),
        Err(err) => error_response(err, &app.metrics),
    }
}

#[handler]
async fn item(
    Path((collection, feature_id)): Path<(String, String)>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let sources = parse_sources(q.sources.as_deref());
    let _permit = match admit(&app, req.headers()).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let _in_flight = InFlight::new((*app).clone());
    match app.item(&collection, &feature_id, &sources).await {
        Ok(Some(body)) => http_cache::success(
            req.headers(),
            &app.metrics,
            body.into_bytes(),
            "application/geo+json",
            true,
        ),
        Ok(None) => {
            app.metrics.count(404);
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("not found")
        }
        Err(err) => error_response(err, &app.metrics),
    }
}

#[handler]
async fn tile(
    Path((collection, z, x, y)): Path<(String, String, String, String)>,
    Query(q): Query<ItemsQuery>,
    req: &Request,
    app: Data<&Arc<App>>,
) -> Response {
    let (Ok(z), Ok(x), Ok(y)) = (z.parse::<u8>(), x.parse::<u32>(), y.parse::<u32>()) else {
        app.metrics.count_request();
        app.metrics.count(400);
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("tile coordinates outside XYZ matrix");
    };
    if let Err(err) = crate::tiles::validate(z, x, y) {
        app.metrics.count_request();
        app.metrics.count(400);
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(err.to_string());
    }
    let sources = parse_sources(q.sources.as_deref());
    let _permit = match admit(&app, req.headers()).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let _in_flight = InFlight::new((*app).clone());
    match app.tile(&collection, z, x, y, &sources).await {
        Ok(Some(mvt)) => http_cache::success(
            req.headers(),
            &app.metrics,
            mvt,
            "application/vnd.mapbox-vector-tile",
            false,
        ),
        Ok(None) => http_cache::no_content(&app.metrics),
        Err(err) => error_response(err, &app.metrics),
    }
}

fn error_response(err: anyhow::Error, metrics: &crate::metrics::Metrics) -> Response {
    let msg = format!("{err:#}");
    let status = if msg.contains("unknown collection") {
        metrics.count(404);
        StatusCode::NOT_FOUND
    } else {
        metrics.count(500);
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Response::builder().status(status).body(msg)
}

pub async fn serve(app: Arc<App>, listen: &str) -> anyhow::Result<()> {
    let route = Route::new()
        .at("/healthz", get(healthz))
        .at("/metrics", get(metrics_endpoint))
        .at("/collections", get(collections))
        .at("/collections/:collection/items", get(items))
        .at("/collections/:collection/items/:feature_id", get(item))
        .at("/collections/:collection/tiles/:z/:x/:y", get(tile))
        .data(app);
    let listener = TcpListener::bind(listen);
    Server::new(listener).run(route).await?;
    Ok(())
}
