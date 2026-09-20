//! poem OGC API: collections, items pages (bbox/limit/offset/cursor),
//! single-item lookup. Renders the same FeatureCollection contract as the
//! archived Go serve (see docs/rust-architecture.md).
use std::sync::Arc;

use poem::http::StatusCode;
use poem::web::{Data, Json, Path, Query};
use poem::{get, handler, listener::TcpListener, EndpointExt, Response, Route, Server};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::app::App;

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
async fn collections(app: Data<&Arc<App>>) -> Json<Value> {
    Json(json!({
        "collections": app.collections.iter().map(|c| json!({
            "id": c,
            "links": [{"rel": "items", "href": format!("/collections/{}/items", c), "type": "application/geo+json"}]
        })).collect::<Vec<_>>()
    }))
}

#[handler]
async fn items(
    Path(collection): Path<String>,
    Query(q): Query<ItemsQuery>,
    app: Data<&Arc<App>>,
) -> Response {
    let limit = q.limit.unwrap_or(101).clamp(1, 10000) as usize;
    let offset = q.offset.unwrap_or(0);
    let bounds = q.bbox.as_deref().and_then(parse_bbox);
    let sources = parse_sources(q.sources.as_deref());
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
        Ok(body) => Response::builder()
            .content_type("application/geo+json")
            .body(body),
        Err(err) => error_response(err),
    }
}

#[handler]
async fn item(
    Path((collection, feature_id)): Path<(String, String)>,
    Query(q): Query<ItemsQuery>,
    app: Data<&Arc<App>>,
) -> Response {
    let sources = parse_sources(q.sources.as_deref());
    match app.item(&collection, &feature_id, &sources).await {
        Ok(Some(body)) => Response::builder()
            .content_type("application/geo+json")
            .body(body),
        Ok(None) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("not found"),
        Err(err) => error_response(err),
    }
}

fn error_response(err: anyhow::Error) -> Response {
    let msg = format!("{err:#}");
    let status = if msg.contains("unknown collection") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Response::builder().status(status).body(msg)
}

pub async fn serve(app: Arc<App>, listen: &str) -> anyhow::Result<()> {
    let route = Route::new()
        .at("/healthz", get(healthz))
        .at("/collections", get(collections))
        .at("/collections/:collection/items", get(items))
        .at("/collections/:collection/items/:feature_id", get(item))
        .data(app);
    let listener = TcpListener::bind(listen);
    Server::new(listener).run(route).await?;
    Ok(())
}
