//! HTTP caching contract: strong ETags over exact bytes, gzip variants,
//! conditional requests — ported from the Go serve's api::body_response.
use poem::http::{header, HeaderMap, HeaderValue, StatusCode};
use poem::Response;

use crate::metrics::Metrics;

pub const CACHE_CONTROL: &str = "public, max-age=60";
pub const VARY: &str = "Accept, Accept-Encoding, X-Source-Ids";

/// Strong ETag: fnv-1a 64 over the exact bytes plus the length.
pub fn etag(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("\"{hash:x}-{}\"", bytes.len())
}

pub fn etag_matches(header: &str, etag: &str) -> bool {
    if header.is_empty() {
        return false;
    }
    let want = etag.trim_matches('"');
    for candidate in header.split(',') {
        let candidate = candidate.trim();
        if candidate == "*" {
            return true;
        }
        let candidate = candidate.strip_prefix("W/").unwrap_or(candidate);
        if candidate.trim_matches('"') == want {
            return true;
        }
    }
    false
}

pub fn wants_gzip(header: &str) -> bool {
    for part in header.split(',') {
        let mut fields = part.split(';');
        let encoding = fields.next().unwrap_or("").trim();
        if !encoding.eq_ignore_ascii_case("gzip") {
            continue;
        }
        let mut q = 1.0f32;
        for param in fields {
            let param = param.trim();
            if let Some(value) = param.strip_prefix("q=") {
                q = value.trim().parse().unwrap_or(0.0);
            }
        }
        if q > 0.0 {
            return true;
        }
    }
    false
}

fn gzip(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).ok()?;
    encoder.finish().ok()
}

fn base_headers(mut response: poem::Response, etag_value: &str) -> poem::Response {
    let headers = response.headers_mut();
    headers.insert(header::VARY, HeaderValue::from_static(VARY));
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(etag_value).expect("etag"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );
    response
}

/// Serve a fresh body: 304 on matching If-None-Match (identity or gzip
/// variant), gzip when negotiated, else identity. `gz=false` for MVT
/// (identity only, matching the Go contract).
pub fn success(
    headers: &HeaderMap,
    metrics: &Metrics,
    body: Vec<u8>,
    content_type: &str,
    gz: bool,
) -> Response {
    let identity = etag(&body);
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if etag_matches(if_none_match, &identity) {
        metrics.count(304);
        let response = base_headers(
            Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .finish(),
            &identity,
        );
        return response;
    }
    let accept_encoding = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if gz && wants_gzip(accept_encoding) {
        if let Some(gzipped) = gzip(&body) {
            let gz_etag = etag(&gzipped);
            if etag_matches(if_none_match, &gz_etag) {
                metrics.count(304);
                return base_headers(
                    Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .finish(),
                    &gz_etag,
                );
            }
            metrics.count(200);
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .content_type(content_type)
                .body(gzipped);
            response
                .headers_mut()
                .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            return base_headers(response, &gz_etag);
        }
    }
    metrics.count(200);
    base_headers(
        Response::builder()
            .status(StatusCode::OK)
            .content_type(content_type)
            .body(body),
        &identity,
    )
}

/// Empty-tile response: 204 with cache headers and no body.
pub fn no_content(metrics: &Metrics) -> Response {
    metrics.count(204);
    let mut response = Response::builder().status(StatusCode::NO_CONTENT).finish();
    let headers = response.headers_mut();
    headers.insert(header::VARY, HeaderValue::from_static(VARY));
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );
    response
}
