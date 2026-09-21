//! Validated, snapshot-bound selection shared by HTTP and Flight.
use serde::{Deserialize, Serialize};

pub const MAX_OFFSET: usize = 100_000;
pub const MAX_PAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct QueryError {
    pub status: u16,
    pub message: String,
}

impl QueryError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for QueryError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowKey {
    pub id: String,
    pub source_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u64,
    collection: String,
    bounds: Option<[f64; 4]>,
    sources: Vec<i64>,
    after: RowKey,
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub collection: String,
    pub bounds: Option<[f64; 4]>,
    pub sources: Vec<i64>,
    pub limit: usize,
    pub offset: usize,
    pub after: Option<RowKey>,
}

impl Selection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection: String,
        bounds: Option<[f64; 4]>,
        mut sources: Vec<i64>,
        limit: usize,
        offset: usize,
        cursor: Option<&str>,
        version: u64,
    ) -> anyhow::Result<Self> {
        if let Some(bounds) = bounds {
            validate_bounds(bounds)?;
        }
        if offset > MAX_OFFSET {
            return Err(QueryError::new(
                400,
                format!("offset must be <= {MAX_OFFSET}; use a cursor"),
            )
            .into());
        }
        sources.sort_unstable();
        sources.dedup();
        let after = if let Some(raw) = cursor {
            if offset != 0 {
                return Err(
                    QueryError::new(400, "cursor and nonzero offset cannot be combined").into(),
                );
            }
            let cursor: Cursor =
                serde_json::from_str(raw).map_err(|_| QueryError::new(400, "invalid cursor"))?;
            if cursor.version != version {
                return Err(QueryError::new(
                    409,
                    "cursor snapshot is no longer served by this reader",
                )
                .into());
            }
            if cursor.collection != collection
                || cursor.bounds != bounds
                || cursor.sources != sources
            {
                return Err(QueryError::new(400, "cursor does not match the query").into());
            }
            Some(cursor.after)
        } else {
            None
        };
        Ok(Self {
            collection,
            bounds,
            sources,
            limit,
            offset,
            after,
        })
    }

    pub fn filter(&self, geo: bool, spatial: bool) -> anyhow::Result<String> {
        if self.bounds.is_some() && !spatial {
            return Err(QueryError::new(400, "bbox requires a geometry column").into());
        }
        let filter = crate::duck::pushed_filter(&self.collection, self.bounds, &self.sources, geo);
        Ok(self.append_cursor(filter))
    }

    /// Keyset lower bound on the (id, source_id) window — shared by the
    /// single and coarse-split selection filters.
    pub fn append_cursor(&self, mut filter: String) -> String {
        if let Some(after) = &self.after {
            let id = crate::duck::quote(&after.id);
            filter.push_str(&format!(
                " AND (id > {id} OR (id = {id} AND source_id > {}))",
                after.source_id
            ));
        }
        filter
    }

    pub fn href(&self, version: u64, after: Option<&RowKey>) -> String {
        let sources = self
            .sources
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut href = format!(
            "/collections/{}/items?sources={sources}&limit={}&snapshot={version}",
            urlencode(&self.collection),
            self.limit
        );
        if let Some([w, s, e, n]) = self.bounds {
            href.push_str(&format!("&bbox={w},{s},{e},{n}"));
        }
        if let Some(after) = after.or(self.after.as_ref()) {
            let cursor = Cursor {
                version,
                collection: self.collection.clone(),
                bounds: self.bounds,
                sources: self.sources.clone(),
                after: after.clone(),
            };
            href.push_str("&cursor=");
            href.push_str(&urlencode(
                &serde_json::to_string(&cursor).expect("finite cursor"),
            ));
        } else if self.offset != 0 {
            href.push_str(&format!("&offset={}", self.offset));
        }
        href
    }
}

pub fn validate_bounds(bounds: [f64; 4]) -> anyhow::Result<()> {
    let [w, s, e, n] = bounds;
    if !bounds.iter().all(|x| x.is_finite())
        || w >= e
        || s >= n
        || w < -180.0
        || e > 180.0
        || s < -90.0
        || n > 90.0
    {
        return Err(QueryError::new(
            400,
            "bbox must be west,south,east,north in CRS84 with west < east and south < north",
        )
        .into());
    }
    Ok(())
}

pub fn parse_bbox(raw: &str) -> anyhow::Result<[f64; 4]> {
    let values = raw
        .split(',')
        .map(|s| s.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| QueryError::new(400, "bbox contains a non-number"))?;
    let bounds = values
        .try_into()
        .map_err(|_| QueryError::new(400, "bbox must contain four numbers"))?;
    validate_bounds(bounds)?;
    Ok(bounds)
}

pub fn parse_sources(raw: Option<&str>) -> anyhow::Result<Vec<i64>> {
    let Some(raw) = raw else {
        return Ok(vec![1]);
    };
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let mut sources = raw
        .split(',')
        .map(|s| s.trim().parse::<i64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| QueryError::new(400, "sources must be comma-separated integers"))?;
    if sources.len() > 256 {
        return Err(QueryError::new(400, "at most 256 sources may be selected").into());
    }
    sources.sort_unstable();
    sources.dedup();
    Ok(sources)
}

pub fn urlencode(s: &str) -> String {
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

pub fn check_snapshot(requested: Option<u64>, actual: u64) -> anyhow::Result<()> {
    if requested.is_some_and(|v| v != actual) {
        return Err(
            QueryError::new(409, "requested snapshot is no longer served by this reader").into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_partial_and_nonfinite_filters() {
        for value in ["1,2,bad,3,4", "1,2,NaN,4", "3,2,1,4", "1,2,3", "-181,0,1,1"] {
            assert!(parse_bbox(value).is_err(), "{value}");
        }
        assert!(parse_sources(Some("1,bad,2")).is_err());
        assert!(parse_sources(Some("1,")).is_err());
        assert!(parse_sources(Some("")).unwrap().is_empty());
    }

    #[test]
    fn cursor_is_bound_to_snapshot_and_selection() {
        let cursor = Cursor {
            version: 3,
            collection: "a".into(),
            bounds: None,
            sources: vec![2],
            after: RowKey {
                id: "x".into(),
                source_id: 2,
            },
        };
        let raw = serde_json::to_string(&cursor).unwrap();
        let selection =
            |version, sources| Selection::new("a".into(), None, sources, 2, 0, Some(&raw), version);
        assert!(selection(3, vec![2]).is_ok());
        assert_eq!(
            selection(4, vec![2])
                .unwrap_err()
                .downcast::<QueryError>()
                .unwrap()
                .status,
            409
        );
        assert!(selection(3, vec![1]).is_err());
    }
}
