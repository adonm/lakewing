//! XYZ tile math + MVT assembly over the same ids-first candidate flow.
/// Convert a tile to a CRS84 bbox [w, s, e, n].
pub fn xyz_to_bbox(z: u8, x: u32, y: u32) -> [f64; 4] {
    let n = f64::from(1u32 << z);
    let west = f64::from(x) / n * 360.0 - 180.0;
    let east = f64::from(x + 1) / n * 360.0 - 180.0;
    let north = mercator_to_lat(std::f64::consts::PI * (1.0 - 2.0 * f64::from(y) / n));
    let south = mercator_to_lat(std::f64::consts::PI * (1.0 - 2.0 * f64::from(y + 1) / n));
    [west, south, east, north]
}

fn mercator_to_lat(m: f64) -> f64 {
    180.0 / std::f64::consts::PI * (2.0 * f64::atan(f64::exp(m)) - std::f64::consts::FRAC_PI_2)
}

/// Validate tile coordinates against the XYZ matrix (z <= 30).
pub fn validate(z: u8, x: u32, y: u32) -> anyhow::Result<()> {
    if z > 30 || x >= (1u32 << z) || y >= (1u32 << z) {
        anyhow::bail!("tile coordinates outside XYZ matrix");
    }
    Ok(())
}

/// Web-mercator extent for the tile (west, south, east, north in meters).
pub fn mercator_extent(z: u8, x: u32, y: u32) -> [f64; 4] {
    let half = 6378137.0 * std::f64::consts::PI;
    let span = 2.0 * half / f64::from(1u32 << z);
    let west = -half + f64::from(x) * span;
    let north = half - f64::from(y) * span;
    [west, north - span, west + span, north]
}

/// Single-scan MVT assembly over the loaded candidates (lw_page), matching
/// the archived Go tiles::MVTSQL.
pub fn mvt_sql(collection: &str, extent: [f64; 4]) -> String {
    let [w, s, e, n] = extent;
    let geom_expr = crate::duck::GEOMETRY_SQL;
    format!(
        "SELECT ST_AsMVT(t, {}) FROM (SELECT lw_page.id, ST_AsMVTGeom(\
         ST_Transform({geom_expr}, 'EPSG:4326', 'EPSG:3857', always_xy := true), \
         ST_Extent(ST_MakeEnvelope({w}, {s}, {e}, {n})), 4096, 64, true) AS geom \
          FROM lw_page ORDER BY lw_page.id, source_id LIMIT 5000) t \
         WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) HAVING count(*) > 0",
        crate::duck::quote(collection)
    )
}

/// Candidate cap for tiles (ORDER BY id LIMIT 5000 inside the MVT SQL).
pub const TILE_LIMIT: usize = 5000;
