# Lance experiment (SDK gate, full fixture)

Goal: measure whether an indexed Lance dataset is viable for the OGC serving
surfaces, against the DuckLake/Parquet path, before committing to any serve
integration. Harness: `scripts/lancebench/` (`just bench-lance`), run on the
existing `kind-lake-cache` rig (NVMe-backed work dirs, SeaweedFS origin behind
the meter + 25 ms delay injector, reads through the node-local `s3cache`
proxy bounded at 32 MiB, 1 MiB slices).

Run: `.tmp/cache-bench/reader/lancebench/run-20260920T134150Z` (25,358,254
features, all of snapshot 6; source exported by the pinned engine
`v2.0.0-alpha42069`, queried by DuckDB 1.5.2 + pylance 12.0.0 + lance
extension `1b4ef68`; 4 CPU / 6 Gi pod; DuckDB `threads=4`, `memory_limit=1GB`).

## Encodings compared

| backend | layout | indexes |
| --- | --- | --- |
| `ducklake` | zstd parquet, 512 MB files, 65 536 row groups, grid sort (the fixture's own layout) | none (zonemap pruning) |
| `wkb_ids` | Lance 2.2, WKB `geom` blob, ≤512 MB fragments | BTREE on `id` |
| `geo_ids` | Lance 2.2, GeoArrow multipolygon `geom` (+ `was_polygon` restore bit) | BTREE on `id`, RTREE on `geom` |

Lance paths use ids-first pagination (narrow scan for ids, then payload fetch
by id) and the SDK scanner streaming into DuckDB over Arrow; DuckDB performs
the exact spatial recheck, ordering and limits in all paths.

## Equality (fail-closed gates, all passed)

- Every page result (ID / CITY / FULL / DEEP) identical to DuckLake on every
  repeat, for both Lance encodings.
- Full-dataset order-free digest (XOR/sum fold of per-row SHA-256 over all 13
  columns) identical across the three encodings: 25,358,254 rows.
- CITY with the RTREE disabled returns identical rows (index correctness
  control; unindexed S3 control read 4.06 GB / 840 GETs to answer it).

## Storage and generation

| | DuckLake | Lance wkb | Lance geo |
| --- | --- | --- | --- |
| build wall / CPU | 17.5 s / 68.6 s | 23.5 s / 35.0 s | 45.6 s / 60.5 s |
| index build | – | 5.6 s (BTREE) | 5.5 s + 7.9 s (BTREE + RTREE) |
| stored bytes | 3.16 GB (7 files) + 3.7 MB catalog | 8.30 GB (14 files) | 8.75 GB (13 files) |

Lance stores 2.6–2.8× more bytes at these settings. The geo build cost is
mostly the harness's Python WKB→GeoArrow conversion, not the writer.
Tuning surface for Lance is genuinely smaller (fragment bytes / rows per
group via writer params; no per-file or compression matrix).

## Query medians (warm repeats; S3 phase = through s3cache, 25 ms origin)

local NVMe:

| query | ducklake | wkb_ids | geo_ids |
| --- | --- | --- | --- |
| ID | 26.9 ms | 4.8 ms | 4.5 ms |
| CITY (bbox) | 29.3 ms | 415 ms | 30.8 ms |
| FULL page | 6 837 ms | 761 ms | 803 ms |
| DEEP (offset 50 k) | 1 642 ms | 2 029 ms | 1 984 ms |

S3 (median origin GETs / bytes per warm repeat):

| query | ducklake | wkb_ids | geo_ids |
| --- | --- | --- | --- |
| ID | 34.3 ms | 22.3 ms · 6 g / 7 MB | 21.5 ms · 5 g / 6 MB |
| CITY | 28.9 ms (client-cached) | 2 229 ms · 358 g / 1 187 MB | 28.1 ms · 4 g / 4 MB |
| FULL | 10 482 ms · 923 g / 2 609 MB | 4 765 ms · 808 g / 880 MB | 4 439 ms · 673 g / 734 MB |
| DEEP | 7 520 ms · 1 576 g / 1 754 MB | 4 876 ms · 473 g / 526 MB | 4 757 ms · 427 g / 474 MB |

Notes:

- The fixture is grid-sorted, not id-sorted, so ORDER BY id pages are full
  top-N scans. DuckLake answers them by reading id/layer/source_id columns of
  nearly the whole dataset (2.6 GB over S3 for FULL); Lance reads compact id
  column pages (0.73–0.88 GB) — hence 2.2–2.4× faster and ~3× fewer origin
  bytes despite storing more total bytes.
- CITY: the RTREE matches grid-sort + parquet zonemaps (29 vs 31 ms). The
  WKB-without-RTREE variant is the wrong encoding for bbox queries (2.2 s).
- DuckLake's S3 CITY shows zero origin GETs on warm repeats because DuckDB's
  external file cache held the row groups; the Lance SDK has no equivalent
  in-process file cache and still only pulled 4 MB through the proxy when
  cold. Caching asymmetry, not query asymmetry.
- ID lookups through the persistent BTREE cost ~5 MB of index reads and no
  data fragments — vs DuckLake's 27–34 ms full-file-shard scan shape here.

## Verdict

At full fixture scale, indexed Lance (GeoArrow + BTREE + RTREE with ids-first
pagination) is better on item-by-id (5–6×) and id-ordered pagination
(2.2–2.4× over S3 with ~3× fewer origin bytes), equal on bbox pages, and
slightly behind only on local-NVMe DEEP (1.6 vs 2.0 s). The origin-bytes
property matters most at PB scale with bounded node caches: paying 2.6–2.8×
storage for 3–4× less read traffic per page is a real trade, not a regression.

Remaining blockers are operational, not performance:

1. The pinned engine `v2.0.0-alpha42069` has no `lance` extension build, and
   DuckDB 1.5.2 cannot open the production DuckLake catalog (`1.1-dev1`) —
   the source had to be exported by the pinned CLI. Serving from Lance needs
   either an engine-version decision or a Go-side Lance reader (C ABI/Arrow),
   which is the previously-requested connector work.
2. These are SDK/SQL query surfaces, not the Go HTTP endpoints; tiles,
   concurrent load and the Go integration are unmeasured.
3. GeoArrow generation currently round-trips through WKB in the harness; a
   real builder would write GeoArrow directly.

Reproduce: `just bench-lance --full --queries ID,CITY,FULL,DEEP --backends
ducklake,wkb_ids,geo_ids --repeats 3`; summarize with
`python3 scripts/lancebench/summarize.py <run-dir>`. Artifacts are gitignored
under `.tmp/cache-bench/reader/lancebench/`.
