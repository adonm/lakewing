# lakewing (Rust) architecture

Rewrite of the archived Go serve (`go-legacy` branch, tag `go-archive-v1`).
Motivation and measurement history: `docs/lance-experiment.md`.

```
poem (OGC REST)
  └─ app: one items-page plan
       ├─ catalog: lance-namespace DirectoryNamespace   ← table discovery/location
       ├─ lance 12 (tag-pinned dataset on S3/local)     ← basic retrieval: BTREE id
       │    └─ WrappingObjectStore ─► foyer HybridCache  (NVMe) ─► object store
       └─ duckdb-rs (stable, unpinned)                  ← exact predicates, GeoJSON,
            ▲    appender over the page only                order/render
            └──────────────────────────────────────────
```

## Decisions

- **One release train, pinned together**: `lance`, `lance-io`,
  `lance-namespace`, `lance-namespace-impls` `=12.0.0` — the crates
  release of the rev our datasets were written with (pylance 12 /
  `356acb0`). Writer/reader vintage matching is the discipline the
  GeoArrow decoder bug taught us (`docs/lance-experiment.md`).
- **Catalog of record**: `lance-namespace` DirectoryNamespace (V1 layout:
  `<table>.lance` under a storage root — local dir or `s3://` prefix).
  `--catalog-uri` + `--table` resolve the dataset location via
  `DescribeTable`; `--uri` remains a development bypass. Snapshot pinning
  is dataset-level: serve requires `--tag` (or `--version`), matching the
  publish model (append + tag move; tags exempt from cleanup).
- **DuckDB is unpinned compute**: no catalog, no pinned engine, no
  extension-fork treadmill — any recent duckdb-rs works. Candidates enter
  through the appender (page-sized sets only), which also sidesteps
  arrow-version alignment between the lance and duckdb crates.
- **Always ids-first pagination**: the ordered id window pushes top-N into
  Lance (`order_by` + `limit`), the payload scan is `id IN (...)`, and
  DuckDB applies the exact predicate, ordering and rendering over the page
  only. The appender never sees the candidate set.
- **foyer under Lance**: a `WrappingObjectStore` caches `get_ranges` in a
  foyer HybridCache (memory + NVMe disk tiers). No SigV4 proxy, no HTTP
  hop. Per-pod, not node-shared — the known tradeoff vs the archived Go
  s3cache proxy; a shared foyer proxy can slot in behind the same trait.

## Benchmark (v2 parity gate, full 25.36M-row GeoArrow fixture)

`scripts/lancebench/battery.py` — canonical-JSON equality + medians.
Serving dataset: `geo.lance` (GeoArrow geom, RTREE on geom, BTREE on id,
tag-pinned v3). Local = dataset on NVMe; S3 = same dataset via SeaweedFS
with the foyer NVMe cache (512 MiB) under Lance:

| query | Go serve (DuckLake/parquet) | Rust local | Rust S3+foyer |
| --- | --- | --- | --- |
| ITEM (BTREE) | 35 ms | **7.9 ms** | **7.0 ms** |
| FULL page | **495 ms** | 602 ms | 560 ms |
| CITY (bbox) | 49 ms | 30.5 ms | **30.4 ms** |
| DEEP (offset 50k) | 4008 ms | **718 ms** | **735 ms** |

Equality: all match across every pair (ids, geometry, properties, links),
local vs S3 included.

v2 changes over v1: bbox pages push `ST_Intersects` on the GeoArrow
geometry (drives the RTREE — CITY went 372 ms → 30 ms); the deep-offset
id window uses a bounded max-heap during the narrow scan instead of a
Lance-side ordered scan (DEEP 6.9 s → 0.72 s); payload projections emit
WKB via `ST_AsBinary(geom)` with `was_polygon` restoring single-part
polygons in DuckDB so GeoJSON matches the source byte-for-byte; storage
options (--endpoint/--s3-key/--s3-secret) flow to both the directory
namespace and the dataset; cold-cache S3 first-touch: CITY 189 ms,
FULL 630 ms, ITEM 673 ms.

## Serve

```
just run -- --catalog-uri s3://lake/catalog --table features --tag prod \
            --listen 0.0.0.0:3000 --cache-dir /mnt/nvme/lakewing --cache-bytes 8589934592
```

S3 with credentials (omit --s3-key/--s3-secret for unsigned reads
through the node-local cache gateway):

```
just run -- --catalog-uri s3://lake/catalog --table features --tag prod \
            --endpoint http://s3-gateway:8080 --s3-key ... --s3-secret ... \
            --listen 0.0.0.0:3000 --cache-dir /mnt/nvme/lakewing
```

Local benchmark replica: `--catalog-uri .tmp/cache-bench/reader/lancebench/run-20260920T134150Z --table geo --tag prod`.

## Not yet ported from the Go serve

Tiles, Arrow Flight, ETags/gzip/conditional requests, metrics/OTel,
admission lanes, k8s charts, the build (materialize) pipeline, and the
kind cachebench rig integration (the battery already runs against this
serve over both local and S3 datasets).
