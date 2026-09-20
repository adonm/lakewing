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

## Benchmark (v1 parity gate, full 25.36M-row fixture, local NVMe)

`scripts/lancebench/battery.py` — canonical-JSON equality + medians:

| query | Go serve (DuckLake/parquet) | Rust serve (lance+duckdb) |
| --- | --- | --- |
| ITEM (BTREE) | 43 ms | **6.8 ms** |
| FULL page | 600 ms | **573 ms** |
| CITY (bbox) | **76 ms** | 372 ms |
| DEEP (offset 50k) | **3.9 s** | 6.9 s |

Equality: all match (ids, geometry, properties, links). Known v1 gaps:
CITY scans bbox columns of every fragment (the RTree/GeoArrow dataset
path and `ST_Intersects` pushdown close this — verified in the SDK
battery at 2 ms); DEEP's 50k window does a top-N over all ids (needs
either Lance index-assisted ordering or a DuckDB-side stream).

## Serve

```
just run -- --catalog-uri s3://lake/catalog --table features --tag prod \
            --listen 0.0.0.0:3000 --cache-dir /mnt/nvme/lakewing --cache-bytes 8589934592
```

Local benchmark replica: `--catalog-uri .tmp/cache-bench/reader/lancebench/run-20260920T134150Z --table wkb --tag prod`.

## Not yet ported from the Go serve

Tiles, Arrow Flight, ETags/gzip/conditional requests, metrics/OTel,
admission lanes, k8s charts, the build (materialize) pipeline, S3
endpoint/storage-option wiring for the dataset handle, and the kind
cachebench rig integration (the Python harness is language-neutral and
will be pointed at this serve next).
