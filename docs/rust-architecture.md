# lakewing (Rust) architecture

Rewrite of the archived Go serve (`go-legacy` branch, tag `go-archive-v1`).
Motivation and measurement history: `docs/lance-experiment.md`.

```
poem (OGC REST) + arrow-flight (read-only)
  └─ app: one validated, snapshot-bound selection per request
       ├─ catalog: lance-namespace DirectoryNamespace   ← table discovery/location
       ├─ lance 12 (tag-pinned dataset on S3/local)     ← exact selection: BTREE id,
       │    └─ WrappingObjectStore → foyer HybridCache      ST_Intersects (RTREE /
       │       (per-pod NVMe, immutable ranges)             ST_GeomFromWKB for WKB)
       └─ duckdb (pooled connections, spawn_blocking)  ← GeoJSON/MVT rendering,
                                                            per-connection temp tables
```

## Decisions

- **One release train, pinned together**: `lance`, `lance-io`,
  `lance-namespace(-impls)` `=12.0.0`; arrow 58; duckdb `=1.10505.0` (bundled,
  `appender-arrow` enabled so pages enter columnar, not row-wise);
  object_store 0.14 (the lance-io trait surface). Writer/reader vintage
  matching is the discipline the GeoArrow decoder bug taught us.
- **Catalog of record**: `lance-namespace` DirectoryNamespace (V1 layout:
  `<table>.lance` under a storage root). `--catalog-uri` + `--table` resolve
  via `DescribeTable`; `--uri` is a development bypass. Serving pins a tag or
  version — never latest.
- **Exactness before pagination**: the pushed Lance filter computes exact
  intersection (GeoArrow: `ST_Intersects` drives the RTREE; WKB: bbox-column
  overlap prunes, `ST_GeomFromWKB` refines). The ordered `(id, source_id)`
  window is selected over exactly-matching rows only, so bbox false
  positives (e.g. a polygon whose hole contains the query) can never shift a
  page boundary. A regression test pins this.
- **Keyset pagination, snapshot-bound**: cursors are opaque tokens carrying
  `(version, collection, bbox, sources, after-key)`; a cursor replayed against
  a different snapshot is 409, against a different query 400. `offset` is
  capped (100k) and cannot combine with a cursor.
- **Shared admission, fail fast**: `--concurrency` permits are shared by HTTP
  and Flight via `try_acquire` — saturation rejects immediately (429 +
  Retry-After / RESOURCE_EXHAUSTED), no unbounded wait. In-flight and latency
  metrics are balanced by construction (RAII admission guard).
- **DuckDB is bounded local compute**: a pool of `--concurrency` connections
  (cloned from one handle: spatial + threads + memory limits set once), each
  owning a private `lw_page` temp table; work runs under `spawn_blocking` with
  the permit held until the native call returns, so a cancelled HTTP request
  cannot strand a worker. **Memory model**: the appender is a *push* pipeline
  — `append_record_batch` converts each Arrow batch into DuckDB data chunks
  (vector-size slices) and appends immediately; no reader registration, no
  virtual table, DuckDB never pulls. The only deliberate materialization is
  the selected page: `scan_page` accumulates Arrow bytes and fails closed at
  64 MiB (413), while the key-selection scan streams the whole matching set
  lazily through a `fetch + 1` heap. So RAM is proportional to the page,
  never the collection — pinned by `payload_budget_fails_closed`. Flight
  streams the same payload lazily (per-batch budget) with no intermediate
  collect.
- **foyer under Lance, immutable-only**: `get_ranges`/ranged `get_opts` on
  immutable prefixes (`data/`, `_indices/`, `_deletions/`, `_versions/`) are
  served from a hybrid cache; `get_or_fetch` single-flights concurrent
  misses; a small HEAD metadata cache answers ranged gets without origin
  HEADs. Keys include the store prefix + endpoint (cache identity is not
  conflated across stores). The cache directory takes an exclusive lock, and
  shutdown flushes the disk tier. Tags/mutable pointers always go to origin.
  Per-pod by design; a shared cache would be a separate service.
- **The one known scaling hole (tracked, not hidden)**: ordered key selection
  still scans the matching id/source set in Lance (bounded max-heap; the
  pinned lance 12 `order_by` sorts candidates — `scanner.rs` pushes no limit
  through a sort). `examined` is recorded on the `lance.select` span and
  grows with the matching set, not the page. The fix is either an
  index-ordered scan or a (page, cursor) mapping maintained at build time;
  both are viable follow-ups once profiling shows where page time actually
  goes.

## Performance targets (framing, not promises)

Loosely matched to LanceDB Enterprise's published benchmarks
(docs.lancedb.com/enterprise): warmed-cache selective queries at
**25–50 ms p50 / 35–50 ms p99**, broader filtered queries up to
**65 ms p50 / 100 ms p99**, with throughput scaling horizontally rather than
per-process. Our comparable classes are ITEM (point lookup) and CITY
(selective bbox): both already sit inside the selective band on warm local
runs (ITEM ~8 ms, CITY ~30 ms). FULL/DEEP (101 of 25.36M unordered rows) are
not comparable to filtered-vector-search shapes; they measure the selection
scaling hole above. Measured numbers live in the tables below; every claim
there is from a specific recorded run, not a target.

## Benchmark (v6 gate, full 25.36M-row GeoArrow fixture)

`scripts/lancebench/battery.py` — canonical-JSON equality **gate** (non-zero
exit on mismatch) + medians. v6 re-recorded (single serve, warm local NVMe,
rust-built dataset; equality verified across rust-built and pylance-built
datasets, with and without the foyer cache):

| query | Go serve (DuckLake/parquet) | Rust v6 local | band |
| --- | --- | --- | --- |
| ITEM (BTREE) | 35 ms | **6.1 ms** | inside Enterprise selective (25–50 ms p50) |
| CITY (bbox, RTREE) | 49 ms | **24.9 ms** | inside Enterprise selective |
| FULL page (101 of 25.36M) | **495 ms** | 633 ms | selection-class, see below |
| DEEP (offset 50k) | 4008 ms | **770 ms** | selection-class, see below |

Notes from the re-run: an earlier v6 draft regressed FULL/DEEP ~2.5× (1550 ms)
— traced to `scanner.batch_size(1024)` (24k stream batches per scan; per-batch
scheduling dominated), not to the payload `order_by` (removing it changed
nothing, so the payload scan stays unordered and DuckDB orders the page) nor to
the rust-built dataset (the pylance-built one measured slower under the same
binary). 8192 restored the v2 band; ITEM/CITY never moved.

FULL/DEEP are not comparable to Enterprise's filtered-vector-search shapes:
they measure the known selection-scaling hole below (the whole matching
id/source set streams through a bounded heap). S3+foyer numbers and the
concurrent herd matrix are the remaining rig runs.

## Serve

```
just run -- --catalog-uri s3://lake/catalog --table features --tag prod \
            --listen 0.0.0.0:3000 --cache-dir /mnt/nvme/lakewing \
            --cache-bytes 8589934592 --concurrency 8 --duck-threads 2
```

S3 with credentials (omit `--s3-key/--s3-secret` for unsigned reads through a
signing gateway):

```
just run -- --catalog-uri s3://lake/catalog --table features --tag prod \
            --endpoint http://s3-gateway:8080 --s3-key ... --s3-secret ... \
            --flight-listen 0.0.0.0:50051 --otel-endpoint http://lgtm:4317
```

Local benchmark replica:
`--catalog-uri .tmp/cache-bench/reader/lancebench/run-20260920T134150Z --table geo --tag prod`.

## Build pipeline

`lakewing build` materializes the serving dataset from WKB parquet sources on
the same pinned release train as the serve:

```
just run -- build --source <parquet-or-dir> --out <dir>.lance --tag prod
```

Steps: parquet → GeoArrow multipolygon conversion (polygons promoted,
`was_polygon` from the WKB type word, **null geometries supported** via an
index-based take — geoarrow 0.8's builder mishandles null offsets, so
lakewing never feeds it nulls) → lance 12 write (2.2 storage, 512 MiB
fragments / 10M rows per file) → BTREE(id) + RTREE(geom) → tag →
`lakewing.collections` metadata (serves list collections without a startup
scan; older datasets fall back to a layer-only scan once at open).

## Observability

- `/metrics`: response counters by status, requests/flight requests, in-flight
  gauge, latency histogram (5ms..30s buckets), pinned dataset version, and
  (with a cache) `lakewing_cache_{lookups,memory_hits,disk_hits,origin_ranges,
  origin_bytes,origin_heads,bypass_get_opts}_total`.
- `--otel-endpoint` (OTLP **gRPC**, normally :4317): one subscriber (fmt +
  OTLP layer; no double-init), request spans over both protocols with
  `collection`, `dataset_version`, `examined` (rows the selection scan
  touched) and payload bytes. The provider shuts down on exit to flush.
- Responses carry `X-Lakewing-Snapshot`; errors are `no-store` JSON.

## Deployment (charts/lakewing)

`--flag=value` args (the parser accepts both forms), shared-admission flags,
optional Flight port and OTLP endpoint, and **per-pod cache directories**:
each replica mounts `<cacheHostPath>/<pod-name>` (foyer disk tiers are
single-process; sharing one directory would corrupt). Service exposes HTTP
(+ Flight when enabled). SeaweedFS/meter rig (`scripts/lancebench/rig.py`)
drives the serve from the host against the kind-lake-cache cluster through
port-forwards, so the S3 endpoint is `127.0.0.1:8334` (the forwarded
`httpcache-s3` service), never the in-cluster DNS name.

## Open follow-ups (honest list)

- Selection scaling: index-ordered retrieval or a build-time (page, cursor)
  map; profile `lance.select.examined` first.
- Rig runs post-v6: re-record ITEM/FULL/CITY/DEEP local + S3+foyer, then the
  concurrent herd matrix (the load.py origin-delta attribution is wired).
- Cross-pod/node-shared cache comparison at matched budgets, if per-pod NVMe
  misses show up in the herd matrix.
- Managed publication: builder currently writes `--out` and tags; namespace
  `declare_table`/`register_table` integration and incremental
  append/update/index-maintenance semantics (single writer) remain.
- OTel collector-side verification of exported spans (exporter logs are clean;
  a trace-assertion script is still pending).
