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
- **foyer under Lance, immutable-only, block-aligned**: `get_ranges`/ranged
  `get_opts` on immutable prefixes (`data/`, `_indices/`, `_deletions/`,
  `_versions/`) are served from a hybrid cache on a **fixed block grid**
  (`--cache-block-bytes`, default 256 KiB; 0 = exact ranges). A read is
  served from the blocks covering it, so overlapping-but-different queries
  (take-scattered payload rows, adjacent tiles) reuse cached bytes instead
  of missing on exact-range key mismatches — pinned by a unit test. Ranges
  ≥ 4 blocks fetch as one exact GET (large sequential reads don't shard).
  `get_or_fetch` single-flights concurrent block misses; a HEAD metadata
  cache (shared by alignment clamping and ranged gets) answers without
  origin HEADs; keys include the store prefix + endpoint; the cache
  directory takes an exclusive lock and shutdown flushes the disk tier.
  Tags/mutable pointers always go to origin. Per-pod by design; a shared
  cache would be a separate service. Counters on `/metrics` include
  `lakewing_cache_{lookups,memory_hits,disk_hits,origin_ranges,origin_bytes,
  origin_heads,requested_bytes}_total` — `origin_bytes`/`requested_bytes`
  exposes the amplification tradeoff.
- **Spatial indexes, tile serving**: BTREE(id) + RTREE(geom) + zonemaps on
  the bbox columns (build side). Plan usage is *pinned by tests*, not
  assumed: tile/bbox queries prefilter through `@geom_idx(RTree)`, payload
  `id IN` windows probe `@id_idx(BTree)`, and non-spatial filters touch no
  index. Coarse envelopes (bbox ≥ 1°², i.e. z8-and-out tiles) take a
  **split selection plan**: contained features (bbox inside the envelope —
  intersection implied, no geometry math) come from a plain column scan
  that never materializes index row addresses, and only the thin boundary
  band pays the exact RTREE check; the branches are disjoint and merged.
  Measured on the 25.36M fixture: a z6 tile went from the 30 s deadline
  (504) to **4.5 s cold / 2.1 s warm**; z7/z8 tiles are **~0.6 s**; a
  wide-bbox page from 504 to 2.2 s; fine queries (z10 1.15 s, z12 0.29 s,
  CITY 25 ms) are unchanged by design, and the z12 Amsterdam tile stays
  byte-identical to the archived Go serve
  (`"e1f393cf272fc35b-244593"`). Why the split exists: the single exact
  filter on a coarse envelope enumerates every match through the RTREE
  prefilter and then takes their ids through the index stream — 37 s for a
  5°×7° envelope on the fixture (release build); the contained scan is
  0.7 s and the straddler band 0.4 s (columns) / 3.3 s (with the exact
  RTREE check).
- **The one known scaling hole (tracked, not hidden)**: ordered key selection
  still scans the matching id/source set in Lance (bounded max-heap; the
  pinned lance 12 `order_by` sorts candidates — `scanner.rs` pushes no limit
  through a sort). `examined` is recorded on the `lance.select` span and
  grows with the matching set, not the page. The fix is either an
  index-ordered scan or a (page, cursor) mapping maintained at build time;
  both are viable follow-ups once profiling shows where page time actually
  goes.

- **Rendered-response cache is opt-in** (`--response-cache-bytes`, default
  0 = off): when enabled, successful geo+json/MVT bodies are stored in a
  bounded foyer memory cache keyed by (pinned snapshot, canonical
  selection href) — warm repeats skip all local work and bypass admission.
  The default posture deliberately optimizes the serving path over cached
  *data* instead, so repeat requests still exercise selection and
  rendering against warm storage. Opt-in measurements (256 MiB): battery
  warm repeats 0.3–4.8 ms; warm 16-thread mix 3258 RPS / p50 3.3 ms (vs
  8.8 RPS / p50 1506 ms when rendering repeats).

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

## Benchmark (default posture, full 25.36M-row GeoArrow fixture)

`scripts/lancebench/battery.py` — canonical-JSON equality **gate** (non-zero
exit on mismatch). Default serve (no response cache, block-aligned data
cache), single process on local NVMe; fine spatial queries ride the RTREE
single-filter path, coarse (z8-and-out) tiles the split plan.

| query | Go serve (DuckLake/parquet) | Rust local (default) |
| --- | --- | --- |
| ITEM (BTREE) | 35 ms | **6.1 ms** |
| CITY (bbox, RTREE) | 49 ms | **25–29 ms** |
| FULL page (101 of 25.36M) | **495 ms** | 620–635 ms |
| DEEP (offset 50k) | 4008 ms | **770–790 ms** |
| tile z12 (Amsterdam, byte-identical) | — | 0.29–0.37 s |
| tile z10 | — | 1.15–1.25 s |
| tile z8 / z7 (coarse split) | — | **~0.6 s** |
| tile z6 (coarse split) | deadline 504 | **4.5 s cold / 2.1 s warm** |

With `--response-cache-bytes` opt-in the battery measures warm repeats
(ITEM 0.3 ms, CITY 4.3 ms, FULL 4.8 ms, DEEP 2.3 ms) and a warm 16-thread
mix reaches 3258 RPS / p50 3.3 ms — but that is cached responses, not
serving performance; first-render costs are the table above. Selective
first-render classes sit inside the Enterprise selective band; the FULL/DEEP
*first-render* class is the selection-scaling hole below.

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
fragments / 10M rows per file) → BTREE(id) + RTREE(geom) + **zonemaps on
xmin/ymin/xmax/ymax** (prune the coarse-split contained scan per page; the
25M fixture predates them — its contained scan is a full-column read) →
tag → `lakewing.collections` metadata (serves list collections without a
startup scan; older datasets fall back to a layer-only scan once at open).

## Observability

- `/metrics`: response counters by status, requests/flight requests, in-flight
  gauge, latency histogram (5ms..30s buckets), pinned dataset version,
  `lakewing_response_cache_{hits,misses}_total`, and (with a cache)
  `lakewing_cache_{lookups,memory_hits,disk_hits,origin_ranges,origin_bytes,
  origin_heads,requested_bytes,bypass_get_opts}_total` — `origin_bytes` over
  `requested_bytes` exposes block-alignment amplification.
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

- Coarse straddler cost: the split's boundary branch still pays an RTREE
  envelope evaluation (3.3 s release on the fixture for a 5°×7° envelope);
  a bbox-band scan (0.4 s) with the exact geometry check done in DuckDB on
  the band rows would shave z6-class tiles toward ~1.5 s, at the cost of
  window-extension semantics for pathological all-boundary shapes.
- Selection scaling (first-render FULL/DEEP): index-ordered retrieval or a
  build-time (page, cursor) map; profile `lance.select.examined` first. The
  lance 12 BTREE serves equality/range *prefilters* but `order_by` still
  sorts candidates (no index-ordered early termination), so the heap stands.
- Fixture rebuild with bbox-column zonemaps (the 25M fixture predates
  them); then re-check the coarse-split threshold (z10 may want the split
  once the contained scan prunes).
- Rig runs post-v8: re-record S3+foyer first-render/warm numbers plus the
  concurrent herd matrix with origin-delta attribution (the rig and the
  metered endpoints are wired; the runs need the kind cluster up).
- Response-cache first-touch herd (opt-in mode): concurrent misses may
  render the same page redundantly (origin reads are single-flighted
  beneath; render dedup would need a per-key inflight map).
- Cross-pod/node-shared cache comparison at matched budgets, if per-pod NVMe
  misses show up in the herd matrix.
- Managed publication: builder currently writes `--out` and tags; namespace
  `declare_table`/`register_table` integration and incremental
  append/update/index-maintenance semantics (single writer) remain.
- OTel collector-side verification of exported spans (exporter logs are clean;
  a trace-assertion script is still pending).
