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
   through a `fetch + 1` heap. This bounds application-held keys/page batches;
   it does not bound allocations within Lance/DuckDB or total process RSS.
   `payload_budget_fails_closed` verifies rejection and recovery. Flight
  streams the same payload lazily (per-batch budget) with no intermediate
  collect.
- **foyer under Lance, immutable-only, block-aligned**: `get_ranges`/ranged
  `get_opts` on immutable prefixes (`data/`, `_indices/`, `_deletions/`,
  `_versions/`) are served from a hybrid cache on a **fixed block grid**
  (`--cache-block-bytes`, default 256 KiB; index blocks default 64 KiB;
  0 = exact ranges). A read is
  served from the blocks covering it, so overlapping-but-different queries
  (take-scattered payload rows, adjacent tiles) reuse cached bytes instead
  of missing on exact-range key mismatches — pinned by a unit test. Ranges
  ≥ 4 blocks fetch as one exact GET; ranges above 8 MiB bypass admission.
  `get_or_fetch` single-flights concurrent block misses; a HEAD metadata
  cache (shared by alignment clamping and ranged gets) avoids repeated
  HEADs until eviction; keys include the store prefix + endpoint; the cache
  directory takes an exclusive lock and explicit close flushes the disk tier.
  Tags/mutable pointers always go to origin. Per-pod by design; a shared
  cache would be a separate service. Counters on `/metrics` include
  `lakewing_cache_{lookups,memory_hits,disk_hits,origin_ranges,origin_bytes,
  origin_heads,requested_bytes}_total` — `origin_bytes`/`requested_bytes`
  exposes the reuse/overfetch tradeoff. RAM budgets, fill concurrency,
  alignment thresholds and decoded Lance caches are configurable; see
  [tuning](tuning.md) for defaults, counter definitions and verified recovery.
- **Spatial indexes, tile serving**: BTREE(id) + RTREE(geom) + zonemaps on
  the bbox columns (build side). Plan usage is *pinned by tests*, not
  assumed: tile/bbox queries prefilter through `@geom_idx(RTree)`, payload
  `id IN` windows probe `@id_idx(BTree)`. Selective categorical layer/source
  filters now use bitmaps; bbox comparisons use zonemaps. Coarse envelopes
  (bbox ≥ 1°²; a geographic-area heuristic, not a fixed zoom rule) take a
  **split selection plan**: contained features (bbox inside the envelope —
  intersection implied for nonempty geometries with valid bbox columns)
  use bbox-column predicates, and the boundary branch retains exact
  `ST_Intersects`. RTREE supplies approximate bounding-box candidates;
  exact geometry refinement is performed by Lance. The branches are disjoint
  and merged. Added indexes can change materialization plans, so the original
  plain-scan behavior is not guaranteed on every indexed dataset.
  Measured on the 25.36M fixture: a z6 tile went from the 30 s deadline
  (504) to **4.5 s cold / 2.1 s warm**; z7/z8 tiles are **~0.6 s**; a
  wide-bbox page from 504 to 2.2 s; fine queries (z10 1.15 s, z12 0.29 s,
  CITY 25 ms) are unchanged by design, and the z12 Amsterdam tile stays
  byte-identical to the archived Go serve
  (`"e1f393cf272fc35b-244593"`). Original experiment: the single exact
  filter on a coarse envelope enumerates candidates through the RTREE
  prefilter with late ID materialization — 37 s for a
  5°×7° envelope on the fixture (release build); the contained scan is
  0.7 s and the straddler band 0.4 s (columns) / 3.3 s (with the exact
  spatial filter). Those measurements alone do not isolate index traversal,
  geometry refinement and ID take costs.
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
  selection href) — warm repeats skip selection/rendering and bypass admission;
  ETags and gzip are still computed by the response wrapper.
  The default posture deliberately optimizes the serving path over cached
  *data* instead, so repeat requests still exercise selection and
  rendering against warm storage. Opt-in measurements (256 MiB): battery
  warm repeats 0.3–4.8 ms. The historical 3258 RPS number came from a tiny
  cached-body run and is not comparable to the earlier S3 host-load run.

## Performance targets (framing, not promises)

Loosely matched to LanceDB Enterprise's published benchmarks
(docs.lancedb.com/enterprise): warmed-cache selective queries at
**25–50 ms p50 / 35–50 ms p99**, broader filtered queries up to
**65 ms p50 / 100 ms p99**, with throughput scaling horizontally rather than
per-process. These are workload-specific directional targets, not a matched
comparison with our ITEM/CITY queries. Warm local ITEM/CITY runs are around
8/30 ms. FULL/DEEP (101 of 25.36M unordered rows) are
not comparable to filtered-vector-search shapes; they measure the selection
scaling hole above. Measured numbers live in the tables below; every claim
there is from a specific recorded run, not a target.

## Benchmark (default posture, full 25.36M-row GeoArrow fixture)

Historical `06d2d11` measurements: `scripts/lancebench/battery.py` is a
canonical-JSON equality gate **when given multiple bases** (one base only
measures timings). Response cache off; dataset on local NVMe but the v8/v9
smoke cache directories were on tmpfs. Fine spatial queries ride the RTREE
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
| tile z6 (coarse split) | — | **4.5 s first / 2.1 s repeat** |

With `--response-cache-bytes` opt-in the battery measures warm repeats
(ITEM 0.3 ms, CITY 4.3 ms, FULL 4.8 ms, DEEP 2.3 ms) and a warm 16-thread
mix recorded 3258 RPS / p50 3.3 ms in a brief run. This is not capacity
evidence for unique queries. The z6 30-second 504 baseline was the old Rust
single-filter path, not the Go serve. Current reproducible tuning sweeps and
their measurement boundaries are described in [tuning](tuning.md).

### v10 tuning sweep (2026-09-21, busy development host)

Index-maintenance equality on the **full 25.36M-row fixture**: `lakewing index`
installed 2048-row bbox zonemaps on a reflink copy (new tag `tuned`, version 7,
data files reused), then `scripts/lancebench/tune.py` compared `prod` (v3)
against `tuned` (v7) over 25 query shapes × 4 passes plus a 32-request
concurrent overlapping-bbox herd. MVT bytes identical; GeoJSON identical after
normalizing only snapshot versions in links/cursors — the production-scale
confirmation of the retuning regression test.

Directions from that run (local NVMe, no response cache, one host under
variable background load — directions, not benchmarks):

| class | prod v3 | tuned v7 (bbox zonemaps) |
| --- | ---: | ---: |
| tile z7 first/warm | 1702 / 1161 ms | **713 / 578 ms** |
| tile z8 first/warm | 1576 / 1570 ms | **446 / 678 ms** |
| tile z6 first/warm | 7748 / 8243 ms | 7392 / 6941 ms |
| wide-bbox page first/warm | 9674 / 7872 ms | 7385 / 6264 ms |
| tile z10 warm | 1508 ms | 2232 ms |
| ITEM / CITY / FULL / DEEP / z12 | — | within run variance |

The coarse-split classes improved exactly where zonemaps prune the contained
scan. The z10 warm regression is the one consistent adverse direction and is
unexplained; re-measure on a quiet host before changing defaults.

**Cache-path caveat measured the same day**: on a *local-path* dataset
(`--uri <dir>.lance`) the foyer wrapper recorded ~zero ranged traffic (one
bypass call across the whole sweep) — local file reads do not traverse the
wrapped ranged-`get` path. Cache-behavior claims therefore run against an
object-store dataset.

### v11 nation-scale fixture (2026-09-21)

`lakewing replicate --copies 4 --offset-degrees 10` materialized four
longitude-shifted copies of the real NW-Europe source through DuckDB
`ST_Affine` (bbox columns shifted to match) in ~90 s, and `lakewing build`
indexed the result in ~8 min: **101,433,016 features, 31 GiB Lance, 49
fragments, BTREE 24,764 leaf pages, RTREE + 49,528-zone bbox zonemaps**,
tagged `prod`. Copy 0 keeps original ids (the battery's ITEM id still
resolves); copy k prefixes `k:`. First local smoke (concurrent with other
disk load): ITEM 28 ms (copy 0) / 15 ms (`2:` copy), CITY 61 ms, shifted
CITY at +20° 50 ms, z12 Amsterdam tile 507 ms and **byte-identical
(244,593 B) to the 25M fixture's tile** — each region sees only its copy,
so spatial-index behavior scales as N independent datasets rather than N×
density in one place. Headroom on this workstation (1.7 TiB free,
~312 B/row): ~500M–1B rows before disk; beyond that (and for real
PB-scale) the path is more regions/collections, not one bigger dataset.
The same dataset is seeded into the local origin at
`s3://lake/lancebench/nation/geo.lance`.

Workspace convention: benchmark data, caches, logs and index-training
spills all live in the repo's `.tmp/` (NVMe). `/tmp` here is a 32 GiB
tmpfs and has already caused a spurious `EDQUOT` during RTREE training;
the justfile exports `TMPDIR` into `.tmp/` for exactly that reason.


Same binary and dataset (`s3://lake/lancebench/run-20260920T134150Z`,
25.36M rows) served through a port-forwarded SeaweedFS S3 endpoint; three
cache modes, response cache off, fresh cache directory per case; equality
PASSED across all cases (byte-identical MVT, normalized-JSON identical).
The selective workload = ITEM, 8 jittered overlapping city bboxes, 9
adjacent z12 tiles.

Cold pass (first touch of the distinct overlapping queries; 4 920 logical
range requests, 220.2 MB logical bytes):

| mode | lookups | origin GETs | origin MB | in-pass hits |
| --- | ---: | ---: | ---: | ---: |
| exact ranges (0/0) | 4 920 | 4 658 | 201.6 (0.92×) | 262 (5%) |
| **256 KiB / 64 KiB** | 6 612 | **3 023 (−35%)** | 359.8 (1.63×) | 3 589 (54%) |
| 128 KiB / 32 KiB | 8 131 | 4 719 (+1%) | 285.6 (1.30×) | 3 412 (42%) |

Warm passes (identical queries repeated): **0 origin GETs and 0 origin
bytes in all three modes** — exact keys suffice for repeats. The 32-request
concurrent overlapping-bbox herd on warm data also fetched 0 origin bytes
in all modes; miss coalescing itself is unit-pinned (32 concurrent
overlapping reads → 1 origin fetch + 1 HEAD).

Readings: 256 KiB alignment converts overlapping-but-distinct cold traffic
into cache hits (54% of block lookups during one pass) and cuts origin GETs
35%, at 1.6–1.8× first-touch bytes — the intended trade when origin
round-trips dominate. 128 KiB only sharded mid-size ranges into more GETs
without reducing them, so the 256 KiB default stands for this workload.
Latency differences between modes were within noise here because the
port-forwarded origin has near-zero latency; the GET-count reduction is the
transferable measurement, and modeling it against real S3 latencies needs
the rig's delay endpoint (follow-up). Single host, one dataset, OS/origin
caches not flushed.

Latency-model check (v11, `--origin-latency-ms 20 --origin-mbps 200`
against the local SeaweedFS origin, under concurrent load): the same
sweep did byte-identical work (3 021 origin GETs / 360.6 MB cold, zero
warm) with and without the model. Modeled S3 latency roughly tripled
first-touch times (CITY0 712 → 2 116 ms) while warm passes converged back
to ~25–30 ms — repeat and overlapping traffic is shielded from origin
latency by the block cache, which is the point of caching data instead of
responses.

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
xmin/ymin/xmax/ymax** + selective layer/source bitmaps → tag. The reader
uses `lakewing.collections` metadata when provided by a publisher; this
builder currently leaves discovery to a layer-only startup scan.
`lakewing index` installs or retunes serving indexes on existing data files
and publishes a new tag; no Parquet rebuild is required.

## Observability

- `/metrics`: response counters by status, requests/flight requests, in-flight
  gauge, latency histogram (5ms..30s buckets), pinned dataset version,
  `lakewing_response_cache_{hits,misses}_total`, and (with a cache)
  `lakewing_cache_{requests,lookups,memory_hits,disk_hits,origin_ranges,origin_bytes,
  origin_heads,requested_bytes,bypass_ranges,bypass_get_opts}_total`.
  Logical requests and cache-entry lookups have distinct denominators;
  delegated streaming traffic requires origin-side accounting.
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
  the band rows is a possible follow-up, requiring exact window-extension
  semantics for pathological all-boundary shapes before any speed claim.
- Selection scaling (first-render FULL/DEEP): index-ordered retrieval or a
  build-time (page, cursor) map; profile `lance.select.examined` first. The
  lance 12 BTREE serves equality/range *prefilters* but `order_by` still
  sorts candidates (no index-ordered early termination), so the heap stands.
- Quiet-host re-measurement of fine-tile classes (z10/z12) on the
  zonemap-retuned snapshot: the busy-host sweep showed the coarse-split
  classes improving 1.3–3.5×, but z10 warm moved adversely and needs a clean
  run before any default changes.
- Cache evaluation on object-store datasets only: the v10 sweep measured
  ~zero wrapped ranged traffic on local-path datasets, so local runs cannot
  evidence cache behavior (the S3 sweep in tuning.md is the reference).
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
