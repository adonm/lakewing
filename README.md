# lakewing

> **Rename + rewrite note:** this repo was `iron-feather` (Rust, poem + tonic,
> Cachey HTTP reads). It is now `lakewing` (Go, Huma OGC + Arrow Flight,
> mountpoint-S3-CSI reads, direct-S3 writes, SeaweedFS local rig). `build`,
> `index` and `serve` (OGC + tiles + Flight) are wired against DuckDB 2.0
> (`v2.0.0-alpha42069` via `-tags=duckdb_use_lib`; the preview binding's
> bundled engine is 1.5.x and cannot open 2.0 catalogs). Remaining deltas
> from the old stack: Flight load-benchmarks, refreshed OpenAPI snapshot,
> kind CSI-driver install against real S3 (kind runs hostPath).

**Build a DuckLake snapshot on S3, then serve it through OGC REST and Arrow Flight.**

```text
OSM Layercake GeoParquet (HTTPS)
  → bbox-pruned import → DuckLake catalog + clustered Parquet (local or S3)
                          └─ shared read-only connection pool
                               ├─ OGC Features / XYZ tiles → response bytes
                               └─ Arrow Flight → native DuckDB Arrow batches
```

One binary, three commands, one snapshot. Apache-2.0. DuckDB 2.0 via the
`duckdb-go v2.20000.0-6.preview` binding (CGO; preview until the 2.0 GA).

One binary, three commands (`build`, `index`, `serve`), one shard. Apache-2.0.

## Quickstart

Install Go 1.25+ and `just` with `mise install`, then:

```sh
just fixture-osm --limit 20000    # Layercake buildings, central Berlin
just fmt-check check test
just run                        # fixtures/osm.ducklake; HTTP :3000, Flight :50051
```

```sh
curl localhost:3000/collections
curl 'localhost:3000/collections/buildings/items?sources=1&limit=10'
curl 'localhost:3000/collections/buildings/items?bbox=13.395,52.515,13.405,52.525&sources=1'
# Human-readable API documentation: http://localhost:3000/api.html
```

The DuckDB 2.0 library is bundled by the Go binding (CGO). For direct commands:

```sh
go run ./cmd/lakewing build \
  --from https://data.openstreetmap.us/layercake/buildings.parquet \
  --collection buildings --bbox=13.35,52.48,13.45,52.55 \
  --out fixtures/berlin.ducklake --data-dir fixtures/berlin.files
go run ./cmd/lakewing serve --shard fixtures/berlin.ducklake
```

## Materialization

`build` reads [Layercake](https://layercake.openstreetmap.us/) GeoParquet with
`type`, `id`, `geometry`, and `bbox.{xmin,ymin,xmax,ymax}` columns. It supports
local files and DuckDB HTTP/S3 sources. `--bbox` is required; `--limit` is an
optional development cap. Current flat property columns and older nested
`tags` structs are preserved as JSON. Original geometry is retained; IDs are
`type:id` so an OSM way and relation with the same numeric ID stay distinct.

The resulting schema is:

```text
features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON,
         sortkey BIGINT, xmin/ymin/xmax/ymax DOUBLE, cx DOUBLE, cy DOUBLE, name VARCHAR)
collections(id VARCHAR)
```

Geometry is non-null, 2D CRS84 (longitude/latitude). Import uses both Parquet
bbox statistics and exact geometry intersection, then writes ZSTD Parquet
clustered by `--sort` (`grid` cell, `hilbert`, or `none`) with tight per-file
bbox statistics (`xmin/ymin/xmax/ymax` min/max pruning replaces a spatial
index). Measured on the 25M-row NW-Europe shard (25 files): `grid` keeps
the default — city windows prune to 4–5 files vs 7–8 for `hilbert`, a
rural window to 3 vs 4, at 3.0 vs 3.1 GiB total. `cx`/`cy`/`name` are
build-time derivatives (centroid coordinates and
display name) so Flight `x`/`y`/`name` projections avoid per-row geometry and
JSON work. `--source-id` defaults to `1`. Metadata is discovered from the
shard's actual layers. A versioned `<catalog>.manifest.json` (backend,
schema version, source, bbox, rows, layout) is written alongside the catalog.

Publication is atomic and refuses to overwrite an existing catalog: data
files publish first (additive copy only, never deleting files another
snapshot references), then the catalog that references them. Build a new
snapshot and restart `serve --shard NEW_CATALOG` to switch. The serving
host needs the matching DuckDB `spatial`/`ducklake` extensions installed;
`build` installs them automatically. `--data-url` records the
zone-independent data root in the catalog (an `s3://` prefix for lake
publishes; defaults to the local data dir); each reader mounts the bucket
(via mountpoint S3 CSI) and passes `serve --data-root` pointing at the
mount's data prefix, so relative Parquet paths resolve to local mount paths
everywhere. `build --content-address` names data files by content
hash so unchanged snapshots upload nothing new, and every build writes a
`<catalog>.serving.json` spatial file index beside the catalog (`index`
recompiles it for older catalogs). Validated end to end on SeaweedFS
(`just lake-publish` × 3, serve off the mount): additive-only uploads
(identical republish added zero data keys; shared files dedupe by hash),
coexisting catalog keys, readers pinned across ref moves, snapshots
isolated per bbox.

```sh
just fixture-nw-europe            # ~10 GB Benelux + N. France buildings
just fixture-verify shard=fixtures/nw-europe.ducklake
just workloads                    # saved deterministic request sets
```

`just workloads` writes hot, urban, rural, scattered, broad, empty, deep and
mixed URL sets for `ogc_bench.py --workload`; regenerate with `--region` for a
different shard extent.

### Serving from S3

Reads go through a mountpoint-S3 mount, never through HTTP or SDK range
GETs issued by the server. `serve --shard` takes a mount path to the
`.ducklake` catalog (e.g. `/mnt/lake/catalogs/<sha>.ducklake`) and
`--data-root` takes the mount's data prefix; the catalog attaches read-only
and the stored zone-independent `s3://` data root is remapped to those
local paths. Writes (build/publish) go direct to the S3 API and never
through the mount. In production the mount comes from the mountpoint S3 CSI
driver with a node-local disk cache (see [`docs/mount-lake.md`](docs/mount-lake.md));
locally, `just seaweed-up` + `just lake-mount` reproduce it with SeaweedFS
and `mount-s3 --cache`.
Every pooled connection is switched to the attached catalog; serving reads
resolve at startup to a frozen `read_parquet` file list over the exact
files live at the pinned snapshot, pruned per request to candidate files
via the published `<catalog>.serving.json` index (falling back to the full
list, then the catalog table, on any doubt), so all SQL remains
server-generated and identical rows serve without DuckLake's per-query
snapshot join.

Spatial queries prune by file/row-group bbox statistics, then run exact
`ST_Intersects` only on boundary candidates (fully contained bboxes skip it).
Page-first planning converts geometry only for the returned page. Storage
behavior (mountpoint disk cache, S3-GET accounting, outage proof) is
recorded in [`docs/mount-lake.md`](docs/mount-lake.md).

Layercake data is © [OpenStreetMap contributors](https://www.openstreetmap.org/copyright),
available under the [ODbL](https://opendatacommons.org/licenses/odbl/).

## HTTP API

| Route | Representation |
|---|---|
| `/`, `/conformance` | Landing links and supported conformance classes |
| `/collections`, `/collections/{id}` | Actual collection metadata |
| `/collections/{id}/items` | `application/geo+json` FeatureCollection |
| `/collections/{id}/items/{fid}` | Original geometry and typed properties |
| `/collections/{id}/tiles/{z}/{x}/{y}` | Web Mercator XYZ MVT; 204 if empty |
| `/api`, `/api.html` | OpenAPI 3.0 JSON and HTML documentation |
| `/healthz` | Liveness |

The Features surface implements Core, GeoJSON and OpenAPI 3.0 requirements.
Tiles are an XYZ extension, capped at 5,000 features per tile.

Items support `bbox`, `limit` (1–1,000, default 10), `offset`, `datetime`, and
`sources`. Bboxes support antimeridian crossing, degenerate bounds and 3D
bounds over 2D data. Pages are ordered by feature ID and include `self` and
`next` links. `numberReturned` is included; `numberMatched` is omitted to avoid
an extra count. Layercake edit timestamps are preserved as properties, not
treated as temporal geometry, so static features match every valid datetime.
Unsupported parameters, including `filter` and `properties`, return 400.

### Source selection

Supply `?sources=1,2` or `X-Source-Ids: 1,2`; Flight accepts `sources` in its
ticket or `x-source-ids` metadata. If both are supplied, their **intersection**
is used. Missing both, or an explicitly empty set, returns no features.
These are data filters, not authentication. Responses include the full
effective source set.

## Arrow Flight

`ListFlights`, `GetFlightInfo`, `GetSchema` and `DoGet` are supported. A descriptor
is either a one-component collection path or a command containing the same
JSON used in a `DoGet` ticket:

```json
{"collection":"buildings","bbox":[13.35,52.48,13.45,52.55],"columns":["id","geometry","properties"],"limit":10000,"sources":[1]}
```

Default columns: `id`, `geometry` (WKB), `properties` (JSON string), `source_id`.
Optional projections also include `x`, `y` (centroid coordinates), and `name`.
`offset` defaults to 0; `limit` defaults to 10,000 and is capped at 100,000.
Unknown or duplicate columns are rejected. Empty streams include their schema.

DuckDB produces native Arrow batches, which are encoded directly for Flight without a result cache. A bounded query result is materialized before transmission. The service provides read-only Flight with JSON tickets.

## Bulk access

Bulk consumers use Arrow Flight (same pinned snapshot, bulk admission
lane, byte budgets) or query the published Parquet directly with any
DuckDB. Run broad scans on the dedicated bulk pool
(`bulk.enabled=true` in the chart), not the interactive readers.

## Performance and verification

`--connections` defaults to 8, shared across both protocols. Past the pool,
up to `--max-waiters` (default 128) requests queue for `--max-wait-ms`
(default 250) before the pool fails fast with HTTP **429 + Retry-After: 1** or
Flight **RESOURCE_EXHAUSTED**. `--flight-concurrency` caps concurrent bulk
queries (default: the pool size, i.e. uncapped); lower it to reserve
connections for interactive OGC under bulk load. Heavy HTTP pages
(`limit > 100`, `offset >= 1000`, or broad region slices) share the same bulk
lane. `--threads` sets shared DuckDB threads for the whole process
(default 1; keep at 1 for many small concurrent queries, raise only with
fewer connections for bulk) and `--memory-mb` caps shared DuckDB memory in
MiB (default 4096, sized for the ~10 GB shard urban working set; 0 leaves
DuckDB's unbounded default). Repeated Parquet block reads are absorbed by
the mountpoint local disk cache on each node, not by in-DuckDB tuning:
there are no storage-tuning flags by design (see
[`docs/mount-lake.md`](docs/mount-lake.md)).
`--query-timeout-ms` interrupts HTTP and Flight
queries past their deadline (default 30000; 0 disables). Queries run on blocking
workers with one bounded lifecycle: cancellation is owned from before
execution through final delivery, the worker clears its interrupt handle
before the connection returns to the pool (late drops cannot cancel the next
query), oversized batches drain instead of spinning, and fetch failures
surface as errors instead of truncated streams. Batches flow through a
per-stream byte budget (`--flight-stream-mb`, default 32) plus a process-wide
budget (`--flight-total-mb`, default 128). Narrow scans fetch page payloads
late via file/row-number instead of reading geom+properties first. `/metrics`
is `no-store` and reports `http_requests` plus `duck_setting_*` engine
budgets (threads, memory). Storage-cache benchmarks measure the mountpoint
local disk cache instead: see [`docs/mount-lake.md`](docs/mount-lake.md) for
the local rig (`just seaweed-up`, `just lake-mount`, `just lake-publish`,
`just lake-serve`).
Error responses are always `Cache-Control: no-store` and carry no ETag.

GeoJSON embeds DuckDB's geometry/properties text without a server-side
re-parse, and every response carries an `ETag` with
`Cache-Control: public, max-age=60` (`If-None-Match` returns 304). ETags
are content hashes over the exact bytes, and gzip variants are compressed
per request with their own strong ETag; MVT tiles are already compact and
skip compression. Repeated storage reads are absorbed by the node-local
mountpoint disk cache, not by per-pod memory.

Use a release build. `scripts/ogc_bench.py` warms up before its timed
phase, counts successful responses separately from overload/errors, consumes
complete responses, and reports returned data. Prefer fixed `--requests`
with `--workload FILE` (identical complete passes on every backend) over
ad-hoc URLs:

```sh
just bench-ogc-matrix
CONC=8 just bench-ogc -- --jitter --seed 1
CONC=32 just bench-ogc -- --jitter --seed 2
CONC=32 just bench-ogc -- --route tiles
```

Use a fresh `--seed` per jitter run: every request in a run is unique and
warmup never overlaps measurement. Keep seeds small (1-3 on the Berlin
fixture): larger seeds drift the bbox out of the data and measure empty
pages instead. Defaults match the
Berlin fixture; pass `--bbox` for another shard. These are closed-loop
client measurements; compare successful rps, p50/p99, rejected (429) counts
and wire throughput together. To size capacity, sweep `--connections
4/8/16/32` against rising client concurrency and stop at the last step
holding p99 under 100 ms with under 1% rejected requests.

Pages carry cursor `next` links: `cursor` is an exclusive lower bound on
feature id and takes precedence over `offset`, so deep pages skip leading
rows through the id ordering instead of traversing them with `OFFSET`.
Direct `offset` links keep working.

### Replicas

Snapshots are immutable, so scale past one CPU by running one instance per
core group against the same catalog. Each replica keeps its own pool;
repeated storage reads are shared node-wide through the mountpoint disk
cache. Confirm with `ogc_bench.py` against one instance versus round-robined
replicas at equal total connections.

Current loopback baselines (DuckDB 2.0 alpha, local disk): NW-Europe city
window, 8 connections, `ogc_bench.py`: ~50 rps, p50 ~130 ms nonempty
(large match sets sort by id — same SQL as ever); Berlin seed in kind:
171 rps, p50 34 ms. Mount-cache behavior with S3-GET accounting lives in
[`docs/mount-lake.md`](docs/mount-lake.md).

These are not universal capacity claims; run the included tools on the target
CPU, storage, shard size and response shape.

`just check test` runs gofmt, `go vet` and the Go unit suites (filter,
plan, index, Flight ticket validation). `just test-duckdb` boots the pooled
store over the fixture catalog (DuckDB 2.0 smoke: extensions, pinned
snapshot, frozen file list, pruned query). `just test-mount-cache`
validates mountpoint disk caching against SeaweedFS over real FUSE.
