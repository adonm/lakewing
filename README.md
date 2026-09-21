# lakewing

> **Rust implementation (current):** poem OGC API + read-only Arrow Flight over a
> Lance-Namespace-cataloged, tag-pinned Lance dataset (lance 12 release train),
> DuckDB for local exact rendering, and a foyer NVMe range cache under Lance's
> object store. See `docs/rust-architecture.md` for the design and measurements.
>
> **Go archive:** the previous Go/DuckLake/s3cache implementation lives on the
> `go-legacy` branch (tag `go-archive-v1`), including its benchmarks and the
> kind cachebench rig. The experiment history that motivated the rewrite is in
> `docs/lance-experiment.md`.

**Serve a tag-pinned Lance feature dataset on S3 through OGC REST and Arrow Flight.**

```text
OSM Layercake GeoParquet (WKB)
  → lakewing build  → GeoArrow Lance 2.2 dataset, BTREE(id) + RTREE(geom), tag
                       └─ Lance Namespace catalog (DirectoryNamespace, V1 layout)
                            └─ poem OGC REST / Arrow Flight (shared admission)
                                 ├─ lance 12: exact id/bbox selection (index pushdown)
                                 ├─ duckdb: connection-local GeoJSON/MVT rendering
                                 └─ foyer: NVMe range cache under the object store
```

One binary, two commands (`build`, `serve`), one pinned snapshot per process.
Apache-2.0.

## Quickstart

```sh
mise install                                   # rust, just, python
just setup                                     # protoc + cargo fetch
just check test                               # fmt, clippy -D warnings, 6 regression tests
just run -- build --source fixtures/berlin.parquet --out fixtures/berlin.lance --tag prod
just run -- --catalog-uri fixtures --table berlin --tag prod --listen 127.0.0.1:3000
```

```sh
curl localhost:3000/collections
curl 'localhost:3000/collections/buildings/items?sources=1&limit=10'
curl 'localhost:3000/collections/buildings/items?bbox=4.895,52.365,4.905,52.375&sources=1'
curl 'localhost:3000/collections/buildings/tiles/12/2103/1346?sources=1'
```

Serving requires a pinned tag or version (`--tag prod`); `--uri` bypasses the
catalog for local development only.

## HTTP API

| Route | Representation |
|---|---|
| `/collections`, `/collections/{id}` | Collection metadata + pinned snapshot |
| `/collections/{id}/items` | `application/geo+json` FeatureCollection |
| `/collections/{id}/items/{fid}` | Single feature (original geometry form) |
| `/collections/{id}/tiles/{z}/{x}/{y}` | Web-Mercator XYZ MVT; 204 if empty |
| `/metrics` | Prometheus exposition (`no-store`) |
| `/healthz` | Liveness |

Items support `bbox` (CRS84, validated), `limit` (1–10000, default 101),
`offset` (≤ 100000), `cursor`, `sources` (or `X-Source-Ids` header; both given
→ intersection), and `snapshot`. Unknown parameters return 400. Responses carry
strong ETags over the exact bytes, gzip variants, and
`Cache-Control: public, max-age=60`; `X-Lakewing-Snapshot` names the pinned
dataset version; errors are `no-store` JSON with proper status codes.

**Exactness first:** `bbox` runs exact intersection in Lance — GeoArrow via
`ST_Intersects` (RTREE pushdown), WKB via `ST_GeomFromWKB` — *before* ordered
keyset pagination, so a page that clips a polygon-hole false positive can never
skip or duplicate rows. Cursors are opaque tokens bound to the reader's
snapshot and query shape: replaying a cursor against a different snapshot is
409, against a different query 400.

## Arrow Flight

`ListFlights`, `GetFlightInfo`, `PollFlightInfo`, `GetSchema`, `DoGet` —
read-only, JSON tickets:

```json
{"collection":"buildings","bbox":[4.9,52.3,5.0,52.4],"columns":["id","geometry","properties","source_id"],"limit":10000,"sources":[1]}
```

Default columns `id`, `geometry` (original WKB — the stored MultiPolygon form is
restored via `was_polygon`), `properties`, `source_id`; optional `x`, `y`
(centroid) and any typed source column. Flight shares HTTP's admission
semantics (RESOURCE_EXHAUSTED on saturation), the same exact selection, and
streams batches with backpressure under a 30 s deadline.

## Admission and budgets

`--concurrency` (default 4) permits are shared by HTTP and Flight; saturation
rejects immediately with HTTP 429 + `Retry-After: 1` or Flight
RESOURCE_EXHAUSTED — no unbounded queueing. DuckDB runs on
`--concurrency` pooled connections with `--duck-threads` (default 1) and
`--duck-memory-mb` (default 512) per process; each connection owns its
temporary page table, so requests are isolated and cancellation cannot strand
a worker. Payloads are capped at 64 MiB per page (413 above), requests at a
30 s deadline (504 / DEADLINE_EXCEEDED).

Warm repeats of a successful page or tile are served from the bounded
rendered-response cache (`--response-cache-bytes`, default 256 MiB, 0
disables) keyed by the pinned snapshot plus the canonical selection — they
skip rendering and admission entirely. Hit/miss counters are on `/metrics`
(`lakewing_response_cache_*`).

## Object storage + cache

`--endpoint`, `--s3-key/--s3-secret` (or unsigned reads via a signing gateway)
flow to both the catalog and the dataset. The optional `--cache-dir` mounts a
foyer hybrid cache under Lance's object store:

- immutable objects only (`data/`, `_indices/`, `_deletions/`, `_versions/`);
  tags and mutable pointers keep origin semantics
- keys include the store prefix and endpoint, `get_or_fetch` single-flights
  concurrent misses, and a HEAD metadata cache serves range requests without
  re-heading the origin
- exclusive directory lock (two processes on one dir fail fast), disk tier
  flushed on shutdown, and `lakewing_cache_*` counters on `/metrics`

The disk tier is **per-pod**; the Helm chart gives each replica its own
subdirectory under a node-local hostPath. Cross-pod sharing would need a
shared cache service — see `docs/rust-architecture.md`.

## Build

```sh
just run -- build --source <parquet-or-dir> --out <dir>.lance --tag prod
```

WKB parquet → GeoArrow MultiPolygon (original polygon form kept in
`was_polygon`; nulls supported) → lance 2.2 fragments (512 MiB / 10M rows) →
BTREE(id) + RTREE(geom) → release tag → collection metadata
(`lakewing.collections`) written into the dataset so serves never scan for
layers at startup.

## Deployment

```sh
helm upgrade --install lakewing charts/lakewing \
  --set serve.catalogUri=s3://lake/catalog --set serve.tag=prod \
  --set serve.endpoint=http://s3-gateway:8080 --set serve.s3KeySecret=s3
```

Optional `serve.flightListen` / `serve.otelEndpoint` (OTLP gRPC, e.g.
`http://lgtm.monitoring:4317` — request spans include collection, examined-row
counts and payload timings).

## Verification

```sh
just check test                          # fmt + clippy -D warnings + 9 regression tests
python3 scripts/lancebench/battery.py http://127.0.0.1:3140 http://127.0.0.1:3141
python3 scripts/lancebench/flight_check.py grpc://127.0.0.1:50071 http://127.0.0.1:3140
python3 scripts/lancebench/load.py http://127.0.0.1:3140 none 16 3
```

The battery is a **gate** (non-zero exit on any digest mismatch). The
regression suite (`cargo test`) covers: exact polygon-hole filtering before
pagination, cross-source pagination with duplicate ids, cursor snapshot
binding, strict request validation (400/404/409), per-connection isolation
under concurrent load, DuckDB permit safety under cancellation, the 64 MiB
payload budget failing closed, Flight schema/geometry parity with HTTP,
admission exhaustion and release, non-spatial datasets (bbox → 400, null
geometry), and rendered-response-cache repeats (identical bytes, ETag
conditionals on cached entries, source isolation, disabled mode).

Layercake data is © [OpenStreetMap contributors](https://www.openstreetmap.org/copyright),
available under the [ODbL](https://opendatacommons.org/licenses/odbl/).
