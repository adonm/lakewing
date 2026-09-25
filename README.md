# pgvs3 — S3 on PostgreSQL

pgvs3 is a small S3-compatible gateway that stores objects in PostgreSQL. It is
built to be DuckLake's object store: DuckLake keeps its catalog in PostgreSQL,
and with pgvs3 the data files live there too, so every DuckDB that attaches
the catalog sees the same tables and the same data.

- One Rust binary with no local state: objects and in-progress multipart
  uploads live in PostgreSQL, so any number of gateways can share a database.
- An S3 subset: `GET` (with ranges), `HEAD`, `PUT`, `DELETE`, `ListObjectsV2`,
  `ListBuckets`, `CreateBucket`, `HeadBucket` and multipart uploads. Anything
  else returns `NotImplemented`.
- No data cache: clients such as DuckDB cache what they read. A per-process
  metadata cache only removes the lookup round trip per GET.

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or any PostgreSQL 13+: pass
`--url`).

```sh
just setup    # toolchain (rust, just, python, uv, kind, helm, kubectl) + cargo fetch
just dev-db   # PostgreSQL 18 in Docker, for running the gateway by hand
just smoke    # the tests: kind cluster up, every workload once at smoke scale
```

Run the gateway (SigV4 key `cachebench` / `cachebench-local-only` unless you
pass `--access-key` / `--secret-key`; change them anywhere but localhost):

```sh
./target/release/pgvs3 --url postgres://user:pass@host/db serve --addr 127.0.0.1:8014
```

Use it like any path-style S3:

```sh
curl --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
     -H 'Range: bytes=0-1023' http://127.0.0.1:8014/lake/some-object
```

DuckDB: `SET s3_endpoint='127.0.0.1:8014'; SET s3_use_ssl=false; SET s3_url_style='path';`

Other subcommands: `seed` (load generator), `bench` (GET latency/throughput
matrix plus the direct-PostgreSQL floor), `stat` (logical vs physical size).

## How it works

Two tables (`crates/pgvs3/schema.sql`): `s3p.objects` maps bucket/key to a
file id, size, sha256 ETag and, for multipart objects, the list of part files;
`s3p.chunks` holds each file as numbered rows of 8120 bytes.

- **Writes** stream into a binary `COPY`, one per PUT and one per multipart
  part, all in parallel. An object appears atomically when its `objects` row
  is written. Completing a multipart upload only checks and publishes the
  part list; no data moves.
- **Reads** turn a byte range into a row range by arithmetic (row =
  offset / 8120) and run one row-range query per 8 MiB. Bigger ranges run up
  to 8 of those in parallel on separate connections. Rows go to the client as
  they arrive.
- **Janitor** (at start, then hourly): aborts multipart uploads idle for
  24 hours and deletes chunk rows nothing references, such as those left by
  a gateway killed mid-write.
- **Layout version** (`s3p.layout`): a gateway refuses to run against a layout
  it was not built for.

Why 8120-byte rows (from the PostgreSQL 18 source, `heaptoast.c`,
`heaptoast.h`, `reloptions.c`): PostgreSQL moves a value out of line only when
the row exceeds the table's `toast_tuple_target`, which can be raised to at
most 8160 bytes. With that target, and no nullable columns, a row of
`file_id(8) + no(4) + length(4) + 8120` bytes plus its 24-byte header fits:
one row per 8 KB page, 99.6% full, no TOAST table and no detoasting on read.
Storage overhead is about 1.3% including indexes.

## Configuration

A flag overrides the same-named variable.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | local dev DB | PostgreSQL URL. `sslmode=require` encrypts but does not verify the server certificate. |
| `--addr` | `PGVS3_ADDR` | `127.0.0.1:8014` | Listen address; use `0.0.0.0:8014` in a container. |
| `--access-key` | `PGVS3_ACCESS_KEY` | `cachebench` | SigV4 access key. |
| `--secret-key` | `PGVS3_SECRET_KEY` | `cachebench-local-only` | SigV4 secret key. |
| | `PGVS3_POOL_MIN` | 64 | Connections opened at start and kept warm. Clamped to the max. |
| | `PGVS3_POOL_MAX` | 64 | Connections open at once: the gateway's share of a shared database, not the server's ceiling. A fleet multiplies it — the kind chart states the whole budget once (`connectionBudget`). |
| | `PGVS3_DURABLE=1` | off | Synchronous commits. Off is safe for objects (sha256-addressed, idempotent: a lost commit only means a retried PUT). |

`GET /healthz` is an unauthenticated liveness probe. `GET /_pgvs3/stats`
(SigV4-signed) returns one line of read counters; the gateway also logs it
every 60 s.

## Design decisions

Each is backed by a measurement on the AWS rig:

- **No row cache.** It hit ~0% of the time behind DuckDB's own cache and cost
  ~0.5 ms of CPU per request.
- **Connections are reused most-recently-used first** (`src/pg.rs`). TCP
  restarts slow start on a connection idle for over ~200 ms, so a FIFO pool
  hands out its coldest connection: 1.18 ms vs 2.17 ms for a 64 KiB fetch.
- **Rows stream to the client as they arrive**, not after the whole range:
  ClickBench passes 9% faster, 8 MiB GETs 15.2 → 13.4 ms.
- **Reads up to 8 MiB are one query.** Splitting them across connections
  (2 MiB or 1 MiB parts) made ClickBench and SpatialBench 5–20% slower.
- **Bitmap heap scans.** On cold data they are ~5× faster than index scans,
  thanks to PostgreSQL 18's read-ahead.
- **Chunks are hash-partitioned 32 ways.** One table caps at 32 TiB, parallel
  writers spread over 32 heaps, and each GET touches one partition.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging, and Complete moves no data.

## Performance

### AWS rig, 2026-09-25

EC2 c7gn.2xlarge running DuckDB 2.0 (pre-release) and the gateway, Aurora
Serverless v2 (PostgreSQL 18.6) in the same AZ. DuckDB's file cache is off so
every read goes through the gateway.

| Measure | Result |
| --- | --- |
| ClickBench, 10% of `hits` (43 queries) | ~7.4 s per pass; load 8.1 s |
| SpatialBench SF1 (Q1–Q7) | ~5.6 s per pass; load 10.1 s |
| GET p50, one client | 64 KiB 0.4 ms · 1 MiB 1.7 ms · 8 MiB 13.4 ms |
| GET throughput, 8 clients | ~3.8 GiB/s |
| Aurora round trip (`SELECT 1` over TLS) | 0.12 ms |

Where the time goes: about 80% of a pass is DuckDB's own compute (the same
queries served from DuckDB's cache). Reads are network-bound: one connection
moves ~650 MiB/s (EC2's single-flow cap) and the instance ~3.8 GiB/s. The
gateway adds ~0.25 ms per request, and Aurora spends most of its time waiting
to send (`Client:ClientWrite`).

### kind cluster, 2026-09-25

Three-node kind on a 16-core / 62 GiB workstation: PostgreSQL 18 in-cluster
(single instance, 2 GiB `shared_buffers`), DuckLake over pgvs3, Quickwit
0.9.1 single node with its metastore and splits on the S3 endpoint. Caches
stay on, so the warm pass is the point. Reproduce with `just kind-up` and
`just kind-bench`; results land in `.tmp/pgvs3/kind-bench.jsonl`.

| Measure | Result |
| --- | --- |
| pgbench scale 10, 16 clients | 1,487 tps · 10.8 ms average |
| TPC-H SF 10, 22 queries over DuckLake | 9.9 s cold · 6.1 s warm; load 111 s |
| ClickBench, full 10M-row `hits` (43 queries) | 8.4 s cold · 6.3 s warm; load 10 s |
| Quickwit search, 1M logs (template repeats — see below) | p50 7.9 ms · p95 18.6 ms; 96% under 20 ms |
| GET p50, one client | 64 KiB 0.8 ms · 1 MiB 2.2 ms · 8 MiB 12.1 ms |
| GET throughput, aggregate | 8 MiB 2.25 GiB/s at 128 readers · 1 MiB 1.89 GiB/s at 64 · 64 KiB 670 MiB/s |
| HEAD, aggregate | 28k req/s at 8 readers |
| PostgreSQL floor, 256 KiB direct | 0.49 ms · 485 MiB/s |

The ceiling is the PostgreSQL path: a single 8 MiB stream moves 629 MiB/s
and 128 concurrent ones 2.25 GiB/s, where the postgres process saturates
before the gateway does. DuckDB's file cache works as designed — ClickBench's
cold pass pulled 499 MiB through the gateway and the warm pass 209 MiB.

### Quickwit search at 100M logs, 2026-09-25

100M OTLP-schema logs through the ingest API (8 parallel batches, per-shard
limit raised 5 → 20 MiB/s): 542 s, 185k docs/s, zero backpressure retries.
Stored through pgvs3: 6.28 GB (12 published splits, 16.3 GiB uncompressed).

| Query shape, fresh requests | p50 | p95 | under 20 ms |
| --- | --- | --- | --- |
| Template repeats (Quickwit's result cache answers) | 3.0 ms | 29.6 ms | 93% |
| Random 10% time windows, default (`stable_log`) index | 50.3 ms | 146.4 ms | 0% |
| Random 10% windows, `merge_policy: no_merge` | 9.9 ms | 16.2 ms | 99.5% |

What the code says and the measurements confirm:

- Repeated templates are answered by the per-split result cache
  (`leaf_search_cache` in `leaf.rs`) — great for dashboards, flattering in
  benchmarks. Fresh requests are the honest number.
- Config knobs did not move the needle on this rig: a 20 GiB `split_cache`
  (whole splits on local disk), bigger caches, `count_all` off, an indexed
  timestamp field — all within noise. Warmups are ~13 MB per split-search
  and this rig's storage is nearly free (cold ≈ warm). Against slow object
  storage, `split_cache` and the caches should matter far more.
- A time-scoped query pays per *overlapping split*: the window filter
  evaluates each split's timestamp column, and the default `stable_log`
  merges smear time ranges — a 10% window overlapped 38M docs across two
  merged splits, and a 0.1% window cost the same as a 10% one.
- The tuning that matters for time-scoped log search is therefore index
  layout, not config: keep splits time-disjoint. The same change at 10M
  docs (p50 24.2 → 9.6 ms) and at 100M (p50 50.3 → 9.9 ms, 99.5% under
  20 ms) with `merge_policy: {type: no_merge}`. The trade-off is more
  splits for wide queries and no compaction.

## Benchmarks

Testing lives here too: `just smoke` is `kind-up`, the validate suite and a
smoke-scale `kind-bench`, and CI runs exactly that (`just ci` = fmt, clippy,
build, tests, smoke). One stack, one way to be wrong.

### The kind stack (local or EC2)

`just kind-up && just kind-validate && just kind-bench` brings up one cluster
and runs every workload in well under an hour with caching on — the same
commands on a laptop and on an EC2 instance, so numbers stay comparable.
`QUICK=1 just kind-bench` runs the whole set at smoke scale first: minutes,
to prove the wiring before spending an hour. `just kind-down` tears it down.

The stack: Postgres holds pgvs3's object rows and DuckLake's catalog — two
logical databases on one cluster, the simple single-writer shape. Quickwit
runs one node with its metastore and index splits on the endpoint
(`s3://…/metastore`), so it needs no metadata database at all.

The suites: `pgbench` (OLTP against the rows database), `tpch` (22 queries
over DuckLake), `click` (43 ClickBench queries over the canonical 13.8 GiB
`hits`), `search` (Quickwit on real OTLP-schema logs — latency against an
empty index is a lie) and `stress` (the gateway's concurrency ceiling: HEAD
and GET sweeps reporting aggregate MiB/s and req/s; `just kind-stress` runs
it alone).

Scale from the environment — `SF` (TPC-H), `PARTS` (ClickBench slices),
`DOCS` and `WINDOW_FRAC` (Quickwit logs and query windows), `SEED_GB`,
`REQUESTS`, `CONCURRENCY`, `SIZES`, `SECONDS_RUN` — for example
`SF=30 CONCURRENCY=1,8,32,64,128 just kind-bench`. Each workload's share of
PostgreSQL is stated once in `deploy/charts/postgres/values.yaml`
(`connectionBudget`) and derives `max_connections`. Results land in
`.tmp/pgvs3/kind-bench.jsonl`, one line per measurement; the full records are
in the suite job logs.

### Harness entry points (development)

For working on the gateway or the harness without a cluster: `just dev-db`
runs a plain PostgreSQL, `just seed && just micro` is the GET matrix, and
`just clickbench`, `just spatialbench sf=10` and `just tpch sf=10` drive the
DuckLake harnesses directly (pass arguments through
`extra='--set name=value'` for A/B runs).

### EC2 rig (kind with external Aurora PostgreSQL)

CI and a laptop use in-kind PostgreSQL. The EC2 rig runs **the same kind
charts and benchmark Jobs**, but pgvs3's object database and DuckLake's
catalog are two logical databases on **Aurora PostgreSQL 18.6 Serverless v2,
I/O-Optimized**. Quickwit remains single-node with its metastore and index
splits on pgvs3's S3 endpoint. This separates Aurora round trips and I/O from
the kind host while retaining the CI workload shape.

Configure the rig in the ignored `.env` (see `.env.example`) and authenticate
with the selected AWS profile before running the commands below.

```sh
just rig-up                       # tagged CloudFormation stack; sync, deploy, validate
just rig-validate                 # CI's kind smoke gate against Aurora
SUITES=tpch,click just rig-bench  # or run all five suites with just rig-bench
QUICK=1 just rig-bench            # smoke-scale benchmark
just rig-results                  # download latest JSONL after a disconnected run
just rig-status                   # stack status and endpoints
just rig-teardown                 # terminates the rig and its related resources
```

The rig uses an m7i.4xlarge (16 vCPU, 64 GiB), a 250 GiB gp3 volume and
Aurora scaling from 2 to 16 ACUs. Results are copied to
`.tmp/pgvs3/rig-out/`. **Both EC2 and Aurora keep accruing charges** until
`just rig-teardown`.

## Potential further gains

Small ones: reads are ~20% of a pass and network-bound. Measure before
building.

- **DuckDB read merging.** Try `parquet_prefetch_column_gap` values through
  `extra`; DuckDB already read ~10% fewer bytes once GETs got faster.
  A win is a user setting, not code.
- **Single large streams.** One 64 MiB GET gets ~1.7 of the ~3.8 GiB/s
  available, and SpatialBench's zone queries read ~640 MiB chunks (up to
  ~0.3 s per pass). Profile the forwarding path first.
- **Small-GET overhead.** The gateway adds ~0.25 ms per request, at most
  2–3% of a pass. Act only on a clear profile hotspot (SigV4, s3s, hyper).
- **Cold reads.** Aurora caps PostgreSQL 18's read merging at 128 KiB
  (`io_max_combine_limit`). Raising it is a parameter-group experiment on a
  dataset larger than the cache.
- **Outside the gateway.** A larger instance raises the ~3.8 GiB/s ceiling;
  fleets spread over AZs want an Aurora reader in each (a cross-AZ round trip
  costs ~7×).
- **Tried, no gain:** a proxy cache or prefetcher, splitting reads under
  8 MiB, bigger TCP buffers, DuckDB's curl HTTP client.

Before more tuning: integration tests against a local PostgreSQL (range
edges, multipart lifecycle, overwrite/delete, the orphan sweep).

## Running in a container

Published to GHCR on every push to `main` (amd64 and arm64, so AWS Graviton
works), built by `just image` from a two-stage Dockerfile: `rust:alpine` builds
a static musl binary, and the image is `scratch` — no shell, no libc, 14 MB.
All config comes from the environment (see Configuration), so Kubernetes only
needs a Deployment:

```sh
docker run -p 8014:8014 \
  -e PGVS3_URL="postgres://user:pass@host/db?sslmode=require" \
  -e PGVS3_SECRET_KEY=change-me \
  ghcr.io/adonm/pgvs3:latest
```

```yaml
containers:
  - name: pgvs3
    image: ghcr.io/adonm/pgvs3:latest
    env:
      - name: PGVS3_URL
        valueFrom: { secretKeyRef: { name: pgvs3, key: url } }
      - name: PGVS3_SECRET_KEY
        valueFrom: { secretKeyRef: { name: pgvs3, key: secret-key } }
      - name: PGVS3_ACCESS_KEY
        value: admin
    ports: [{ containerPort: 8014 }]
```

## Repo layout

- `crates/pgvs3/src/`: `main.rs` (CLI), `server.rs` (the S3 API on
  [s3s](https://crates.io/crates/s3s): routes, `/healthz`, the stats line),
  `db.rs` (object storage: metadata, reads, writes, multipart), `ingest.rs`
  (the `COPY` write path), `cache.rs` (metadata cache), `janitor.rs` (the
  orphan sweep), `stats.rs` (read counters), `pg.rs` (PostgreSQL pool and
  TLS), `bench.rs` and `seed.rs` (the GET matrix and load generator).
- `crates/pgvs3/schema.sql`: the storage layout.
- `crates/pgvs3/*.py` and `queries/`: the benchmark harness (ClickBench,
  SpatialBench, TPC-H).
- `deploy/`: `kind/cluster.yaml`, `kind/rig.yaml` (the EC2 +
  Aurora stack), `charts/` — `postgres`, `pgvs3`, `quickwit`, `kind-bench` —
  and `bench/`, the benchmark image (suite runners plus a copy of the
  harness, synced at build time).
- `justfile`: every task (`just` lists them); `.env.example`: the rig's
  settings.
