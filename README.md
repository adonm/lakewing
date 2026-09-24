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
- No cache: clients such as DuckDB cache what they read.

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or any PostgreSQL 13+: pass
`--url`).

```sh
just setup    # toolchain (rust, just, python, uv) + cargo fetch
just dev-db   # PostgreSQL 18 in Docker
just smoke    # build, seed 256 MiB, serve, check one ranged GET byte for byte
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
  24 hours and deletes chunk files nothing references, such as those left by
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

| Setting | Default | Meaning |
| --- | --- | --- |
| `--url` | local dev DB | PostgreSQL URL. `sslmode=require` encrypts but does not verify the server certificate. |
| `PGVS3_POOL_MIN` | 64 | Connections each gateway opens at start. Lower it for large fleets. |
| `PGVS3_DURABLE=1` | off | Synchronous commits. Off is safe for objects (sha256-addressed, idempotent: a lost commit only means a retried PUT). |

`GET /_pgvs3/stats` (SigV4-signed) returns one line of read counters; the
gateway also logs it every 60 s.

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

AWS rig, 2026-09-25: EC2 c7gn.2xlarge running DuckDB 2.0 (pre-release) and
the gateway, Aurora Serverless v2 (PostgreSQL 18.6) in the same AZ. DuckDB's
file cache is off so every read goes through the gateway.

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

## Benchmarks

- Local: `just clickbench`, `just spatialbench sf=10` and `just tpch sf=10`
  run DuckLake through the gateway; `just seed && just micro` is the GET
  matrix.
- AWS: copy `.env.example` to `.env` and fill in the rig's details. Then
  `just rig load` writes the datasets once, and `just rig quick`, `full`,
  `micro` and `verify` (a GET content check) only read them; results land in
  `.tmp/pgvs3/rig-out/`. `BENCH_EXTRA='--set name=value'` passes DuckDB
  settings through for A/B runs. `just rig-stop`, `rig-wait`, `rig-ssh` and
  `rig-teardown` do what they say.

## Repo layout

- `crates/pgvs3/src/`: `main.rs` (CLI), `server.rs` (S3 API on
  [s3s](https://crates.io/crates/s3s)), `db.rs` (storage: reads, writes,
  multipart, janitor, stats), `pg.rs` (PostgreSQL connections: pool and TLS),
  `bench.rs` and `seed.rs` (tools).
- `crates/pgvs3/schema.sql`: the storage layout.
- `crates/pgvs3/*.py` and `queries/`: benchmark harness (ClickBench,
  SpatialBench, TPC-H).
- `justfile`: every task, local and on the AWS rig (`just` lists them);
  `.env.example`: the rig settings.
