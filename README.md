# pgvs3 — S3 on PostgreSQL

The absolute-fastest S3-compatible object store on PostgreSQL: the storage
layer under **DuckLake 2.0+** (catalog in PostgreSQL, data files on `s3://`
served by this gateway from PostgreSQL byte rows).

One object = one `s3p.objects` row + fixed-size `s3p.chunks` rows. A multipart
object is the ordered list of its part files: every part streams into its own
COPY the moment it arrives (in parallel, no staging), upload state lives in
PostgreSQL so any gateway can take any part, and Complete only publishes. Read
ranges are contiguous row-range queries (one per part file touched, fetched in
parallel) that stream exactly the requested bytes; fully covered rows return
verbatim, edge rows slice by memcpy.

Implemented with [`s3s`](https://crates.io/crates/s3s) (REST + SigV4 + hyper):
`HEAD`, `GET`(+`Range`, suffix ranges), `PUT`, `DELETE`, `ListObjectsV2`,
`ListBuckets`, `CreateBucket`, `HeadBucket`, multipart upload. Everything else
is `NotImplemented`. Designed for 100 GB–1 TB object stores with **no
caching** — consumers (e.g. DuckDB's `ExternalFileCache`) own caching.

## Quick start (fresh host)

Prereqs: [`mise`](https://mise.jdx.dev/) and Docker (for the Postgres service;
any PostgreSQL 13+ URL works instead — pass `--url`). Everything else comes
from `mise install`, which `just setup` triggers:

```sh
git clone <this repo> && cd pgvs3
just setup        # mise toolchain (rust, just, python, uv, duckdb) + cargo fetch
just dev-db       # PostgreSQL 18 in Docker (or point --url at one)
just smoke        # build, seed 256MiB, serve, byte-exact ranged GET
```

Then the workload tests and microbench:

```sh
just seed && just micro                          # latency matrix (start the gateway first)
just tpch sf=10 stack=lake-s3                    # TPC-H on DuckLake (stable duckdb)
just tpch2 sf=10 stack=lake-s3                   # same on the DuckDB 2.0 pre-release line
```

Manual control (the gateway serves on `127.0.0.1:8014` with SigV4 key
`cachebench`/`cachebench-local-only`):

```sh
./target/release/pgvs3 serve --addr 127.0.0.1:8014
./target/release/pgvs3 seed  --gigabytes 100     # optional load gen
./target/release/pgvs3 bench --endpoint http://127.0.0.1:8014
./target/release/pgvs3 stat
```

Talk to it like any S3 (path-style, SigV4):

```sh
curl --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
     -H 'Range: bytes=0-1023' http://127.0.0.1:8014/lake/some-object
```

DuckDB: `SET s3_endpoint='127.0.0.1:8014', s3_use_ssl=false, s3_url_style='path';`

An AWS test rig (Aurora Serverless v2 + EC2 in `platform-dev`) can be stood up
per the notes in `teardown-rig.sh`; that script tears it down.

## Storage layout (derived from PostgreSQL 18 source, not folklore)

`s3p.chunks(file_id int8, no int4, data bytea STORAGE EXTERNAL)` with
`WITH (toast_tuple_target = 8160)`, payload **8120 bytes per row**:

- `heaptoast.c` only pushes values out-of-line while
  `heap_compute_data_size > RelationGetToastTupleTarget(rel, TOAST_TUPLE_TARGET) - hoff`
  (phase 1/2; `TOAST_TUPLE_TARGET_MAIN` is the last-resort phase 4 bound).
  `toast_tuple_target` is capped at `TOAST_TUPLE_TARGET_MAIN =
  MaximumBytesPerTuple(1) = 8160` (`heaptoast.h`, `reloptions.c`).
- With the target raised and all columns `NOT NULL` (no null bitmap),
  `hoff = 24`, so `file_id(8) + no(4) + varlena(4) + 8120 = 8136 ≤ 8160 - 24`
  keeps rows **fully inline**: one 8160-byte tuple per 8 KB page (99.6% fill),
  zero TOAST rows, zero toast-pointer indirection.
- Slices are pure `memcpy`: `detoast_attr_slice()` only branches to
  `toast_fetch_datum_slice()` for on-disk externals, which inline values never
  are.
- Row math is arithmetic, not catalog lookups: byte range → row span
  `[off/8120, (off+len-1)/8120]`; `no int4` allows objects up to ~17 TB.

Why not TOASTed big values? A 1996-byte `TOAST_MAX_CHUNK_SIZE` layout has the
same page density (4 tuples/page) but 4× the rows, a second relation + toast
index per read, and a detoast indirection — with no read-amplification win at
8 KB page granularity.

## Measured

TPC-H SF10 on DuckLake (Postgres catalog, data files through this gateway):

| environment | load | cold pass | warm pass |
| --- | --- | --- | --- |
| local NVMe, DuckDB 2.0-alpha | 23.6 s | 7.8 s | 6.3 s |
| local NVMe, DuckDB 1.5.5 | 49.2 s | 11.8 s | 7.4 s |
| local NVMe, plain DuckDB (ceiling) | 23.5 s | 6.2 s | 6.4 s |
| Aurora Serverless v2 (EC2 → gateway → Aurora PG 18.6) | 144 s | 14.3 s | 9.0 s |

Range GETs (p50, conc=1):

| size | local warm | Aurora warm | Aurora cold >RAM | Aurora cold, bitmap plans |
| --- | --- | --- | --- | --- |
| 256 KiB | 0.66 ms | 2.66 ms | 14.1 ms | **2.8 ms** |
| 1 MiB | 1.60 ms | 3.49 ms | 18.7 ms | **3.8 ms** |
| 8 MiB | 8.9 ms (1.0 GB/s) | 15.4 ms (519 MB/s) | — | — |

- The proxy adds **~0.1–0.2 ms** over the raw-PG floor in every environment;
  everything else is storage latency (Aurora ≈ 2.1 ms RTT per round trip).
- **Bitmap heap scans (the default) are load-bearing on cold reads**: scattered
  8 KB page fetches become PG18 async read-stream prefetches — 5× on cold
  >RAM ranges, no warm cost. Opt out with `PGVS3_INDEXSCAN=1`.
- Ingest ≈ 275 MiB/s wall locally (binary COPY rows); storage overhead
  **≈ 1.3%** including indexes (8120-byte inline rows, one tuple per 8 KB page).
  `bench --sample N` spreads reads over N objects — essential for honest cold
  tests.

## Repo layout

- `crates/pgvs3/queries/` — vendored benchmark SQL: ClickBench `duckdb/queries.sql`
  (43, verbatim from ClickHouse/ClickBench) and Sedona-SpatialBench's 12
  DuckDB-dialect queries (from apache/sedona-spatialbench `print_queries.py`).
- `crates/pgvs3/analytics_bench.py` — ClickBench / SpatialBench on the
  lake-s3 / lake-local / plain stacks (load, timed passes, per-query timeout,
  cache telemetry). `tpch_bench.py` is the same for TPC-H; both share
  `benchlib.py`. Recipes: `just clickbench`, `just spatialbench sf=10`.

- `crates/pgvs3` — the service (`lib` + `bin`; the lib is embeddable, e.g.
  in-process in an application) plus `tpch_bench.py`, the DuckLake workload
  verification.
