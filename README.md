# pgvs3 — S3 on PostgreSQL

pgvs3 is a small S3-compatible gateway that stores objects in PostgreSQL. It is
built to be DuckLake's object store: DuckLake keeps its catalog in PostgreSQL,
and with pgvs3 the data files live there too, so every DuckDB that attaches
the catalog sees the same tables and the same data.

- One Rust binary with no local state: buckets, objects and in-progress
  multipart uploads live in PostgreSQL, so gateways share one S3 namespace.
- An S3 subset: `GET` (with ranges), `HEAD`, `PUT`, `DELETE`, `ListObjectsV2`,
  `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket` and multipart
  uploads. Anything else returns `NotImplemented`.
- No data cache: clients such as DuckDB cache what they read. A per-process
  metadata cache only removes the lookup round trip per GET.

> Postgres is all you need. ;P — for durable bytes and metadata here; DuckDB
> and Quickwit still do the actual analytics and search.

## Results at a glance

On the EC2/Aurora test rig, pgvs3 served DuckLake's full ClickBench table
(99,997,497 rows) and SpatialBench SF10 (60M trips). With the kind benchmark's
pinned DuckDB **2.0.0 development build**, the 43 ClickBench queries took
**35.35 s first / 30.13 s warm**; four spatial area-of-interest queries took
**7.85 s first / 4.54 s warm**. Quickwit 0.9.1
indexed **100M logs** with OpenSearch Benchmark (OSB) at ~230k docs/s and zero
errors; fresh 10%-time-window search was **43.76 ms p50 / 52.04 ms p95** at
eight clients. Prior DuckDB 1.5.2 numbers remain below for context.

### Rough cost context, not a service benchmark

Illustrative public US East on-demand rates, assuming 730 active hours/month
(prices checked September 2026). These are **different architectures**, not
same-hardware or measured ClickHouse/OpenSearch results:

| Example | Compute if active 24/7 | Storage and caveat |
| --- | ---: | --- |
| This **test rig**: one m7i.4xlarge + 2–16 Aurora Serverless v2 I/O-Optimized ACUs | ~$1.12–$3.30/h (~$816–$2,411/mo) | Aurora ~$0.225/GB-month plus the EC2 gp3 volume (~$50/mo at 250 GB / 6k IOPS / 500 MiB/s); not a production HA topology. |
| ClickHouse Cloud **Enterprise**: two 32 GiB replicas (8 compute units) | ~$3.12/h (~$2,279/mo) | $25.30/TB-month of stored data; backups, transfer and actual sizing add cost. Not benchmarked here. |
| OpenSearch Serverless **NextGen**: *illustrative* four active OCUs | ~$0.96/h (~$701/mo) | Hot storage extra; can scale to zero when idle. Four OCUs are **not** validated to handle our 100M-log workload. |

Sources: [Aurora](https://aws.amazon.com/rds/aurora/pricing/) ($0.156/ACU-hour),
[EC2](https://aws.amazon.com/ec2/pricing/on-demand/) and
[m7i.4xlarge rate](https://cloudprice.net/aws/ec2/instances/m7i.4xlarge)
(~$0.8064/hour), [gp3](https://aws.amazon.com/ebs/pricing/),
[ClickHouse Cloud](https://clickhouse.com/pricing/) (Enterprise, AWS US East:
$0.39030/compute-unit-hour), and
[OpenSearch Service](https://aws.amazon.com/opensearch-service/pricing/)
(example $0.24/OCU-hour). Storage formats, replication, availability, support,
egress and sustained capacity differ. Use each vendor's calculator for a real
deployment estimate; don't infer a price/performance winner from this table.

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or any PostgreSQL 13+: pass
`--url`).

```sh
just setup    # toolchain (rust, mbx, just, python, uv, kind, helm, kubectl) + cargo fetch
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
     -X PUT http://127.0.0.1:8014/lake  # create the bucket once
curl --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
     -H 'Range: bytes=0-1023' http://127.0.0.1:8014/lake/some-object
```

DuckDB: `SET s3_endpoint='127.0.0.1:8014'; SET s3_use_ssl=false; SET s3_url_style='path';`

Other subcommands: `seed` (load generator), `bench` (GET latency/throughput
matrix plus the direct-PostgreSQL floor), `stat` (logical vs physical size).

## How it works

Five storage tables (`crates/pgvs3/schema.sql`): `s3p.buckets` stores S3-managed
bucket existence and creation times; `s3p.objects` maps bucket/key to a
file id, size, sha256 ETag and, for multipart objects, the list of part files;
`s3p.chunks` holds each file as numbered rows of 8120 bytes. The separate
`s3p.uploads` and `s3p.upload_parts` tables track incomplete multipart work.

- **Writes** stream into a binary `COPY`, one per PUT and one per multipart
  part. A direct PUT commits its chunks and object row in one transaction:
  interrupted writes cannot leave committed, unaddressable bytes. Parts
  commit with their upload record; Complete publishes the ordered part list
  without moving data. `If-None-Match: *` and strong `If-Match` PUTs are
  checked atomically at publication; mutations of the same key serialize
  so concurrent creates cannot strand chunks. Unsupported conditional DELETEs
  fail instead of silently deleting. Acknowledged writes use synchronous commits.
- **Reads** turn a byte range into a row range by arithmetic (row =
  offset / 8120) and run one row-range query per 8 MiB. Bigger ranges run up
  to 8 of those in parallel on separate connections. Rows go to the client as
  they arrive.
- **Maintenance** belongs to PostgreSQL: a bounded cleanup function reaps
  abandoned multipart parts after 24 hours (scheduled by `pg_cron` in kind
  and on Aurora), and per-partition autovacuum runs without a gateway janitor
  or manual table VACUUM. `kind-up` registers the cron job after schema setup;
  the standalone `dev-db` command does not install a scheduler.
- **Buckets** are explicit: create them through S3 before writing. Empty
  buckets remain listed; deleting a bucket with objects or an incomplete upload
  returns `BucketNotEmpty`. The kind harness provisions its three workload
  buckets through S3 at startup. The `seed` load generator creates its own
  bucket if absent.
- **Layout version** (`s3p.layout`): a gateway refuses to run against a layout
  it was not built for. The pre-release v3 bucket layout requires a fresh
  database; `just kind-reset` or `just rig-reset` discards only that
  environment's three disposable test databases and redeploys them.

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
| | `PGVS3_POOL_MAX` | 64 | Connections open at once, per gateway. Include all replicas, rollout headroom and other clients in the local Postgres `maxConnections` or Aurora connection budget. |

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
(single instance, 1 GiB `shared_buffers`), DuckLake over pgvs3, Quickwit
0.9.1 single node. **This run used Quickwit's S3 file-backed metastore**;
the current chart uses PostgreSQL for metadata and pgvs3 for index splits.
Caches stay on, so the warm pass is the point. Reproduce the workload shape
with `just kind-up` and `just kind-bench`; results land in
`.tmp/pgvs3/kind-bench.jsonl`.

| Measure | Result |
| --- | --- |
| pgbench scale 10, 16 clients | 1,487 tps · 10.8 ms average |
| TPC-H SF 10, 22 queries over DuckLake | 9.9 s cold · 6.1 s warm; load 111 s |
| ClickBench, 10% of canonical `hits` (10M rows, 43 queries) | 8.4 s cold · 6.3 s warm; load 10 s |
| Quickwit search, 1M logs (template repeats — see below) | p50 7.9 ms · p95 18.6 ms; 96% under 20 ms |
| GET p50, one client | 64 KiB 0.8 ms · 1 MiB 2.2 ms · 8 MiB 12.1 ms |
| GET throughput, aggregate | 8 MiB 2.25 GiB/s at 128 readers · 1 MiB 1.89 GiB/s at 64 · 64 KiB 670 MiB/s |
| HEAD, aggregate | 28k req/s at 8 readers |
| PostgreSQL floor, 256 KiB direct | 0.49 ms · 485 MiB/s |

The ceiling is the PostgreSQL path: a single 8 MiB stream moves 629 MiB/s
and 128 concurrent ones 2.25 GiB/s, where the postgres process saturates
before the gateway does. DuckDB's file cache works as designed — ClickBench's
cold pass pulled 499 MiB through the gateway and the warm pass 209 MiB.

### EC2 kind with Aurora I/O-Optimized, 2026-09-25 UTC

Three-node kind on an m7i.4xlarge (16 vCPU, 64 GiB), two pgvs3 replicas,
Quickwit core plus two search-only nodes, and Aurora PostgreSQL 18.6
Serverless v2 (2–16 ACUs, I/O-Optimized) in the same AZ. Pgvs3 objects,
DuckLake's catalog and Quickwit's metastore are separate databases on that
Aurora cluster; Quickwit split files live on pgvs3. The full run used caches
on and Quickwit's then-enabled disk split cache. The sample was `PARTS=10`
(10M ClickBench rows), TPC-H SF10, 1M Quickwit logs and an 8 GiB object seed.

| Measure | Result |
| --- | --- |
| pgbench scale 10, 16 clients | 1,763 TPS · 9.1 ms average |
| TPC-H SF10, 22 queries | 7.4 s first pass · 5.8 s warm; load 110 s |
| ClickBench 10M rows, 43 queries | 5.2 s first pass · 4.3 s warm; load 8.1 s |
| Quickwit repeated templates, 1M logs | p50 6.9 ms · p95 8.8 ms; 0 errors |
| 64 KiB GET, 32 readers | 847 MiB/s · 13.6k req/s |
| 1 MiB / 8 MiB GET throughput | ~1.4 GiB/s aggregate at 32–64 readers |
| Aurora direct floor, 256 KiB | p50 0.69 ms · 333 MiB/s |

The ~1.4 GiB/s plateau is observed end-to-end throughput, **not** proof that
Aurora itself is saturated; distinguish EC2 networking, pgvs3 and Aurora
with node/CloudWatch metrics before tuning. The full JSONL record is saved
locally under `.tmp/pgvs3/rig-out/` (not tracked in Git).

To test the cost of Quickwit's disk split cache on this same 1,005,000-log
index, we ran 400 fresh random 10%-time-window queries with the in-memory
caches enabled in every pass:

| Whole-split disk cache | p50 | p95 | Under 20 ms |
| --- | ---: | ---: | ---: |
| 20 GiB, before restart | 10.83 ms | 16.30 ms | 99.8% |
| Disabled, first pass after restart | 10.94 ms | 16.15 ms | 99.8% |
| Disabled, next pass | 11.11 ms | 16.41 ms | 99.8% |

There is no measured win for spending 20 GiB **per searcher** on disk here,
so the chart now leaves it disabled. This remote 1M-log A/B and the local
100M-log result below both support that choice, but they do not substitute
for an Aurora-backed 100M-log test. Quickwit's selective in-memory caches
remain on; putting entire splits in tmpfs would spend much more RAM without
evidence of a latency benefit.

### Aurora full rerun, 2026-09-26

The bucket/atomic-write layout on DuckDB 1.5.2 ran the same scale as the earlier EC2
kind full test: TPC-H SF10, ClickBench 10M rows, 1M Quickwit logs, 8 GiB seed,
two gateway replicas, one Quickwit core and two searchers. Disk split caching
is now off. The full result is in `.tmp/pgvs3/rig-out/` (ignored by Git).

| Measure | Current | Earlier full run |
| --- | ---: | ---: |
| pgbench scale 10, 16 clients | 1,675 TPS | 1,763 TPS |
| TPC-H 22 queries | 7.35 s first · 5.79 s warm | 7.4 s first · 5.8 s warm |
| ClickBench 43 queries | 5.36 s first · 4.28 s warm | 5.2 s first · 4.3 s warm |
| Quickwit repeated queries | p50 6.95 ms · p95 9.15 ms, 0 errors | p50 6.9 ms · p95 8.8 ms |
| 1–8 MiB GET, 8–64 readers | ~1.39 GiB/s | ~1.4 GiB/s |
| 64 KiB GET, 32 readers | 611 MiB/s | 847 MiB/s |

An additional 400 fresh 10%-time-window Quickwit queries against the loaded
1M-log index had p50 14.7 ms, p95 18.7 ms and zero errors. Small-GET
throughput merits a controlled A/B; the unchanged large-GET plateau does not
rule out a regression there. Minute-granularity writer metrics reached 16 ACUs
and 147 connections, with CPU at most 57% for a minute. Those metrics cannot
identify the 1.39 GiB/s bottleneck by themselves; EC2 network metrics were
only available at five-minute resolution.

### OpenSearch Benchmark at 100M Quickwit logs, 2026-09-26

The current PostgreSQL-metastore Quickwit 0.9.1 cluster (one indexer and two
search-only nodes, disk split cache off) indexed a fresh 100M-document OTLP
corpus through OSB 2.4's ES `create` bulk operation. The deterministic corpus
was 20,218,013,718 bytes (SHA-256
`1809bdce44079641a640f7eabee1fd922e497279a05d0dcc189501136f04b234`).
OSB reported zero bulk errors and 230k docs/s mean bulk throughput with one
ordered client; the complete corpus became searchable in 558 seconds after
generation. The adapter checks exact indexed count before measuring queries.

| Fresh severity-filtered 10%-time-window search | p50 | p99 | Throughput |
| --- | ---: | ---: | ---: |
| 1 client | 34.1 ms | 44.6 ms | 28.7 ops/s |
| 8 clients | 43.6 ms | 55.0 ms | 179.5 ops/s |

The run used Quickwit's default `stable_log` merge policy, not the tuned
`no_merge` layout from the historical native-API 100M run below. These are
OSB results from a portable custom workload, not stock-track or head-to-head
OpenSearch results. The complete OSB metrics are in ignored
`.tmp/pgvs3/rig-out/osb-100m-20260926.json`.

### Full-scale analytics on Aurora, 2026-09-26

With DuckDB 1.5.2, the canonical ClickBench `hits.parquet` loaded
**99,997,497 rows** into DuckLake in 47.9 seconds. All 43 queries passed:
34.19 seconds on the first pass and 27.21 seconds warm. Per-gateway GET counts
from this run are not reliable: containers shared PID 1, so the harness could
mix replica counters.

Sedona-SpatialBench SF10 loaded 60M trips and 454,710 zones in 27.1 seconds.
The area-of-interest subset Q1–Q3 and Q6 passed in 9.05 seconds first pass
and 3.64 seconds warm:

| AOI query | First pass | Warm |
| --- | ---: | ---: |
| Q1: nearby trip pickups | 1.87 s | 0.44 s |
| Q2: county intersection count | 2.04 s | 0.80 s |
| Q3: buffered-box monthly stats | 0.75 s | 0.59 s |
| Q6: bounding-box zone/trip stats | 4.38 s | 1.81 s |

The first full ClickBench attempt hit the benchmark Pod's 8 GiB memory limit;
the full Aurora profile now permits 24 GiB with a 16 GiB DuckDB working
limit. CI smoke keeps its smaller limit. Results live in ignored
`.tmp/pgvs3/rig-out/`; these are DuckDB-over-DuckLake query times, not
direct PostgreSQL scan timings.

### DuckDB 2.0 full-scale rerun, 2026-09-26

The same 99,997,497 ClickBench rows loaded in 50.39 seconds. All 43 queries
passed in 35.35 seconds first pass and 30.13 seconds warm. SpatialBench SF10
loaded 60M trips in 25.52 seconds; the AOI subset took 7.85 seconds first
pass and 4.54 seconds warm:

| AOI query | First pass | Warm |
| --- | ---: | ---: |
| Q1 | 2.70 s | 0.58 s |
| Q2 | 2.45 s | 1.25 s |
| Q3 | 0.77 s | 0.77 s |
| Q6 | 1.93 s | 1.94 s |

With the retained 100M-log index, a search-only OSB pass had zero errors:
fresh 10%-window searches at eight clients were p50 43.76 ms, p95 52.04 ms,
p99 57.09 ms (176 ops/s). The workloads and query times are valid, but
per-gateway byte/GET deltas in these runs could mix two PID-1 pods; those
counters are excluded from cross-version comparisons. This is one pass per
version, not a controlled multi-run DuckDB A/B.

### Aurora overwrite/abort churn, 2026-09-26

`just rig-churn` overwrote one key 128 times with 32 MiB PUTs (4 GiB written),
periodically deleted it and aborted multipart parts while another gateway read
a stable 256 KiB range. The test found no stranded chunks. Across 683 reads,
read p95 rose from 2.8 ms before churn to 18.0 ms during it; PUT p95 was
449 ms and DELETE p95 46 ms. Estimated dead chunk tuples rose from 3,655 to
26,736 immediately after churn, then fell to 2,844 after 90 seconds as
partition autovacuum counts increased by 93. Partition storage stayed near
1 GiB. This is one run, not proof of the cause of the read-latency increase;
profile pool waits and Aurora I/O before moving deletes to a queue.

### Quickwit search at 100M logs, 2026-09-25

This historical run used a file-backed S3 metastore; current deployments
use PostgreSQL for metadata. 100M OTLP-schema logs through the ingest API
(8 parallel batches, per-shard limit raised 5 → 20 MiB/s): 542 s, 185k
docs/s, zero backpressure retries.
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
  storage, `split_cache` and the caches might matter more; the chart now
  disables the disk cache by default until that is measured.
- A time-scoped query pays per *overlapping split*: the window filter
  evaluates each split's timestamp column, and the default `stable_log`
  merges smear time ranges — a 10% window overlapped 38M docs across two
  merged splits, and a 0.1% window cost the same as a 10% one.
- The tuning that matters for time-scoped log search is therefore index
  layout, not config: keep splits time-disjoint. The same change at 10M
  docs (p50 24.2 → 9.6 ms) and at 100M (p50 50.3 → 9.9 ms, 99.5% under
  20 ms) with `merge_policy: {type: no_merge}`. The trade-off is more
  splits for wide queries and no compaction.

### Read-scaling topology

Keep writes deliberately simple: one DuckLake catalog writer and one
Quickwit indexer/control-plane node; add query-only DuckDB workers, pgvs3
gateways and Quickwit **searcher-only** nodes as read load grows. Quickwit
[recommends PostgreSQL for distributed metadata](https://quickwit.io/docs/configuration/metastore-config):
the former file-backed S3 metastore has no cross-process write lock. The
three databases share one PostgreSQL cluster (Aurora on EC2), while immutable
Quickwit splits stay on pgvs3. The one-indexer chart uses a `Recreate` rollout
to avoid temporarily running two writers. `QUICKWIT_SEARCHERS=2 just kind-up`
adds search-only replicas behind the `quickwit` query Service; ingestion and
admin calls use `quickwit-core`. A
[headless DNS seed](https://quickwit.io/docs/configuration/node-config)
exposes direct gossip **UDP 7280** and gRPC **TCP 7281**. In kind, the core
and both searchers joined the same cluster, and the query service searched
logs ingested through the core (100 queries, zero errors). This verifies
discovery and routing, **not** performance scaling at 100M records. The
earlier two-node experiment failed because a ClusterIP seed did not carry
gossip.

Search performance depends on time-pruning **and** bounded split counts.
`no_merge` won on our sequential timestamped dataset, but Quickwit discourages
it for general searches. A better next A/B is an ingest-time day/hour key and
[partitioned splits](https://quickwit.io/docs/overview/concepts/querying#partitioning):
Quickwit keeps partitions isolated during merges, so it can compact within a
time bucket without smearing time ranges across buckets. Measure wide scans
and scale transitions too, not only the 10%-window p95.

KEDA can eventually control that searcher Deployment from Prometheus
`quickwit_search_root_search_requests_total{kind="server"}` (QPS) and
`quickwit_search_leaf_search_single_split_tasks{status="pending"}` (queue
pressure); both are exposed by this Quickwit version. Keep at least one warm
searcher and a slow scale-down to avoid cache churn; use the root-search
latency histogram as an SLO check, not an untested scaling threshold. If a
disk `split_cache` is enabled later, budget its full size per replica; do not
mount whole splits in tmpfs just to cache them in RAM. Quickwit's fast-field
and footer caches already do that selectively. Scale pgvs3 replicas from
read concurrency, but cap the fleet by Aurora's
connection budget (`replicas × PGVS3_POOL_MAX`, plus other clients and
rollout headroom). Neither Aurora ACUs nor a KEDA trigger make connections
unlimited. Do **not** autoscale the one ingest writer: Quickwit's ingest
queue uses local disk, so reliable restart needs a persistent WAL or an
upstream durable queue/replay path before treating that pod as disposable.

## Benchmarks

Testing lives here too: `just smoke` is `kind-up`, the validate suite and a
smoke-scale `kind-bench`, and CI runs exactly that (`just ci` = `hk check
--all`, Rust build/unit tests, kind contract tests and smoke workloads).
`hk.pkl` checks Rust formatting/Clippy, shell and Python syntax, chart
rendering and the justfile with the same mise-pinned tools on developer
machines and CI. Install repository-local, mise-aware Git hooks once with
`mise exec -- hk install --mise`; checks never fix/stash/stage files during a
commit. One stack, one way to be wrong.
Mise installs Mr Boxington (`mbx`) for the native Rust builds in `just ci`
and the benchmark image build; the CI check job restores its Cargo cache with
`jdx/mr-boxington-action`. The published multi-arch Docker image compiles
inside its own builder without a remote compiler cache.

### The kind stack (local or EC2)

`just kind-up && just kind-validate && just kind-bench` brings up one cluster
and runs every workload with caching on — the same
commands on a laptop and on an EC2 instance, so numbers stay comparable.
`QUICK=1 just kind-bench` runs the whole set at smoke scale first: minutes,
to prove the wiring before spending an hour. `just kind-down` tears it down.
The scripts always address Kubernetes context `kind-pgvs3` explicitly; they
cannot accidentally install benchmarks into another current cluster.
Use `QUICKWIT_SEARCHERS=2 just kind-up` to prove read-only searcher scaling;
default zero additional searchers preserves the single-node CI shape.

The stack: one PostgreSQL cluster holds three logical databases — pgvs3's
object rows, the DuckLake catalog and Quickwit's metastore. Quickwit runs
one indexer/control-plane node and stores immutable index splits on pgvs3.
One idempotent kind Job creates the three databases before Quickwit starts,
both locally and against Aurora.

| Chart | Local and CI kind | EC2 kind |
| --- | --- | --- |
| `postgres` | One standalone PostgreSQL 18 pod and PVC; not a PostgreSQL HA set | Omitted; Aurora is external |
| `pgvs3` | Two stateless gateway replicas | Same Deployment; DB URL from a Kubernetes Secret |
| `quickwit` | One core node + optional search-only replicas; PostgreSQL metastore, splits on pgvs3 | Same chart |
| `kind-bench` | One short-lived Job per invocation | Same Jobs; DB settings from the same Secret |

`just` is the interface; `deploy/kind/up.sh`, `db.sh` and `bench.sh`
orchestrate these small charts. `PGVS3_DB_SECRET` selects an external database
configuration; unset uses local PostgreSQL.

The suites: `pgbench` (OLTP against the rows database), `tpch` (22 queries
over DuckLake), `click` (43 ClickBench queries over the full 100M-row
`hits.parquet`), `spatial` (Sedona-SpatialBench AOIs Q1–Q3 and Q6 over SF10),
`search` (OpenSearch Benchmark 2.4 bulk ingestion and fresh time-window queries
against Quickwit) and `stress` (the gateway's GET concurrency ceiling).
The standard full run defaults to 100M Quickwit logs, the full ClickBench
dataset and SpatialBench SF10; `QUICK=1` keeps CI small (5k logs, 1M ClickBench
rows, SpatialBench SF0.1). Use the EC2 rig for the large profile; its source
downloads and generated corpus need tens of GiB of temporary disk. Reset the
disposable rig databases before repeating a 100M-log ingestion run.
Full Aurora benchmark Jobs allow 24 GiB of container memory with a 16 GiB
DuckDB working limit; CI smoke retains the smaller default Job budget.

Scale from the environment — `SF` (TPC-H), `PARTS` (ClickBench 1% slices;
`0` is the full dataset), `SPATIAL_SF` and `SPATIAL_QUERIES` (AOI subset),
`DOCS` and `WINDOW_FRAC` (Quickwit logs and query windows), `SEED_GB`,
`REQUESTS`, `CONCURRENCY`, `SIZES`, `SECONDS_RUN` — for example
`SF=30 CONCURRENCY=1,8,32,64,128 just kind-bench`. The local-only PostgreSQL
chart caps connections at 384: two 64-connection gateway pools normally,
temporary warm pools during rolling updates, DuckLake/benchmark clients and
headroom. If the gateway replica or pool count changes, update
`maxConnections` in `deploy/charts/postgres/values.yaml`.
Aurora-backed kind does not install that chart. Results land in
`.tmp/pgvs3/kind-bench.jsonl`, one line per measurement; individual logs
are in `.tmp/pgvs3/jobs/`.

The Quickwit search suite uses the same OSB corpus, ES `create` bulk requests,
and query DSL that can be run against OpenSearch. A loopback-only benchmark
adapter supplies two OSB node-info responses missing from Quickwit 0.9.1;
`_bulk` goes to the core and `_search` to the search service unchanged. OSB's
per-item errors must be zero **and** the indexed count must match the corpus
before search starts. The driver prints the corpus SHA-256 and both OSB runs'
operation metrics. This is a portable custom workload, **not** a stock OSB
track or a claim that the old native-API search numbers are cross-engine
comparable. Quickwit's ES compatibility root identifies itself as ES 7.17 in
OSB's raw metadata; the deployment is Quickwit 0.9.1, and OpenSearch node
telemetry is not reported for it.

To generate exactly the same corpus and OSB workloads for another ES-compatible
stack:

```sh
OSB_OUT_DIR=.tmp/pgvs3/osb-portable python3 deploy/bench/suites/osb_run.py \
  --generate-only --docs 100000000 --index otel-logs-v0_9
```

An example OpenSearch mapping is in
`deploy/bench/workloads/opensearch-otel-mapping.json`. Pre-create the index,
then run OSB `benchmark-only` against the generated `ingest/` workload,
followed by `search/` with `OSB_DOCS=100000000 OSB_WINDOW_FRAC=0.1` and
`--randomization-enabled --randomization-repeat-frequency=0`. Compare only
equivalent index settings, data counts and client counts. The `search/`
workload uses one and eight concurrent clients; ingestion has one ordered bulk
client. Corpus generation is outside OSB ingest timing; `indexed_s` also
includes the wait until every document is searchable.
The generated 1k-document corpus and both OSB stages were smoke-tested against
OpenSearch 2.19.4 with zero errors, an exact indexed-count check and nonempty
matching time-window searches. No 100M OpenSearch comparison has been run yet.

### Harness entry points (development)

For working on the gateway or the harness without a cluster: `just dev-db`
runs a plain PostgreSQL, `just seed && just micro` is the GET matrix, and
`just clickbench`, `just spatialbench sf=10` and `just tpch sf=10` drive the
DuckLake harnesses directly (pass arguments through
`extra='--set name=value'` for A/B runs).

### EC2 rig (kind with external Aurora PostgreSQL)

CI and a laptop use in-kind PostgreSQL. The EC2 rig runs **the same kind
charts and benchmark Jobs**, but pgvs3's objects, the DuckLake catalog and
Quickwit's metastore are three logical databases on **Aurora PostgreSQL 18.6
Serverless v2, I/O-Optimized**. Quickwit has one core writer and optional
search-only replicas, with its index splits on pgvs3's S3 endpoint. This
separates Aurora round trips and I/O from the kind host while retaining the CI
workload shape.

Configure the rig in the ignored `.env` (see `.env.example`) and authenticate
with the selected AWS profile before running the commands below.

```sh
just rig-up                       # tagged CloudFormation stack; sync, deploy, validate
just rig-contract                 # Rust S3 contract against Aurora's gateway pods
just rig-churn                    # opt-in large overwrite/delete + multipart churn and DB stats
just rig-validate                 # CI's kind smoke gate against Aurora
SUITES=tpch,click just rig-bench  # or run all six suites with just rig-bench
QUICK=1 just rig-bench            # smoke-scale benchmark
just rig-reset                    # pre-release test data only; explicit confirmation
just rig-results                  # download latest JSONL after a disconnected run
just rig-status                   # stack status and endpoints
just rig-teardown                 # terminates the rig and its related resources
```

`just kind-churn` and `just rig-churn` run a separate, opt-in two-gateway test
over 64 × 32 MiB PUTs (override with `PGVS3_CHURN_ROUNDS` and
`PGVS3_CHURN_MIB`). They audit old chunk file IDs and aborted parts, measure
read and write p95, and sample partition dead tuples/autovacuum before and
90 seconds after. Aurora churn logs go to `.tmp/pgvs3/rig-out/`; the test
cleans its objects through S3. These are measurements, not an assertion that
autovacuum shrinks relation files.

The rig uses an m7i.4xlarge (16 vCPU, 64 GiB), a 250 GiB gp3 volume and
Aurora scaling from 2 to 16 ACUs. Results are copied to
`.tmp/pgvs3/rig-out/`. **Both EC2 and Aurora keep accruing charges** until
`just rig-teardown`.

The AMI needs only a small mise installer in cloud-init. After the checkout
arrives, `mise -E ec2 bootstrap --yes` applies `mise.ec2.toml`: host Docker
packages/service/group plus the same pinned tools as CI. CI and local
development run only `mise install`, so EC2 host changes never leak into CI.

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

Before more tuning: run `just kind-contract` against the two gateway pods
(bucket lifecycle, range edges, multipart, overwrite/delete, scheduled expiry).

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
  (the `COPY` write path), `cache.rs` (metadata cache), `warmup.rs` (index
  warmup), `stats.rs` (read counters), `pg.rs` (PostgreSQL pool and
  TLS), `bench.rs` and `seed.rs` (the GET matrix and load generator).
- `crates/pgvs3/schema.sql`: the storage layout.
- `crates/pgvs3/*.py` and `queries/`: the benchmark harness (ClickBench,
  SpatialBench, TPC-H).
- `deploy/`: `kind/cluster.yaml`, `kind/up.sh`, `kind/db.sh`, `kind/bench.sh`,
  `kind/rig.yaml` (the EC2 +
  Aurora stack), `charts/` — `postgres`, `pgvs3`, `quickwit`, `kind-bench` —
  and `bench/`, the suite runners and benchmark image. The image copies the
  canonical harness directly from `crates/pgvs3`; there is no tracked duplicate.
- `justfile`: every task (`just` lists them); `mise.ec2.toml`: the opt-in EC2
  host bootstrap; `.env.example`: the rig's
  settings.
