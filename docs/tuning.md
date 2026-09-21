# Cache and index tuning

Response caching defaults to **off**. These controls tune reusable data and
index pages. Byte budgets below are per reader process, not per request.

## Auto-derived budgets

Every size-oriented budget may be left unset (CLI flag absent, chart value
empty), in which case startup derives it from the resources the pod actually
has — the cgroup memory limit (v2 `memory.max`, v1 fallback, `/proc/meminfo`
last), the effective CPU quota (cgroup `cpu.max`/CFS, affinity), and the
free space of the cache directory's filesystem — and logs both the detected
resources and the effective values. Explicit flags always win.

| Flag | Derivation when unset |
|---|---|
| `--cache-bytes` | quarter of cache-volume free space, clamped 64 MiB..256 GiB |
| `--cache-memory-bytes` | memory limit / 128, clamped 16 MiB..1 GiB |
| `--cache-metadata-bytes` | memory limit / 4096, clamped 1 MiB..16 MiB |
| `--cache-fetch-concurrency` | CPUs × 4, clamped 8..64 |
| `--lance-index-cache-bytes` | memory limit / 32, clamped 64 MiB..2 GiB |
| `--lance-metadata-cache-bytes` | memory limit / 128, clamped 8 MiB..512 MiB |

The formulas reproduce the previous static defaults on an 8 GiB / 4-CPU pod
(64 MiB / 2 MiB / 16 / 256 MiB / 64 MiB). They are deliberately small
fractions: these budgets coexist with DuckDB's memory limit, in-flight Arrow
batches and engine buffers, so treat them as shares, not as the pod's whole
memory. The disk derivation assumes the cache directory is (mostly)
dedicated; on a shared volume set `--cache-bytes` explicitly. Geometry
controls (block sizes, alignment threshold, admission ceiling) are workload
choices, not resource shares, and keep static defaults. Admission
(`--concurrency`) and DuckDB budgets stay explicit for the same reason.

## Reader controls

| Flag | Default | Purpose |
|---|---:|---|
| `--cache-dir` | unset | Enable the foyer raw-byte cache on node-local NVMe |
| `--cache-bytes` | auto | Disk tier; quarter of volume free space |
| `--cache-memory-bytes` | auto | Raw data/index byte RAM tier |
| `--cache-metadata-bytes` | auto | Immutable-object HEAD metadata RAM |
| `--cache-block-bytes` | 256 KiB | Data block grid; 0 = exact ranges |
| `--cache-index-block-bytes` | 64 KiB | Separate `_indices/` grid; 0 = exact ranges |
| `--cache-align-max-blocks` | 4 | Reads at least this many blocks wide use one exact entry |
| `--cache-max-range-bytes` | 8 MiB | Larger reads bypass cache admission |
| `--cache-fetch-concurrency` | auto | Global cached-range fill GET/HEAD concurrency, shared across requests/stores |
| `--lance-index-cache-bytes` | auto | Decoded Lance index RAM; 0 = disabled |
| `--lance-metadata-cache-bytes` | auto | Lance file metadata RAM; 0 = disabled |

Raw foyer bytes and decoded Lance pages are separate budgets. Add DuckDB's
memory budget, in-flight Arrow batches, query-engine buffers, and allocator
overhead when sizing a pod; the sum of cache settings is not an RSS limit.
Cache fills are semaphore-limited after single-flight coalescing, so waiters
for the same block do not each consume an origin slot. Delegated whole-object,
conditional and oversized streaming reads retain the origin implementation;
the fill limit does not bound those streams.

Block sizes accept 0 or 4 KiB..4 MiB and must fit the admission ceiling
(4 KiB..64 MiB). Alignment threshold accepts 1..64 and fill concurrency 1..256.
Invalid values fail startup. Both `--flag=value` and `--flag value` work.

### Choosing settings

- Nearby tiles and jittered bbox queries reuse aligned data blocks. Larger
  blocks can reduce origin GETs but overfetch sparse point lookups. Tune data
  and index blocks independently; index probes commonly need smaller blocks.
- Long scans should not evict the entire small working set: lower the range
  admission ceiling when one-pass large reads dominate. This only excludes
  individual large ranges; many small reads from a scan can still enter.
- Increase decoded-index RAM when repeatedly decoding/probing the same index
  pages costs more than fetching their raw bytes. This memory is additional
  to foyer RAM, even when the disk tier is large.
- Raise fill concurrency only while origin latency and available bandwidth
  justify it. Hits bypass the semaphore; admission still bounds HTTP/Flight work.
- The HEAD cache is bounded and evicts; a HEAD can recur after eviction or
  restart. Immutable-object identity includes endpoint and store prefix.
- Foyer directories are single-process and locked. Explicit `close()` flushes
  and the regression suite verifies disk reuse on reopen. Signal-driven
  graceful shutdown is a separate follow-up; do not assume SIGKILL flushes RAM.

### Metrics

All names below have the `lakewing_cache_` prefix and `_total` suffix:

| Counter | Meaning |
|---|---|
| `requests`, `requested_bytes` | Normalized logical ranges entering the cached-range path |
| `lookups` | Cache-entry lookups (one or more per logical range) |
| `memory_hits`, `disk_hits` | Entry results served from those tiers |
| `origin_ranges`, `origin_bytes` | Physical GETs/returned bytes through the cached-range path, including `get_ranges` admission bypasses |
| `origin_heads` | Physical HEADs used to populate metadata |
| `bypass_ranges` | Oversized `get_ranges` reads not admitted |
| `bypass_get_opts` | Delegated requests, including mutable/conditional/HEAD/whole-object/oversized streaming gets |

Use **deltas** over the same workload. `(memory_hits + disk_hits) / lookups`
is an entry hit fraction, not a logical-request fraction. Single-flight
waiters served by an in-progress origin fetch have source `Outer`, so their
benefit appears in fewer physical GETs, not memory-hit counts.
`origin_bytes / requested_bytes` captures net origin-byte demand for this path
(overfetch plus reuse). Use an origin-side meter to include delegated traffic.

## Writer/index controls

`build` and `index` share these controls:

| Flag | Default | Tradeoff |
|---|---:|---|
| `--btree-page-rows` | 4096 | Smaller ID leaf pages fetch fewer bytes per point probe but increase page count |
| `--rtree-page-rows` | 4096 | Spatial tree page fanout; smaller pages trade probe size against tree depth/GETs |
| `--zonemap-rows` | 2048 | Smaller bbox zones prune more tightly on spatially clustered data; larger index metadata |
| `--bitmap-max-values` | 4096 | Only build layer/source bitmaps for 2..this many distinct non-null values; 0 skips creation |

BTREE pages accept 64..65536 rows, RTREE pages 16..65536 entries,
zonemaps 64..1048576 rows, bitmap cardinality 0 or 2..65536. These are validated
before writes and passed to Lance's public scalar-index parameter API.

- `id_idx`: BTREE for OGC point lookups, cursor predicates and payload IN windows.
- `geom_idx`: RTREE on native GeoArrow geometry. It supplies bounding-box
  candidates; Lance retains exact `ST_Intersects` semantics before pagination.
- `xmin/ymin/xmax/ymax_zonemap`: pruning for bbox comparisons, including the
  coarse contained/boundary selection. Source spatial clustering matters;
  the builder preserves source order and does not spatially sort arbitrary input.
- `layer_bitmap`, `source_id_bitmap`: categorical equality/IN pruning for
  multi-collection/source data, usable alongside spatial and ID indexes.
  Singletons are skipped because matching them prunes nothing. High-cardinality
  columns are skipped after a bounded distinct-value probe.

```sh
# Install missing serving indexes on the latest snapshot; keep existing indexes.
lakewing index --uri s3://lake/features.lance --tag indexed-v2 --zonemap-rows 2048
# Rebuild lakewing's named indexes with new parameters.
lakewing index --uri s3://lake/features.lance --tag indexed-v3 --replace \
  --btree-page-rows 2048 --rtree-page-rows 1024 --zonemap-rows 1024
```

This is single-writer index maintenance. It commits new versions, reuses data
files, logs index statistics/coverage, and creates the requested **new** tag only
after all requested work succeeds. It fails early if the tag already exists.
An interrupted run can leave intermediate untagged versions; existing tags
are not moved. `--replace` replaces named serving indexes that are eligible
for creation; disabling bitmaps does not delete previously published indexes.
Namespace registration and append/delete compaction remain separate work.

## Reproducible sweeps

Use the same binary, CPU allocation, dataset data, cache budgets and request
sequence for each case. `scripts/lancebench/tune.py` runs distinct overlapping
city bboxes, neighboring tiles, wide queries, and a 32-request concurrent bbox
mix. It fails on HTTP errors or any result mismatch. GeoJSON comparison keeps
all fields, normalizing only snapshot versions in links/cursors; MVT is compared
byte-for-byte. It records first-pass and repeat timings plus cache-counter deltas.

```sh
mise exec -- python scripts/lancebench/tune.py \
  --uri .tmp/tuning/geo.lance --out .tmp/tuning/index-results \
  --case original:prod:262144:65536 --case tuned:indexed-v2:262144:65536

mise exec -- python scripts/lancebench/tune.py \
  --uri s3://lake/path/geo.lance --endpoint http://127.0.0.1:8336 \
  --out .tmp/tuning/cache-results --selective-only \
  --case exact:prod:0:0 --case blocks:prod:262144:65536
```

The cache directory is fresh for each case; the OS page cache and origin cache
are not flushed. Use a real NVMe-backed `--out`, not tmpfs. The script runs one
host reader at a time; it is a tuning/equality diagnostic, not a matched-cluster
capacity benchmark or proof of PB-scale performance.

Two measured caveats: local-path datasets do not exercise the wrapped ranged
path (run cache sweeps against an object-store `--uri`), and a port-forwarded
localhost origin hides the latency benefit of fewer GETs. Recorded v10 sweep
numbers (equality plus origin-GET/byte attribution) live in
`docs/rust-architecture.md`.

## Local S3-like origin (one process, no cluster)

`just dev-origin` starts a single SeaweedFS process on local NVMe (ports
8337 S3 / 19333 master / 18888 filer; data under `.tmp/origin/`), with fixed
benchmark credentials (`cachebench` / `cachebench-local-only`) and a `lake`
bucket. `just dev-seed <dir> s3/prefix` uploads a dataset into it with
rclone. This replaces the kind-cluster rig for everyday cache work: same S3
semantics (HTTP, range GETs, path-style), no port-forwards, no cluster state.
The metered/delay kind rig remains available for origin-side attribution when
needed.

To model real-world S3 latency without any infrastructure, pass
`--origin-latency-ms 20 --origin-mbps 500` to the serve (or tune.py): every
origin GET beneath the cache then sleeps the fixed latency and is capped at
the modeled throughput (deterministic, via object_store's `ThrottledStore`).
Sweeps then show latency/throughput savings from GET-count reductions, not
just counter deltas. Measured: identical origin work with and without the
model; cold CITY0 712 → 2 116 ms while warm passes stayed ~25–30 ms — the
cache shields repeat traffic from origin latency. These flags are benchmark
controls, never production tunables.

## Scale datasets

`lakewing replicate --source <parquet> --out-dir <dir> --copies N
--offset-degrees 10` materializes N longitude-shifted copies of a real
source through DuckDB spatial (`ST_Affine`; bbox columns shifted to match),
then `lakewing build --source <dir>` indexes them as usual. Copy 0 keeps the
original ids (existing workload ids still resolve); copy k ≥ 1 prefixes
`k:`. The result is real geometry at realistic density spread over a
nation-scale extent — see rust-architecture.md for the measured run.
