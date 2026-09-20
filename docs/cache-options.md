# Storage-cache options: where to cut the warm path

Goal: lowest-overhead WARM reads under a bounded node NVMe cache for a
lake that will never fit on any node (PB scale). Serving reads are
Parquet row-group column-chunk ranges over an immutable snapshot:
repetitive across requests, never invalidated by writes — but the cache
holds only a hot subset, so eviction and scan pollution matter.

Reads go over S3_DIRECT through the node-local s3cache proxy
(`docs/s3cache.md`); writers go direct to S3. No CSI driver, no FUSE
mounts anywhere in the stack.

## Measured (in-kind DuckLake, 512 MiB cache vs ~3.0 GiB fixture)

Dedicated kind `lake-cache`: SeaweedFS + LGTM + Alloy, real-disk
`/nvme` binds. Matched DuckDB (4 threads, 1 GB, snapshot 6, engine
v2.0.0-alpha42069). Every result fully consumed and SHA-256 compared
across backends and repetitions — all hashes match.
Harness: `scripts/cachebench/rig.py` (`just bench-paths`).

| Query (phase, n) | Direct | s3cache proxy |
|---|---|---|
| CITY first | 121 ms, 50 GETs / 93 MiB | 350 ms, 105 GETs / 104 MiB |
| CITY warm ×5 (median) | 79 ms, 3 GETs / ~15 MiB each | 162 ms, **~0 S3** (3 GETs total) |
| BROAD first | 560 ms, 168 GETs / 420 MiB | 967 ms, 403 GETs / 398 MiB |
| BROAD warm ×5 (median) | 530 ms, ~40 GETs / ~185 MiB each | 800 ms, **~2 S3** (10 GETs total) |
| FULL first | 2.7 s, 842 GETs / 2.1 GiB | 3.3 s, 1804 GETs / 1.8 GiB |
| FULL warm ×5 (median) | ~2.6 s, full re-fetch each | ~3.2 s, full re-fetch each |
| DEEP first | 7.0 s, 1844 GETs / 4.5 GiB | 6.2 s, 4049 GETs / 4.0 GiB |
| DEEP warm (median) | ~7.1 s, full re-fetch each | ~8.0 s, full re-fetch each |
| SCAN (full-payload) | 4.9 s, 1442 GETs / 3.8 GiB | 5.6 s, 3029 GETs / 3.0 GiB |
| Cross-pod peer CITY | direct 106 ms / full re-fetch | 289 ms / 15.7 MiB, then 180 ms / **0 S3** |
| After cgroup page reclaim | re-fetch catalog+data once, then warm | 140 ms / 7 MiB (disk serves) |
| After SCAN pollution | CITY warm unchanged | 157 ms / 103 MiB refill, then 90 ms / **~0 S3** |
| Proxy restart, disk intact | n/a (no cache) | **161 ms / 10 MiB** (disk recovered; 10 MiB is read-ahead overfetch) |
| Catalog attach | ~20 ms + ~110 ms metadata, 11 GETs / 2.5 MiB | ~61 ms + ~324 ms metadata, 6 GETs / 5.5 MiB |

S3 counts metered per backend/operation (catalog vs data) through a
reverse proxy.

## Readings

1. **On fast (loopback-kind) S3, direct is fastest everywhere.**
   Caches save S3 bytes but not wall time when RTT ≈ 0. With real RTT
   the byte savings convert to latency wins; the rig is gaining a
   configurable origin-latency injector (`--s3-latency-ms`) so the next
   comparison measures it instead of assuming it.
2. **Warm small-query caching holds.** Proxy CITY/BROAD warm pay ~0–2
   S3 GETs total across 5 repeats (vs 93 MiB / ~1 GiB re-fetched
   direct). Cross-pod, post-reclaim and post-pollution reuse all hold.
3. **Large scans (≥ cache) re-fetch everywhere.** FULL/DEEP/SCAN touch
   2–8 GiB > 512 MiB cache: every backend re-reads fully. The proxy
   moves ~2x the GET count of direct (1 MiB slice alignment) at
   +10–25% wall time — the request-count tax on misses, and the first
   place to look for proxy wins (see below).
4. **Proxy restart keeps serving from disk** (161 ms / 10 MiB via
   index + length recovery), and SCAN eviction needs one refill.

## Cut point (s3cache proxy recommended)

- **Default: s3cache proxy** (DaemonSet, NVMe hostPath, bounded
  hot-subset size, one Secret). Node-shared, restart-proof, OOM-safe
  (nothing larger than a slice in RAM), and within +25% of direct on
  bulk scans — with warm small queries at ~0 S3.
- **Bulk (FULL/DEEP/SCAN)**: direct S3 remains fastest in this
  loopback rig; narrow the +10–25% proxy tax (slice/request-count
  overhead) before calling bulk done.
- Keep DuckDB parquet/HTTP metadata caches on; external file cache on
  (default) for metadata/small wins only. Kernel page cache is an
  unplannable second chance. The `s3direct` secret path
  (`S3_ENDPOINT`, no worker credentials) is implemented in
  `internal/store/store.go` `setupSession`.

## Proxy perf backlog (in priority order once latency is on)

1. Miss-path request count: 1 MiB slices double GETs vs direct's
   ranges — measure 4 MiB slices and coalesced range fetching.
2. Warm-path per-request overhead: profile slice open/seek/copy loop
   vs `sendfile`.
3. Read-ahead overfetch (10 MiB on restart refill): gate on
   sequential reuse before prefetching cold neighbors.
4. DuckDB httpfs knobs: keep-alive/connection reuse to the proxy.

Evidence: `.tmp/cache-bench/run-20260920T045933Z/` (proxy, 45 samples,
all fingerprints match direct hashes, reclaim deltas, DuckDB profiles,
Tempo traces). Historical mountpoint/rclone runs (CSI/FUSE, since
removed) live in git history under `docs/cache-options.md` at `fc161bd`.
