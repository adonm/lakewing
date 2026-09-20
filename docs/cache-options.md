# Storage-cache options: where to cut the warm path

Goal: lowest-overhead WARM reads under a bounded node NVMe cache for a
lake that will never fit on any node (PB scale). Serving reads are
Parquet row-group column-chunk ranges over an immutable snapshot:
repetitive across requests, never invalidated by writes — but the cache
holds only a hot subset, so eviction and scan pollution matter.

## Measured (in-kind DuckLake, 512 MiB cache vs ~3.0 GiB fixture)

Dedicated kind `lake-cache`: SeaweedFS + Mountpoint CSI v2.8.0 + LGTM +
Alloy, real-disk `/nvme` binds. Matched DuckDB (4 threads, 1 GB,
snapshot 6, engine v2.0.0-alpha42069). Every result fully consumed and
SHA-256 compared across backends and repetitions — all hashes match.
Harness: `scripts/cachebench/rig.py` (`just bench-paths`).

| Query (phase, n) | Direct | Mountpoint CSI | rclone VFS-full (one node mount) |
|---|---|---|---|
| CITY first | 121 ms, 50 GETs / 93 MiB | 338 ms, 74 GETs / 167 MiB | 265 ms, 122 GETs / 466 MiB |
| CITY warm ×5 (median) | 79 ms, 3 GETs / ~15 MiB each | 132 ms, ~0–5 GETs / ~1 MiB each | 102 ms, **0 S3** |
| BROAD first | 560 ms, 168 GETs / 420 MiB | 2690 ms, 382 GETs / 750 MiB | 906 ms, 359 GETs / 1387 MiB |
| BROAD warm ×5 (median) | 530 ms, ~40 GETs / ~185 MiB each | 961 ms, ~2–20 GETs / ~5–118 MiB each | 556 ms, **~0 S3** (7 GETs total) |
| FULL first | 2.7 s, 842 GETs / 2.1 GiB | 23.3 s, 2025 GETs / 2.9 GiB | OOM (see below) |
| FULL warm ×5 (median) | ~2.6 s, full re-fetch each | ~31–43 s, full re-fetch each | — |
| DEEP first | 7.0 s, 1844 GETs / 4.5 GiB | 54.1 s, 3568 GETs / 7.1 GiB | — |
| DEEP warm (median) | ~7.1 s, full re-fetch each | ~52 s, full re-fetch each | — |
| SCAN (full-payload) | 4.9 s, 1442 GETs / 3.8 GiB | 32.4 s, 1939 GETs / 5.1 GiB | — |
| Cross-pod peer CITY | direct 106 ms / full re-fetch | mountpoint 153 ms / 1.8 MiB (shared) | not reached |
| After cgroup page reclaim | both re-fetch catalog+data once, then warm | mountpoint CITY 179 ms / 0.4 MiB (disk serves) | not reached |
| After SCAN pollution | CITY warm unchanged | CITY warm back to ~147 ms / ~0 S3 after one refill | not reached |
| Mount restart, disk intact | n/a (no cache) | CITY 520 ms / full re-fetch (**cache cleared on remount**) | not reached |
| Catalog attach | ~20 ms + ~110 ms metadata, 11 GETs / 2.5 MiB | ~20 ms + ~110 ms, 7 GETs / ~10 MiB + LIST | ~25 ms + ~108 ms, 3 GETs / 8 MiB |

S3 counts metered per backend/operation (catalog vs data) through a
reverse proxy; 502s are mountpoint/rclone speculative-range cancels
(client-cancelled, zero bytes), not SeaweedFS errors.

## Readings

1. **On fast (loopback-kind) S3, direct is fastest everywhere.**
   FUSE costs dominate: mountpoint 2.8x (CITY) to ~9x (FULL) slower,
   rclone 2.2x (CITY) to 1.6x (BROAD) slower. Caches save S3 bytes but
   not wall time when RTT ≈ 0. With real RTT the byte savings would
   convert to latency wins; this rig does not simulate RTT.
2. **Warm small-query caching works on both mounts.**
   rclone CITY/BROAD warm: zero S3. Mountpoint CITY warm: ~1 MiB/query
   (vs 93 first), peer reuse 1.8 MiB, post-reclaim 0.4 MiB. Direct warm
   still pays 15/185 MiB per CITY/BROAD (DuckDB external cache helps
   only marginally at this shape).
3. **Large scans (≥ cache) re-fetch everywhere and mounts amplify.**
   FULL/DEEP/SCAN touch 2–8 GiB > 512 MiB cache: every backend re-reads
   fully; mountpoint moves 1.3–1.5x more GETs than direct (1 MiB
   blocks + prefetch/cancel churn) at ~8–10x wall time.
4. **Mountpoint cache does not survive remount** (restart-first CITY
   fully re-fetches) and SCAN evicts the hot subset (one refill
   restores it). Sharing holds across pods on a node (peer-first
   1.8 MiB) when PV/options/identity match.
5. **rclone VFS-full OOMs on multi-GB scans**: 1 GiB limit died on
   BROAD; 3 GiB survived CITY/BROAD but died on FULL first (25-file
   concurrent chunk caching). It is the most byte-efficient small-query
   cache and the most memory-hungry large-scan reader.

## Sharing and persistence facts

- Mountpoint CSI shares one Mountpoint pod per node only when PV,
  mount options, authentication, FSGroup and pod identity match
  ([local-cache](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#local-cache),
  [pod sharing](https://github.com/awslabs/mountpoint-s3-csi-driver/blob/main/docs/MOUNTPOINT_POD_SHARING.md)).
  It clears its local-cache subdirectory at mount time and on exit.
- CSI v2 places the cache on explicit local NVMe via generic ephemeral
  volumes + a local StorageClass
  ([caching](https://github.com/awslabs/mountpoint-s3-csi-driver/blob/main/docs/CACHING.md)).
  Ordinary `emptyDir` uses the kubelet's default medium.
- rclone VFS-full uses sparse range caching and CAN share one mount
  across pods on a node; never share cache dirs between independent
  rclone processes.
- GeeseFS has disk caching. Alluxio can cache existing plain-S3 data
  (operational weight is the real objection). `cache_httpfs` (shared
  on-disk blocks, process-local metadata, careful eviction) is
  unverified against the pinned 2.0 alpha.

## Cut point (current default: no single winner)

- **Serving (small hot pages)**: node-shared rclone VFS-full is the
  lowest-overhead warm cache measured (zero-S3 CITY/BROAD warm) —
  *provided* mount memory is sized for the largest concurrent scan or
  large scans are routed elsewhere. It OOMs where mountpoint survives.
- **Node-shared default that survives everything**: Mountpoint CSI
  (`ephemeral` on a local-NVMe StorageClass, bounded hot-subset size,
  long metadata TTL for immutable snapshots only). Warm CITY costs
  ~50 ms more than direct here but shares across pods and reuses after
  page reclaim; expect it to win on wall time once RTT is real.
- **Bulk (FULL/DEEP/SCAN)**: direct S3 is fastest and most robust in
  this rig; route multi-GB scans direct (or to a bulk pool) rather than
  through any FUSE mount.
- Keep DuckDB parquet/HTTP metadata caches on; external file cache on
  (default) for metadata/small wins only. Kernel page cache is an
  unplannable second chance. Production chart templates
  (`charts/lakewing`) now expose CSI cache type/size and mount options
  for the ephemeral-NVMe shape.

Evidence: `.tmp/cache-bench/run-20260920T022047Z/` (direct+mountpoint,
90 hash-validated samples, csi-attachments/pods, mount logs, reclaim
deltas, DuckDB profiles, bench Prometheus metrics, Loki logs, Tempo
traces, mountpoint Pyroscope profiles) and
`.tmp/cache-bench/run-20260920T024438Z/` (rclone CITY/BROAD + FULL OOM).
Prior loopback claims ("mountpoint always wins 2–7x / zero warm GETs /
catalog always 4–7x slower") are superseded by the table above.
