# Storage-cache options: where to cut the warm path

Goal: lowest-overhead WARM reads, shareable across pods on a node (one
copy per node, not per pod or per process). Serving reads are Parquet
row-group column-chunk ranges over an immutable snapshot: highly
repetitive across requests, never invalidated by writes.

## Measured (25M-row lake, loopback SeaweedFS, then +20 ms RTT)

File-list queries, threads=8. Loopback first (S3 artificially fast),
then with 20 ms loopback latency to simulate real S3 RTT:

| Query | Direct cold | Direct warm | Mount cold | Mount warm | Mount warm +20 ms RTT | Direct warm +20 ms RTT |
|---|---|---|---|---|---|---|
| City page | 203 ms | 50 ms | 1233 ms | 291 ms | 328 ms | 2285 ms |
| Broad quarter | 601 ms | 529 ms | 8664 ms | 1698 ms | ~1500 ms | 2135 ms |
| Full-region page | 2025 ms | 3452/2383 ms | 32565 ms | 5790 ms | — | 3373/4133 ms |
| Deep offset | 5722 ms | 8713 ms | 9729 ms | 12263 ms† | — | 9610 ms |

† run-to-run variance under a loaded rig; content identical. (An early
+20 ms RTT broad run showed 4.4 s from a partial cache miss after a cache
wipe; clean re-runs hold ~1.5 s with no new S3 GETs.)

DuckLake catalog-table reads (ATTACH + `features`, no frozen file list):
direct 6.3 s, mount cold 42 s / warm 9.3 s — 4-7x slower than the frozen
file list. This is why serve resolves to `read_parquet` file lists; the
catalog path is boot/fallback only.

## Readings

1. **On loopback, direct HTTP beats warm mount** (no RTT to amortize;
   FUSE costs ~1.5-3x on data-heavy queries, ~1x on small ones). Loopback
   flatters direct and must not drive the decision.
2. **With 20 ms RTT, warm mount wins 2-7x** (CITY 2285 → 328 ms) because
   warm mount issues ~zero S3 (verified in mount metrics across ~40 s of
   flushes). Direct warm still pays full transfer: DuckDB's external file
   cache measured ~1-1.4x on loopback, sometimes negative (cache blocks
   compete with query memory under `memory_limit`).
3. **Catalog reads are never hot-path material** on either backend.

## Options matrix

| Option | Shareable by node? | Granularity | Cold cost | Warm cost (real RTT) | Verdict |
|---|---|---|---|---|---|
| Mountpoint S3 CSI disk cache (**current**) | Yes — one Mountpoint pod per node when mount config is identical; disk-sized, survives process restarts | ~1 MB blocks, p50 ~170 µs | Full fetch once per node | ~0 S3, disk speed | **Keep**: only node-shared option that fits immutable Parquet ranges |
| DuckDB 2.0 external file cache (in-memory blocks + spill) | No — per process, bounded by `memory_limit`, competes with queries | 2 MB blocks | Same as direct | ~1-1.4x loopback, ~neutral under RTT | Keep ON (default) for metadata/small wins; does not replace the disk cache |
| DuckDB parquet/http metadata caches | No (per process) | Footers/HEADs | — | Cheap, always worth it | Keep ON (serve sets both + `NO_VALIDATION`) |
| Kernel page cache | Per node, but memory-competing and droppable | 4 KB pages | — | Free second chance above disk cache | Comes for free; not plannable |
| S3 Express / directory buckets | N/A (lower RTT, not a cache) | — | Lower | Lower floor for misses | Consider for miss-heavy estates; orthogonal |
| JuiceFS / Alluxio | Yes (distributed) | Block/file | Full fetch | Disk/network speed | Rejected: own metadata/format, operational weight; no fit for plain-S3 lake layout |
| HTTP CDN / Cachey-style sidecar | Zone-shared, but HTTP-range only | Pages | Full fetch | Fast | Rejected: incompatible with the mount/POSIX model (retired) |
| SeaweedFS gateway cache | Shareable via Service | Chunks | Full fetch | Fast | Fallback if CSI is ever unavailable in an estate; sidecar cost |

## Cut point

Cache at the **block level on the node** (mountpoint disk cache):
file-level wastes whole 130 MB objects, row-level doesn't exist, and
per-process memory caches can't share. DuckDB-side caches stay on for
metadata only. Production mounts: cache ≥ dataset, `--metadata-ttl
indefinite` (immutable snapshots), serving index pruning so each query
touches few files.

## Alternative mounts (measured 2026-09-20, same rig + queries)

Same 25-file lake, same DuckDB CLI, cold then warm. All byte-identical
results (md5-verified across mounts):

| Query | mountpoint cold / warm | mountpoint `--read-part-size 32M` cold / warm | rclone VFS-full cold / warm | weed native mount cold / warm |
|---|---|---|---|---|
| City page | 1233 / 291 ms | — | 267 / 173 ms | 354 / 203 ms |
| Broad quarter | 8664 / 1698 ms | — | 822 / 620 ms | 1009 / 643 ms |
| Full-region page | 32565 / 5790 ms | 10614 / 6958 ms | 2811 / 2222 ms | 2792 / 2082 ms |

- **rclone mount** (`--vfs-cache-mode full`, sparse chunk cache,
  `--vfs-read-chunk-size 16M`, `--dir-cache-time 9999h`, `--no-modtime`):
  2-11x faster than mountpoint here. Bigger sequential chunks + read-ahead
  + parallel streams fit DuckDB's row-group reads better than mountpoint's
  on-demand parts. Caveats: one mount process + cache per pod (VFS cache
  dirs are not safe to share between processes), no CSI driver — each pod
  carries its own copy and its own FUSE process.
- **weed native mount** (`-cacheCapacityMB`, filer protocol, no S3
  translation): tied with rclone, the fastest option against SeaweedFS.
  SeaweedFS-only (does not apply to AWS S3). Same per-mount cache caveat.
- **mountpoint tuning**: `--read-part-size 32M` cut cold FULL 32.5 s →
  10.6 s (3x); warm unchanged within noise. Worth setting; does not close
  the gap to rclone/weed on this workload.
- Disqualified without benchmarking: **s3fs-fuse** (slow, weak cache),
  **goofys/geesefs** (fast streaming, no persistent disk cache — every
  repeat pays S3), **JuiceFS/Alluxio** (own metadata/format and infra;
  cannot adopt a plain-S3 lake layout).

Recommendation stands: **mountpoint S3 CSI in AWS estates** — it is the
only option with a truly node-shared cache (one Mountpoint pod per node
when mount config is identical) and zero extra infrastructure, and warm
issues ~zero S3 either way. Where per-pod caches are acceptable, rclone
VFS-full is faster per mount; against SeaweedFS backends, weed native
mount is fastest. Revisit if mountpoint's prefetch story improves.
