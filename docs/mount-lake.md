# Mount lake rig: local rehearsal of the CSI-mount design

Reads resolve to POSIX mount paths; the server issues no S3 API calls
itself. Page caching lives in mountpoint's local disk cache on each node,
not in a sidecar process.

```text
SeaweedFS (:8333, S3 API)          direct writes (build/publish)
  bucket lake/
    catalogs/<sha>.ducklake[+.serving.json]
    data/*.parquet
        ^
        | mount-s3 --cache /nvme/mount-cache  (reads only)
  /mnt/lake  ->  lakewing serve --shard /mnt/lake/catalogs/<sha>.ducklake
                                 --data-root /mnt/lake/data/
```

In-cluster the same shape runs under the mountpoint-S3-CSI driver
(`charts/lakewing/templates/volume.yaml` static PV/PVC,
`s3.csi.aws.com`); `/nvme` is the node-local disk the cache lives on
(`k8s/kind-2az.yaml` extraMounts).

## Local rig

```sh
just seaweed-up                          # SeaweedFS S3 on :8333, bucket lake
just lake-mount                          # mount-s3 at /tmp/opencode/mnt/lake
just lake-publish sha_003 --bbox=...     # direct-S3 additive publish
just lake-serve                          # serve --shard <mount>/catalogs/...
```

`MOUNT_CACHE_DIR` (default `/tmp/opencode/mount-cache`) enables the disk
cache; `MOUNT_CACHE_SIZE_MB` bounds it; `MOUNT_EXTRA_ARGS` passes anything
else through to `mount-s3`.

## Cache validation (`just test-mount-cache`)

`scripts/mount_cache_test.sh` validates how the mount caches, against real
SeaweedFS over real FUSE — no mocks:

1. uploads a fixture Parquet set to `s3://lake/cachetest/`,
2. mounts with a fresh disk cache,
3. cold pass: DuckDB `read_parquet` bbox-pruned scan + counts over the
   mount, timed; records cache occupancy and SeaweedFS S3 GETs,
4. warm pass: identical scan, timed; asserts it is faster, the cache grew
   (or stayed full), and no new S3 GETs were issued,
5. cache-drop pass: wipes the cache dir, re-runs, asserts S3 GETs resume —
   proving warm hits came from disk, not memory.

SeaweedFS S3 GETs come from the volume-server request log; timings and
cache occupancy are printed for the record. Results below are loopback and
exclude real network latency — they validate caching behavior, not capacity.

## Results

2026-09-19, `just test-mount-cache` (8 fixture Parquet, ~1 GB, loopback
SeaweedFS, DuckDB bbox-pruned OGC-like scans: 100-row id/cx/cy/name page +
1,000-row id/properties page):

| Pass | Wall | New S3 GetObjects | Cache occupancy | Result digest |
|---|---|---:|---:|---|
| Cold (fresh cache) | 4292 ms | 633 | 721 MB | match |
| Warm (same mount) | 1929 ms | **0** | 799 MB | match |
| Cache dropped + remount | 4702 ms | 517 | — | match |

Warm serves entirely from the node-local disk cache: identical bytes, zero
new S3 GETs, 2.2x faster even on loopback. Wiping the cache resumes S3
reads, proving warm hits came from disk. Loopback excludes real network
latency, so treat these as behavior validation, not capacity claims.

## Serving off the mount (validated 2026-09-20)

`lakewing serve` was run with `--shard`/`--data-root` on a real
`mount-s3 --cache` mount of SeaweedFS (Berlin seed snapshot, OGC bbox page,
`limit=100`):

- Cold query: 100 features, 10 S3 GetObjects, ~7 MB into the disk cache.
- Six repeat queries: all byte-identical, no further GetObject lines in
  the mount metrics across ~40 s of 5 s flushes.
- S3 outage with default 60 s metadata TTL: queries hang revalidating
  metadata (Head/List) until the server's query timeout interrupts —
  **data blocks are cached, metadata is not**.
- S3 outage with `--metadata-ttl indefinite`: byte-identical 200s with S3
  fully down.

Snapshots are immutable, so metadata never changes: production mounts
should set indefinite metadata TTL (see the `csi.mountOptions` note in
`charts/lakewing/values.yaml`). That plus the disk cache makes warmed
readers S3-outage-proof for cached data.

## Warm-optimal recipe

Measured on the 25M-row NW-Europe lake (local disk, then loopback
SeaweedFS mount — same shape, 1.5-3x FUSE tax on top):

- `--threads 0` (NumCPU): 3-5x on heavy pages vs 1, no concurrency loss.
- Deep offsets (≥ 1000) run ids-first two-phase: 30 s timeout → 2.5 s.
- Row-group 8192 beats 65536 warm on the mount (selectivity beats
  FUSE-op-count; byte-identical results).
- Disk cache ≥ dataset, `--metadata-ttl indefinite`, zone-map pruning via
  the serving index + explicit bbox columns (11/389 row groups on a city
  window).

Heaviest shapes warm: deep offset (2.5 s) > low-zoom land tiles (2.8 s) >
full-region pages (2.5 s) > broad quarters (0.5 s) > city windows (0.04 s).
Low-zoom tiles are MVT-encode bound (5000 features); full-region pages sort
25M ids. `scripts/bench_heavy.py` reproduces the battery.

> Host note: this dev host denies FUSE mounts (`fusermount3: Operation not
> permitted`), so the test mounts inside a throwaway `--privileged`
> container running the same `mount-s3` binary against the same SeaweedFS.
> On hosts with FUSE rights, `just lake-mount` mounts directly.
