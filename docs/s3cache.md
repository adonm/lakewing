# s3cache: node-local read-through S3 slice cache

One DaemonSet pod per node (`k8s/s3cache.yaml`, `hostNetwork: true`,
NVMe `hostPath` at `/nvme/s3cache`). DuckDB workers point their S3
endpoint at the node-local address so every pod shares one disk copy —
no CSI driver, no FUSE mounts.

```sh
# workers (S3_DIRECT mode): catalog + data are s3:// URLs through the proxy
export NODE_IP=... # downward API spec.nodeName/nodeIP in-cluster
export S3_ENDPOINT="http://$NODE_IP:8345"
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
```

`internal/store` creates the `s3direct` secret from `S3_ENDPOINT` when
set (`internal/store/store.go` `setupSession`); otherwise serving keeps
using CSI mount paths. Writers always go direct to the origin.

## Design

- **Slice granularity** (default 1 MiB): each `Range` is split into
  aligned slices keyed `method + path` (`?x-id` SDK telemetry stripped
  from the key, forwarded upstream verbatim). Auth headers are forwarded
  on a miss and excluded from the key, so pods share entries. Object
  length comes from the first fetched slice's `Content-Range` — the
  proxy never invents a second SigV4 request shape (an upstream `HEAD`
  carrying a `GET`'s query string breaks strict signers). Full GETs
  assemble from slices; nothing larger than a slice is ever buffered
  in RAM.
- **Collapse + publish**: per-slice singleflight (the 600+ concurrent
  range GETs of FULL scans fetch each slice once), atomic `.tmp`→rename
  publish, fd-pinned serving safe against eviction.
- **Bounded**: synchronous LRU over `CACHE_BYTES` (hot subset for a PB
  lake); oversized objects stay correct via on-demand refetch at the
  cost of re-downloads. Synchronous bounds were chosen over
  frequency-sketch admission deliberately: predictable under scan churn.
- **Read-only**: non-GET → `403`; `LIST`/query-string GETs and `HEAD`
  pass through with SigV4 intact (v1 does not disk-cache them).
- **Observability**: `/metrics` (`s3cache_hits_total`,
  `s3cache_hit_bytes_total`, `s3cache_origin_fetches_total`,
  `s3cache_origin_bytes_total`, `s3cache_evictions_total`,
  `s3cache_origin_errors_total`, `s3cache_disk_used_bytes`),
  `/healthz` readiness. Scrape the `metrics` port from Alloy.

## Limits (honest)

- v1 forwards **all** non-hop-by-hop headers (allowlist removals, not
  additions) with `DisableCompression` so `Accept-Encoding` passes
  through verbatim: rclone signs `accept-encoding`, `amz-sdk-*`, `host`,
  `x-amz-*`, and dropping/rewriting any of them breaks SigV4
  (`SignatureDoesNotMatch`, verified live against SeaweedFS). The
  incoming `Host` is preserved for the same reason.
- Works with SeaweedFS/MinIO and public buckets. Strict AWS validates
  the signed host, so private AWS buckets need proxy-side re-signing
  (proxy holds IRSA and signs upstream) — structured as a follow-up,
  single function swap at `forwardHeaders` (`internal/s3cache/s3cache.go`).
- Conditionals (`If-None-Match`) are ignored; safe because snapshots
  are immutable. `Content-Type` is served as `application/octet-stream`
  (DuckDB doesn't care); `ETag`s are not synthesized.
- Multipart ranges fall back to full-body `200` (legal).

## Validate

```sh
just s3cache-test   # unit: reuse, auth exclusion, singleflight, eviction bound
```

Then add a `proxy` backend to `scripts/cachebench/rig.py` reusing the
DuckLake phases (first/warm/peer/reclaim/pollution) with SHA-256 result
checks before trusting it past SeaweedFS.
