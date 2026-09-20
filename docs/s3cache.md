# s3cache: node-local read-through S3 slice cache

One DaemonSet pod per node (`k8s/s3cache.yaml`, `hostNetwork: true`,
NVMe `hostPath` at `/nvme/s3cache`). DuckDB workers point their S3
endpoint at the node-local address so every pod shares one disk copy —
no CSI driver, no FUSE mounts.

```sh
# one Secret on the DaemonSet; the proxy is the only S3 signer
kubectl -n lake create secret generic s3cache-auth \
  --from-literal=key_id=$S3_USER --from-literal=secret=$S3_PASS
# workers: no credentials at all, just the endpoint
export S3_ENDPOINT="http://$NODE_IP:8345"
```

`internal/store` creates the `s3direct` secret from `S3_ENDPOINT` when
set (`internal/store/store.go` `setupSession`) — **without** `KEY_ID`,
i.e. anonymous to the proxy, when workers have no AWS env. Writers
always go direct to the origin.

## Auth model (simple on purpose)

The proxy is the **sole SigV4 signer**:

- Workers hold no S3 credentials; their requests arrive unsigned (or
  with anything else) and are served from disk or re-signed upstream.
- Client `Authorization` never crosses the proxy and is never part of
  the cache key — pods share entries by construction.
- One `s3cache-auth` Secret on the DaemonSet replaces credentials in
  every reader pod; rotation happens in one place.
- No fragile signed-header forwarding: the proxy builds each upstream
  request itself and signs `host;x-amz-content-sha256;x-amz-date`
  (minimal valid SigV4, per-date signing key cached).
- Trust boundary: anything on the node network can read the lake
  through the proxy — the same node-trust model as a CSI mount
  (`allow-other`). Put it behind auth at the LB/ingress if exposed
  beyond the node.
- Strict AWS works with static proxy credentials today; IRSA/web-
  identity token refresh is the remaining follow-up in `sign.go`.

## Speed design

- **Origin connection pooling**: 256/64 idle conns with keepalive
  (Go's default `MaxIdleConnsPerHost: 2` serialized 8 fetchers).
  `DisableCompression` keeps Content-Length/Range math byte-exact.
- **Cached HEADs**: recurring per-file-open probes serve from learned
  lengths — zero RTT once touched (immutable snapshots).
- **Parallel slice fetch** (`FETCHERS`, default 32): slices within one
  request fetch concurrently, bounded by one global semaphore; the
  first error cancels the rest. Sized for RTT-bound origins (8-way at
  25 ms RTT caps at ~320 slices/s vs ~1,700 per FULL scan).
  Unit-tested via peak in-flight at the origin.
- **Read-ahead** (`READAHEAD`, default 4): after a **fully cached**
  range is served, the next slices prefetch off the serving path
  (bounded lane, never blocks serving, bounded by object length).
  Gated on full hits deliberately: under churn, prefetch evicts slices
  the active query still needs, cascading into multi-second stalls
  (observed as a DuckDB HTTP timeout on DEEP under 25 ms RTT).
  `0` disables.
- **Slice granularity** (default 1 MiB): each `Range` splits into
  aligned slices keyed `method + path` (`?x-id` SDK telemetry stripped
  from the key, forwarded upstream verbatim). Length comes from the
  first fetched slice's `Content-Range` — no second request shape.
  Full GETs assemble from slices; nothing larger than a slice is ever
  buffered in RAM.
- **Collapse + publish**: per-slice singleflight (the 600+ concurrent
  range GETs of FULL scans fetch each slice once), atomic
  `.tmp`→rename publish, fd-pinned serving safe against eviction.
- **Serve path**: pooled 1 MiB copy buffers (one disk read + one
  socket write per slice); per-slice flush streams progressively.
- **Bounded**: synchronous LRU over `CACHE_BYTES` (hot subset for a PB
  lake); object-length map bounded at 64K entries; oversized objects
  stay correct via on-demand refetch.
- **Read-only**: non-GET → `403`; `LIST`/`HEAD` pass through signed by
  the proxy.
- **Observability**: `/metrics` (`s3cache_hits_total`,
  `s3cache_hit_bytes_total`, `s3cache_origin_fetches_total`,
  `s3cache_origin_bytes_total`, `s3cache_evictions_total`,
  `s3cache_origin_errors_total`, `s3cache_disk_used_bytes`),
  `/healthz` readiness. Scrape the `metrics` port from Alloy.

## Limits (honest)

- Conditionals (`If-None-Match`) ignored; safe because snapshots are
  immutable. `Content-Type` is `application/octet-stream`; no `ETag`
  synthesis. Multipart ranges fall back to full-body `200`.
- Client conditionals/auth are dropped at the proxy by design; if the
  origin needs per-pod identity, this cache is the wrong layer.
- The benchmark harness comparison is done: `proxy` backend in
  `scripts/cachebench/rig.py`, run `run-20260920T045933Z` (45 samples,
  all SHA-256 match direct/mountpoint). CITY warm ~0 S3, FULL/DEEP/SCAN
  within +25% of direct, restart-first 161 ms / 10 MiB via index
  recovery. See `docs/cache-options.md` for the full side-by-side.

## Validate

```sh
just s3cache-test   # unit: reuse, sole-signer, singleflight, parallel
                    # fetch, read-ahead, eviction bound, 416, x-id
just s3cache-build   # image
```
