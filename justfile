# Go via mise/toolchain, just via mise. DuckDB 2.0 links the pinned
# prebuilt library in .deps/duckdb (populated by `just setup-duckdb`):
# the preview binding's bundled engine is 1.5.x and cannot open 2.0
# catalogs, so every Go command builds with duckdb_use_lib.
DUCKDB_DIR := justfile_directory() + "/.deps/duckdb"
export CGO_LDFLAGS := "-L" + DUCKDB_DIR
export LD_LIBRARY_PATH := DUCKDB_DIR
export DYLD_LIBRARY_PATH := DUCKDB_DIR
export GOFLAGS := "-tags=duckdb_use_lib,duckdb_arrow"
# DuckDB spills temp files under the working directory on memory pressure;
# keep them out of the repo.
export TMPDIR := "/tmp/opencode"

default: check

setup: setup-duckdb
    go mod download

setup-duckdb:
    python3 scripts/setup_duckdb.py

check: setup-duckdb
    gofmt -l cmd internal
    go vet ./...

test: setup-duckdb
    go test ./...

# Real-DuckDB tests: pooled store boot + Flight round trip over the
# fixture catalog (needs the 2.0 prebuilt lib + fixture files).
test-duckdb shard="fixtures/nw-europe.ducklake": setup-duckdb
    LAKEWING_TEST_SHARD="{{justfile_directory()}}/{{shard}}" go test -count=1 ./internal/store/ ./internal/flight/ -v 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|PASS|FAIL|ok)"

build: setup-duckdb
    go build ./...

fmt:
    gofmt -w cmd internal

fmt-check:
    gofmt -l cmd internal

# --- Fixtures: reproducible Layercake shards (git-ignored, see .gitignore) ---

# Tiny Berlin slice for fast iteration (add --limit 20000).
fixture-osm *args:
    go run ./cmd/lakewing build --bbox=13.35,52.48,13.45,52.55 {{args}}

# ~3 GB / 25M-row Benelux + northern France buildings for capacity work.
fixture-nw-europe out="fixtures/nw-europe.ducklake" datadir="fixtures/nw-europe.files" *args:
    go run ./cmd/lakewing build --bbox=2,48,6,54 --out {{quote(out)}} --data-dir {{quote(datadir)}} {{args}}

# Row counts and collections for a built shard (DuckDB CLI for poking).
fixture-verify shard="fixtures/nw-europe.ducklake":
    "{{justfile_directory()}}/.deps/duckdb/duckdb" :memory: "LOAD ducklake; ATTACH 'ducklake:{{shard}}' AS s; USE s; SELECT count(*) AS features FROM features; SELECT * FROM collections;"

# Saved deterministic workloads for a completed shard's region.
workloads region="2,48,6,54" dir="workloads/nw-europe":
    python3 scripts/make_workload.py --region {{quote(region)}} --out-dir {{quote(dir)}}

# Deterministic Berlin workload (seed 7; regenerates byte-identical).
workloads-berlin dir="workloads/berlin":
    python3 scripts/make_berlin_workload.py --out-dir {{quote(dir)}}

# --- SeaweedFS lake rig ------------------------------------------------------
# Local rehearsal of the mount design (see docs/mount-lake.md): SeaweedFS
# (S3) for direct writes + a mount-s3 mount with local disk cache for reads,
# the local equivalent of mountpoint-S3-CSI.

# Start SeaweedFS (idempotent).
seaweed-up:
    bash scripts/seaweed_up.sh

# Stop SeaweedFS. Pass --wipe to drop containers and stored state.
seaweed-down *args:
    bash scripts/seaweed_down.sh {{args}}

# Mount the lake bucket locally (needs mount-s3 + seaweed-up).
lake-mount mnt="/tmp/opencode/mnt/lake":
    bash scripts/lake_mount.sh {{quote(mnt)}}

# Validate mountpoint disk caching against real SeaweedFS over real FUSE:
# cold / warm / cache-dropped scans, asserting zero warm S3 GETs.
test-mount-cache *args:
    bash scripts/mount_cache_test.sh {{args}}

# Side-by-side storage backends: DuckDB httpfs/S3 (2.0 external file
# cache) vs mountpoint disk cache, same OGC-shaped queries cold + warm,
# plus DuckLake catalog-table variants.
bench-paths:
    bash scripts/bench_paths.sh

# Publish a new immutable snapshot, then move a ref at it. Writes go
# direct to S3 (never via the mount); reads resolve mount paths.
# Uploads are additive only and catalog keys are never overwritten.
# Example: just lake-publish sha_003 --bbox=13.38,52.50,13.42,52.54 --limit 20000
lake-publish sha *args: seaweed-up
    #!/usr/bin/env bash
    set -euo pipefail
    source "{{justfile_directory()}}/scripts/dev-s3.env"
    export RCLONE_CONFIG_LAKE_TYPE=s3 RCLONE_CONFIG_LAKE_PROVIDER=Other
    export RCLONE_CONFIG_LAKE_ENDPOINT=http://127.0.0.1:8333
    export RCLONE_CONFIG_LAKE_ACCESS_KEY_ID="$S3_USER"
    export RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY="$S3_PASS"
    export RCLONE_CONFIG_LAKE_REGION="$S3_REGION" RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE=true
    STAGE=$(mktemp -d /tmp/opencode/publish-XXXXXX)
    trap 'rm -rf "$STAGE"' EXIT
    sha={{quote(sha)}}
    name="$sha.ducklake"
    if rclone lsf "lake:lake/catalogs/" 2>/dev/null | grep -qx "$name"; then
      echo "catalog key $name already published; refusing to overwrite" >&2
      exit 1
    fi
    go run ./cmd/lakewing build --out "$STAGE/$name" --content-address \
      --data-dir "$STAGE/files" --data-url "s3://lake/data/" {{args}}
    rclone copy "$STAGE/files" lake:lake/data/
    rclone copyto "$STAGE/$name" "lake:lake/catalogs/$name"
    rclone copyto "$STAGE/$name.serving.json" "lake:lake/catalogs/$name.serving.json"
    bash scripts/lake_ref.sh set "$name" latest
    bash scripts/lake_ref.sh list

# Serve the catalog a ref points at (default: latest), resolved once at
# startup; the reader stays pinned to that snapshot across later publishes.
# Reads resolve mount paths (mount via just lake-mount first).
lake-serve ref="latest" mnt="/tmp/opencode/mnt/lake" *args: seaweed-up
    #!/usr/bin/env bash
    set -euo pipefail
    catalog=$(bash scripts/lake_ref.sh get {{quote(ref)}})
    [ -n "$catalog" ] || { echo "empty ref {{quote(ref)}}" >&2; exit 1; }
    go run ./cmd/lakewing serve --shard "{{quote(mnt)}}/catalogs/$catalog" --data-root "{{quote(mnt)}}/data/" {{args}}

# Smoke-check a running lake server (default: local :3000).
lake-verify base="http://127.0.0.1:3000":
    #!/usr/bin/env bash
    set -euo pipefail
    curl -sf "{{base}}/healthz"
    curl -sf "{{base}}/collections" | head -c 200; echo
    curl -sf "{{base}}/collections/buildings/items?sources=1&limit=1" | head -c 200; echo
    curl -s "{{base}}/metrics" | head -12

run shard="fixtures/osm.ducklake" *args:
    go run ./cmd/lakewing serve --shard {{quote(shard)}} {{args}}

# --- kind whole-stack ------------------------------------------------------
kind-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! kind get clusters | grep -qx lake; then
      mkdir -p /tmp/opencode/kind-nvme-a /tmp/opencode/kind-nvme-b
      kind create cluster --config k8s/kind-2az.yaml
    fi

kind-down:
    kind delete cluster --name lake

# Build the serve image and load it into kind.
kind-image:
    docker build -t lakewing:kind .
    kind load docker-image lakewing:kind --name lake

# Install/upgrade the whole stack (CSI-mounted read pool + bulk pool).
kind-install *args:
    helm upgrade --install lake charts/lakewing --namespace lake --create-namespace -f k8s/kind-values.yaml --set image.tag=kind {{args}}

kind-status:
    kubectl -n lake get pods,deployments,services 2>&1 | head -30

# Seed the kind lake: build the Berlin snapshot and stage it onto the
# az-a worker's /nvme hostPath (catalogs/ + data/ + sidecars), then
# provision the hostPath PV/PVC the chart mounts with csi.enabled=false.
kind-seed:
    #!/usr/bin/env bash
    set -euo pipefail
    go run ./cmd/lakewing build --bbox=13.35,52.48,13.45,52.55 --limit 20000 \
      --collection buildings --content-address \
      --out /tmp/opencode/kind-seed/sha_seed.ducklake \
      --data-dir /tmp/opencode/kind-seed/files \
      --data-url 's3://lake/data/'
    dest=/tmp/opencode/kind-nvme-a/lake
    mkdir -p "$dest/catalogs" "$dest/data"
    cp /tmp/opencode/kind-seed/sha_seed.ducklake* "$dest/catalogs/"
    cp -r /tmp/opencode/kind-seed/files/. "$dest/data/"
    ls "$dest/catalogs"
    kubectl create namespace lake --dry-run=client -o yaml | kubectl apply -f -
    kubectl apply -f k8s/kind-data.yaml

# Berlin benchmark against a port-forwarded read pool.
kind-bench base="http://127.0.0.1:3000" workload="workloads/berlin/mixed.txt": workloads-berlin
    python3 scripts/ogc_bench.py --base {{quote(base)}} --concurrency 8 --requests 100 --workload {{quote(workload)}}

# Observability: Grafana LGTM (metrics/logs/traces backends) + Alloy
# (scrapes lakewing /metrics into Mimir, ships pod logs into Loki).
kind-obs:
    kubectl apply -f k8s/lgtm.yaml
    kubectl apply -f k8s/alloy.yaml
    kubectl -n monitoring rollout status deployment/lgtm --timeout=300s
    kubectl -n monitoring rollout status daemonset/alloy --timeout=300s

# In-cluster consistency tests against the live read pool.
kind-test:
    kubectl -n lake delete job/lake-test --ignore-not-found
    kubectl apply -f k8s/kind-test.yaml
    kubectl -n lake wait --for=condition=complete --timeout=600s job/lake-test
    kubectl -n lake logs job/lake-test | tail -25

bench-ogc *args:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}

# Fixed client-side matrix against a running server.
bench-ogc-matrix:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 8 --requests 100 --warmup 20
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 32 --requests 100 --warmup 20
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 8 --requests 100 --warmup 20 --jitter --seed 1
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 32 --requests 100 --warmup 20 --route tiles
