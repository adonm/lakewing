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

# Race-detector suite for the concurrent proxy surface (pure Go, fast).
test-race:
    go test -race -count=1 ./internal/s3cache/

# golangci-lint (staticcheck, gosec, errcheck…) via mise.
lint:
    golangci-lint run ./...

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
# Local S3 origin (see docs/s3cache.md): SeaweedFS for direct writes and
# direct reads; the node-local s3cache proxy sits between readers and S3
# in kind/prod. Writers always go direct to S3.

# Start SeaweedFS (idempotent).
seaweed-up:
    bash scripts/seaweed_up.sh

# Stop SeaweedFS. Pass --wipe to drop containers and stored state.
seaweed-down *args:
    bash scripts/seaweed_down.sh {{args}}

# Bounded node-NVMe cache comparison: DuckLake direct vs s3cache proxy
# inside kind, with LGTM observability + profiling.
# Default creates kind lake-cache; use `run` to reuse an initialized rig.
bench-paths *args: setup-duckdb
    bash scripts/bench_paths.sh {{args}}

# Indexed Lance sample gate: requires the existing kind-lake-cache rig.
bench-lance *args: setup-duckdb
    python scripts/lancebench/run.py {{args}}

# Node-local S3 slice cache (no CSI/FUSE): unit tests + image build.
s3cache-test:
    go test -count=1 ./internal/s3cache/
s3cache-build:
    docker build -f scripts/s3cache/Dockerfile -t lakewing-s3cache:dev .

# Publish a new immutable snapshot, then move a ref at it. Writes go
# direct to S3; reads go over S3_DIRECT through the node-local s3cache.
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
# Reads go over S3_DIRECT (anonymous secret when no AWS env is set: point
# S3_ENDPOINT at the node-local s3cache proxy, or direct at S3).
# Example against the loopback rig: just lake-serve
lake-serve ref="latest" endpoint="http://127.0.0.1:8333" *args: seaweed-up
    #!/usr/bin/env bash
    set -euo pipefail
    catalog=$(bash scripts/lake_ref.sh get {{quote(ref)}})
    [ -n "$catalog" ] || { echo "empty ref {{quote(ref)}}" >&2; exit 1; }
    S3_ENDPOINT={{quote(endpoint)}} go run ./cmd/lakewing serve --shard "s3://lake/catalogs/$catalog" --data-root "s3://lake/data/" {{args}}

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

# Build the serve + s3cache images and load them into kind.
kind-image:
    docker build -t lakewing:kind .
    docker build -f scripts/s3cache/Dockerfile -t lakewing-s3cache:kind .
    kind load docker-image lakewing:kind lakewing-s3cache:kind --name lake

# Install/upgrade the whole stack (S3_DIRECT read pool + s3cache DaemonSet).
kind-install *args:
    helm upgrade --install lake charts/lakewing --namespace lake --create-namespace --kube-context kind-lake -f k8s/kind-values.yaml --set image.tag=kind --set s3cache.image.tag=kind {{args}}

kind-status:
    kubectl -n lake get pods,deployments,services 2>&1 | head -30

# Seed the kind lake: build the Berlin snapshot and upload it to the
# in-chart SeaweedFS origin (catalogs/ + data/ + sidecars), then create
# the dev S3 secret the s3cache proxy and writer sidecars read.
# Requires `just kind-install` first (creates the seaweed Service).
kind-seed:
    #!/usr/bin/env bash
    set -euo pipefail
    K="kubectl --context kind-lake"
    rm -rf /tmp/opencode/kind-seed
    go run ./cmd/lakewing build --bbox=13.35,52.48,13.45,52.55 --limit 20000 \
      --collection buildings --content-address \
      --out /tmp/opencode/kind-seed/sha_seed.ducklake \
      --data-dir /tmp/opencode/kind-seed/files \
      --data-url 's3://lake/data/'
    $K create namespace lake --dry-run=client -o yaml | $K apply -f -
    $K -n lake create secret generic lake-s3-dev \
      --from-literal=key_id=dev --from-literal=secret=dev-local-only \
      --from-literal=AWS_ACCESS_KEY_ID=dev --from-literal=AWS_SECRET_ACCESS_KEY=dev-local-only \
      --dry-run=client -o yaml | $K apply -f -
    $K -n lake port-forward svc/lake-seaweed 8333:8333 >/dev/null 2>&1 &
    pf=$!; trap 'kill $pf' EXIT
    sleep 2
    export RCLONE_CONFIG_LAKE_TYPE=s3 RCLONE_CONFIG_LAKE_PROVIDER=Other
    export RCLONE_CONFIG_LAKE_ENDPOINT=http://127.0.0.1:8333
    export RCLONE_CONFIG_LAKE_ACCESS_KEY_ID=dev RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY=dev-local-only
    export RCLONE_CONFIG_LAKE_REGION=us-east-1 RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE=true
    rclone copy /tmp/opencode/kind-seed/files lake:lake/data/
    rclone copyto /tmp/opencode/kind-seed/sha_seed.ducklake lake:lake/catalogs/sha_seed.ducklake
    rclone copyto /tmp/opencode/kind-seed/sha_seed.ducklake.serving.json lake:lake/catalogs/sha_seed.ducklake.serving.json
    rclone lsf lake:lake/catalogs/

# Berlin benchmark against a port-forwarded read pool.
kind-bench base="http://127.0.0.1:3000" workload="workloads/berlin/mixed.txt": workloads-berlin
    python3 scripts/ogc_bench.py --base {{quote(base)}} --concurrency 8 --requests 100 --workload {{quote(workload)}}

# Observability: Grafana LGTM + Alloy (metrics to Prometheus, logs to Loki).
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
