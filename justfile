# pgvs3 tasks. Toolchain: `mise install`. Testing runs in kind.
# The AWS rig recipes read their settings from .env (start from .env.example).
set dotenv-load

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"
# Local development database. Smoke and CI use the kind stack instead.
PG_URL := env("PG_URL", URL)

# List the recipes.
default:
    @{{ just_executable() }} --list

# Fetch everything the build needs (mise installs the toolchain).
[group('dev')]
setup:
    mise install
    cargo fetch
    @echo "ready. next: just smoke (or just dev-db for local development)"

# PostgreSQL 18 in Docker (any reachable PostgreSQL works: set PG_URL).
[group('dev')]
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "${PG_URL:-}" ] && [ "${PG_URL:-}" != "{{ URL }}" ]; then
      echo "using PG_URL=$PG_URL"
      exit 0
    fi
    if (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null; then
      echo "postgres already listening on :5432"
    elif docker inspect pgvs3-pg >/dev/null 2>&1; then
      docker start pgvs3-pg >/dev/null
    else
      docker run -d --name pgvs3-pg -p 127.0.0.1:5432:5432 \
        -e POSTGRES_PASSWORD=postgres postgres:18
    fi
    for i in $(seq 1 30); do
      docker exec pgvs3-pg psql -U postgres -c 'SELECT 1' >/dev/null 2>&1 && break
      [ "$i" = 30 ] && { echo "postgres did not come up; try: just dev-db-clean && just dev-db"; docker logs --tail 5 pgvs3-pg; exit 1; }
      sleep 1
    done
    for db in pgvs3_bench ducklake_catalog ducklake_catalog_local; do
      docker exec pgvs3-pg psql -U postgres -c "CREATE DATABASE $db" 2>/dev/null || true
    done
    echo "postgres up: {{ URL }}"

# Delete the Docker PostgreSQL and its data.
[group('dev')]
dev-db-clean:
    docker rm -f pgvs3-pg || true

# The tests, all on kind: cluster up, validate and every workload at smoke scale.
[group('kind')]
smoke: kind-up
    #!/usr/bin/env bash
    set -euo pipefail
    QUICK=1 SUITES=validate,pgbench,tpch,click,search,stress {{ just_executable() }} kind-bench

# Load generator: 8 GiB of 64 MiB objects (a stable set for `just micro`).
[group('bench')]
seed:
    ./target/release/pgvs3 seed --gigabytes 8 --object-mib 64 --tasks 16

# GET latency/throughput matrix against the gateway on :8014.
[group('bench')]
micro:
    ./target/release/pgvs3 bench --endpoint http://127.0.0.1:8014 --bucket lake --requests 2000

# Image name for `just image`/`image-test`. CI overrides it with the repo's
# GHCR path.
IMAGE := env("IMAGE", "ghcr.io/adonm/pgvs3")

# Build the container image (amd64 + arm64, so AWS Graviton works); push=true publishes to GHCR.
[group('image')]
image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p pgvs3
    # buildx cannot --load a multi-platform build; only the publish needs both.
    args=(-t "{{ IMAGE }}:latest")
    if [ "{{ push }}" = "true" ]; then
      args+=(--platform linux/amd64,linux/arm64 --push)
    else
      args+=(--load)
    fi
    docker buildx build "${args[@]}" .

# Run the image against a PostgreSQL (PG_URL, default local dev-db).
[group('image')]
image-test:
    docker run --rm -p 8014:8014 -e PGVS3_URL="${PG_URL:-{{ URL }}}" {{ IMAGE }}:latest &
    sleep 3
    curl -sf -o /dev/null http://127.0.0.1:8014/ && echo "image ok"

# What CI runs (fmt, clippy, build, tests, then `just smoke` on kind).
[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo build --release --locked
    cargo test --workspace
    just smoke

# --- kind: Postgres 18 + pgvs3 + DuckLake + Quickwit on one disk -----------
# One command per step. `kind-bench` runs all five suites in under an hour
# with caching on (real-world numbers). Results: .tmp/pgvs3/kind-bench.jsonl.

# Stand up the cluster (Postgres 18, pgvs3, DuckLake, Quickwit); idempotent, same on laptop and EC2.
[group('kind')]
kind-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! kind get clusters | grep -qx pgvs3; then
      kind create cluster --name pgvs3 --config deploy/kind/cluster.yaml
    fi
    # Benchmark this checkout's code, not whatever image is lying around. The
    # tag never changes, so restart to pick up the build.
    docker build -q -t {{ IMAGE }}:latest .
    kind load docker-image {{ IMAGE }}:latest --name pgvs3
    kubectl create namespace pgvs3 --dry-run=client -o yaml | kubectl apply -f -
    if [ -n "${PGVS3_DB_SECRET:-}" ]; then
      # EC2 kind: the Secret points to Aurora; do not deploy another Postgres.
      kubectl -n pgvs3 get secret "$PGVS3_DB_SECRET" >/dev/null
      helm upgrade --install pgvs3 deploy/charts/pgvs3 --namespace pgvs3 \
        --set image={{ IMAGE }}:latest --set-string "urlSecretName=$PGVS3_DB_SECRET"
    else
      # Local / CI: the same workloads use a PostgreSQL pod in kind.
      helm upgrade --install postgres deploy/charts/postgres --namespace pgvs3 \
        --set storage="${PG_STORAGE:-20Gi}"
      helm upgrade --install pgvs3 deploy/charts/pgvs3 --namespace pgvs3 \
        --set image={{ IMAGE }}:latest --set urlSecretName=
    fi
    helm upgrade --install quickwit deploy/charts/quickwit --namespace pgvs3
    kubectl -n pgvs3 rollout restart deployment/pgvs3
    # ConfigMap changes do not change a Deployment's pod template.
    kubectl -n pgvs3 rollout restart deployment/quickwit
    if [ -z "${PGVS3_DB_SECRET:-}" ]; then
      kubectl -n pgvs3 rollout status statefulset/postgres --timeout 180s
    fi
    kubectl -n pgvs3 rollout status deployment/pgvs3 --timeout 180s
    kubectl -n pgvs3 rollout status deployment/quickwit --timeout 180s
    echo "kind up. next: just kind-validate && just kind-bench"

# Verify services and the overwrite regression in the same kind test runner.
[group('kind')]
kind-validate:
    SUITES=validate {{ just_executable() }} kind-bench

# Run selected suites sequentially; QUICK=1 is smoke scale.
[group('kind')]
kind-bench:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p pgvs3
    cp target/release/pgvs3 deploy/bench/pgvs3-bin
    trap 'rm -f deploy/bench/pgvs3-bin' EXIT
    # Harness sources live in crates/pgvs3; sync so the image cannot drift.
    cp crates/pgvs3/benchlib.py crates/pgvs3/tpch_bench.py crates/pgvs3/analytics_bench.py deploy/bench/harness/
    cp crates/pgvs3/queries/*.sql deploy/bench/harness/queries/
    docker build -q -t kind-bench:latest -f deploy/bench/Dockerfile deploy/bench
    mkdir -p .tmp/pgvs3
    kind load docker-image kind-bench:latest --name pgvs3
    suites="${SUITES:-pgbench,tpch,click,search,stress}"
    # Suite knobs flow through the caller's environment to run.sh.
    env_args=()
    if [ -n "${PGVS3_DB_SECRET:-}" ]; then
      kubectl -n pgvs3 get secret "$PGVS3_DB_SECRET" >/dev/null
      env_args+=(--set-string "pgSecretName=$PGVS3_DB_SECRET")
    fi
    for k in QUICK SCALE CLIENTS SECONDS_RUN SF PASSES PARTS QUERIES DOCS WORKERS WINDOW_FRAC SEARCH_INDEX SEED_GB REQUESTS CONCURRENCY SIZES; do
      if [ -n "${!k:-}" ]; then
        value=${!k}
        # Helm parses commas in --set-string as separators unless escaped.
        value=${value//,/\\,}
        env_args+=(--set-string "suiteEnv.$k=$value")
      fi
    done
    : > .tmp/pgvs3/kind-bench.jsonl
    # Only one Job runs at a time: they share a database and a CI runner.
    wait=2700; case "${QUICK:-}" in 1 | true) wait=300 ;; esac
    for suite in ${suites//,/ }; do
      case "$suite" in validate|pgbench|tpch|click|search|stress) ;; *) echo "unknown suite: $suite" >&2; exit 2 ;; esac
      echo "=== $suite ==="
      # Jobs are immutable: a new image needs a new Job, even with :latest.
      kubectl -n pgvs3 delete job "bench-$suite" --ignore-not-found --wait=true >/dev/null
      helm upgrade --install kind-bench deploy/charts/kind-bench --namespace pgvs3 \
        --set image=kind-bench:latest "${env_args[@]}" --set "suites={$suite}"
      waited=0
      while :; do
        s=$(kubectl -n pgvs3 get job "bench-$suite" -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
        case "$s" in *Complete=True*|*Failed=True*) break ;; esac
        if [ "$waited" -ge "$wait" ]; then
          echo "TIMEOUT $suite after ${wait}s" >&2
          kubectl -n pgvs3 describe job "bench-$suite" >&2
          exit 1
        fi
        sleep 2; waited=$((waited + 2))
      done
      kubectl -n pgvs3 logs "job/bench-$suite" | tee "/tmp/kind-$suite.log"
      if [[ "$s" != *Complete=True* ]]; then
        echo "FAILED $suite: $s" >&2
        exit 1
      fi
      if [ "$suite" != validate ] && ! grep -q '^{.*}$' "/tmp/kind-$suite.log"; then
        echo "FAILED $suite: no result JSON" >&2
        exit 1
      fi
      grep '^{.*}$' /tmp/kind-$suite.log >> .tmp/pgvs3/kind-bench.jsonl || true
    done
    echo "----"
    echo "results: .tmp/pgvs3/kind-bench.jsonl"

# pgvs3 ceiling: concurrency sweep; reports aggregate MiB/s and req/s.
[group('kind')]
kind-stress concurrency="1,8,32,64" requests="4000":
    SUITES=stress CONCURRENCY='{{ concurrency }}' REQUESTS='{{ requests }}' {{ just_executable() }} kind-bench

# Tear down the kind cluster.
[group('kind')]
kind-down:
    kind delete cluster --name pgvs3

# TPC-H on DuckLake through the gateway, stable DuckDB (extra = harness args).
[group('bench')]
tpch sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_STABLE" python crates/pgvs3/tpch_bench.py --stack {{ stack }} --sf {{ sf }} --load --passes 2 {{ extra }}

# TPC-H on the DuckDB 2.0 pre-release.
[group('bench')]
tpch2 sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/tpch_bench.py --stack {{ stack }} --sf {{ sf }} --load --passes 2 {{ extra }}

# ClickBench (43 queries) on DuckLake through the gateway.
[group('bench')]
clickbench stack="lake-s3" passes="3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/analytics_bench.py --bench click --stack {{ stack }} --download --load --passes {{ passes }} {{ extra }}

# SpatialBench (12 queries) on DuckLake through the gateway.
[group('bench')]
spatialbench sf="10" stack="lake-s3" passes="3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/analytics_bench.py --bench spatial --sf {{ sf }} --stack {{ stack }} --download --load --passes {{ passes }} {{ extra }}

# Stand up an EC2 kind rig and Aurora Serverless v2 I/O-Optimized.
[group('rig')]
rig-up:
    bash deploy/kind/rig.sh up

# Ship this checkout, refresh the Aurora Secret and deploy the kind charts.
[group('rig')]
rig-sync:
    bash deploy/kind/rig.sh sync

# Run the same smoke gate as CI, but with Aurora outside kind.
[group('rig')]
rig-validate:
    bash deploy/kind/rig.sh validate

# Run the kind benchmark suites on EC2; SUITES, QUICK, SF, DOCS, etc. work here too.
[group('rig')]
rig-bench:
    bash deploy/kind/rig.sh bench

# Inspect only this rig's stack and endpoints (no credentials).
[group('rig')]
rig-status:
    bash deploy/kind/rig.sh status

# Download the latest results even if a remote session disconnected.
[group('rig')]
rig-results:
    bash deploy/kind/rig.sh results

# Open an SSH shell on the rig (restricted to the current operator IP).
[group('rig')]
rig-ssh:
    bash deploy/kind/rig.sh ssh

# Delete only this rig's stack and SSH key pair; stops ongoing AWS charges.
[group('rig')]
[confirm("Terminate the pgvs3 EC2 kind rig AND its Aurora Serverless cluster?")]
rig-teardown:
    bash deploy/kind/rig.sh teardown
