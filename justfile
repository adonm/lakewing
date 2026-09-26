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
    {{ just_executable() }} kind-contract
    QUICK=1 SUITES=validate,pgbench,tpch,click,search,stress {{ just_executable() }} kind-bench

# Rust S3 contract against both kind gateway pods and its PostgreSQL storage.
[group('kind')]
kind-contract:
    bash deploy/kind/contract.sh

# Opt-in sustained overwrite/delete/multipart churn with DB vacuum evidence.
[group('kind')]
kind-churn:
    bash deploy/kind/contract.sh churn

# Load generator: 8 GiB of 64 MiB objects (a stable set for `just micro`).
[group('bench')]
seed:
    ./target/release/pgvs3 seed --gigabytes 8 --object-mib 64 --tasks 16

# GET latency/throughput matrix against the gateway on :8014.
[group('bench')]
micro:
    ./target/release/pgvs3 bench --endpoint http://127.0.0.1:8014 --bucket lake --requests 2000

# Image name for `just image` and the kind gateway chart. CI may override it.
IMAGE := env("IMAGE", "ghcr.io/adonm/pgvs3")

# Build the container image (amd64 + arm64, so AWS Graviton works); push=true publishes to GHCR.
[group('image')]
image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    # buildx cannot --load a multi-platform build; only the publish needs both.
    args=(-t "{{ IMAGE }}:latest")
    if [ "{{ push }}" = "true" ]; then
      args+=(--platform linux/amd64,linux/arm64 --push)
    else
      args+=(--load)
    fi
    docker buildx build "${args[@]}" .

# What CI runs (fmt, clippy, build, tests, then `just smoke` on kind).
[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    hk check --all
    mbx build --release --locked
    mbx test --workspace
    just smoke

# --- kind: Postgres 18 + pgvs3 + DuckLake + Quickwit on one disk -----------
# One command per step. `kind-bench` runs all five suites in under an hour
# with caching on (real-world numbers). Results: .tmp/pgvs3/kind-bench.jsonl.

# Stand up pgvs3, DuckLake and Quickwit in kind; PostgreSQL is local or external.
[group('kind')]
kind-up:
    bash deploy/kind/up.sh '{{ IMAGE }}'

# Verify services and the overwrite regression in the same kind test runner.
[group('kind')]
kind-validate:
    SUITES=validate {{ just_executable() }} kind-bench

# Run selected suites sequentially; QUICK=1 is smoke scale.
[group('kind')]
kind-bench:
    bash deploy/kind/bench.sh

# pgvs3 ceiling: concurrency sweep; reports aggregate MiB/s and req/s.
[group('kind')]
kind-stress concurrency="1,8,32,64" requests="4000":
    SUITES=stress CONCURRENCY='{{ concurrency }}' REQUESTS='{{ requests }}' {{ just_executable() }} kind-bench

# Tear down the kind cluster.
[group('kind')]
kind-down:
    kind delete cluster --name pgvs3

# Pre-release test data only: replace three kind databases, not the cluster.
[group('kind')]
[confirm("Discard pgvs3, DuckLake and Quickwit data in this kind cluster?")]
kind-reset:
    bash deploy/kind/reset.sh

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

# Rust S3/DB contract against the Aurora-backed two-gateway rig.
[group('rig')]
rig-contract:
    bash deploy/kind/rig.sh contract

# Run kind-churn against Aurora; results are copied into .tmp/pgvs3/rig-out.
[group('rig')]
rig-churn:
    bash deploy/kind/rig.sh churn

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

# Pre-release test data only: reset the three rig databases and redeploy.
[group('rig')]
[confirm("Discard all pgvs3, DuckLake and Quickwit data on the EC2/Aurora rig?")]
rig-reset:
    bash deploy/kind/rig.sh reset
    bash deploy/kind/rig.sh sync

# Open an SSH shell on the rig (restricted to the current operator IP).
[group('rig')]
rig-ssh:
    bash deploy/kind/rig.sh ssh

# Delete only this rig's stack and SSH key pair; stops ongoing AWS charges.
[group('rig')]
[confirm("Terminate the pgvs3 EC2 kind rig AND its Aurora Serverless cluster?")]
rig-teardown:
    bash deploy/kind/rig.sh teardown
