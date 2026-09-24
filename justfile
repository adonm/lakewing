# pgvs3 dev helpers. Toolchain: `mise install`. Services: Docker.
URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"

default:
    @echo "setup | dev-db | smoke | seed | micro | tpch sf=10 stack=lake-s3 | dev-origin | dev-origin-clean"

# Fetch everything the build needs (mise auto-installs the toolchain).
setup:
    mise install
    cargo fetch
    @echo "ready. next: just dev-db && just smoke"

# PostgreSQL 18 in Docker (any reachable PG works too: pass --url to the binary).
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null; then
      echo "postgres already listening on :5432"
    elif docker inspect pgvs3-pg >/dev/null 2>&1; then
      docker start pgvs3-pg >/dev/null
    else
      docker run -d --name pgvs3-pg -p 127.0.0.1:5432:5432 \
        -e POSTGRES_PASSWORD=postgres -v pgvs3-pg-data:/var/lib/postgresql/data postgres:18
    fi
    for i in $(seq 1 30); do
      docker exec pgvs3-pg pg_isready -U postgres >/dev/null 2>&1 && break
      (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null && break
      sleep 1
    done
    docker exec pgvs3-pg psql -U postgres -c 'CREATE DATABASE pgvs3_bench' 2>/dev/null || true
    echo "postgres up: {{URL}}"

# End-to-end proof on a fresh host: build, seed a little, serve, ranged GET.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p pgvs3
    ./target/release/pgvs3 seed --gigabytes 0.25 --object-mib 8 --tasks 2 >/dev/null
    ./target/release/pgvs3 serve --addr 127.0.0.1:8014 & SRV=$!
    trap 'kill $SRV 2>/dev/null || true' EXIT
    sleep 1
    BYTES=$(curl -sf -H 'Range: bytes=8000-17999' \
      --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
      http://127.0.0.1:8014/lake/obj-00000.bin | wc -c)
    [ "$BYTES" = "10000" ] && echo "smoke ok: cross-row ranged GET returned exactly 10000 bytes"

# Microbench matrix (seed first for a stable object set).
seed:
    ./target/release/pgvs3 seed --gigabytes 8 --object-mib 64 --tasks 16

micro:
    ./target/release/pgvs3 bench --endpoint http://127.0.0.1:8014 --bucket lake --requests 2000

# TPC-H on DuckLake (Postgres catalog, data files via the gateway on :8014).
# DuckDB wheel pins live in mise.toml [env]; `extra` passes harness args
# (e.g. --catalog/--data-path on a remote rig).
tpch sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_STABLE" python crates/pgvs3/tpch_bench.py --stack {{stack}} --sf {{sf}} --load --passes 2 {{extra}}

# Same, on the DuckDB 2.0 pre-release line (async I/O).
tpch2 sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/tpch_bench.py --stack {{stack}} --sf {{sf}} --load --passes 2 {{extra}}

# Real-S3-semantics test origin (SeaweedFS) for conformance tests.
dev-origin:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .tmp/origin
    if curl -s -o /dev/null --max-time 1 http://127.0.0.1:8337/; then
      echo "origin already listening on :8337"
      exit 0
    fi
    if docker inspect pgvs3-origin >/dev/null 2>&1; then
      docker start pgvs3-origin
    else
      printf '%s' '{"identities": [{"name": "bench", "actions": ["Admin"], "credentials": [{"accessKey": "cachebench", "secretKey": "cachebench-local-only"}]}]}' > .tmp/origin/s3.json
      docker run -d --name pgvs3-origin \
        -p 127.0.0.1:8337:8337 \
        -v pgvs3-origin-data:/data \
        -v "$(pwd)/.tmp/origin/s3.json:/etc/seaweed/s3.json" \
        chrislusf/seaweedfs:4.13 \
        server -dir=/data -master.volumeSizeLimitMB=4096 -s3 -s3.port=8337 -s3.config=/etc/seaweed/s3.json
    fi
    for i in $(seq 1 30); do
      curl -s -o /dev/null --max-time 1 http://127.0.0.1:8337/ && break
      sleep 1
    done
    printf 's3.bucket.create -name lake\n' | docker exec -i pgvs3-origin weed shell -master=localhost:9333 -filer=localhost:8888 >/dev/null 2>&1 || true
    echo "origin up: s3://lake@http://127.0.0.1:8337 (cachebench/cachebench-local-only)"

dev-origin-clean:
    docker rm -f pgvs3-origin || true
    docker volume rm pgvs3-origin-data || true

dev-db-clean:
    docker rm -f pgvs3-pg || true
    docker volume rm pgvs3-pg-data || true

rig-teardown:
    ./teardown-rig.sh
