# Rust via mise toolchain. lance builds need protoc (setup-protoc fetches
# a pinned binary into .tmp/protoc when absent). Index training spills to
# TMPDIR — keep it on this NVMe tree, not the small tmpfs /tmp.
export PROTOC := justfile_directory() + "/.tmp/protoc/bin/protoc"
export TMPDIR := justfile_directory() + "/.tmp/tmp"

default: check

setup: setup-protoc
    cargo fetch

setup-protoc:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .tmp/protoc .tmp/tmp
    if [ ! -x "{{PROTOC}}" ]; then
      curl -sL -o .tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip
      python3 -c "import zipfile; zipfile.ZipFile('.tmp/protoc.zip').extractall('.tmp/protoc')"
      chmod +x "{{PROTOC}}"
    fi
    "{{PROTOC}}" --version

check: setup-protoc
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings

build: setup-protoc
    cargo build --release

test: setup-protoc
    cargo test

# Serve a pinned Lance dataset (WKB-geometry; see docs/rust-architecture.md).
# Example local: just run -- --uri .tmp/.../wkb.lance --tag prod --listen 127.0.0.1:3129
run *args: setup-protoc
    cargo run --release -- {{args}}

# Benchmark battery against a running serve (python; equality + timings).
lance-bench base="http://127.0.0.1:3129" *args:
    python3 scripts/lancebench/battery.py {{base}} {{args}}

# --- Local S3-like origin (one container; no kind cluster, no port-forwards) ---
# Real S3 semantics (HTTP + range GETs, path-style) backed by the docker
# volume `lakewing-origin-data`, with fixed benchmark credentials. Uses the
# seaweedfs image already present on this host.
dev-origin:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .tmp/origin
    if curl -s -o /dev/null --max-time 1 http://127.0.0.1:8337/; then
      echo "origin already listening on :8337"
      exit 0
    fi
    if docker inspect lakewing-origin >/dev/null 2>&1; then
      docker start lakewing-origin
    else
      printf '%s' '{"identities": [{"name": "bench", "actions": ["Admin"], "credentials": [{"accessKey": "cachebench", "secretKey": "cachebench-local-only"}]}]}' > .tmp/origin/s3.json
      docker run -d --name lakewing-origin \
        -p 127.0.0.1:8337:8337 \
        -v lakewing-origin-data:/data \
        -v "$(pwd)/.tmp/origin/s3.json:/etc/seaweed/s3.json" \
        chrislusf/seaweedfs:4.13 \
        server -dir=/data -master.volumeSizeLimitMB=4096 -s3 -s3.port=8337 -s3.config=/etc/seaweed/s3.json
    fi
    for i in $(seq 1 30); do
      # Any HTTP response means the S3 API is up (404 on / is normal);
      # only a refused connection keeps waiting.
      curl -s -o /dev/null --max-time 1 http://127.0.0.1:8337/ && break
      sleep 1
    done
    curl -s -o /dev/null --max-time 2 http://127.0.0.1:8337/ || {
      docker logs lakewing-origin 2>&1 | tail -5
      echo "origin failed to come up on :8337" >&2
      exit 1
    }
    printf 's3.bucket.create -name lake\n' | docker exec -i lakewing-origin weed shell -master=localhost:9333 -filer=localhost:8888 >/dev/null 2>&1 || true
    for i in $(seq 1 10); do
      curl -sf -o /dev/null --max-time 2 --aws-sigv4 aws:amz:us-east-1:s3 \
        --user cachebench:cachebench-local-only "http://127.0.0.1:8337/lake?list-type=2&max-keys=0" && break
      printf 's3.bucket.create -name lake\n' | docker exec -i lakewing-origin weed shell -master=localhost:9333 -filer=localhost:8888 >/dev/null 2>&1 || true
      sleep 1
    done
    curl -sf -o /dev/null --max-time 2 --aws-sigv4 aws:amz:us-east-1:s3 \
      --user cachebench:cachebench-local-only "http://127.0.0.1:8337/lake?list-type=2&max-keys=0" || {
      echo "bucket creation failed" >&2
      exit 1
    }
    echo "origin up: s3://lake@http://127.0.0.1:8337 (cachebench/cachebench-local-only)"

# Remove the local origin container and its data volume.
dev-origin-clean:
    docker rm -f lakewing-origin || true
    docker volume rm lakewing-origin-data || true

# Upload a local .lance dataset (or any dir) into the local origin.
dev-seed src s3prefix:
    #!/usr/bin/env bash
    set -euo pipefail
    curl -s -o /dev/null --max-time 2 http://127.0.0.1:8337/ || { echo "run 'just dev-origin' first"; exit 1; }
    RCLONE_CONFIG_ORIGIN_TYPE=s3 \
    RCLONE_CONFIG_ORIGIN_PROVIDER=Other \
    RCLONE_CONFIG_ORIGIN_ENDPOINT=http://127.0.0.1:8337 \
    RCLONE_CONFIG_ORIGIN_ACCESS_KEY_ID=cachebench \
    RCLONE_CONFIG_ORIGIN_SECRET_ACCESS_KEY=cachebench-local-only \
    RCLONE_CONFIG_ORIGIN_REGION=us-east-1 \
    RCLONE_CONFIG_ORIGIN_FORCE_PATH_STYLE=true \
    rclone copy "{{src}}" "origin:lake/{{s3prefix}}" --transfers 8
    echo "seeded s3://lake/{{s3prefix}}"

# Serve the local origin with modeled S3 latency (example: 20 ms + 500 Mbit/s).
# The throttle applies to origin GETs beneath the cache — sweeps then show
# latency savings, not just GET-count savings.
dev-serve-latency uri="s3://lake/lancebench/local/geo.lance" latency_ms="20" mbps="500" port="3200":
    cargo run --release -- --uri {{uri}} --tag prod --listen 127.0.0.1:{{port}} \
      --endpoint http://127.0.0.1:8337 --s3-key cachebench --s3-secret cachebench-local-only \
      --cache-dir .tmp/origin/serve-cache --origin-latency-ms {{latency_ms}} --origin-mbps {{mbps}}
