# Rust via mise toolchain. lance builds need protoc (setup-protoc fetches
# a pinned binary into .tmp/protoc when absent).
export PROTOC := justfile_directory() + "/.tmp/protoc/bin/protoc"

default: check

setup: setup-protoc
    cargo fetch

setup-protoc:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -x "{{PROTOC}}" ]; then
      mkdir -p .tmp/protoc /tmp/opencode
      curl -sL -o /tmp/opencode/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip
      python3 -c "import zipfile; zipfile.ZipFile('/tmp/opencode/protoc.zip').extractall('.tmp/protoc')"
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
