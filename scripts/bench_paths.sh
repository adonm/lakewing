#!/usr/bin/env bash
# Matched DuckLake storage-path measurements inside the dedicated kind rig.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec python3 "$ROOT/scripts/cachebench/rig.py" "$@"
