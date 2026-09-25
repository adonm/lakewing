#!/usr/bin/env bash
# Pre-release rig/CI data only. Stop writers before replacing the three
# dedicated test databases, then use kind-up to redeploy them from scratch.
set -euo pipefail
cd "$(dirname "$0")/../.."

kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
active=$("${kubectl[@]}" get jobs -o json | python3 -c '
import json, sys
print(sum(job.get("status", {}).get("active", 0) for job in json.load(sys.stdin)["items"]))
')
[ "$active" = 0 ] || { echo "$active benchmark/setup Jobs still running; refusing reset" >&2; exit 1; }
for app in quickwit-searcher quickwit pgvs3; do
  if "${kubectl[@]}" get deployment "$app" >/dev/null 2>&1; then
    "${kubectl[@]}" scale "deployment/$app" --replicas=0
  fi
done
for app in quickwit-searcher quickwit pgvs3; do
  if [ -n "$("${kubectl[@]}" get pods -l "app=$app" -o name)" ]; then
    "${kubectl[@]}" wait --for=delete pod -l "app=$app" --timeout=180s
  fi
done
bash deploy/kind/db.sh "${PGVS3_DB_SECRET:-postgres}" reset
echo 'test databases reset; run just kind-up to bring the stack back'
