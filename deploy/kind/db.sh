#!/usr/bin/env bash
# Idempotent database setup shared by local kind and external Aurora kind.
set -euo pipefail
cd "$(dirname "$0")/../.."

secret=${1:?pass the database Secret name}
[[ "$secret" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo "invalid database Secret name: $secret" >&2
  exit 2
}
kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
"${kubectl[@]}" get secret "$secret" >/dev/null
"${kubectl[@]}" delete job pgvs3-databases --ignore-not-found --wait=true >/dev/null
sed "s/__DB_SECRET__/$secret/g" deploy/kind/db-job.yaml | "${kubectl[@]}" apply -f -

for _ in $(seq 1 90); do
  status=$("${kubectl[@]}" get job pgvs3-databases \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  case "$status" in *Complete=True*|*Failed=True*|*FailureTarget=True*) break ;; esac
  sleep 2
done
"${kubectl[@]}" logs job/pgvs3-databases
if [[ "$status" != *Complete=True* ]]; then
  echo "database setup failed or timed out: $status" >&2
  "${kubectl[@]}" describe job pgvs3-databases >&2
  exit 1
fi
