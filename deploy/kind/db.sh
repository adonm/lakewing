#!/usr/bin/env bash
# Idempotent database setup shared by local kind and external Aurora kind.
set -euo pipefail
cd "$(dirname "$0")/../.."

secret=${1:?pass the database Secret name}
phase=${2:-databases}
[[ "$secret" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo "invalid database Secret name: $secret" >&2
  exit 2
}
case "$phase" in databases|schedule|reset) ;; *) echo "invalid database phase: $phase" >&2; exit 2 ;; esac
job="pgvs3-db-$phase"
kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
"${kubectl[@]}" get secret "$secret" >/dev/null
"${kubectl[@]}" delete job "$job" --ignore-not-found --wait=true >/dev/null
sed -e "s/__DB_SECRET__/$secret/g" -e "s/__JOB_NAME__/$job/g" \
  -e "s/__PHASE__/$phase/g" deploy/kind/db-job.yaml | "${kubectl[@]}" apply -f -

for _ in $(seq 1 90); do
  status=$("${kubectl[@]}" get job "$job" \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  case "$status" in *Complete=True*|*Failed=True*|*FailureTarget=True*) break ;; esac
  sleep 2
done
"${kubectl[@]}" logs "job/$job"
if [[ "$status" != *Complete=True* ]]; then
  echo "database $phase failed or timed out: $status" >&2
  "${kubectl[@]}" describe job "$job" >&2
  exit 1
fi
