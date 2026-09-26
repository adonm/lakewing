#!/usr/bin/env bash
# Run the Rust S3 contract through both gateway pods, with DB audit queries.
# Port-forwards are local to this host (also works on the EC2 kind rig).
set -euo pipefail
cd "$(dirname "$0")/../.."
mode=${1:-contract}
case "$mode" in contract|churn) ;; *) echo "unknown contract mode: $mode" >&2; exit 2 ;; esac

kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
mapfile -t pods < <("${kubectl[@]}" get pods -l app=pgvs3 -o json | python3 -c '
import json, sys
for pod in json.load(sys.stdin)["items"]:
    statuses = pod["status"].get("containerStatuses", [])
    if (pod["status"]["phase"] == "Running"
            and not pod["metadata"].get("deletionTimestamp")
            and statuses and all(s["ready"] for s in statuses)):
        print(pod["metadata"]["name"])
')
[ "${#pods[@]}" -eq 2 ] || { echo 'expected exactly two running gateway pods' >&2; exit 1; }

mkdir -p .tmp/pgvs3
exec 9>.tmp/pgvs3/contract.lock
flock -n 9 || { echo 'another S3 contract test is running' >&2; exit 3; }
pids=()
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

"${kubectl[@]}" port-forward "pod/${pods[0]}" 18014:8014 > .tmp/pgvs3/contract-a.log 2>&1 & pids+=("$!")
"${kubectl[@]}" port-forward "pod/${pods[1]}" 18015:8014 > .tmp/pgvs3/contract-b.log 2>&1 & pids+=("$!")
secret=${PGVS3_DB_SECRET:-postgres}
url=$("${kubectl[@]}" get secret "$secret" -o jsonpath='{.data.url}' | base64 --decode)
if [ -z "${PGVS3_DB_SECRET:-}" ]; then
  "${kubectl[@]}" port-forward service/postgres 15432:5432 > .tmp/pgvs3/contract-pg.log 2>&1 & pids+=("$!")
  url=$(printf '%s' "$url" | python3 -c 'import sys,urllib.parse as u; p=u.urlsplit(sys.stdin.read()); auth=p.netloc.rsplit("@",1)[0]; print(u.urlunsplit((p.scheme, auth+"@127.0.0.1:15432", p.path, p.query, p.fragment)))')
fi

for port in 18014 18015; do
  ready=0
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
  done
  if [ "$ready" -ne 1 ]; then
    echo "gateway port $port unavailable; see .tmp/pgvs3/contract-{a,b}.log" >&2
    exit 1
  fi
done
if [ -z "${PGVS3_DB_SECRET:-}" ]; then
  ready=0
  for _ in $(seq 1 60); do
    if (echo > /dev/tcp/127.0.0.1/15432) 2>/dev/null; then ready=1; break; fi
    sleep 0.25
  done
  [ "$ready" -eq 1 ] || { echo 'PostgreSQL port-forward unavailable' >&2; exit 1; }
fi

export PGVS3_TEST_ENDPOINT_A=http://127.0.0.1:18014
export PGVS3_TEST_ENDPOINT_B=http://127.0.0.1:18015
export PGVS3_TEST_DB_URL="$url"
export PGVS3_POOL_MIN=1 PGVS3_POOL_MAX=4
bash deploy/kind/buckets.sh "$PGVS3_TEST_ENDPOINT_A" pgvs3-contract
# A previous failed run may have left published *test* objects behind. Purge
# only our dedicated contract prefix through S3, never with table operations.
mise exec -- mbx test -p pgvs3 --test s3_contract clean_failed_prior_contract_objects -- --ignored --exact
if [ "$mode" = churn ]; then
  mise exec -- mbx test -p pgvs3 --test s3_contract sustained_churn_and_db_reclaim -- --ignored --exact --nocapture
else
  mise exec -- mbx test -p pgvs3 --test s3_contract -- --ignored \
    --skip clean_failed_prior_contract_objects --skip sustained_churn_and_db_reclaim --test-threads=1
  mise exec -- mbx test -p pgvs3 --lib -- --ignored --test-threads=1
fi
