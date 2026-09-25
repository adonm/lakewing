#!/usr/bin/env bash
# Build the benchmark image once, then run each Job in turn against the same
# kind stack. A failed Job must fail the recipe (and therefore CI).
set -euo pipefail
cd "$(dirname "$0")/../.."

cluster=pgvs3
namespace=pgvs3
kubectl=(kubectl --context "kind-$cluster" -n "$namespace")
helm=(helm --kube-context "kind-$cluster")
"${kubectl[@]}" get namespace "$namespace" >/dev/null

mbx build --release -p pgvs3
cp target/release/pgvs3 deploy/bench/pgvs3-bin
trap 'rm -f deploy/bench/pgvs3-bin' EXIT
# The canonical harness sources live in crates/pgvs3, not the image context.
cp crates/pgvs3/benchlib.py crates/pgvs3/tpch_bench.py \
  crates/pgvs3/analytics_bench.py deploy/bench/harness/
cp crates/pgvs3/queries/*.sql deploy/bench/harness/queries/
docker build -q -t kind-bench:latest -f deploy/bench/Dockerfile deploy/bench
kind load docker-image kind-bench:latest --name "$cluster"

suites=${SUITES:-pgbench,tpch,click,search,stress}
env_args=()
if [ -n "${PGVS3_DB_SECRET:-}" ]; then
  "${kubectl[@]}" get secret "$PGVS3_DB_SECRET" >/dev/null
  env_args+=(--set-string "pgSecretName=$PGVS3_DB_SECRET")
fi
for key in QUICK SCALE CLIENTS SECONDS_RUN SF PASSES PARTS QUERIES DOCS \
           WORKERS WINDOW_FRAC SEARCH_INDEX SEED_GB REQUESTS CONCURRENCY SIZES; do
  if [ -n "${!key:-}" ]; then
    value=${!key}
    # Helm's --set-string treats unescaped commas as value separators.
    value=${value//,/\\,}
    env_args+=(--set-string "suiteEnv.$key=$value")
  fi
done

mkdir -p .tmp/pgvs3/jobs
results=.tmp/pgvs3/kind-bench.jsonl
: > "$results"
wait_s=2700
case "${QUICK:-}" in 1 | true) wait_s=300 ;; esac

for suite in ${suites//,/ }; do
  case "$suite" in
    validate|pgbench|tpch|click|search|stress) ;;
    *) echo "unknown suite: $suite" >&2; exit 2 ;;
  esac
  echo "=== $suite ==="
  # A Job's pod template is immutable, so each new image needs a new Job.
  "${kubectl[@]}" delete job "bench-$suite" --ignore-not-found --wait=true >/dev/null
  "${helm[@]}" upgrade --install kind-bench deploy/charts/kind-bench \
    --namespace "$namespace" --reset-values --set image=kind-bench:latest \
    "${env_args[@]}" --set "suite=$suite"

  waited=0
  while :; do
    status=$("${kubectl[@]}" get job "bench-$suite" \
      -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
    case "$status" in *Complete=True*|*Failed=True*|*FailureTarget=True*) break ;; esac
    if [ "$waited" -ge "$wait_s" ]; then
      echo "TIMEOUT $suite after ${wait_s}s" >&2
      "${kubectl[@]}" describe job "bench-$suite" >&2
      exit 1
    fi
    sleep 2; waited=$((waited + 2))
  done

  log=".tmp/pgvs3/jobs/$suite.log"
  "${kubectl[@]}" logs "job/bench-$suite" | tee "$log"
  if [[ "$status" != *Complete=True* ]]; then
    echo "FAILED $suite: $status" >&2
    exit 1
  fi
  if [ "$suite" != validate ] && ! grep -q '^{.*}$' "$log"; then
    echo "FAILED $suite: no result JSON" >&2
    exit 1
  fi
  grep '^{.*}$' "$log" >> "$results" || true
done

echo "results: $results"
