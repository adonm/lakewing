#!/usr/bin/env bash
# One kind workload stack. Local/CI uses the single PostgreSQL pod; EC2 kind
# sets PGVS3_DB_SECRET and uses an external Aurora cluster instead.
set -euo pipefail
cd "$(dirname "$0")/../.."

image=${1:?pass the pgvs3 image name}
searchers=${QUICKWIT_SEARCHERS:-0}
[[ "$searchers" =~ ^[0-9]+$ ]] || { echo 'QUICKWIT_SEARCHERS must be a non-negative integer' >&2; exit 2; }
cluster=pgvs3
namespace=pgvs3
kubectl=(kubectl --context "kind-$cluster")
helm=(helm --kube-context "kind-$cluster")

if ! kind get clusters | grep -Fxq "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind/cluster.yaml
fi

# The tag does not change between builds. Load this checkout, then restart
# the pods: IfNotPresent keeps kind from trying to pull an unpublished image.
docker build -q -t "$image:latest" .
kind load docker-image "$image:latest" --name "$cluster"
"${kubectl[@]}" create namespace "$namespace" --dry-run=client -o yaml \
  | "${kubectl[@]}" apply -f -

if [ -n "${PGVS3_DB_SECRET:-}" ]; then
  # External PostgreSQL: the Secret contains the URL, not Helm values/history.
  "${kubectl[@]}" -n "$namespace" get secret "$PGVS3_DB_SECRET" >/dev/null
  secret=$PGVS3_DB_SECRET
else
  "${helm[@]}" upgrade --install postgres deploy/charts/postgres --namespace "$namespace" \
    --reset-values --set "storage=${PG_STORAGE:-20Gi}"
  "${kubectl[@]}" -n "$namespace" rollout status statefulset/postgres --timeout=300s
  secret=postgres
fi
# Both backends get the same three logical databases before Quickwit starts.
bash deploy/kind/db.sh "$secret"
"${helm[@]}" upgrade --install pgvs3 deploy/charts/pgvs3 --namespace "$namespace" \
  --reset-values --set "image=$image:latest" --set-string "urlSecretName=$secret"
# Existing clusters used RollingUpdate. Helm 4 server-side apply retains its
# rollingUpdate field when changing type to Recreate, which Kubernetes rejects;
# make the one-time strategy migration explicitly before the chart upgrade.
strategy=$("${kubectl[@]}" -n "$namespace" get deployment quickwit \
  -o jsonpath='{.spec.strategy.type}' 2>/dev/null || true)
if [ "$strategy" = RollingUpdate ]; then
  "${kubectl[@]}" -n "$namespace" patch deployment quickwit --type=merge \
    -p '{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null}}}'
fi
"${helm[@]}" upgrade --install quickwit deploy/charts/quickwit --namespace "$namespace" \
  --reset-values --set-string "metastoreSecretName=$secret" --set "searcher.replicas=$searchers"
"${kubectl[@]}" -n "$namespace" rollout restart deployment/pgvs3
# A ConfigMap update does not change Quickwit's Deployment pod template.
"${kubectl[@]}" -n "$namespace" rollout restart deployment/quickwit
"${kubectl[@]}" -n "$namespace" rollout status deployment/pgvs3 --timeout=300s
"${kubectl[@]}" -n "$namespace" rollout status deployment/quickwit --timeout=300s
if [ "$searchers" -gt 0 ]; then
  "${kubectl[@]}" -n "$namespace" rollout status deployment/quickwit-searcher --timeout=300s
fi
echo 'kind up. next: just kind-validate && just kind-bench'
