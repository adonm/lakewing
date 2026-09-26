#!/usr/bin/env bash
# Suite runner for the kind benchmarks. One suite per invocation; each
# write a JSON line to stdout (the `just kind-bench` recipe concatenates them
# into .tmp/pgvs3/kind-bench.jsonl).
#
# Caching is intentional and left ON everywhere: this is "real world"
# performance — DuckDB keeps its external file cache, PostgreSQL keeps its
# buffer cache, Quickwit keeps its fast-field/split-footer caches. Passes
# after the first are warm.
set -euo pipefail

PG_HOST=${PG_HOST:-postgres}
PG_USER=${PG_USER:-postgres}
PG_PASSWORD=${PG_PASSWORD:-postgres}
PG_URL=${PG_URL:-"postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:5432/pgvs3"}
# pgbench authenticates with PGPASSWORD, not the URL.
export PGPASSWORD="$PG_PASSWORD"
# The harness talks to the gateway directly (DuckDB s3_endpoint, stats route).
QW_URL=${QUICKWIT_URL:-http://quickwit:7280}
QW_INGEST_URL=${QUICKWIT_INGEST_URL:-$QW_URL}
PGVS3_URL=${PGVS3_URL:-http://pgvs3:8014}
export PGVS3_URL
export PGVS3_ENDPOINT=${PGVS3_ENDPOINT:-${PGVS3_URL#*://}}
# One DuckLake data root: the catalog records its data path at first attach
# and every later attach must match it.
LAKE_ROOT=${LAKE_ROOT:-s3://lake/v/}
OUT=/bench/out
mkdir -p "$OUT"

# Smoke-scale parameters (QUICK=1): enough to prove a suite works end to end,
# not enough to publish. Full runs use the defaults in each suite below.
case "${QUICK:-0}" in
1 | true)
  SCALE=${SCALE:-1}
  CLIENTS=${CLIENTS:-4}
  SECONDS_RUN=${SECONDS_RUN:-10}
  SF=${SF:-1}
  PASSES=${PASSES:-1}
  PARTS=${PARTS:-1}
  QUERIES=${QUERIES:-20}
  DOCS=${DOCS:-5000}
  SEED_GB=${SEED_GB:-0.25}
  REQUESTS=${REQUESTS:-200}
  CONCURRENCY=${CONCURRENCY:-1,8}
  SIZES=${SIZES:-65536,262144}
  SPATIAL_SF=${SPATIAL_SF:-0.1}
  ;;
esac

suite=${1:?suite: validate | pgbench | tpch | click | spatial | search | stress}

case "$suite" in

# --- validate: each workload once, PASS/FAIL -------------------------------
# So a benchmark failure is never "the cluster wasn't ready".
validate)
  pass=0; fail=0
  check() {
    local name=$1; shift
    if out=$("$@" 2>&1); then echo "PASS  $name"; pass=$((pass + 1));
    else echo "FAIL  $name: $(echo "$out" | tail -1)"; fail=$((fail + 1)); fi
  }
  check "postgres (pg_isready)" pg_isready -h "$PG_HOST" -U "$PG_USER"
  check "ducklake (catalog database)" psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d ducklake_catalog -tAc 'SELECT 1'
  check "quickwit (metastore database)" psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d quickwit_metastore -tAc 'SELECT 1'
  # /healthz, not /_pgvs3/stats: the stats route is behind SigV4 (s3s
  # rejects unsigned GETs with 403), so it is not a probe path.
  check "pgvs3 (healthz)" curl -fsS --max-time 10 "$PGVS3_URL/healthz"
  # Quickwit's REST API has no /healthz; /api/v1/cluster is the live node view.
  check "quickwit (search API)" curl -fsS --max-time 10 "$QW_URL/api/v1/cluster"
  check "quickwit (ingest API)" curl -fsS --max-time 10 "$QW_INGEST_URL/api/v1/cluster"
  check "ducklake (attach + read)" python3 /bench/suites/duck_check.py
  check "pgvs3 (overwrite self-heal)" python3 /bench/suites/overwrite_check.py
  echo "validate: $pass passed, $fail failed"
  [ "$fail" = 0 ]
  ;;

# --- PostgreSQL OLTP (pgbench) ---------------------------------------------
# Classic pgbench: init at --scale, then a plain read/write run. pgbench
# reports its own latency percentiles; we echo them and add a JSON summary.
pgbench)
  scale=${SCALE:-10}
  clients=${CLIENTS:-16}
  seconds=${SECONDS_RUN:-60}
  # Init failure surfaces (set -e) instead of leaving an empty database to
  # time; the run's output is captured even on failure so the log says why.
  pgbench -h "$PG_HOST" -U "$PG_USER" -i -s "$scale" pgvs3 >/dev/null
  out=$(pgbench -h "$PG_HOST" -U "$PG_USER" -c "$clients" -j "$clients" -T "$seconds" pgvs3 2>&1)
  echo "$out"
  tps=$(echo "$out" | sed -n 's/.*tps = \([0-9.]*\).*/\1/p' | tail -1)
  lat=$(echo "$out" | sed -n 's/.*latency average = \([0-9.]*\) ms.*/\1/p' | tail -1)
  printf '{"suite":"pgbench","scale":%s,"clients":%s,"seconds":%s,"tps":%s,"latency_avg_ms":%s}\n' \
    "$scale" "$clients" "$seconds" "${tps:-0}" "${lat:-0}" > "$OUT/pgbench.json"
  cat "$OUT/pgbench.json"
  ;;

# --- OLAP: TPC-H over DuckLake ----------------------------------------------
tpch)
  # TPC-H via DuckDB's tpch extension, tables copied into DuckLake (catalog =
  # PostgreSQL, data = pgvs3). --passes 2 = one cold, one warm.
  python3 /bench/harness/tpch_bench.py --stack lake-s3 --sf "${SF:-10}" --load --passes "${PASSES:-2}" \
    --data-path "$LAKE_ROOT" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER sslmode=${PGSSLMODE:-prefer}" \
    --out "$OUT/tpch.json"
  cat "$OUT/tpch.json"
  ;;

# --- OLAP: ClickBench over DuckLake ----------------------------------------
click)
  # --parts N downloads N disjoint 1% slices of canonical hits.parquet
  # (typed like the full set) so the run fits the budget; full is 13.8 GiB.
  python3 /bench/harness/analytics_bench.py --bench click --stack lake-s3 --download --load \
    --parts "${PARTS:-0}" --passes "${PASSES:-2}" \
    --memory-limit "${DUCKDB_MEMORY_LIMIT:-6GiB}" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER sslmode=${PGSSLMODE:-prefer}" \
    --data-path "$LAKE_ROOT" \
    --out "$OUT/click.json"
  cat "$OUT/click.json"
  ;;

# --- Spatial: AOI filters over Sedona SpatialBench trip/zone data ---------
spatial)
  python3 /bench/harness/analytics_bench.py --bench spatial --stack lake-s3 --download --load \
    --sf "${SPATIAL_SF:-10}" --queries "${SPATIAL_QUERIES:-1-3,6}" \
    --query-timeout "${SPATIAL_QUERY_TIMEOUT:-600}" --passes "${PASSES:-2}" \
    --memory-limit "${DUCKDB_MEMORY_LIMIT:-6GiB}" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER sslmode=${PGSSLMODE:-prefer}" \
    --data-path "$LAKE_ROOT" \
    --out "$OUT/spatial.json"
  cat "$OUT/spatial.json"
  ;;

# --- Search: OSB bulk + queries via Quickwit's ES-compatible API ----------
search)
  python3 /bench/suites/osb_run.py --docs "${DOCS:-100000000}" \
    --index "${SEARCH_INDEX:-auto}" --window-frac "${WINDOW_FRAC:-0.1}" \
    --queries "${QUERIES:-400}" --out "$OUT/search.json"
  ;;

# --- pgvs3 stress: the ceiling ---------------------------------------------
# The built-in load generator (it reports aggregate MiB/s and req/s over the
# wall clock). Objects come from the `lake` bucket, which `just kind-bench`
# seeds first. Concurrency sweep finds where throughput stops climbing.
stress)
  # Objects first: the bench reads seeded objects through the gateway.
  pgvs3 --url "$PG_URL" seed --bucket lake --gigabytes "${SEED_GB:-8}" --object-mib 64 --tasks 16
  pgvs3 --url "$PG_URL" bench --endpoint "$PGVS3_URL" --bucket lake \
    --requests "${REQUESTS:-4000}" --concurrency "${CONCURRENCY:-1,8,32,64,128}" --sizes "${SIZES:-65536,1048576,8388608}" \
    | tee "$OUT/stress.txt"
  ;;

*)
  echo "unknown suite: $suite" >&2
  exit 2
  ;;
esac
