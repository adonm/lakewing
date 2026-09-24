#!/usr/bin/env bash
# Read-focused benchmark loops on the AWS rig (Aurora catalog + data through
# pgvs3). Results land in .tmp/pgvs3/rig-out/ (JSON per run + gateway perf).
#
#   ./bench-rig.sh load [quick|full]  the ONLY mode that writes: (re)load the
#                           datasets once per layout version (default: both)
#   ./bench-rig.sh quick    reads: clickbench 10% + spatialbench sf1 Q1-Q7
#   ./bench-rig.sh full     reads: clickbench 100% + spatialbench sf10 Q1-Q7
#   ./bench-rig.sh micro    reads, no engine: GET latency/throughput matrix
#                           over the loaded objects + direct-PostgreSQL floor
#   ./bench-rig.sh sweep    reads: gateway-config A/B on the quick data
#   ./bench-rig.sh stop     kill any running driver/gateway (safe to run anytime)
#   ./bench-rig.sh wait     re-attach to an in-flight run and fetch results
#
# Read runs: fresh DuckDB + fresh gateway per bench, DuckDB's external file
# cache off (every pass reads through the proxy), 3 passes, per-pass proxy
# GETs/MiB/MiB/s and gateway GET p50/p95/p99. Aurora stays warm: evicting its
# buffer cache (pg_buffercache_evict_*) needs superuser, which Aurora withholds.
# ALL storage is Aurora: the pgvs3 chunks (pgvs3_bench) and the DuckLake
# catalogs (ducklake_click_lake_s3, ducklake_spatial_lake_s3) share the one
# cluster, so any setup attaching them sees the same schema (multi_check
# proves it after every load via a second gateway + fresh session).
# Needs .tmp/pgvs3/aws-rig.env (PGVS3_PG_PASSWORD) and the rig keypair.
set -euo pipefail
MODE=${1:-quick}
P=${AWS_PROFILE:-platform-dev}
INST=i-037e91b323d000a1b
KEY=.tmp/pgvs3/pgvs3-perf-key.pem
[ -f .tmp/pgvs3/aws-rig.env ] || { echo "missing .tmp/pgvs3/aws-rig.env"; exit 1; }
# shellcheck disable=SC1091
source .tmp/pgvs3/aws-rig.env

if IP=$(aws ec2 describe-instances --profile "$P" --region us-east-2 --instance-ids $INST \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null); then
  echo "$IP" > .tmp/pgvs3/rig-ip
else
  # The public IP only changes on stop/start: without an AWS session (e.g. an
  # expired SSO login) reuse the last one seen.
  IP=$(cat .tmp/pgvs3/rig-ip)
  echo "(aws unavailable, using the cached rig IP; refresh: aws sso login --profile $P)"
fi
echo "rig: $IP (mode: $MODE)"
SSH=(ssh -i $KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 "ec2-user@$IP")

if [ "$MODE" = "stop" ]; then
  # Bash scripts have comm="bash": match with the [x] trick, never bare -f.
  "${SSH[@]}" "pkill -f '[r]un-analytics.sh'; pkill -x uv; pkill -x python; pkill -x pgvs3; rm -f ~/BENCH.DONE /tmp/pgvs3-bench.lock; echo stopped"
  exit 0
fi

if [ "$MODE" != "wait" ]; then
  tar czf - Cargo.toml Cargo.lock .cargo crates mise.toml justfile | "${SSH[@]}" 'tar xzf -'
  # Rebuild only when the Rust build inputs changed (a fat-LTO relink is ~1.5
  # min; harness/SQL edits need none). Built on the rig: Fedora-built binaries
  # do not run on AL2023. The hash is recorded only after a successful build.
  HASH=$(find crates Cargo.toml Cargo.lock .cargo -type f \( -name '*.rs' -o -name Cargo.toml \
           -o -name Cargo.lock -o -name schema.sql -o -path '.cargo/*' \) -print0 \
         | sort -z | xargs -0 sha256sum | sha256sum | cut -c1-16)
  "${SSH[@]}" "if [ -x target/release/pgvs3 ] && [ \"\$(cat .pgvs3-build 2>/dev/null)\" = $HASH ]; then
      echo 'binary current ($HASH)'
    elif touch crates/pgvs3/src/lib.rs && ~/.cargo/bin/cargo build --release -p pgvs3 >/tmp/pgvs3-build.log 2>&1; then
      tail -1 /tmp/pgvs3-build.log; echo $HASH > .pgvs3-build
    else
      tail -20 /tmp/pgvs3-build.log; exit 1
    fi"
  "${SSH[@]}" 'cat > ~/run-analytics.sh' <<'DRV'
#!/usr/bin/env bash
# $1 = quick | full | sweep. Results in ~/bench-out, ~/BENCH.DONE marks the end.
set -uo pipefail
exec 9>/tmp/pgvs3-bench.lock
flock -n 9 || { echo "another bench driver is running"; exit 3; }
cd /home/ec2-user
export PATH=$HOME/.local/bin:$HOME/.cargo/bin:$PATH
MODE=${1:?load|quick|full|micro|sweep}
TARGET=${2:-all}
PW=$(cat ~/.pgpw)
END=pgvs3-perf.cluster-csmlp5ndujwv.us-east-2.rds.amazonaws.com
BASE="postgres://pgvs3admin:${PW}@${END}:5432/pgvs3_bench?sslmode=require"
ADMIN="postgres://pgvs3admin:${PW}@${END}:5432/postgres?sslmode=require"
CAT_C3="dbname=ducklake_click_lake_s3 host=$END user=pgvs3admin password=$PW sslmode=require"
CAT_S3="dbname=ducklake_spatial_lake_s3 host=$END user=pgvs3admin password=$PW sslmode=require"
PRE=$(mise exec -- printenv DUCKDB_PY_PRE)
mkdir -p bench-out bench-data
rm -f BENCH.DONE

# --stack/--catalog are passed per run_one call (one Aurora instance for the
# pgvs3 chunks AND every DuckLake catalog - no localhost fallbacks).
CLICK=(--bench click --data-path 's3://lake/run-31/'
       --src-dir /home/ec2-user/bench-data/clickbench
       --local-dir /home/ec2-user/bench-data/click-lake-local
       --plain-db /home/ec2-user/bench-data/click-plain.duckdb
       --scratch-db /home/ec2-user/bench-data/scratch-click.duckdb)
SPAT=(--bench spatial --data-path 's3://lake/run-32/'
      --src-dir /home/ec2-user/bench-data/spatialbench
      --local-dir /home/ec2-user/bench-data/spatial-lake-local
      --plain-db /home/ec2-user/bench-data/spatial-plain.duckdb
      --scratch-db /home/ec2-user/bench-data/scratch-spatial.duckdb)

SPLIT=8388608  # parallel part bytes (PGVS3_SPLIT_BYTES; 0 = one query per span)
# Full runs keep their own catalogs + data paths so the quick loop's data
# survives them.
CAT_CF="dbname=ducklake_click_full host=$END user=pgvs3admin password=$PW sslmode=require"
CAT_SF="dbname=ducklake_spatial_full host=$END user=pgvs3admin password=$PW sslmode=require"

fresh_catalogs() {
  for db in "$@"; do
    psql "$ADMIN" -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
    psql "$ADMIN" -c "CREATE DATABASE $db" >/dev/null 2>&1
  done
}

gw_start() {
  pkill -x pgvs3 || true
  sleep 1
  PGVS3_SPLIT_BYTES=$SPLIT \
    nohup ./target/release/pgvs3 --url "$BASE" serve --addr 127.0.0.1:8014 >/tmp/s-bench.log 2>&1 &
  for _ in $(seq 1 20); do
    curl -s -o /dev/null --max-time 1 http://127.0.0.1:8014/ && return 0
    sleep 0.5
  done
  echo "gateway failed to start"
  tail -5 /tmp/s-bench.log
  exit 1
}

run_one() {
  local label=$1 catalog="" prev=""
  shift
  for a in "$@"; do [ "$prev" = "--catalog" ] && catalog=$a; prev=$a; done
  # Banner shows the effective catalog (db + host, never the password): every
  # DuckLake catalog must be on the pgvs3 Aurora instance.
  local catdesc
  catdesc=$(sed -E 's/.*(dbname=[^ ]+).*(host=[^ ]+).*/\1 \2/' <<<"$catalog")
  echo "=== $label [SPLIT=$SPLIT${BENCH_EXTRA:+ $BENCH_EXTRA}] catalog=${catdesc:-n/a} $(date -u +%H:%M:%S) ==="
  gw_start
  # BENCH_EXTRA: extra harness args for A/B runs (word-split on purpose), e.g.
  # BENCH_EXTRA='--set httpfs_client_implementation=curl' ./bench-rig.sh quick
  # shellcheck disable=SC2086
  mise exec -- uv run --with "duckdb==$PRE" python crates/pgvs3/analytics_bench.py "$@" ${BENCH_EXTRA:-} \
    --out "/home/ec2-user/bench-out/$label.json" 2>&1 | tail -8
  echo "--- $label stats:"
  curl -s --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
    http://127.0.0.1:8014/_pgvs3/stats || true
}

# Multi-setup consistency: a SECOND gateway (own cache, :8015) and a fresh
# DuckDB session attach the same Aurora catalog + pg-backed S3 path and must
# see the same schema and rows. This is the product property - any worker can
# join on the same catalog+storage and see identical state.
multi_check() {
  local label=$1 catalog=$2 datapath=$3 tables=$4
  echo "=== multi-setup check: $label ==="
  nohup ./target/release/pgvs3 --url "$BASE" serve --addr 127.0.0.1:8015 >/tmp/s-bench2-$label.log 2>&1 &
  local gw2=$!
  sleep 1
  mise exec -- uv run --with "duckdb==$PRE" python - "$catalog" "$datapath" "$tables" <<'PY' || echo "multi-setup $label: FAIL"
import sys
import duckdb

cat, data_path, tables = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
con = duckdb.connect()
for e in ("postgres", "httpfs", "ducklake"):
    con.sql(f"INSTALL {e}")
    con.sql(f"LOAD {e}")
con.sql("SET s3_endpoint='127.0.0.1:8015'")  # the second gateway, not the runner's
con.sql("SET s3_use_ssl=false")
con.sql("SET s3_url_style='path'")
con.sql("SET s3_access_key_id='cachebench'")
con.sql("SET s3_secret_access_key='cachebench-local-only'")
con.sql(f"ATTACH 'ducklake:postgres:{cat}' AS lake (DATA_PATH '{data_path}')")
names = sorted(r[0] for r in con.sql("SHOW TABLES FROM lake").fetchall())
print("tables:", ",".join(names))
ok = True
for t in tables:
    n = con.sql(f"SELECT count(*) FROM lake.{t}").fetchone()[0]
    print(f"  {t}: {n} rows")
    ok = ok and n >= 0
# one real ranged read through the second gateway's cache
t0 = tables[0]
row = con.sql(f"SELECT * FROM lake.{t0} LIMIT 1").fetchall()
print("multi-setup:", "PASS" if names and row else "FAIL")
PY
  kill $gw2 2>/dev/null || true
}

case $MODE in
load)
  # The only mode that writes: (re)create the benchmark datasets once per
  # layout version; every other mode reads them.
  if [ "$TARGET" != full ]; then
    fresh_catalogs ducklake_click_lake_s3 ducklake_spatial_lake_s3
    run_one load-click "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" --parts 10 --download --load --passes 0
    multi_check click "$CAT_C3" 's3://lake/run-31/' hits
    # sf1 not sf0.1: at sf0.1 the generator emits 0 buildings and DuckLake
    # inlines the small tables into the catalog (no proxy traffic at all).
    run_one load-spatial "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 1 --download --load --passes 0
    multi_check spatial "$CAT_S3" 's3://lake/run-32/' 'trip,customer,driver,vehicle,zone,building'
  fi
  if [ "$TARGET" != quick ]; then
    fresh_catalogs ducklake_click_full ducklake_spatial_full
    run_one load-click-full "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_CF" --data-path 's3://lake/run-33/' \
      --download --load --passes 0
    multi_check click-full "$CAT_CF" 's3://lake/run-33/' hits
    run_one load-spatial-full "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_SF" --data-path 's3://lake/run-34/' \
      --sf 10 --download --load --passes 0
    multi_check spatial-full "$CAT_SF" 's3://lake/run-34/' 'trip,customer,driver,vehicle,zone,building'
  fi
  ;;
quick | full)
  # Reads only, on data from `load`. SpatialBench Q1-Q7: Q8-Q12 are DuckDB's
  # CPU-bound spatial joins (and a 2.0-alpha binder bug), no read-path signal.
  if [ "$MODE" = quick ]; then
    run_one click-quick "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" \
      --views-only --passes 3 --no-file-cache --query-timeout 300
    run_one spatial-quick "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 1 \
      --views-only --passes 3 --no-file-cache --queries 1-7 --query-timeout 120
  else
    run_one click-full "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_CF" --data-path 's3://lake/run-33/' \
      --views-only --passes 3 --no-file-cache --query-timeout 600
    run_one spatial-full "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_SF" --data-path 's3://lake/run-34/' --sf 10 \
      --views-only --passes 3 --no-file-cache --queries 1-7 --query-timeout 600
  fi
  ;;
micro)
  # The raw read path with no engine in it: GET latency/throughput over the
  # loaded objects (sizes x concurrency), plus the direct-PostgreSQL floor.
  gw_start
  ./target/release/pgvs3 --url "$BASE" bench --endpoint http://127.0.0.1:8014 --bucket lake \
    --requests 2000 --sizes 65536,262144,1048576,8388608,67108864 --concurrency 1,8,32 2>&1 | tee bench-out/micro.txt
  echo "--- micro stats:"
  curl -s --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
    http://127.0.0.1:8014/_pgvs3/stats || true
  ;;
sweep)
  # Gateway-config A/B on the quick data (reads only). SPLIT sizes the
  # fan-out of one-buffer (<= 8 MiB) spans across connections; 0 = one query
  # per span.
  for cfg in "8388608 8m" "2097152 2m" "1048576 1m"; do
    read -r SPLIT TAG <<<"$cfg"
    run_one "sweep-click-$TAG" "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" \
      --views-only --passes 3 --no-file-cache --query-timeout 300
    run_one "sweep-spatial-$TAG" "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 1 \
      --views-only --passes 3 --no-file-cache --queries 1-7 --query-timeout 120
  done
  ;;
*)
  echo "unknown mode: $MODE"; exit 2
  ;;
esac

pkill -x pgvs3 || true
cp /tmp/s-bench.log /home/ec2-user/bench-out/gateway-last.log 2>/dev/null || true
df -h / > /home/ec2-user/bench-out/df.txt
touch BENCH.DONE
echo DONE
DRV
  printf '%s' "$PGVS3_PG_PASSWORD" | "${SSH[@]}" 'cat > ~/.pgpw && chmod 600 ~/.pgpw'
  # rm before nohup (sync), and the driver flocks /tmp/pgvs3-bench.lock so two
  # drivers can never coexist and kill each other's gateway mid-upload.
  "${SSH[@]}" "chmod +x ~/run-analytics.sh; rm -f ~/BENCH.DONE; BENCH_EXTRA='${BENCH_EXTRA:-}' nohup ~/run-analytics.sh $MODE ${2:-} >~/bench-driver-$MODE-\$(date +%H%M%S).log 2>&1 & echo driver-started-$MODE"
fi

echo "waiting for ~/BENCH.DONE ..."
until "${SSH[@]}" 'test -f BENCH.DONE' 2>/dev/null; do sleep 30; done
mkdir -p .tmp/pgvs3/rig-out
scp -i $KEY -o StrictHostKeyChecking=accept-new -q "ec2-user@$IP:bench-out/*" .tmp/pgvs3/rig-out/ 2>/dev/null || true
"${SSH[@]}" 'ls -t ~/bench-driver-*.log | head -1 | xargs tail -30' || true
echo "results in .tmp/pgvs3/rig-out/"
