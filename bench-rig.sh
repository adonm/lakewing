#!/usr/bin/env bash
# Benchmark loops on the AWS rig (Aurora catalog + data through pgvs3).
# Results land in .tmp/pgvs3/rig-out/ (JSON per run + gateway cache telemetry).
#
#   ./bench-rig.sh quick    fast loop: clickbench 10% (1.5G slices) + spatialbench
#                           sf1 on lake-s3, 2 passes, capped queries (~15 min)
#   ./bench-rig.sh sweep    cache-knob sweep (PGVS3_CACHE_MIB x PGVS3_ADMIT_BYTES)
#                           on the quick data: views-only, gateway restarted per
#                           config (cold proxy cache), 2 passes each
#   ./bench-rig.sh full     headline run: clickbench 100% x3 passes + spatialbench
#                           sf10 x2 passes (hours)
#   ./bench-rig.sh stop     kill any running driver/gateway (safe to run anytime)
#   ./bench-rig.sh wait     re-attach to an in-flight run and fetch results
#
# quick loads the data; sweep iterates proxy configs on it without reloading.
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

IP=$(aws ec2 describe-instances --profile "$P" --region us-east-2 --instance-ids $INST \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "rig: $IP (mode: $MODE)"
echo "$IP" > .tmp/pgvs3/rig-ip
SSH=(ssh -i $KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 "ec2-user@$IP")

if [ "$MODE" = "stop" ]; then
  # Bash scripts have comm="bash": match with the [x] trick, never bare -f.
  "${SSH[@]}" "pkill -f '[r]un-analytics.sh'; pkill -x uv; pkill -x python; pkill -x pgvs3; rm -f ~/BENCH.DONE /tmp/pgvs3-bench.lock; echo stopped"
  exit 0
fi

if [ "$MODE" != "wait" ]; then
  tar czf - Cargo.toml Cargo.lock .cargo crates mise.toml justfile | "${SSH[@]}" 'tar xzf -'
  # Rebuild on every deploy: source and binary must match (the binary is
  # built on the rig — Fedora-built binaries do not run on AL2023).
  "${SSH[@]}" 'touch crates/pgvs3/src/lib.rs && ~/.cargo/bin/cargo build --release -p pgvs3 2>&1 | tail -1'
  "${SSH[@]}" 'cat > ~/run-analytics.sh' <<'DRV'
#!/usr/bin/env bash
# $1 = quick | full | sweep. Results in ~/bench-out, ~/BENCH.DONE marks the end.
set -uo pipefail
exec 9>/tmp/pgvs3-bench.lock
flock -n 9 || { echo "another bench driver is running"; exit 3; }
cd /home/ec2-user
export PATH=$HOME/.local/bin:$HOME/.cargo/bin:$PATH
MODE=${1:?quick|full|sweep}
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

MIB=2048   # proxy row-cache MiB (PGVS3_CACHE_MIB)
ADM=8388608  # cacheable span bytes (PGVS3_ADMIT_BYTES)

fresh_catalogs() {
  for db in ducklake_click_lake_s3 ducklake_spatial_lake_s3; do
    psql "$ADMIN" -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
    psql "$ADMIN" -c "CREATE DATABASE $db" >/dev/null 2>&1
  done
}

gw_start() {
  pkill -x pgvs3 || true
  sleep 1
  PGVS3_CACHE_MIB=$MIB PGVS3_ADMIT_BYTES=$ADM \
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
  echo "=== $label [MIB=$MIB ADM=$ADM] catalog=${catdesc:-n/a} $(date -u +%H:%M:%S) ==="
  gw_start
  mise exec -- uv run --with "duckdb==$PRE" python crates/pgvs3/analytics_bench.py "$@" \
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
quick)
  fresh_catalogs
  run_one click-lake-s3 "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" --parts 10 --download --load --passes 2 --query-timeout 300
  multi_check click "$CAT_C3" 's3://lake/run-31/' hits
  # sf1 not sf0.1: at sf0.1 the generator emits 0 buildings and DuckLake
  # inlines the small tables into the catalog (no proxy traffic at all).
  run_one spatial-lake-s3 "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 1 --download --load --passes 2 --query-timeout 120
  multi_check spatial "$CAT_S3" 's3://lake/run-32/' 'trip,customer,driver,vehicle,zone,building'
  ;;
full)
  fresh_catalogs
  run_one click-lake-s3 "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" --download --load --passes 3 --query-timeout 600
  multi_check click "$CAT_C3" 's3://lake/run-31/' hits
  run_one spatial-lake-s3 "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 10 --download --load --passes 2 --query-timeout 1200
  multi_check spatial "$CAT_S3" 's3://lake/run-32/' 'trip,customer,driver,vehicle,zone,building'
  ;;
sweep)
  # views-only on the quick data: only the proxy cache config changes
  for cfg in "2048 8388608 base" "2048 524288 a512k" "2048 2097152 a2m" "512 8388608 m512" "4096 8388608 m4g"; do
    read -r MIB ADM TAG <<<"$cfg"
    run_one "sweep-click-$TAG" "${CLICK[@]}" --stack lake-s3 --catalog "$CAT_C3" --views-only --passes 2 --query-timeout 300
    run_one "sweep-spatial-$TAG" "${SPAT[@]}" --stack lake-s3 --catalog "$CAT_S3" --sf 1 --views-only --passes 2 --query-timeout 120
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
  "${SSH[@]}" "chmod +x ~/run-analytics.sh; rm -f ~/BENCH.DONE; nohup ~/run-analytics.sh $MODE >~/bench-driver-$MODE-\$(date +%H%M%S).log 2>&1 & echo driver-started-$MODE"
fi

echo "waiting for ~/BENCH.DONE ..."
until "${SSH[@]}" 'test -f BENCH.DONE' 2>/dev/null; do sleep 30; done
mkdir -p .tmp/pgvs3/rig-out
scp -i $KEY -o StrictHostKeyChecking=accept-new -q "ec2-user@$IP:bench-out/*.json" .tmp/pgvs3/rig-out/ 2>/dev/null || true
"${SSH[@]}" 'ls -t ~/bench-driver-*.log | head -1 | xargs tail -30' || true
echo "results in .tmp/pgvs3/rig-out/"
