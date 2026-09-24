#!/usr/bin/env bash
# Benchmark loops on the AWS rig (Aurora catalog + data through pgvs3).
# Results land in .tmp/pgvs3/rig-out/ (JSON per run + gateway cache telemetry).
#
#   ./bench-rig.sh quick    fast loop: clickbench 10% (1.5G slices) + spatialbench
#                           sf0.1 on lake-s3, 2 passes, capped queries (~15 min)
#   ./bench-rig.sh sweep    cache-knob sweep (PGVS3_CACHE_MIB x PGVS3_ADMIT_BYTES)
#                           on the quick data: views-only, gateway restarted per
#                           config (cold proxy cache), 2 passes each
#   ./bench-rig.sh full     headline matrix: clickbench 100% x3 stacks x3 passes +
#                           spatialbench sf10 x3 stacks x2 passes (hours)
#   ./bench-rig.sh wait     re-attach to an in-flight run and fetch results
#
# quick loads the data; sweep iterates proxy configs on it without reloading.
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

if [ "$MODE" != "wait" ]; then
  tar czf - Cargo.toml Cargo.lock .cargo crates mise.toml justfile | "${SSH[@]}" 'tar xzf -'
  "${SSH[@]}" 'cat > ~/run-analytics.sh' <<'DRV'
#!/usr/bin/env bash
# $1 = quick | full | sweep. Results in ~/bench-out, ~/BENCH.DONE marks the end.
set -uo pipefail
cd /home/ec2-user
export PATH=$HOME/.local/bin:$HOME/.cargo/bin:$PATH
MODE=${1:?quick|full|sweep}
PW=$(cat ~/.pgpw)
END=pgvs3-perf.cluster-csmlp5ndujwv.us-east-2.rds.amazonaws.com
BASE="postgres://pgvs3admin:${PW}@${END}:5432/pgvs3_bench?sslmode=require"
ADMIN="postgres://pgvs3admin:${PW}@${END}:5432/postgres?sslmode=require"
CAT="dbname=ducklake_catalog_click host=$END user=pgvs3admin password=$PW sslmode=require"
CATS="dbname=ducklake_catalog_spatial host=$END user=pgvs3admin password=$PW sslmode=require"
PRE=$(mise exec -- printenv DUCKDB_PY_PRE)
mkdir -p bench-out bench-data
rm -f BENCH.DONE

CLICK=(--bench click --stack lake-s3 --catalog "$CAT" --data-path 's3://lake/run-31/'
       --src-dir /home/ec2-user/bench-data/clickbench
       --local-dir /home/ec2-user/bench-data/click-lake-local
       --plain-db /home/ec2-user/bench-data/click-plain.duckdb
       --scratch-db /home/ec2-user/bench-data/scratch-click.duckdb)
SPAT=(--bench spatial --stack lake-s3 --catalog "$CATS" --data-path 's3://lake/run-32/'
      --src-dir /home/ec2-user/bench-data/spatialbench
      --local-dir /home/ec2-user/bench-data/spatial-lake-local
      --plain-db /home/ec2-user/bench-data/spatial-plain.duckdb
      --scratch-db /home/ec2-user/bench-data/scratch-spatial.duckdb)

MIB=2048   # proxy row-cache MiB (PGVS3_CACHE_MIB)
ADM=8388608  # cacheable span bytes (PGVS3_ADMIT_BYTES)

fresh_catalogs() {
  for db in ducklake_catalog_click ducklake_catalog_spatial; do
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
  local label=$1
  shift
  echo "=== $label [MIB=$MIB ADM=$ADM] $(date -u +%H:%M:%S) ==="
  gw_start
  mise exec -- uv run --with "duckdb==$PRE" python crates/pgvs3/analytics_bench.py "$@" \
    --out "/home/ec2-user/bench-out/$label.json" 2>&1 | tail -8
  echo "--- $label stats:"
  curl -s --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
    http://127.0.0.1:8014/_pgvs3/stats || true
}

case $MODE in
quick)
  fresh_catalogs
  run_one click-lake-s3 "${CLICK[@]}" --parts 10 --download --load --passes 2 --query-timeout 300
  run_one spatial-lake-s3 "${SPAT[@]}" --sf 0.1 --download --load --passes 2 --query-timeout 120
  ;;
full)
  fresh_catalogs
  for stack in lake-s3 lake-local plain; do
    CLICK[3]=$stack
    run_one "click-$stack" "${CLICK[@]}" --download --load --passes 3 --query-timeout 600
  done
  for stack in lake-s3 lake-local plain; do
    SPAT[3]=$stack
    run_one "spatial-$stack" "${SPAT[@]}" --sf 10 --download --load --passes 2 --query-timeout 1200
  done
  ;;
sweep)
  # views-only on the quick data: only the proxy cache config changes
  for cfg in "2048 8388608 base" "2048 524288 a512k" "2048 2097152 a2m" "512 8388608 m512" "4096 8388608 m4g"; do
    read -r MIB ADM TAG <<<"$cfg"
    run_one "sweep-click-$TAG" "${CLICK[@]}" --views-only --passes 2 --query-timeout 300
    run_one "sweep-spatial-$TAG" "${SPAT[@]}" --sf 0.1 --views-only --passes 2 --query-timeout 120
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
  "${SSH[@]}" "chmod +x ~/run-analytics.sh && nohup ~/run-analytics.sh $MODE >~/bench-driver.log 2>&1 & echo driver-started ($MODE)"
fi

echo "waiting for ~/BENCH.DONE ..."
until "${SSH[@]}" 'test -f BENCH.DONE' 2>/dev/null; do sleep 30; done
mkdir -p .tmp/pgvs3/rig-out
scp -i $KEY -o StrictHostKeyChecking=accept-new -q "ec2-user@$IP:bench-out/*.json" .tmp/pgvs3/rig-out/ 2>/dev/null || true
"${SSH[@]}" 'tail -30 ~/bench-driver.log' || true
echo "results in .tmp/pgvs3/rig-out/"
