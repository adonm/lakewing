#!/usr/bin/env bash
# ClickBench + Sedona-SpatialBench on the AWS rig: DuckDB 2.0 + DuckLake over
# pgvs3, Aurora PostgreSQL for catalog AND data (lake-s3), with lake-local and
# plain stacks for the overhead decomposition. Results land in
# .tmp/pgvs3/rig-out/ (one JSON per bench/stack + gateway telemetry).
#
#   ./bench-rig.sh                ship + launch + wait + fetch (runs for hours)
#   ./bench-rig.sh --wait-only    re-attach to an in-flight run
#
# Needs .tmp/pgvs3/aws-rig.env (PGVS3_PG_PASSWORD) and the rig keypair.
set -euo pipefail
P=${AWS_PROFILE:-platform-dev}
INST=i-037e91b323d000a1b
KEY=.tmp/pgvs3/pgvs3-perf-key.pem
[ -f .tmp/pgvs3/aws-rig.env ] || { echo "missing .tmp/pgvs3/aws-rig.env"; exit 1; }
# shellcheck disable=SC1091
source .tmp/pgvs3/aws-rig.env

IP=$(aws ec2 describe-instances --profile "$P" --region us-east-2 --instance-ids $INST \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "rig: $IP"
echo "$IP" > .tmp/pgvs3/rig-ip
SSH=(ssh -i $KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 "ec2-user@$IP")

if [ "${1:-}" != "--wait-only" ]; then
  tar czf - Cargo.toml Cargo.lock .cargo crates mise.toml justfile | "${SSH[@]}" 'tar xzf -'
  # Remote orchestrator: one gateway per run (cold proxy cache), fresh catalog
  # DBs per bench, results in ~/bench-out, ~/BENCH.DONE marks completion.
  "${SSH[@]}" 'cat > ~/run-analytics.sh' <<'DRV'
#!/usr/bin/env bash
set -uo pipefail
cd /home/ec2-user
export PATH=$HOME/.local/bin:$HOME/.cargo/bin:$PATH
PW=$(cat ~/.pgpw)
END=pgvs3-perf.cluster-csmlp5ndujwv.us-east-2.rds.amazonaws.com
BASE="postgres://pgvs3admin:${PW}@${END}:5432/pgvs3_bench?sslmode=require"
ADMIN="postgres://pgvs3admin:${PW}@${END}:5432/postgres?sslmode=require"
CAT="dbname=ducklake_catalog_click host=$END user=pgvs3admin password=$PW sslmode=require"
CATS="dbname=ducklake_catalog_spatial host=$END user=pgvs3admin password=$PW sslmode=require"
PRE=$(mise exec -- printenv DUCKDB_PY_PRE)
mkdir -p bench-out bench-data
rm -f BENCH.DONE
for db in ducklake_catalog_click ducklake_catalog_spatial; do
  psql "$ADMIN" -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
  psql "$ADMIN" -c "CREATE DATABASE $db" >/dev/null 2>&1
done

gw_start() {
  pkill -x pgvs3 || true
  sleep 1
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
  echo "=== $label $(date -u +%H:%M:%S) ==="
  gw_start
  mise exec -- uv run --with "duckdb==$PRE" python crates/pgvs3/analytics_bench.py "$@" \
    --out "/home/ec2-user/bench-out/$label.json" 2>&1 | tail -8
  echo "--- $label stats:"
  curl -s --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
    http://127.0.0.1:8014/_pgvs3/stats || true
}

for stack in lake-s3 lake-local plain; do
  run_one "click-$stack" --bench click --stack $stack --download --load --passes 3 \
    --catalog "$CAT" --data-path 's3://lake/run-31/' \
    --src-dir /home/ec2-user/bench-data/clickbench \
    --local-dir /home/ec2-user/bench-data/click-lake-local \
    --plain-db /home/ec2-user/bench-data/click-plain.duckdb \
    --scratch-db /home/ec2-user/bench-data/scratch-click.duckdb
done

for stack in lake-s3 lake-local plain; do
  run_one "spatial-$stack" --bench spatial --sf 10 --stack $stack --download --load \
    --passes 2 --query-timeout 1200 \
    --catalog "$CATS" --data-path 's3://lake/run-32/' \
    --src-dir /home/ec2-user/bench-data/spatialbench \
    --local-dir /home/ec2-user/bench-data/spatial-lake-local \
    --plain-db /home/ec2-user/bench-data/spatial-plain.duckdb \
    --scratch-db /home/ec2-user/bench-data/scratch-spatial.duckdb
done

pkill -x pgvs3 || true
cp /tmp/s-bench.log /home/ec2-user/bench-out/gateway-last.log 2>/dev/null || true
df -h / > /home/ec2-user/bench-out/df.txt
touch BENCH.DONE
echo DONE
DRV
  printf '%s' "$PGVS3_PG_PASSWORD" | "${SSH[@]}" 'cat > ~/.pgpw && chmod 600 ~/.pgpw'
  "${SSH[@]}" 'chmod +x ~/run-analytics.sh && nohup ~/run-analytics.sh >~/bench-driver.log 2>&1 & echo driver-started'
  echo "driver launched; run takes hours (clickbench x3 stacks, spatialbench sf10 x3 stacks)"
fi

echo "waiting for ~/BENCH.DONE ..."
until "${SSH[@]}" 'test -f BENCH.DONE' 2>/dev/null; do sleep 60; done
mkdir -p .tmp/pgvs3/rig-out
for f in click-lake-s3 click-lake-local click-plain spatial-lake-s3 spatial-lake-local spatial-plain; do
  "${SSH[@]}" "cat bench-out/$f.json" > ".tmp/pgvs3/rig-out/$f.json" 2>/dev/null || echo "missing: $f"
done
"${SSH[@]}" 'tail -40 ~/bench-driver.log; echo; df -h / | tail -1' || true
echo "results in .tmp/pgvs3/rig-out/"
