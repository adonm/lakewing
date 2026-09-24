# pgvs3 tasks. Toolchain: `mise install`. Local PostgreSQL: Docker.
# The rig recipes read their settings from .env (start from .env.example).
set dotenv-load

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"

# List the recipes.
default:
    @{{ just_executable() }} --list

# Fetch everything the build needs (mise installs the toolchain).
[group('dev')]
setup:
    mise install
    cargo fetch
    @echo "ready. next: just dev-db && just smoke"

# PostgreSQL 18 in Docker (any reachable PostgreSQL works: pass --url).
[group('dev')]
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null; then
      echo "postgres already listening on :5432"
    elif docker inspect pgvs3-pg >/dev/null 2>&1; then
      docker start pgvs3-pg >/dev/null
    else
      docker run -d --name pgvs3-pg -p 127.0.0.1:5432:5432 \
        -e POSTGRES_PASSWORD=postgres -v pgvs3-pg-data:/var/lib/postgresql/data postgres:18
    fi
    for i in $(seq 1 30); do
      docker exec pgvs3-pg pg_isready -U postgres >/dev/null 2>&1 && break
      (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null && break
      sleep 1
    done
    for db in pgvs3_bench ducklake_catalog ducklake_catalog_local; do
      docker exec pgvs3-pg psql -U postgres -c "CREATE DATABASE $db" 2>/dev/null || true
    done
    echo "postgres up: {{ URL }}"

# Delete the Docker PostgreSQL and its data.
[group('dev')]
dev-db-clean:
    docker rm -f pgvs3-pg || true
    docker volume rm pgvs3-pg-data || true

# Build, seed 256 MiB, serve, and check one cross-row ranged GET byte for byte.
[group('dev')]
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p pgvs3
    ./target/release/pgvs3 seed --gigabytes 0.25 --object-mib 8 --tasks 2 >/dev/null
    ./target/release/pgvs3 serve --addr 127.0.0.1:8014 & SRV=$!
    trap 'kill $SRV 2>/dev/null || true' EXIT
    sleep 1
    BYTES=$(curl -sf -H 'Range: bytes=8000-17999' \
      --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only \
      http://127.0.0.1:8014/lake/obj-00000.bin | wc -c)
    [ "$BYTES" = "10000" ] && echo "smoke ok: cross-row ranged GET returned exactly 10000 bytes"

# Load generator: 8 GiB of 64 MiB objects (a stable set for `just micro`).
[group('bench')]
seed:
    ./target/release/pgvs3 seed --gigabytes 8 --object-mib 64 --tasks 16

# GET latency/throughput matrix against the gateway on :8014.
[group('bench')]
micro:
    ./target/release/pgvs3 bench --endpoint http://127.0.0.1:8014 --bucket lake --requests 2000

# TPC-H on DuckLake through the gateway, stable DuckDB (extra = harness args).
[group('bench')]
tpch sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_STABLE" python crates/pgvs3/tpch_bench.py --stack {{ stack }} --sf {{ sf }} --load --passes 2 {{ extra }}

# TPC-H on the DuckDB 2.0 pre-release.
[group('bench')]
tpch2 sf="10" stack="lake-s3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/tpch_bench.py --stack {{ stack }} --sf {{ sf }} --load --passes 2 {{ extra }}

# ClickBench (43 queries) on DuckLake through the gateway.
[group('bench')]
clickbench stack="lake-s3" passes="3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/analytics_bench.py --bench click --stack {{ stack }} --download --load --passes {{ passes }} {{ extra }}

# SpatialBench (12 queries) on DuckLake through the gateway.
[group('bench')]
spatialbench sf="10" stack="lake-s3" passes="3" extra="":
    uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/analytics_bench.py --bench spatial --sf {{ sf }} --stack {{ stack }} --download --load --passes {{ passes }} {{ extra }}

# Run a benchmark on the AWS rig; results land in .tmp/pgvs3/rig-out/.
[group('rig')]
rig mode="quick" target="all":
    #!/usr/bin/env bash
    # Modes: `load [quick|full|all]` writes the datasets (once per storage
    # layout); `quick`, `full`, `micro` and `verify` only read them.
    # BENCH_EXTRA='--set name=value' passes DuckDB settings to the harness.
    set -euo pipefail
    case {{ quote(mode) }} in
      load | quick | full | micro | verify) ;;
      *) echo "unknown mode {{ quote(mode) }}: load | quick | full | micro | verify"; exit 2 ;;
    esac
    ip=$({{ just_executable() }} _rig-ip)
    ssh=(ssh -i "$RIG_SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 "$RIG_SSH_USER@$ip")
    echo "rig: $ip (mode: {{ mode }})"
    # Ship the repo, and .env (the rig side needs the Aurora login), into ~.
    tar czf - Cargo.toml Cargo.lock .cargo crates mise.toml justfile .env | "${ssh[@]}" 'tar xzf -'
    # Build on the rig (Fedora builds do not run on AL2023), only when the Rust
    # inputs changed: a fat-LTO build takes ~1.5 min.
    hash=$(find crates Cargo.toml Cargo.lock .cargo -type f \( -name '*.rs' -o -name Cargo.toml \
             -o -name Cargo.lock -o -name schema.sql -o -path '.cargo/*' \) -print0 \
           | sort -z | xargs -0 sha256sum | sha256sum | cut -c1-16)
    "${ssh[@]}" "if [ -x target/release/pgvs3 ] && [ \"\$(cat .pgvs3-build 2>/dev/null)\" = $hash ]; then
        echo 'binary current ($hash)'
      elif touch crates/pgvs3/src/lib.rs && ~/.cargo/bin/cargo build --release -p pgvs3 >/tmp/pgvs3-build.log 2>&1; then
        tail -1 /tmp/pgvs3-build.log; echo $hash > .pgvs3-build
      else
        tail -20 /tmp/pgvs3-build.log; exit 1
      fi"
    # nohup: the job outlives this SSH session (`just rig-wait` re-attaches).
    "${ssh[@]}" "rm -f BENCH.DONE; BENCH_EXTRA=$(printf %q "${BENCH_EXTRA:-}") \
      nohup ~/.local/bin/mise exec -- just _rig-run {{ quote(mode) }} {{ quote(target) }} \
      >bench-driver-{{ mode }}-\$(date +%H%M%S).log 2>&1 & echo started"
    {{ just_executable() }} rig-wait

# Wait for the rig job to finish, then fetch its results.
[group('rig')]
rig-wait:
    #!/usr/bin/env bash
    set -euo pipefail
    ip=$({{ just_executable() }} _rig-ip)
    ssh=(ssh -i "$RIG_SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 "$RIG_SSH_USER@$ip")
    echo "waiting for the rig job ..."
    until "${ssh[@]}" 'test -f BENCH.DONE' 2>/dev/null; do sleep 30; done
    mkdir -p .tmp/pgvs3/rig-out
    scp -i "$RIG_SSH_KEY" -o StrictHostKeyChecking=accept-new -q "$RIG_SSH_USER@$ip:bench-out/*" .tmp/pgvs3/rig-out/ || true
    "${ssh[@]}" 'cat "$(ls -t bench-driver-*.log | head -1)"'
    echo "results in .tmp/pgvs3/rig-out/"

# Stop the rig job and its gateway (safe any time).
[group('rig')]
rig-stop:
    #!/usr/bin/env bash
    set -euo pipefail
    ssh -i "$RIG_SSH_KEY" -o StrictHostKeyChecking=accept-new "$RIG_SSH_USER@$({{ just_executable() }} _rig-ip)" \
      "pkill -f '[_]rig-run'; pkill -x uv; pkill -x python; pkill -x pgvs3; rm -f BENCH.DONE /tmp/pgvs3-bench.lock; echo stopped"

# Open a shell on the rig.
[group('rig')]
rig-ssh:
    ssh -i "$RIG_SSH_KEY" -o StrictHostKeyChecking=accept-new "$RIG_SSH_USER@$({{ just_executable() }} _rig-ip)"

# Delete the rig: every Aurora instance, the cluster, the EC2 instance, the key pair.
[group('rig')]
[confirm("Delete the AWS rig (Aurora cluster, EC2 instance, key pair)?")]
rig-teardown:
    #!/usr/bin/env bash
    set -euo pipefail
    aws=(aws --profile "$RIG_AWS_PROFILE" --region "$RIG_AWS_REGION")
    for db in $("${aws[@]}" rds describe-db-clusters --db-cluster-identifier "$RIG_DB_CLUSTER" \
                  --query 'DBClusters[0].DBClusterMembers[].DBInstanceIdentifier' --output text); do
      "${aws[@]}" rds delete-db-instance --db-instance-identifier "$db"  # snapshots/backups are the cluster's
    done
    "${aws[@]}" rds delete-db-cluster --db-cluster-identifier "$RIG_DB_CLUSTER" --skip-final-snapshot
    "${aws[@]}" ec2 terminate-instances --instance-ids "$RIG_INSTANCE"
    "${aws[@]}" ec2 delete-key-pair --key-name "$RIG_KEY_PAIR"
    echo "deleted; check: aws --profile $RIG_AWS_PROFILE rds describe-db-clusters --db-cluster-identifier $RIG_DB_CLUSTER"

# The rig's public IP. It changes on stop/start; without an AWS session (e.g.
# an expired SSO login) fall back to the last one seen.
_rig-ip:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${RIG_INSTANCE:?rig settings missing: copy .env.example to .env}"
    mkdir -p .tmp/pgvs3
    if ip=$(aws ec2 describe-instances --profile "$RIG_AWS_PROFILE" --region "$RIG_AWS_REGION" \
              --instance-ids "$RIG_INSTANCE" --query 'Reservations[0].Instances[0].PublicIpAddress' \
              --output text 2>/dev/null); then
      echo "$ip" > .tmp/pgvs3/rig-ip
    else
      ip=$(cat .tmp/pgvs3/rig-ip)
      echo "(aws unavailable, using the last rig IP; refresh: aws sso login --profile $RIG_AWS_PROFILE)" >&2
    fi
    echo "$ip"

# Runs on the rig, in ~ (where `just rig` unpacks the repo): one benchmark
# mode. Every run gets a fresh gateway and DuckDB; ~/BENCH.DONE marks the end.
_rig-run mode target="all":
    #!/usr/bin/env bash
    set -uo pipefail
    exec 9>/tmp/pgvs3-bench.lock
    flock -n 9 || { echo "another rig job is running"; exit 3; }
    mode={{ quote(mode) }}
    target={{ quote(target) }}
    pg="postgres://$RIG_PG_USER:$RIG_PG_PASSWORD@$RIG_PG_HOST:5432"
    base="$pg/pgvs3_bench?sslmode=require"
    catalog() { echo "dbname=$1 host=$RIG_PG_HOST user=$RIG_PG_USER password=$RIG_PG_PASSWORD sslmode=require"; }
    mkdir -p bench-out bench-data
    rm -f BENCH.DONE
    d=$HOME/bench-data
    click=(--bench click --stack lake-s3 --src-dir "$d/clickbench" --local-dir "$d/click-lake-local"
           --plain-db "$d/click-plain.duckdb" --scratch-db "$d/scratch-click.duckdb")
    spatial=(--bench spatial --stack lake-s3 --src-dir "$d/spatialbench" --local-dir "$d/spatial-lake-local"
             --plain-db "$d/spatial-plain.duckdb" --scratch-db "$d/scratch-spatial.duckdb")
    spatial_tables=trip,customer,driver,vehicle,zone,building
    # Read runs: 3 passes with DuckDB's file cache off, so every pass reads
    # through the gateway.
    reads=(--views-only --passes 3 --no-file-cache)

    s3() { curl -sf --aws-sigv4 aws:amz:us-east-1:s3 --user cachebench:cachebench-local-only "$@"; }

    fresh_catalogs() {
      for db in "$@"; do
        psql "$pg/postgres?sslmode=require" -qc "DROP DATABASE IF EXISTS $db" >/dev/null
        psql "$pg/postgres?sslmode=require" -qc "CREATE DATABASE $db" >/dev/null
      done
    }

    gw_start() {  # a fresh gateway on :8014
      pkill -x pgvs3
      sleep 1
      nohup ./target/release/pgvs3 --url "$base" serve --addr 127.0.0.1:8014 >/tmp/s-bench.log 2>&1 &
      for _ in $(seq 1 20); do
        curl -s -o /dev/null --max-time 1 http://127.0.0.1:8014/ && return 0
        sleep 0.5
      done
      echo "gateway failed to start"
      tail -5 /tmp/s-bench.log
      exit 1
    }

    bench() {  # label, harness args...
      local label=$1
      shift
      echo "=== $label${BENCH_EXTRA:+ [$BENCH_EXTRA]} $(date -u +%T) ==="
      gw_start
      # shellcheck disable=SC2086  # BENCH_EXTRA splits into arguments on purpose
      uv run --with "duckdb==$DUCKDB_PY_PRE" python crates/pgvs3/analytics_bench.py "$@" ${BENCH_EXTRA:-} \
        --out "$HOME/bench-out/$label.json" 2>&1 | tail -8
      echo "--- $label stats:"
      s3 http://127.0.0.1:8014/_pgvs3/stats
      echo
    }

    # A second gateway (:8015, its own metadata cache) and a fresh DuckDB must
    # see the same tables and rows: any worker can join the same catalog and
    # storage and see the same state.
    multi_check() {  # label, catalog, data path, tables
      echo "=== multi-setup check: $1 ==="
      nohup ./target/release/pgvs3 --url "$base" serve --addr 127.0.0.1:8015 >/tmp/s-bench2.log 2>&1 &
      local gw2=$!
      sleep 1
      uv run --with "duckdb==$DUCKDB_PY_PRE" python - "$2" "$3" "$4" <<'PY' || echo "multi-setup $1: FAIL"
    import sys
    import duckdb

    cat, data_path, tables = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
    con = duckdb.connect()
    for e in ("postgres", "httpfs", "ducklake"):
        con.sql(f"INSTALL {e}")
        con.sql(f"LOAD {e}")
    con.sql("SET s3_endpoint='127.0.0.1:8015'")
    con.sql("SET s3_use_ssl=false")
    con.sql("SET s3_url_style='path'")
    con.sql("SET s3_access_key_id='cachebench'")
    con.sql("SET s3_secret_access_key='cachebench-local-only'")
    con.sql(f"ATTACH 'ducklake:postgres:{cat}' AS lake (DATA_PATH '{data_path}')")
    names = sorted(r[0] for r in con.sql("SHOW TABLES FROM lake").fetchall())
    print("tables:", ",".join(names))
    for t in tables:
        print(f"  {t}: {con.sql(f'SELECT count(*) FROM lake.{t}').fetchone()[0]} rows")
    row = con.sql(f"SELECT * FROM lake.{tables[0]} LIMIT 1").fetchall()
    print("multi-setup:", "PASS" if names and row else "FAIL")
    PY
      kill $gw2 2>/dev/null || true
    }

    # GET content check: whole objects against their sha256 ETag (single-file
    # objects) and against odd-sized ranged reads, which cross row, chunk, part
    # and file boundaries at other offsets than the whole GET.
    verify() {
      gw_start
      local step=$((3 * 1024 * 1024 + 12345)) fail=0 key size etag single whole ranged ok off
      while IFS='|' read -r key size etag single; do
        whole=$(s3 "http://127.0.0.1:8014/lake/$key" | sha256sum | cut -c1-64)
        ranged=$(for ((off = 0; off < size; off += step)); do
                   s3 -H "Range: bytes=$off-$((off + step - 1))" "http://127.0.0.1:8014/lake/$key"
                 done | sha256sum | cut -c1-64)
        ok=ok
        [ "$whole" = "$ranged" ] || ok=RANGED-MISMATCH
        if [ "$single" = t ] && [ "$whole" != "$etag" ]; then ok=ETAG-MISMATCH; fi
        [ "$ok" = ok ] || fail=1
        printf '%-15s %11s  %s\n' "$ok" "$size" "$key"
      done < <(psql "$base" -XqAt -c "
        (SELECT key, size, encode(etag, 'hex'), parts IS NULL FROM s3p.objects
          WHERE bucket = 'lake' AND parts IS NOT NULL ORDER BY size DESC LIMIT 2)
        UNION ALL
        (SELECT key, size, encode(etag, 'hex'), parts IS NULL FROM s3p.objects
          WHERE bucket = 'lake' AND parts IS NULL ORDER BY size DESC LIMIT 2)")
      if [ "$fail" = 0 ]; then echo "verify: PASS"; else echo "verify: FAIL"; fi
    }

    case $mode in
    load)
      if [ "$target" != full ]; then
        fresh_catalogs ducklake_click_lake_s3 ducklake_spatial_lake_s3
        bench load-click "${click[@]}" --catalog "$(catalog ducklake_click_lake_s3)" \
          --data-path s3://lake/run-31/ --parts 10 --download --load --passes 0
        multi_check click "$(catalog ducklake_click_lake_s3)" s3://lake/run-31/ hits
        # sf1, not sf0.1: at sf0.1 the generator emits no buildings and DuckLake
        # inlines the small tables into the catalog (no gateway traffic at all).
        bench load-spatial "${spatial[@]}" --catalog "$(catalog ducklake_spatial_lake_s3)" \
          --data-path s3://lake/run-32/ --sf 1 --download --load --passes 0
        multi_check spatial "$(catalog ducklake_spatial_lake_s3)" s3://lake/run-32/ "$spatial_tables"
      fi
      if [ "$target" != quick ]; then
        fresh_catalogs ducklake_click_full ducklake_spatial_full
        bench load-click-full "${click[@]}" --catalog "$(catalog ducklake_click_full)" \
          --data-path s3://lake/run-33/ --download --load --passes 0
        multi_check click-full "$(catalog ducklake_click_full)" s3://lake/run-33/ hits
        bench load-spatial-full "${spatial[@]}" --catalog "$(catalog ducklake_spatial_full)" \
          --data-path s3://lake/run-34/ --sf 10 --download --load --passes 0
        multi_check spatial-full "$(catalog ducklake_spatial_full)" s3://lake/run-34/ "$spatial_tables"
      fi
      ;;
    quick)
      # SpatialBench Q1-Q7: Q8-Q12 are CPU-bound spatial joins, no read signal.
      bench click-quick "${click[@]}" "${reads[@]}" --catalog "$(catalog ducklake_click_lake_s3)" \
        --data-path s3://lake/run-31/ --query-timeout 300
      bench spatial-quick "${spatial[@]}" "${reads[@]}" --catalog "$(catalog ducklake_spatial_lake_s3)" \
        --data-path s3://lake/run-32/ --sf 1 --queries 1-7 --query-timeout 120
      ;;
    full)
      bench click-full "${click[@]}" "${reads[@]}" --catalog "$(catalog ducklake_click_full)" \
        --data-path s3://lake/run-33/ --query-timeout 600
      bench spatial-full "${spatial[@]}" "${reads[@]}" --catalog "$(catalog ducklake_spatial_full)" \
        --data-path s3://lake/run-34/ --sf 10 --queries 1-7 --query-timeout 600
      ;;
    micro)
      # The read path without an engine: GET latency/throughput over the loaded
      # objects, plus the direct-PostgreSQL floor.
      gw_start
      ./target/release/pgvs3 --url "$base" bench --endpoint http://127.0.0.1:8014 --bucket lake \
        --requests 2000 --sizes 65536,262144,1048576,8388608,67108864 --concurrency 1,8,32 2>&1 \
        | tee bench-out/micro.txt
      echo "--- micro stats:"
      s3 http://127.0.0.1:8014/_pgvs3/stats
      echo
      ;;
    verify)
      verify
      ;;
    *)
      echo "unknown mode: $mode"
      ;;
    esac
    pkill -x pgvs3
    cp /tmp/s-bench.log bench-out/gateway-last.log 2>/dev/null
    touch BENCH.DONE
    echo DONE
