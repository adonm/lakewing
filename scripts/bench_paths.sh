#!/usr/bin/env bash
# Side-by-side: DuckDB httpfs/S3 client (with 2.0 external file cache +
# metadata caches) vs mountpoint + disk cache. Same 4 OGC-shaped queries,
# cold then warm each path. Usage: bench_paths.sh
# Prints ms per query per pass. S3 GETs come from mount metrics (mount
# path only); the direct path has no client-side counter.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BOX=mp-testbox
# shellcheck disable=SC1091
source "$ROOT/scripts/dev-s3.env"

declare -A QUERIES=(
  [CITY]="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM read_parquet(__SRC__) WHERE layer = 'buildings' AND source_id IN (1) AND xmax >= 4.895 AND xmin <= 4.905 AND ymax >= 52.365 AND ymin <= 52.375 AND ((xmin >= 4.895 AND xmax <= 4.905 AND ymin >= 52.365 AND ymax <= 52.375) OR (ST_Intersects(geom, ST_MakeEnvelope(4.895, 52.365, 4.905, 52.375)))) ORDER BY id LIMIT 101 OFFSET 0) AS page ORDER BY id;"
  [BROAD]="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM read_parquet(__SRC__) WHERE layer = 'buildings' AND source_id IN (1) AND xmax >= 2 AND xmin <= 4 AND ymax >= 48 AND ymin <= 51 AND ((xmin >= 2 AND xmax <= 4 AND ymin >= 48 AND ymax <= 51) OR (ST_Intersects(geom, ST_MakeEnvelope(2, 48, 4, 51)))) ORDER BY id LIMIT 101 OFFSET 0) AS page ORDER BY id;"
  [FULL]="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM read_parquet(__SRC__) WHERE layer = 'buildings' AND source_id IN (1) ORDER BY id LIMIT 1001 OFFSET 0) AS page ORDER BY id;"
  [DEEP]="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM read_parquet(__SRC__) WHERE layer = 'buildings' AND source_id IN (1) ORDER BY id LIMIT 101 OFFSET 50000) AS page ORDER BY id;"
)

DIRECT_SETTINGS="LOAD spatial; CREATE SECRET IF NOT EXISTS s3sec (TYPE S3, KEY_ID '$S3_USER', SECRET '$S3_PASS', ENDPOINT '127.0.0.1:8333', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'); SET parquet_metadata_cache=true; SET enable_http_metadata_cache=true; SET enable_external_file_cache=true; SET validate_external_file_cache='NO_VALIDATION'; SET threads=8; SET memory_limit='4GB';"
MOUNT_SETTINGS="LOAD spatial; SET parquet_metadata_cache=true; SET threads=8; SET memory_limit='4GB';"
DIRECT_SRC="['s3://lake/nw/data/main/features/*.parquet']"
MOUNT_SRC="['/mnt/nw/data/main/features/*.parquet']"
CAT_DIRECT="ATTACH 'ducklake:s3://lake/nw/catalogs/nw-europe.ducklake' AS s (READ_ONLY, DATA_PATH 's3://lake/nw/data/', OVERRIDE_DATA_PATH true); USE s;"
CAT_MOUNT="ATTACH 'ducklake:/mnt/nw/catalogs/nw-europe.ducklake' AS s (READ_ONLY, DATA_PATH '/mnt/nw/data/', OVERRIDE_DATA_PATH true); USE s;"
PRED_CITY="layer = 'buildings' AND source_id IN (1) AND xmax >= 4.895 AND xmin <= 4.905 AND ymax >= 52.365 AND ymin <= 52.375 AND ((xmin >= 4.895 AND xmax <= 4.905 AND ymin >= 52.365 AND ymax <= 52.375) OR (ST_Intersects(geom, ST_MakeEnvelope(4.895, 52.365, 4.905, 52.375))))"
PRED_FULL="layer = 'buildings' AND source_id IN (1)"
QC_CITY="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM features WHERE __PRED__ ORDER BY id LIMIT 101 OFFSET 0) AS page ORDER BY id;"
QC_FULL="SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM (SELECT id, geom, properties FROM features WHERE __PRED__ ORDER BY id LIMIT 1001 OFFSET 0) AS page ORDER BY id;"

run_host() { # $1 = sql file, prints ms, fails loud on query errors
  local start end out
  start=$(date +%s%3N)
  out=$("$ROOT/.deps/duckdb/duckdb" :memory: -csv -noheader "$(cat "$1")" 2>&1)
  end=$(date +%s%3N)
  if grep -qm1 -E "^[A-Za-z ]*Error:" <<<"$out"; then
    printf 'query failed:\n%s\n' "$out" >&2
    return 1
  fi
  echo $((end - start))
}

run_box() { # $1 = sql file, prints ms, fails loud on query errors
  local start end out
  start=$(date +%s%3N)
  out=$(docker exec "$BOX" /deps/duckdb/duckdb :memory: -csv -noheader "$(cat "$1")" 2>&1)
  end=$(date +%s%3N)
  if grep -qm1 -E "^[A-Za-z ]*Error:" <<<"$out"; then
    printf 'query failed:\n%s\n' "$out" >&2
    return 1
  fi
  echo $((end - start))
}

echo "== seaweed =="
bash "$ROOT/scripts/seaweed_up.sh" >/dev/null

mkdir -p /tmp/opencode/paths
printf '%s\n%s\n' "$DIRECT_SETTINGS" "$CAT_DIRECT" > /tmp/opencode/paths/cat_direct_attach.sql
printf '%s\n%s\n' "$MOUNT_SETTINGS" "$CAT_MOUNT" > /tmp/opencode/paths/cat_mount_attach.sql
for q in CITY BROAD FULL DEEP; do
  printf '%s\n%s\n' "$DIRECT_SETTINGS" "${QUERIES[$q]//__SRC__/$DIRECT_SRC}" > "/tmp/opencode/paths/direct_$q.sql"
  printf '%s\n%s\n' "$MOUNT_SETTINGS" "${QUERIES[$q]//__SRC__/$MOUNT_SRC}" > "/tmp/opencode/paths/mount_$q.sql"
done

echo "== direct S3 (httpfs + external file cache), cold =="
echo "-- cold: fresh process per query (nothing cached anywhere)"
for q in CITY BROAD FULL DEEP; do
  ms=$(run_host "/tmp/opencode/paths/direct_$q.sql")
  echo "direct cold $q: ${ms}ms"
done
echo "-- warm: one process, each query twice (.timer splits cold/warm)"
{
  echo ".timer on"
  echo "$DIRECT_SETTINGS"
  for q in CITY BROAD FULL DEEP; do
    echo "${QUERIES[$q]//__SRC__/$DIRECT_SRC}"
    echo "${QUERIES[$q]//__SRC__/$DIRECT_SRC}"
  done
} > /tmp/opencode/paths/direct_warm.sql
if grep -qE "Error" /tmp/opencode/paths/direct_CITY.sql; then echo "spec error" >&2; exit 1; fi
"$ROOT/.deps/duckdb/duckdb" :memory: -csv -noheader < /tmp/opencode/paths/direct_warm.sql > /tmp/opencode/paths/direct_warm.out 2>&1 || { grep -m3 -i "error" /tmp/opencode/paths/direct_warm.out >&2; exit 1; }
grep -E "Run Time" /tmp/opencode/paths/direct_warm.out | tail -8 | awk '{printf "%dms ", $5*1000} END {print ""}'
echo "(pairs: CITY cold/warm, BROAD cold/warm, FULL cold/warm, DEEP cold/warm)"

echo "== mountpoint + disk cache =="
echo "-- wiping disk cache for a true cold pass"
docker exec "$BOX" sh -c 'rm -rf /cache-nw/*'
for q in CITY BROAD FULL DEEP; do
  ms=$(run_box "/tmp/opencode/paths/mount_$q.sql")
  echo "mount cold $q: ${ms}ms"
done
echo "-- warm: disk cache persists, fresh CLI processes"
for q in CITY BROAD FULL DEEP; do
  ms=$(run_box "/tmp/opencode/paths/mount_$q.sql")
  echo "mount warm $q: ${ms}ms"
done
echo "== DuckLake catalog path (ATTACH + features table) =="
echo "-- direct catalog, cold then warm-in-one-process"
cat /tmp/opencode/paths/cat_direct_attach.sql > /tmp/opencode/paths/direct_cat.sql
{
  echo "${QC_CITY//__PRED__/$PRED_CITY}"
  echo "${QC_CITY//__PRED__/$PRED_CITY}"
  echo "${QC_FULL//__PRED__/$PRED_FULL}"
  echo "${QC_FULL//__PRED__/$PRED_FULL}"
} >> /tmp/opencode/paths/direct_cat.sql
ms=$(run_host /tmp/opencode/paths/direct_cat.sql)
echo "direct catalog cold-ish CITY+FULL: ${ms}ms (first touch)"
"$ROOT/.deps/duckdb/duckdb" :memory: -csv -noheader < /tmp/opencode/paths/direct_cat.sql > /tmp/opencode/paths/direct_cat.out 2>&1 || { grep -m3 -E "^[A-Za-z ]*Error:" /tmp/opencode/paths/direct_cat.out >&2; exit 1; }
echo "-- mount catalog, cold (wiped cache) then warm"
docker exec "$BOX" sh -c 'rm -rf /cache-nw/*'
cat /tmp/opencode/paths/cat_mount_attach.sql > /tmp/opencode/paths/mount_cat.sql
{
  echo "${QC_CITY//__PRED__/$PRED_CITY}"
  echo "${QC_FULL//__PRED__/$PRED_FULL}"
} >> /tmp/opencode/paths/mount_cat.sql
ms=$(run_box /tmp/opencode/paths/mount_cat.sql)
echo "mount catalog cold CITY+FULL: ${ms}ms"
ms=$(run_box /tmp/opencode/paths/mount_cat.sql)
echo "mount catalog warm CITY+FULL: ${ms}ms"
docker exec "$BOX" du -sb /cache-nw | cut -f1 | xargs echo "cache bytes:"
