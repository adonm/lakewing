#!/usr/bin/env bash
# Minimal refs "API" for the local lake rig, backed by tiny S3 objects. In
# production the catalog API owns latest/main/tag -> catalog-hash mapping
# (versioning TBD); here this script plays that role so the flow is explicit.
#
#   lake_ref.sh get [ref]        # print catalog filename for a ref (default: latest)
#   lake_ref.sh set <catalog> [ref]  # point a ref at an immutable catalog snapshot
#   lake_ref.sh list             # show all refs
set -euo pipefail

# Dev-only credentials; single source is scripts/dev-s3.env.
# shellcheck disable=SC1091
source "$(dirname "$0")/dev-s3.env"
export RCLONE_CONFIG_LAKE_TYPE=s3 RCLONE_CONFIG_LAKE_PROVIDER=Other
export RCLONE_CONFIG_LAKE_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:8333}"
export RCLONE_CONFIG_LAKE_ACCESS_KEY_ID="$S3_USER"
export RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY="$S3_PASS"
export RCLONE_CONFIG_LAKE_REGION="$S3_REGION" RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE=true

cmd="${1:-list}"
ref="${3:-latest}"

ref_key() {
  if [ "$1" = latest ] || [ "$1" = main ]; then
    echo "lake:lake/refs/$1"
  else
    echo "lake:lake/refs/tags/$1"
  fi
}

case "$cmd" in
get)
  name="${2:-latest}"
  val=$(rclone cat "$(ref_key "$name")" 2>/dev/null) || { echo "no such ref: $name" >&2; exit 1; }
  [ -n "$val" ] || { echo "empty ref: $name" >&2; exit 1; }
  printf '%s' "$val"
  ;;
set)
  catalog="${2:?usage: lake_ref.sh set <catalog.ducklake> [ref]}"
  rclone lsf "lake:lake/catalogs/" 2>/dev/null | grep -qx "$catalog" \
    || { echo "no such published catalog: $catalog" >&2; exit 1; }
  rclone rcat "$(ref_key "$ref")" < <(printf '%s' "$catalog") >/dev/null
  echo "$ref -> $catalog"
  ;;
list)
  echo "== branches =="
  for r in latest main; do
    val=$(rclone cat "lake:lake/refs/$r" 2>/dev/null) || continue
    [ -n "$val" ] && echo "$r -> $val"
  done
  echo "== tags =="
  rclone lsf "lake:lake/refs/tags/" 2>/dev/null | while read -r f; do
    echo "$f -> $(rclone cat "lake:lake/refs/tags/$f" 2>/dev/null)"
  done
  ;;
*)
  echo "usage: lake_ref.sh {get [ref]|set <catalog> [ref]|list}" >&2
  exit 1
  ;;
esac
