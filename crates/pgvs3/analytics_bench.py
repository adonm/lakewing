#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.5.2"]
# ///
"""ClickBench and Sedona-SpatialBench on DuckLake over pgvs3.

Same stack model as tpch_bench.py (see benchlib.connect):
  lake-s3     DuckLake, catalog=PostgreSQL, DATA_PATH = s3:// via the pgvs3 gateway
  lake-local  DuckLake, catalog=PostgreSQL, DATA_PATH = local directory (baseline)
  plain       native tables in a local DuckDB file (engine ceiling)

Benches:
  click    ClickBench `hits.parquet` + 43 queries (queries/clickbench.sql)
  spatial  apache-sedona/spatialbench parquet + 12 queries (queries/spatialbench.sql)

Examples:
  ./target/release/pgvs3 serve &                     # the gateway
  python3 crates/pgvs3/analytics_bench.py --bench click --stack lake-s3 --download --load --passes 3
  python3 crates/pgvs3/analytics_bench.py --bench spatial --sf 10 --stack lake-s3 --download --load
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.request

import duckdb

import benchlib

CLICK_TABLES = ["hits"]
SPATIAL_TABLES = ["trip", "customer", "driver", "vehicle", "zone", "building"]
FILE = {"click": "clickbench", "spatial": "spatialbench"}
CLICK_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
CLICK_BYTES = 14_779_976_446
HF_TREE = "https://huggingface.co/api/datasets/apache-sedona/spatialbench/tree/main"
HF_DL = "https://huggingface.co/datasets/apache-sedona/spatialbench/resolve/main"


def load_queries(bench: str) -> list[str]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries", f"{FILE[bench]}.sql")
    text = open(path).read()
    if bench == "click":  # one query per non-empty, non-comment line
        return [ln.strip() for ln in text.splitlines()
                if ln.strip() and not ln.strip().startswith("--")]
    queries, cur = [], None  # spatialbench: -- @qN markers
    for ln in text.splitlines():
        if ln.strip().startswith("-- @"):
            queries.append([])
            cur = queries[-1]
        elif cur is not None:
            cur.append(ln)
    return ["\n".join(q).strip() for q in queries]


def download_click(src: str, parts: int) -> None:
    if parts:  # fast loop: 1% slices of the canonical data (typed like it too)
        os.makedirs(src, exist_ok=True)
        for i in range(parts):
            dest = os.path.join(src, f"hits_{i}.parquet")
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                continue
            print(f"fetch hits_{i}.parquet")
            subprocess.run(["curl", "-fsL", "-o", dest,
                            f"{CLICK_URL.rsplit('/', 1)[0]}/athena_partitioned/hits_{i}.parquet"],
                           check=True)
        return
    dest = os.path.join(src, "hits.parquet")
    if os.path.exists(dest) and os.path.getsize(dest) == CLICK_BYTES:
        return
    os.makedirs(src, exist_ok=True)
    print(f"fetch {CLICK_URL} (13.8 GiB)")
    subprocess.run(["curl", "-fL", "-C", "-", "-o", dest, CLICK_URL], check=True)
    assert os.path.getsize(dest) == CLICK_BYTES, "truncated hits.parquet"


def download_spatial(src: str, sf: float) -> None:
    tag = f"v0.1.0/sf{sf:g}"
    listing = json.loads(urllib.request.urlopen(f"{HF_TREE}/{tag}?recursive=true").read())
    for f in listing:
        if f.get("type") != "file":
            continue
        rel = f["path"]  # v0.1.0/sf10/trip/trip.1.parquet
        dest = os.path.join(src, rel[len("v0.1.0/"):])
        if os.path.exists(dest) and os.path.getsize(dest) == f.get("size", -1):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        print(f"fetch {rel} ({f.get('size', 0) / 2**20:.0f} MiB)")
        subprocess.run(["curl", "-fsL", "-o", dest, f"{HF_DL}/{rel}"], check=True)


def src_glob(bench: str, src: str, sf: float, table: str, parts: int) -> str:
    if bench == "click":
        # exact file for the full run; numbered slices for --parts (kept
        # disjoint so both can share a source dir)
        return os.path.join(src, "hits_[0-9]*.parquet" if parts else "hits.parquet")
    return os.path.join(src, f"sf{sf:g}", table, "*.parquet")


# ClickBench's duckdb/load normalization (verbatim from ClickHouse/ClickBench
# duckdb/load): hits.parquet stores packed ints and binary strings; the 43
# queries expect the typed schema. Applied identically to every stack.
CLICK_SELECT = """* REPLACE (
    make_date(EventDate) AS EventDate,
    epoch_ms(EventTime * 1000) AS EventTime,
    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"""


def load(con, stack: str, bench: str, src: str, sf: float, parts: int) -> tuple[float, dict]:
    tables = CLICK_TABLES if bench == "click" else SPATIAL_TABLES
    t0 = __import__("time").perf_counter()
    rows = {}
    for t in tables:
        dest = f"lake.{t}" if stack != "plain" else f"main.{t}"
        sel = CLICK_SELECT if bench == "click" else "*"
        rd = (f"read_parquet('{src_glob(bench, src, sf, t, parts)}', binary_as_string=True)"
              if bench == "click" else f"read_parquet('{src_glob(bench, src, sf, t, parts)}')")
        con.sql(f"DROP TABLE IF EXISTS {dest}")
        con.sql(f"CREATE TABLE {dest} AS SELECT {sel} FROM {rd}")
        rows[t] = con.sql(f"SELECT count(*) FROM {dest}").fetchone()[0]
    if stack != "plain":
        con.sql("CALL ducklake_flush_inlined_data('lake')")  # force data into files on the data path
        for t in tables:
            con.sql(f"CREATE OR REPLACE VIEW main.{t} AS SELECT * FROM lake.{t}")
    return __import__("time").perf_counter() - t0, rows


def run_pass(con, queries: list[str], lo: int, hi: int, timeout: float) -> tuple[dict, dict]:
    times, errors = {}, {}
    for n in range(lo, hi + 1):
        secs, err = benchlib.run_sql(con, queries[n - 1], timeout)
        times[f"Q{n}"] = secs
        if err:
            errors[f"Q{n}"] = err
    return times, errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", choices=["click", "spatial"], required=True)
    ap.add_argument("--stack", choices=["lake-s3", "lake-local", "plain"], required=True)
    ap.add_argument("--sf", type=float, default=10.0, help="spatialbench scale factor (0.1/1/10/100)")
    ap.add_argument("--download", action="store_true", help="fetch the source parquet first")
    ap.add_argument("--load", action="store_true", help="load source parquet before querying")
    ap.add_argument("--views-only", action="store_true", help="skip load; just make main views over lake")
    ap.add_argument("--passes", type=int, default=2)
    ap.add_argument("--parts", type=int, default=0,
                    help="clickbench fast loop: load N of the 100 1%% slices (0 = full hits.parquet)")
    ap.add_argument("--queries", default=None, help="range like 1-43 (default: all)")
    ap.add_argument("--query-timeout", type=float, default=0, help="per-query seconds (0 = unlimited)")
    ap.add_argument("--memory-limit", default=None, help="DuckDB memory_limit (default: DuckDB's 80%% of RAM)")
    ap.add_argument("--src-dir", default=None)
    ap.add_argument("--local-dir", default=None)
    ap.add_argument("--plain-db", default=None)
    ap.add_argument("--scratch-db", default=".tmp/pgvs3/scratch.duckdb")
    ap.add_argument("--data-path", default=None)
    ap.add_argument("--catalog", default=None,
                    help="DuckLake catalog DSN (defaults: lake-s3 ducklake_catalog, lake-local ducklake_catalog_local)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    bench = args.bench
    name = FILE[bench]
    for attr, default in [
        ("src_dir", f".tmp/pgvs3/data/{name}"),
        ("local_dir", f".tmp/pgvs3/ducklake-{name}-local"),
        ("plain_db", f".tmp/pgvs3/{name}-plain.duckdb"),
        ("data_path", f"s3://lake/{name}/"),
    ]:
        if getattr(args, attr) is None:
            setattr(args, attr, default)

    queries = load_queries(bench)
    lo, _, hi = (args.queries or f"1-{len(queries)}").partition("-")
    lo, hi = int(lo), int(hi or lo)

    if args.download:
        (download_click(args.src_dir, args.parts) if bench == "click"
         else download_spatial(args.src_dir, args.sf))

    con = benchlib.connect(args.stack, args, extensions=("spatial",) if bench == "spatial" else ())
    record = {"bench": bench, "stack": args.stack, "duckdb": duckdb.__version__, "passes": []}
    if bench == "spatial":
        record["sf"] = args.sf
    if args.views_only:
        for t in (CLICK_TABLES if bench == "click" else SPATIAL_TABLES):
            con.sql(f"CREATE OR REPLACE VIEW main.{t} AS SELECT * FROM lake.{t}")
    elif args.load:
        record["load_s"], record["rows"] = load(con, args.stack, bench, args.src_dir, args.sf, args.parts)
        print(f"[{args.stack}] load: {record['load_s']}s rows={record['rows']}")

    for p in range(args.passes):
        times, errors = run_pass(con, queries, lo, hi, args.query_timeout or None)
        record["passes"].append({"times": times, "errors": errors})
        total = sum(times.values())
        print(f"[{args.stack}] pass {p + 1}: total {total:.1f}s")
        print("  " + "  ".join(f"{k}={v:.2f}" for k, v in times.items()))
        for k, v in errors.items():
            print(f"  {k}: {v}")

    # Gateway telemetry (signed debug route): span histogram + GET stage
    # counters, captured per run.
    if st := benchlib.gateway_stats():
        record["gateway_stats"] = st
        print("gateway:", st)

    benchlib.write_record(record, args.out or f".tmp/pgvs3/{name}-{args.stack}-sf{args.sf:g}.json")


if __name__ == "__main__":
    main()
