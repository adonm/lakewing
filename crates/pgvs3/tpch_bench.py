#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.5.2"]
# ///
"""TPC-H (DuckDB `tpch` extension) on DuckLake over pgvs3.

Stacks: see benchlib.connect (lake-s3 / lake-local / plain).

Loads with `CALL dbgen`, times every TPC-H query for `--passes` passes, and
writes a JSON record. Example:

  ./target/release/pgvs3 serve &                     # the gateway
  python3 crates/pgvs3/tpch_bench.py --stack lake-s3 --sf 10 --load
  python3 crates/pgvs3/tpch_bench.py --stack lake-local --sf 10 --load
  python3 crates/pgvs3/tpch_bench.py --stack plain --sf 10 --load
"""

import argparse
import time

import duckdb

import benchlib

TABLES = ["region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]


def load(con, stack: str, sf: float) -> float:
    t0 = time.perf_counter()
    for t in TABLES:  # scratch db may hold views/tables from earlier runs
        for stmt in (f"DROP TABLE IF EXISTS main.{t}", f"DROP VIEW IF EXISTS main.{t}"):
            try:
                con.sql(stmt)
            except Exception:
                pass
    con.sql(f"CALL dbgen(sf={sf})")
    if stack == "plain":
        return time.perf_counter() - t0
    for t in TABLES:
        con.sql(f"CREATE TABLE lake.{t} AS SELECT * FROM main.{t}")
        con.sql(f"DROP TABLE main.{t}")  # bound scratch space at one table
    con.sql("CALL ducklake_flush_inlined_data('lake')")  # force data into files on the data path
    for t in TABLES:
        con.sql(f"CREATE VIEW main.{t} AS SELECT * FROM lake.{t}")
    return time.perf_counter() - t0


def run_pass(con, queries) -> dict:
    times = {}
    for q in queries:
        times[q] = benchlib.run_sql(con, f"PRAGMA tpch({q})")[0]
    return times


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stack", choices=["lake-s3", "lake-local", "plain"], required=True)
    ap.add_argument("--sf", type=float, default=10.0)
    ap.add_argument("--load", action="store_true", help="dbgen + load before querying")
    ap.add_argument("--views-only", action="store_true", help="skip load; just make main views over lake")
    ap.add_argument("--passes", type=int, default=2)
    ap.add_argument("--queries", default="1-22")
    ap.add_argument("--local-dir", default=".tmp/pgvs3/ducklake-local")
    ap.add_argument("--plain-db", default=".tmp/pgvs3/plain.duckdb")
    ap.add_argument("--scratch-db", default=".tmp/pgvs3/scratch.duckdb")
    ap.add_argument("--data-path", default="s3://lake/ducklake/")
    ap.add_argument("--catalog", default=None,
                    help="DuckLake catalog DSN (defaults: lake-s3 ducklake_catalog, lake-local ducklake_catalog_local)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    lo, _, hi = args.queries.partition("-")
    queries = list(range(int(lo), int(hi or lo) + 1))

    con = benchlib.connect(args.stack, args, extensions=("tpch",))
    record = {"stack": args.stack, "sf": args.sf, "duckdb": duckdb.__version__, "passes": []}
    if args.views_only:
        for t in TABLES:
            con.sql(f"CREATE OR REPLACE VIEW main.{t} AS SELECT * FROM lake.{t}")
    elif args.load:
        record["load_s"] = round(load(con, args.stack, args.sf), 3)
        print(f"[{args.stack}] load sf={args.sf}: {record['load_s']}s")

    for p in range(args.passes):
        times = run_pass(con, queries)
        record["passes"].append(times)
        total = sum(times.values())
        print(f"[{args.stack}] pass {p + 1}: total {total:.1f}s")
        print("  " + "  ".join(f"Q{q}={times[q]:.2f}" for q in queries))

    # Gateway telemetry (signed debug route): span histogram + GET stage
    # counters, captured per run.
    if st := benchlib.gateway_stats():
        record["gateway_stats"] = st
        print("gateway:", st)

    benchlib.write_record(record, args.out or f".tmp/pgvs3/tpch-{args.stack}-sf{args.sf:g}.json")


if __name__ == "__main__":
    main()
