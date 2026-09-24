"""Shared DuckLake/duckdb stack wiring for the bench harnesses.

One home for connection setup (the lake-s3 / lake-local / plain stacks and
their session settings), the timeout-aware single-query runner, and the
signed gateway telemetry fetch.
"""
import json
import os
import subprocess
import threading
import time

import duckdb

PG = "dbname=ducklake_catalog host=127.0.0.1 user=postgres password=postgres"
PG_LOCAL = "dbname=ducklake_catalog_local host=127.0.0.1 user=postgres password=postgres"


def connect(stack: str, args, extensions: tuple = ()) -> duckdb.DuckDBPyConnection:
    """Open a scratch (lake stacks) or plain-database connection, attaching
    the stack's storage as `lake`. `extensions` are extras (e.g. tpch, spatial)."""
    # File-backed scratch: raw inputs and spills do not fit in RAM. Spills go
    # to the NVMe tree, not the tmpfs /tmp.
    db = args.scratch_db if stack.startswith("lake") else args.plain_db
    try:
        con = duckdb.connect(db)
    except duckdb.IOException:
        # DuckDB files carry a format version (e.g. 2.0-alpha dev files are
        # unreadable by 1.x) — scratch state is disposable, start fresh.
        os.remove(db)
        con = duckdb.connect(db)
    con.sql("SET temp_directory='.tmp/pgvs3/duckdb-temp'")
    # Multipart Completes flush through the proxy into Aurora; the default
    # 30s response window is smaller than a big flush under load, and httpfs
    # refuses to retry an unknown-outcome Complete. Generous window.
    con.sql("SET http_timeout=300")
    if getattr(args, "memory_limit", None):
        con.sql(f"SET memory_limit='{args.memory_limit}'")
    for ext in ("postgres", "httpfs", "ducklake", *extensions):
        if stack == "plain" and ext in ("postgres", "httpfs", "ducklake"):
            continue
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    if stack == "lake-s3":
        con.sql("SET s3_endpoint='127.0.0.1:8014'")
        con.sql("SET s3_use_ssl=false")
        con.sql("SET s3_url_style='path'")
        con.sql("SET s3_access_key_id='cachebench'")
        con.sql("SET s3_secret_access_key='cachebench-local-only'")
        con.sql(f"ATTACH 'ducklake:postgres:{args.catalog or PG}' AS lake (DATA_PATH '{args.data_path}')")
    elif stack == "lake-local":
        os.makedirs(args.local_dir, exist_ok=True)
        # --catalog overrides the local default (rig runs point this at Aurora)
        con.sql(f"ATTACH 'ducklake:postgres:{args.catalog or PG_LOCAL}' AS lake (DATA_PATH '{args.local_dir}')")
    return con


def run_sql(con, sql: str, timeout: float | None = None) -> tuple[float, str | None]:
    """Run one query to completion: (elapsed seconds, error or None).
    A wall-clock timeout interrupts the connection and reports 'timeout'."""
    timer = None
    if timeout:
        timer = threading.Timer(timeout, con.interrupt)
        timer.daemon = True
        timer.start()
    t0 = time.perf_counter()
    err = None
    try:
        con.execute(sql).fetchall()
    except Exception as e:
        name = type(e).__name__
        err = f"timeout>{timeout}s" if "Interrupt" in name else f"{name}: {e}"[:300]
    finally:
        if timer:
            timer.cancel()
    return round(time.perf_counter() - t0, 3), err


def gateway_stats():
    """Signed debug route: cache telemetry for the run record."""
    try:
        st = subprocess.run(
            ["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
             "--user", "cachebench:cachebench-local-only",
             "http://127.0.0.1:8014/_pgvs3/stats"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
    except Exception:
        return None
    return st if st.startswith("perf:") else None


def write_record(record: dict, out: str) -> None:
    with open(out, "w") as f:
        json.dump(record, f, indent=1)
    print(f"wrote {out}")
