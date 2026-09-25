"""Shared DuckLake/duckdb stack wiring for the bench harnesses.

One home for connection setup (the lake-s3 / lake-local / plain stacks and
their session settings), the timeout-aware single-query runner, and the
signed gateway telemetry fetch.
"""
import json
import os
import re
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
    # Fresh containers have neither directory; duckdb does not create them.
    os.makedirs(os.path.dirname(db) or ".", exist_ok=True)
    os.makedirs(".tmp/pgvs3", exist_ok=True)
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
    if getattr(args, "no_file_cache", False):
        # Every pass then re-reads through httpfs, i.e. through the proxy:
        # passes after the first measure the read path, not DuckDB's cache.
        con.sql("SET enable_external_file_cache = false")
    for ext in ("postgres", "httpfs", "ducklake", *extensions):
        if stack == "plain" and ext in ("postgres", "httpfs", "ducklake"):
            continue
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    for kv in getattr(args, "set", None) or []:
        name, _, value = kv.partition("=")
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
            raise SystemExit(f"--set: bad setting name {name!r}")
        con.sql(f"SET {name} = '{value.replace(chr(39), chr(39) * 2)}'")
    if stack == "lake-s3":
        # The gateway location: 127.0.0.1:8014 on the rig, a Service in k8s.
        con.sql(f"SET s3_endpoint='{os.environ.get('PGVS3_ENDPOINT', '127.0.0.1:8014')}'")
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
             f"{os.environ.get('PGVS3_URL', 'http://127.0.0.1:8014')}/_pgvs3/stats"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
    except Exception:
        return None
    return st if st.startswith("perf:") else None


def gateway_counters(st):
    """(MiB served, GETs, pid) from a gateway perf line, or None.

    pid says which gateway process answered: the Service round-robins across
    replicas and counters are per-process, so a diff across two instances is
    meaningless (it can go negative)."""
    m = st and re.search(r"pid=(\d+).*small n=(\d+) .*\| stream n=(\d+) .* served=(\d+)MiB", st)
    return (int(m.group(4)), int(m.group(2)) + int(m.group(3)), int(m.group(1))) if m else None


def write_record(record: dict, out: str) -> None:
    with open(out, "w") as f:
        json.dump(record, f, indent=1)
    print(f"wrote {out}")
