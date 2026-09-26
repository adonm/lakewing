#!/usr/bin/env python3
"""Run one portable OSB bulk corpus and search workload against Quickwit."""

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from search_bench import TS_BASE, make_doc

OUT = Path(os.environ.get("OSB_OUT_DIR", "/bench/out/osb"))
SCRIPT = Path(__file__).resolve()


def quickwit_json(endpoint, path):
    with urllib.request.urlopen(endpoint + path, timeout=120) as response:
        return json.load(response)


def index_name(endpoint, requested):
    if requested != "auto":
        return requested
    indexes = quickwit_json(endpoint, "/api/v1/indexes")
    names = [(entry.get("index_id") or entry.get("index_uid", "")).split(":")[0]
             for entry in indexes]
    matches = [name for name in names if name.startswith("otel-logs-")]
    if len(matches) != 1:
        raise RuntimeError(f"expected one OTLP log index, found {matches}")
    return matches[0]


def indexed_count(endpoint, index):
    indexes = quickwit_json(endpoint, "/api/v1/_elastic/_cat/indices?format=json")
    return next(int(row["docs.count"]) for row in indexes if row["index"] == index)


def check_searchable(endpoint, index, docs):
    start = TS_BASE + docs // 3
    body = {
        "size": 0, "track_total_hits": True,
        "query": {"bool": {
            "must": [{"term": {"severity_text": {"value": "ERROR"}}}],
            "filter": [{"range": {"timestamp_nanos": {
                "gte": start, "lt": start + max(1, docs // 10),
            }}}],
        }},
    }
    index = urllib.parse.quote(index, safe="")
    request = urllib.request.Request(
        f"{endpoint}/api/v1/_elastic/{index}/_search",
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        hits = json.load(response)["hits"]["total"]["value"]
    if hits < 1:
        raise RuntimeError("indexed corpus produced an empty filtered search")


def corpus(path, index, docs):
    random.seed(42)
    action = (json.dumps({"create": {"_index": index}}, separators=(",", ":")) + "\n").encode()
    digest = hashlib.sha256()
    with path.open("wb", buffering=4 * 1024 * 1024) as output:
        for i in range(docs):
            doc = (json.dumps(make_doc(i), separators=(",", ":")) + "\n").encode()
            output.write(action)
            output.write(doc)
            digest.update(action)
            digest.update(doc)
            if (i + 1) % 1_000_000 == 0:
                print(f"OSB corpus: {i + 1:,}/{docs:,} documents", flush=True)
    return path.stat().st_size, digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def bulk_workload(directory, index, docs, size):
    write_json(directory / "workload.json", {
        "version": 2,
        "description": "OTLP-shaped logs; create-only ES bulk ingest, index pre-created",
        "corpora": [{"name": "otel-logs", "documents": [{
            "source-file": "docs.json", "document-count": docs,
            "uncompressed-bytes": size, "includes-action-and-meta-data": True,
        }]}],
        "schedule": [{
            "operation": {"name": "bulk-create-otel", "operation-type": "bulk", "bulk-size": 2000},
            "clients": 1,
        }],
    })


def search_workload(directory, index, docs, queries, fraction):
    body = {"size": 10, "track_total_hits": False,
            "query": {"term": {"severity_text": {"value": "ERROR"}}}}
    if fraction:
        body["query"] = {"bool": {
            "must": [body["query"]],
            "filter": [{"range": {"timestamp_nanos": {
                "gte": TS_BASE, "lt": TS_BASE + max(1, int(docs * fraction)),
            }}}],
        }}
    write_json(directory / "workload.json", {
        "version": 2,
        "description": "OTLP-shaped logs, fixed or randomized time-window search",
        "schedule": [{
            "operation": {"name": f"{'fresh-window' if fraction else 'fixed-term'}-{clients}",
                          "operation-type": "search",
                          "index": index, "body": body},
            "clients": clients,
            "warmup-iterations": min(40, queries),
            "iterations": queries,
        } for clients in (1, 8)],
    })
    if fraction:
        shutil.copyfile(SCRIPT.with_name("osb_workload.py"), directory / "workload.py")


def run_osb(directory, home, *, randomize=False):
    runs = home / ".benchmark/benchmarks/test-runs"
    before = set(runs.glob("*/test_run.json"))
    command = [
        "opensearch-benchmark", "run", "--pipeline=benchmark-only",
        f"--workload-path={directory}",
        "--target-hosts=http://127.0.0.1:19200/api/v1/_elastic", "--offline",
        "--latency-percentiles=50,95,99",
    ]
    if randomize:
        command += ["--randomization-enabled", "--randomization-repeat-frequency=0",
                    "--randomization-n=128"]
    env = os.environ.copy()
    env["HOME"] = str(home)
    subprocess.run(command, env=env, check=True)
    new = set(runs.glob("*/test_run.json")) - before
    if len(new) != 1:
        raise RuntimeError(f"expected one OSB result, found {len(new)}")
    metrics = json.loads(new.pop().read_text())["results"]["op_metrics"]
    if not metrics or any(row.get("error_rate") != 0 for row in metrics):
        raise RuntimeError(f"OSB reported operation failures: {metrics}")
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, required=True)
    parser.add_argument("--queries", type=int, default=400)
    parser.add_argument("--window-frac", type=float, default=0.1)
    parser.add_argument("--index", default="auto")
    parser.add_argument("--out", default="/bench/out/search.json")
    parser.add_argument("--generate-only", action="store_true")
    args = parser.parse_args()
    if (not 0 <= args.docs <= 500_000_000 or not 1 <= args.queries <= 100_000
            or not 0 <= args.window_frac <= 1):
        parser.error("invalid corpus or query size")

    OUT.mkdir(parents=True, exist_ok=True)
    home = OUT / "home"
    home.mkdir(exist_ok=True)
    if args.docs:
        available = shutil.disk_usage(OUT).free
        if available < args.docs * 300 + 2 * 1024**3:
            raise RuntimeError("insufficient disk for the OSB corpus")
    if args.generate_only:
        if args.docs == 0 or args.index == "auto":
            parser.error("portable corpus generation needs --docs and an explicit --index")
        ingest_dir = OUT / "ingest"
        ingest_dir.mkdir(exist_ok=True)
        size, digest = corpus(ingest_dir / "docs.json", args.index, args.docs)
        bulk_workload(ingest_dir, args.index, args.docs, size)
        search_dir = OUT / "search"
        search_dir.mkdir(exist_ok=True)
        search_workload(search_dir, args.index, args.docs, args.queries, args.window_frac)
        print(json.dumps({"docs": args.docs, "bytes": size, "sha256": digest,
                          "ingest_workload": str(ingest_dir),
                          "search_workload": str(search_dir)}), flush=True)
        return

    search = os.environ.get("QUICKWIT_URL", "http://quickwit:7280")
    ingest = os.environ.get("QUICKWIT_INGEST_URL", search)
    index = index_name(ingest, args.index)
    before = indexed_count(search, index)
    if args.docs >= 10_000_000 and before:
        raise RuntimeError("large OSB corpus needs a fresh Quickwit index; reset the test databases")
    adapter = subprocess.Popen([sys.executable, str(SCRIPT.with_name("osb_adapter.py"))],
                               env={**os.environ, "QUICKWIT_URL": search,
                                    "QUICKWIT_INGEST_URL": ingest})
    try:
        for _ in range(60):
            if adapter.poll() is not None:
                raise RuntimeError("OSB compatibility adapter stopped")
            try:
                quickwit_json("http://127.0.0.1:19200", "/api/v1/_elastic/")
                break
            except OSError:
                time.sleep(0.2)
        else:
            raise RuntimeError("OSB compatibility adapter did not start")

        size = digest = None
        ingest_metrics = []
        started = time.monotonic()
        if args.docs:
            directory = OUT / "ingest"
            directory.mkdir(exist_ok=True)
            size, digest = corpus(directory / "docs.json", index, args.docs)
            bulk_workload(directory, index, args.docs, size)
            started = time.monotonic()
            ingest_metrics = run_osb(directory, home)
            timeout = max(120, args.docs // 50_000)
            deadline = time.monotonic() + timeout
            while indexed_count(search, index) < before + args.docs and time.monotonic() < deadline:
                time.sleep(2)
        total = indexed_count(search, index)
        if total != before + args.docs:
            raise RuntimeError(f"indexed count mismatch: expected {before + args.docs}, found {total}")
        query_span = args.docs or total
        check_searchable(search, index, query_span)
        indexed_seconds = round(time.monotonic() - started, 3) if args.docs else 0.0

        directory = OUT / "search"
        directory.mkdir(exist_ok=True)
        search_workload(directory, index, query_span, args.queries, args.window_frac)
        env_docs = str(query_span)
        os.environ["OSB_DOCS"] = env_docs
        os.environ["OSB_WINDOW_FRAC"] = str(args.window_frac)
        search_metrics = run_osb(directory, home, randomize=bool(args.window_frac))
        record = {
            "suite": "search", "framework": "opensearch-benchmark", "index": index,
            "docs": args.docs, "index_total": total, "query_span_docs": query_span,
            "window_frac": args.window_frac,
            "corpus_bytes": size,
            "corpus_sha256": digest, "indexed_s": indexed_seconds,
            "ingest": ingest_metrics, "search": search_metrics,
        }
        write_json(Path(args.out), record)
        print(json.dumps(record, separators=(",", ":")), flush=True)
    finally:
        adapter.terminate()
        adapter.wait(timeout=10)


if __name__ == "__main__":
    main()
