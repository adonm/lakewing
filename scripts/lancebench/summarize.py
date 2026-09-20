#!/usr/bin/env python3
"""Summarize a lancebench run directory: builds, sizes, per-query medians, S3 I/O."""

import argparse
import json
from pathlib import Path
import statistics

QUERY_ORDER = ["ID", "CITY", "BROAD", "FULL", "CURSOR", "DEEP"]


def load(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def summarize_run(root: Path):
    provenance = json.loads((root / "provenance.json").read_text())
    print(f"== {root.name} ==")
    selection = provenance.get("selection", provenance.get("sample", "?"))
    print(f"selection: {selection}")
    print(f"rows/source_sha256: {provenance.get('source_sha256', '?')[:16]}")
    for phase in ("local", "s3"):
        rows = load(root / f"{phase}.jsonl")
        if not rows:
            continue
        print(f"\n-- {phase} --")
        env = next(r for r in rows if r["operation"] == "environment")
        print(f"duckdb {env['duckdb']} lance {env['lance']} threads {env['threads']}")
        for r in rows:
            if r["operation"] in ("build", "index"):
                print(f"  {r['operation']:>6} {r['backend']:>8}"
                      + (f".{r['column']}" if "column" in r else "")
                      + f": {r['ms'] / 1000:8.2f}s cpu {r['cpu_s']:6.2f}s")
        for r in rows:
            if r["operation"] == "size":
                stage = r.get("stage", "final")
                extra = f" catalog {r['catalog_bytes'] / 1e6:.1f}MB" if "catalog_bytes" in r else ""
                print(f"  size   {r['backend']:>8} {stage:>9}: {r['bytes'] / 1e6:9.1f}MB"
                      f" data_files {r['data_files']:3d} index {r['index_bytes'] / 1e6:7.1f}MB{extra}")
        for r in rows:
            if r["operation"] in ("dataset-equality", "complete", "unindexed-control"):
                detail = {k: v for k, v in r.items()
                          if k not in ("operation", "cpu_s", "ms", "process_peak_rss_kib", "status", "trace_id")}
                print(f"  {r['operation']}: {detail}")
        queries = [q for q in QUERY_ORDER
                   if any(r["operation"] == "query" and r["query"] == q for r in rows)]
        backends = []
        for r in rows:
            if r["operation"] == "query" and r["backend"] not in backends:
                backends.append(r["backend"])
        header = f"  {'query':>6} | " + " | ".join(f"{b:>16}" for b in backends)
        print(header)
        for q in queries:
            cells = []
            for b in backends:
                warm = [r for r in rows if r["operation"] == "query" and r["query"] == q
                        and r["backend"] == b and r["repeat"] > 0]
                if not warm:
                    cells.append(f"{'-':>16}")
                    continue
                med = statistics.median(r["ms"] for r in warm)
                gets = [sum(v["requests"] for k, v in r.get("s3", {}).items() if "/GET/" in k) for r in warm]
                mib = [sum(v["bytes"] for v in r.get("s3", {}).values()) / 1e6 for r in warm]
                io = ""
                if any(gets):
                    io = f" {int(statistics.median(gets))}g {statistics.median(mib):.0f}M"
                cells.append(f"{med:8.1f}ms{io:>8}")
            print(f"  {q:>6} | " + " | ".join(cells))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    roots = [args.root] if args.root.is_file() is False and args.root.exists() else []
    if args.root.exists():
        roots = [args.root]
    else:
        base = args.root
        roots = sorted(p for p in base.parent.glob(base.name + "*") if p.is_dir())
    for root in roots:
        summarize_run(root)


if __name__ == "__main__":
    main()
