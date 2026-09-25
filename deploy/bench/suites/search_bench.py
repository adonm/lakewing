#!/usr/bin/env python3
"""Search benchmark: Quickwit's REST API against its OTLP log schema.

Latency against an empty index is a lie, so the suite first ingests `--docs`
synthetic logs shaped like the otel-logs mapping (unix-seconds timestamps,
json body/attributes) through the ingest API. Ingest is asynchronous — docs
land seconds later — so it polls until they do.

Caches stay ON (Quickwit's split-footer / fast-field caches) — this is
"real world" performance, so warm passes are the point, not a bias to hide.

Emits one JSON line: per-query p50/p95/p99 and the share of queries under
20ms (the number the architecture claims).
"""
import argparse
import json
import random
import time
import urllib.parse
import urllib.request

SERVICES = ["checkout", "cart", "search", "auth", "payments"]
TS_BASE = 1727240000  # unix seconds; make_doc walks forward one second per doc
LEVELS = ["ERROR", "WARN", "INFO", "DEBUG"]
LEVEL_WEIGHTS = [1, 2, 5, 2]  # ERROR-heavy enough to be selective
HOSTS = ["web-03", "web-07", "web-11", "api-02"]
MSGS = ["timeout after 100ms", "connection reset", "upstream slow", "all good"]

# Validated against the otel-logs mapping: raw-tokenized text fields
# (service_name, severity_text), json body/attributes (dotted subfields),
# numeric range on attributes.duration_ms.
QUERIES = [
    "severity_text:ERROR",
    "severity_text:WARN AND service_name:cart",
    "service_name:checkout",
    "service_name:search AND severity_text:INFO",
    "body.message:timeout",
    "attributes.host:web-03",
    "attributes.duration_ms:>100",
    "severity_text:ERROR AND attributes.duration_ms:>500",
]


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0.0


def find_index(url, want):
    if want != "auto":
        return want
    idx = json.load(urllib.request.urlopen(f"{url}/api/v1/indexes", timeout=30))
    ids = [(i.get("index_id") or i.get("index_uid", "")).split(":")[0] for i in idx]
    for i in ids:
        if i.startswith("otel-logs"):
            return i
    return ids[0] if ids else "otel-logs-v0_9"


def search(url, index, q, max_hits=10, start=None, end=None):
    u = f"{url}/api/v1/{index}/search?query={urllib.parse.quote(q)}&max_hits={max_hits}"
    if start is not None:
        # Time-scoped queries: Quickwit prunes whole splits on the timestamp
        # range (remove_redundant_timestamp_range in leaf.rs), and a fresh
        # window keeps the per-split result cache from answering repeats.
        u += f"&start_timestamp={start}&end_timestamp={end}"
    with urllib.request.urlopen(u, timeout=30) as r:
        return json.load(r)


def make_doc(i):
    # The mapping wants unix seconds and json bodies; anything else is
    # rejected at ingest, silently, which is how empty-index numbers happen.
    return {
        "timestamp_nanos": TS_BASE + i,
        "severity_text": random.choices(LEVELS, weights=LEVEL_WEIGHTS)[0],
        "service_name": random.choice(SERVICES),
        "body": {"message": random.choice(MSGS)},
        "attributes": {"host": random.choice(HOSTS), "duration_ms": random.randint(1, 900)},
    }


def ingest_batch(url, index, start, n):
    body = "".join(json.dumps(make_doc(start + i)) + "\n" for i in range(n))
    req = urllib.request.Request(
        f"{url}/api/v1/{index}/ingest",
        data=body.encode(),
        headers={"Content-Type": "application/x-ndjson"},
    )
    retries = 0
    # Quickwit answers 503 when a shard falls behind: backpressure, not
    # failure. A real client backs off and paces; so does this one.
    for attempt in range(60):
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                resp = json.load(r)
            break
        except Exception as e:
            if getattr(e, "code", None) in (429, 500, 502, 503, 504) and attempt < 59:
                retries += 1
                time.sleep(min(2.0 * (attempt + 1), 15.0))
                continue
            raise
    if resp.get("num_rejected_docs"):
        raise SystemExit(f"ingest rejected {resp['num_rejected_docs']} docs: {resp}")
    return retries


def ingest(url, index, docs, batch=2000, workers=8):
    from concurrent.futures import ThreadPoolExecutor

    batches = [(s, min(batch, docs - s)) for s in range(0, docs, batch)]
    retries = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for r in ex.map(lambda b: ingest_batch(url, index, *b), batches):
            retries += r
    return docs, retries


def wait_searchable(url, index, want, timeout=120.0):
    t0, hits = time.perf_counter(), 0
    while time.perf_counter() - t0 < timeout:
        hits = search(url, index, "*")["num_hits"]
        if hits >= want:
            break
        time.sleep(1.0)
    return hits, round(time.perf_counter() - t0, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://quickwit:7280")
    ap.add_argument("--index", default="auto", help="'auto' picks the otel-logs index")
    ap.add_argument("--docs", type=int, default=100_000)
    ap.add_argument("--workers", type=int, default=8, help="parallel ingest batches")
    ap.add_argument("--queries", type=int, default=200)
    ap.add_argument("--window-frac", type=float, default=0.0,
                    help="scope each query to a random window covering this fraction of the "
                         "index (exercises split pruning and defeats the result cache)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    index = find_index(args.url, args.index)
    pre = search(args.url, index, "*")["num_hits"]
    t0 = time.perf_counter()
    ingested, retries = (
        ingest(args.url, index, args.docs, workers=args.workers) if args.docs else (0, 0)
    )
    ingest_s = round(time.perf_counter() - t0, 1)
    if ingested:
        landed, waited_s = wait_searchable(args.url, index, pre + ingested)
        if landed < pre + ingested:
            raise SystemExit(f"only {landed} of {pre + ingested} documents searchable after 120s")
    else:
        landed, waited_s = pre, 0.0

    lats, errors, first_err = [], 0, None
    for _ in range(args.queries):
        q = random.choice(QUERIES)
        start = end = None
        if args.window_frac:
            span = max(1, int(landed * args.window_frac))
            start = TS_BASE + random.randrange(0, max(1, landed - span))
            end = start + span
        t0 = time.perf_counter()
        try:
            search(args.url, index, q, start=start, end=end)
        except Exception as e:
            errors += 1
            if first_err is None:
                body = e.read(120).decode()[:120] if hasattr(e, "read") else ""
                first_err = f"{e} {body}".strip()
        lats.append((time.perf_counter() - t0) * 1000.0)

    r = {
        "suite": "search",
        "index": index,
        "docs": ingested,
        "index_total": landed,
        "ingest_s": ingest_s,
        "ingest_docs_per_s": round(ingested / max(ingest_s, 1e-9)),
        "ingest_retries": retries,
        "ingest_wait_s": waited_s,
        "window_frac": args.window_frac,
        "queries": args.queries,
        "errors": errors,
        "p50_ms": round(pct(lats, 0.50), 3),
        "p95_ms": round(pct(lats, 0.95), 3),
        "p99_ms": round(pct(lats, 0.99), 3),
        "sub_20ms_pct": round(100.0 * sum(1 for x in lats if x < 20.0) / max(len(lats), 1), 1),
    }
    if first_err:
        r["first_error"] = first_err
    line = json.dumps(r)
    if args.out:
        with open(args.out, "w") as f:
            f.write(line + "\n")
    print(line)
    if errors:
        raise SystemExit(f"{errors} search queries failed: {first_err}")


if __name__ == "__main__":
    main()
