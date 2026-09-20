#!/usr/bin/env python3
"""Gated Lance SDK/Arrow experiment; timings are named and results fail closed."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import time
import urllib.request
import uuid

# These must precede the native library imports. Pod limits bound total resources.
os.environ.setdefault("LANCE_CPU_THREADS", "4")
os.environ.setdefault("TOKIO_WORKER_THREADS", "4")
os.environ.setdefault("RAYON_NUM_THREADS", "4")

import duckdb
import geoarrow.pyarrow as ga
import lance
import pyarrow as pa
import pyarrow.parquet as pq


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def canonical(rows):
    return [[row[0], json.loads(row[1]), json.loads(row[2])] for row in rows]


def fingerprint(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def inventory(path):
    files = [p for p in Path(path).rglob("*") if p.is_file()]
    return {"files": len(files), "bytes": sum(p.stat().st_size for p in files),
            "index_bytes": sum(p.stat().st_size for p in files if "_indices" in p.parts),
            "data_files": len([p for p in files if p.suffix in (".lance", ".parquet") and "_indices" not in p.parts])}


def geo_batches(source):
    for batch in pq.ParquetFile(source).iter_batches(batch_size=65536):
        table = pa.Table.from_batches([batch])
        raw = table["geom"]
        # The sample gate validates that all input is 2D Polygon/MultiPolygon.
        # Keep one bit to restore Polygon after homogeneous MultiPolygon storage.
        types = ga.as_geoarrow(ga.as_wkb(raw), promote_multi=True)
        field = pa.field("geom", types.type.storage_type, metadata={
            b"ARROW:extension:name": types.type.extension_name.encode(),
            b"ARROW:extension:metadata": types.type.__arrow_ext_serialize__(),
        })
        table = table.set_column(table.schema.get_field_index("geom"), field,
                                 pa.chunked_array([chunk.storage for chunk in types.chunks]))
        # WKB byte order is explicit; the OGC type word follows it.
        was_polygon = [int.from_bytes(wkb[1:5], "little" if wkb[0] else "big") == 3
                       for wkb in raw.to_pylist()]
        table = table.append_column("was_polygon", pa.array(was_polygon))
        yield from table.to_batches()


class Bench:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.log = (self.root / (args.label + ".jsonl")).open("x")
        self.db = duckdb.connect(config={"threads": 4, "memory_limit": "1GB",
                                        "temp_directory": str(self.root / "spill")})
        self.db.execute("LOAD spatial; LOAD ducklake; LOAD lance; LOAD httpfs;")
        self.db.execute(f"SET enable_external_file_cache={str(args.external_file_cache).lower()}")
        self.storage = {}
        if args.endpoint:
            self.storage = {"endpoint": args.endpoint, "allow_http": "true",
                            "virtual_hosted_style_request": "false", "skip_signature": "true",
                            "region": "us-east-1"}
            endpoint = args.endpoint.removeprefix("http://")
            self.db.execute(f"CREATE SECRET (TYPE s3, KEY_ID '', SECRET '', REGION 'us-east-1', "
                            f"ENDPOINT {quote(endpoint)}, URL_STYLE 'path', USE_SSL false)")
        self.emit("environment", duckdb=duckdb.__version__, lance=lance.__version__,
                  arrow=pa.__version__, threads=4, duckdb_memory="1GB", arguments=vars(args),
                  extensions=self.db.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded").fetchall())

    def emit(self, operation, **fields):
        record = {"operation": operation, **fields}
        line = json.dumps(record, sort_keys=True)
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()

    def timed(self, name, action, **fields):
        counts = self.meter() if self.args.meter else None
        start = time.perf_counter()
        started_ns = time.time_ns()
        before = resource.getrusage(resource.RUSAGE_SELF)
        try:
            result = action()
        except Exception as error:
            self.emit(name, status="error", error=str(error), **fields)
            raise
        after = resource.getrusage(resource.RUSAGE_SELF)
        elapsed = (time.perf_counter() - start) * 1000
        ended_ns = time.time_ns()
        if counts is not None:
            end = self.meter()
            fields["s3"] = {k: {m: v[m] - counts.get(k, {}).get(m, 0) for m in v}
                            for k, v in end.items() if v != counts.get(k)}
        if self.args.otlp:
            trace_id = uuid.uuid4().hex
            fields["trace_id"] = trace_id
            body = {"resourceSpans": [{"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "lakewing-lancebench"}}]},
                "scopeSpans": [{"scope": {"name": "lancebench"}, "spans": [{
                    "traceId": trace_id, "spanId": uuid.uuid4().hex[:16], "name": name,
                    "startTimeUnixNano": str(started_ns), "endTimeUnixNano": str(ended_ns),
                    "attributes": [{"key": k, "value": {"stringValue": str(v)}}
                                   for k, v in fields.items() if k != "s3"],
                    "status": {"code": 1}}]}]}]}
            req = urllib.request.Request(self.args.otlp, json.dumps(body).encode(),
                                         {"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as response:
                response.read()
        self.emit(name, status="ok", ms=elapsed,
                  cpu_s=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                  process_peak_rss_kib=after.ru_maxrss, **fields)
        return result

    def meter(self):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with urllib.request.urlopen(self.args.meter + "/stats", timeout=10) as response:
                stats = json.load(response)
            if stats["active"] == 0:
                return stats["counts"]
            time.sleep(0.02)
        raise RuntimeError("origin did not quiesce; refusing ambiguous I/O attribution")

    def build(self):
        self.db.execute("SET memory_limit='2GB'")
        self.emit("build-settings", duckdb_memory="2GB", pod_memory="6Gi", target_file_bytes=512 * 1024**2)
        source = Path(self.args.source).resolve()
        schema = pq.read_schema(source)
        types = self.db.execute(f"SELECT DISTINCT ST_GeometryType(ST_GeomFromWKB(geom))::VARCHAR "
                                f"FROM read_parquet({quote(source)})").fetchall()
        if not set(x[0] for x in types) <= {"POLYGON", "MULTIPOLYGON"}:
            raise ValueError(f"native conversion only supports Polygon/MultiPolygon: {types}")
        # Refuse overwrites: versions, orphaned fragments and old indexes affect size.
        for name in ("parquet.ducklake", "parquet.files", "wkb.lance", "geo.lance"):
            if (self.root / name).exists():
                raise FileExistsError(self.root / name)

        def parquet():
            self.db.execute(f"ATTACH {quote('ducklake:' + str(self.root / 'parquet.ducklake'))} AS build "
                            f"(DATA_PATH {quote(str(self.root / 'parquet.files') + '/')})")
            self.db.execute("CALL build.set_option('target_file_size', '512MB'); "
                            "CALL build.set_option('parquet_row_group_size', 65536); "
                            "CALL build.set_option('parquet_compression', 'zstd'); "
                            "CALL build.set_option('parquet_compression_level', 3)")
            self.db.execute(f"CREATE TABLE build.features AS SELECT * REPLACE "
                            f"(ST_GeomFromWKB(geom) AS geom) FROM read_parquet({quote(source)})")
            self.db.execute("DETACH build")

        self.timed("build", parquet, backend="ducklake")
        self.emit("size", backend="ducklake", **inventory(self.root / "parquet.files"),
                  catalog_bytes=(self.root / "parquet.ducklake").stat().st_size)
        for backend in ("wkb", "geo"):
            def write():
                if backend == "geo":
                    batches = iter(geo_batches(source))
                    first = next(batches)
                    import itertools
                    reader = pa.RecordBatchReader.from_batches(first.schema, itertools.chain([first], batches))
                else:
                    reader = pa.RecordBatchReader.from_batches(schema, pq.ParquetFile(source).iter_batches(65536))
                return lance.write_dataset(reader, str(self.root / f"{backend}.lance"),
                                           data_storage_version="2.2", max_bytes_per_file=512 * 1024**2,
                                           max_rows_per_file=10_000_000)
            ds = self.timed("build", write, backend=backend)
            self.emit("size", backend=backend, stage="unindexed", **inventory(self.root / f"{backend}.lance"))
            self.timed("index", lambda: ds.create_scalar_index("id", "BTREE"), backend=backend, column="id")
            if backend == "geo":
                self.timed("index", lambda: ds.create_scalar_index("geom", "RTREE"), backend=backend, column="geom")
            self.emit("size", backend=backend, stage="indexed", **inventory(self.root / f"{backend}.lance"))
        self.db.execute("SET memory_limit='1GB'")

    def attach(self):
        prefix = self.args.uri or str(self.root)
        self.timed("attach", lambda: self.db.execute(
            f"ATTACH {quote('ducklake:' + prefix + '/parquet.ducklake')} AS lake "
            f"(READ_ONLY, DATA_PATH {quote(prefix + '/parquet.files/')}, OVERRIDE_DATA_PATH true)"), backend="ducklake")
        self.db.execute("CREATE TEMP VIEW baseline AS SELECT * FROM lake.features")
        return prefix

    def queries(self):
        sample = self.db.execute("SELECT id FROM baseline ORDER BY id LIMIT 1 OFFSET 1000").fetchone()[0]
        base = "layer='buildings' AND source_id=1"
        out = {"ID": (base + " AND id=" + quote(sample), None, 101, 0),
               "CITY": (base, (4.895, 52.365, 4.905, 52.375), 101, 0),
               "BROAD": (base, (2, 48, 4, 51), 101, 0),
               "FULL": (base, None, 1001, 0),
               "CURSOR": (base + " AND id>" + quote(sample), None, 101, 0),
               "DEEP": (base, None, 101, 50000)}
        return {name: out[name] for name in self.args.queries.split(",")}

    @staticmethod
    def predicate(where, bounds, native=False):
        if not bounds:
            return where
        w, s, e, n = bounds
        if native:
            return where + f" AND ST_Intersects(geom, ST_GeomFromText('POLYGON (({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))'))"
        return where + (f" AND xmax>={w} AND xmin<={e} AND ymax>={s} AND ymin<={n}"
                        f" AND ST_Intersects(geom,ST_MakeEnvelope({w},{s},{e},{n}))")

    def sql(self, table, where, bounds, limit, offset):
        predicate = self.predicate(where, bounds)
        if table == "baseline" and offset:
            return (f"WITH page_ids AS (SELECT id FROM {table} WHERE {predicate} ORDER BY id LIMIT {limit} OFFSET {offset}) "
                    f"SELECT p.id,ST_AsGeoJSON(f.geom),f.properties::VARCHAR FROM page_ids p "
                    f"JOIN {table} f ON f.id=p.id ORDER BY p.id")
        return (f"SELECT id,ST_AsGeoJSON(geom),properties::VARCHAR FROM {table} "
                f"WHERE {predicate} ORDER BY id LIMIT {limit} OFFSET {offset}")

    def verify_dataset(self, ds, backend):
        if backend == "ducklake":
            table = "baseline"
        else:
            cols = {name: ("ST_AsBinary(geom)" if name == "geom" else name)
                    for name in ds.schema.names} if backend == "geo" else None
            self.db.register("all_rows", ds.scanner(columns=cols, batch_readahead=2).to_reader())
            geom = "CASE WHEN was_polygon THEN (ST_Dump(geom)[1]).geom ELSE geom END" if backend == "geo" else "ST_GeomFromWKB(geom)"
            self.db.execute(f"CREATE OR REPLACE TEMP VIEW verify_rows AS SELECT * REPLACE ({geom} AS geom) FROM all_rows")
            table = "verify_rows"
        # Order-free digest (XOR/sum fold of per-row hashes): a full ORDER BY
        # over 25M rows exceeds the 1GB query limit, and order-independence
        # needs no sorter at all. Streaming keeps memory constant.
        self.db.execute(f"SELECT id,layer,source_id,hex(ST_AsWKB(geom)),properties::VARCHAR,"
                        f"sortkey,xmin,ymin,xmax,ymax,cx,cy,name FROM {table}")
        fold, total, count = 0, 0, 0
        while rows := self.db.fetchmany(8192):
            for row in rows:
                row_hash = int.from_bytes(hashlib.sha256(
                    json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode()
                ).digest()[:16], "big")
                fold ^= row_hash
                total = (total + row_hash) % 2**128
                count += 1
        return {"rows": count, "sha256": f"{fold:032x}", "row_sum": f"{total:032x}"}

    def sdk(self, ds, backend, spec, use_index=True, ids_first=False):
        where, bounds, limit, offset = spec
        if backend == "geo":
            columns = {"id": "id", "geom": "ST_AsBinary(geom)", "properties": "properties",
                       "layer": "layer", "source_id": "source_id", "was_polygon": "was_polygon"}
            filt = self.predicate(where, bounds, native=True)
        else:
            columns = ["id", "geom", "properties", "layer", "source_id", "xmin", "ymin", "xmax", "ymax"]
            filt = where
            if bounds:
                w, s, e, n = bounds
                filt += f" AND xmax>={w} AND xmin<={e} AND ymax>={s} AND ymin<={n}"
        if ids_first:
            if not bounds:
                columns = ["id", "layer", "source_id"]
            elif isinstance(columns, dict):
                columns.pop("properties")
            else:
                columns.remove("properties")
        scanner = ds.scanner(columns=columns, filter=filt, use_scalar_index=use_index,
                             batch_readahead=2, fragment_readahead=2, io_buffer_size=64 * 1024**2)
        self.db.register("candidates", scanner.to_reader())
        if ids_first and not bounds:
            view = "SELECT * FROM candidates"
        elif backend == "geo":
            # GeoArrow WKB metadata imports as GEOMETRY, not BLOB, in DuckDB.
            view = "SELECT * REPLACE (CASE WHEN was_polygon THEN (ST_Dump(geom)[1]).geom ELSE geom END AS geom) FROM candidates"
        else:
            view = "SELECT * REPLACE (ST_GeomFromWKB(geom) AS geom) FROM candidates"
        self.db.execute("CREATE OR REPLACE TEMP VIEW sdk_rows AS " + view)
        # SDK performs exact spatial filtering before Arrow; still recheck in
        # DuckDB to establish identical semantics before applying LIMIT/OFFSET.
        if backend == "geo" and bounds:
            w, s, e, n = bounds
            where += f" AND ST_Intersects(geom,ST_MakeEnvelope({w},{s},{e},{n}))"
            bounds = None
        if ids_first:
            pred = self.predicate(where, bounds)
            ids = self.db.execute(f"SELECT id FROM sdk_rows WHERE {pred} ORDER BY id LIMIT {limit} OFFSET {offset}").fetchall()
            if not ids:
                return []
            payload_filter = "id IN (" + ",".join(quote(row[0]) for row in ids) + ")"
            return self.sdk(ds, backend, (payload_filter, None, limit, 0), use_index)
        return self.db.execute(self.sql("sdk_rows", where, bounds, limit, offset)).fetchall()

    def run(self):
        prefix = self.attach()
        specs = self.queries()
        expected = {}
        for name, spec in specs.items():
            expected[name] = canonical(self.db.execute(self.sql("baseline", *spec)).fetchall())
            if not expected[name]:
                raise AssertionError(f"empty control workload: {name}")
        (self.root / (self.args.label + "-expected.json")).write_text(json.dumps(expected))
        verified = set()
        for backend in self.args.backends.split(","):
            encoding = backend.removesuffix("_ids")
            ds = None if backend == "ducklake" else self.timed("attach", lambda: lance.dataset(
                prefix + f"/{encoding}.lance", storage_options=self.storage), backend=backend)
            for name, spec in specs.items():
                for repeat in range(self.args.repeats):
                    def query():
                        rows = self.db.execute(self.sql("baseline", *spec)).fetchall() if ds is None else self.sdk(ds, encoding, spec, ids_first=backend.endswith("_ids") and name != "ID")
                        return canonical(rows)
                    profile = self.root / f"{self.args.label}-{backend}-{name}-{repeat}.profile.json"
                    self.db.execute(f"PRAGMA enable_profiling='json'; SET profiling_output={quote(profile)}")
                    rows = self.timed("query", query, backend=backend, query=name, repeat=repeat,
                                      phase="first-no-eviction" if repeat == 0 else "warm")
                    self.db.execute("PRAGMA disable_profiling")
                    if rows != expected[name]:
                        raise AssertionError(f"full result mismatch: {backend}/{name}: {fingerprint(rows)} != {fingerprint(expected[name])}")
                    self.emit("equality", backend=backend, query=name, repeat=repeat,
                              rows=len(rows), sha256=fingerprint(rows), equal=True)
            if encoding == "geo" and "CITY" in specs:
                filt = self.predicate(*specs["CITY"][:2], native=True)
                plan = ds.scanner(filter=filt).explain_plan()
                (self.root / (self.args.label + "-rtree-plan.txt")).write_text(plan)
                if "RTree" not in plan or "ScalarIndexQuery" not in plan:
                    raise AssertionError("R-tree is not used")
                unindexed = self.timed("unindexed-control", lambda: canonical(self.sdk(ds, encoding, specs["CITY"], False)))
                if unindexed != expected["CITY"]:
                    raise AssertionError("indexed/unindexed mismatch")
            if not self.args.uri and encoding not in verified:
                digest = self.timed("verify-dataset", lambda: self.verify_dataset(ds, encoding), backend=backend)
                if backend == "ducklake":
                    baseline_digest = digest
                elif digest != baseline_digest:
                    raise AssertionError(f"full dataset mismatch: {backend}: {digest} != {baseline_digest}")
                self.emit("dataset-equality", backend=backend, equal=True, **digest)
                verified.add(encoding)
        self.emit("complete", equality=True, queries=len(specs), repeats=self.args.repeats)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--source")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--label", default="local")
    parser.add_argument("--uri", help="S3 prefix containing catalog/data and both Lance datasets")
    parser.add_argument("--endpoint", help="Unsigned S3 cache endpoint")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--meter")
    parser.add_argument("--otlp")
    parser.add_argument("--external-file-cache", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--queries", default="ID,CITY,BROAD,FULL,CURSOR,DEEP")
    parser.add_argument("--backends", default="ducklake,wkb,geo,wkb_ids,geo_ids")
    args = parser.parse_args()
    if args.repeats < 1 or (args.build and not args.source):
        parser.error("positive --repeats and --source with --build are required")
    if args.backends.split(",")[0] != "ducklake" or not set(args.backends.split(",")) <= {"ducklake", "wkb", "geo", "wkb_ids", "geo_ids"}:
        parser.error("backends must start with ducklake and contain known variants")
    if not set(args.queries.split(",")) <= {"ID", "CITY", "BROAD", "FULL", "CURSOR", "DEEP"}:
        parser.error("unknown query")
    bench = Bench(args)
    if args.build:
        bench.build()
    bench.run()


if __name__ == "__main__":
    main()
