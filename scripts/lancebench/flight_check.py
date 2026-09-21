#!/usr/bin/env python3
"""Flight client test: list flights, get schema, DoGet a ticket, verify
rows against the OGC serve's identical ticket parameters."""

import sys

import pyarrow as pa
import pyarrow.flight as fl


def main():
    addr = sys.argv[1] if len(sys.argv) > 1 else "grpc://127.0.0.1:50071"
    client = fl.FlightClient(addr)

    flights = list(client.list_flights())
    print(f"list_flights: {len(flights)} flight(s)")
    for f in flights:
        d = f.descriptor
        ids = d.path[0].decode() if d.path else "<cmd>"
        print(f"  collection={ids} rows={f.total_records}")

    # GetSchema via one-part path descriptor
    schema = client.get_schema(fl.FlightDescriptor.for_path("buildings")).schema
    print("schema:", [(f.name, str(f.type)) for f in schema])

    # DoGet: default ticket (all default columns)
    ticket = fl.Ticket(b'{"collection":"buildings","limit":101}')
    table = client.do_get(ticket).read_all()
    print(f"do_get default: rows={table.num_rows} cols={[c for c in table.column_names]}")

    # DoGet: bbox + columns + offset, matching the OGC DEEP page
    deep = fl.Ticket(
        b'{"collection":"buildings","limit":101,"offset":50000,'
        b'"columns":["id","geometry","properties"]}'
    )
    deep_table = client.do_get(deep).read_all()
    print(f"do_get deep: rows={deep_table.num_rows} first_id={deep_table.column('id')[0]}")

    # Cross-check against the HTTP serve on the same ticket parameters
    import json
    import urllib.request
    with urllib.request.urlopen(
        "http://127.0.0.1:3140/collections/buildings/items?sources=1&limit=101&offset=50000",
        timeout=120,
    ) as r:
        body = json.loads(r.read())
    http_ids = [f["id"] for f in body["features"]]
    flight_ids = deep_table.column("id").to_pylist()
    same = http_ids == flight_ids
    print(f"id order matches OGC serve: {same} ({len(http_ids)} vs {len(flight_ids)})")
    if not same:
        for i, (a, b) in enumerate(zip(http_ids, flight_ids)):
            if a != b:
                print(f"  first diff at {i}: {a} vs {b}")
                break
        sys.exit(1)

    # Geometry spot-check: WKB round-trips through ST_GeomFromWKB to the
    # same GeoJSON the serve emits for the first row.
    first = body["features"][0]
    wkb = deep_table.column("geometry")[0].as_py()
    import duckdb
    con = duckdb.connect()
    con.execute("LOAD spatial")
    geojson = con.execute(
        "SELECT ST_AsGeoJSON(ST_GeomFromWKB(?))", [wkb]
    ).fetchone()[0]
    match = json.loads(geojson) == first["geometry"]
    print(f"geometry matches OGC render: {match}")
    if not match:
        sys.exit(1)
    print("flight OK")


if __name__ == "__main__":
    main()
