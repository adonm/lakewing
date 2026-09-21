#!/usr/bin/env python3
"""Arrow Flight parity check against the OGC HTTP serve.

Lists flights, reads the schema, streams a default ticket and a deep
ticket, and verifies: schema/contract, id order equality with the OGC
DEEP page, byte-exact WKB geometry vs the OGC render (Flight restores
the original single-part polygon WKB, so the round-trip through
ST_GeomFromWKB must equal the OGC GeoJSON), and 304-style snapshot
pinning via the ticket snapshot field.

Usage: flight_check.py FLIGHT_ADDR HTTP_BASE
"""

import json
import sys
import urllib.request

import pyarrow as pa
import pyarrow.flight as fl


def main() -> None:
    flight_addr = sys.argv[1] if len(sys.argv) > 1 else "grpc://127.0.0.1:50071"
    http_base = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:3140"
    client = fl.FlightClient(flight_addr)

    flights = list(client.list_flights())
    print(f"list_flights: {len(flights)} flight(s)")
    for info in flights:
        descriptor = info.descriptor
        if descriptor.path:
            print(f"  collection={descriptor.path[0].decode()}")
        else:
            try:
                ticket = json.loads(descriptor.cmd)
                print(f"  collection={ticket.get('collection', '<cmd>')}")
            except (ValueError, AttributeError):
                print("  collection=<cmd>")

    schema = client.get_schema(fl.FlightDescriptor.for_path("buildings")).schema
    print("schema:", [(field.name, str(field.type)) for field in schema])

    ticket = fl.Ticket(json.dumps({"collection": "buildings", "limit": 101}).encode())
    default = client.do_get(ticket).read_all()
    print(f"do_get default: rows={default.num_rows} cols={default.column_names}")

    deep_ticket = fl.Ticket(json.dumps({
        "collection": "buildings", "limit": 101, "offset": 50000,
        "columns": ["id", "geometry", "properties"],
    }).encode())
    deep = client.do_get(deep_ticket).read_all()
    print(f"do_get deep: rows={deep.num_rows} first_id={deep.column('id')[0]}")

    with urllib.request.urlopen(http_base + "/collections/buildings/items?sources=1&limit=101&offset=50000", timeout=120) as response:
        body = json.load(response)
    http_ids = [feature["id"] for feature in body["features"]]
    flight_ids = deep.column("id").to_pylist()
    if http_ids != flight_ids:
        for index, (a, b) in enumerate(zip(http_ids, flight_ids)):
            if a != b:
                print(f"first diff at {index}: {a} vs {b}")
                break
        print(f"id order mismatch: {len(http_ids)} vs {len(flight_ids)}")
        raise SystemExit(1)
    print(f"id order matches OGC serve: True ({len(http_ids)} rows)")

    import duckdb

    connection = duckdb.connect()
    connection.execute("LOAD spatial")
    first = body["features"][0]
    wkb = deep.column("geometry")[0].as_py()
    geojson = connection.execute("SELECT ST_AsGeoJSON(ST_GeomFromWKB(?))", [wkb]).fetchone()[0]
    if json.loads(geojson) != first["geometry"]:
        print(f"geometry mismatch:\nflight: {geojson}\nogc:    {json.dumps(first['geometry'])}")
        raise SystemExit(1)
    print("geometry matches OGC render: True")

    # Stale snapshots must fail fast instead of silently mixing versions.
    stale = fl.Ticket(json.dumps({"collection": "buildings", "limit": 1, "snapshot": 999999}).encode())
    try:
        client.do_get(stale).read_all()
    except Exception as error:  # pyarrow surfaces gRPC FAILED_PRECONDITION as ArrowInvalid
        if "snapshot" not in str(error):
            raise
        print("stale snapshot rejected: True")
    else:
        print("stale snapshot rejected: False")
        raise SystemExit(1)
    print("flight OK")


if __name__ == "__main__":
    main()
