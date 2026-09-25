#!/usr/bin/env python3
"""Turn an RDS-managed master secret into a Kubernetes Secret (stdout only).

The input comes from Secrets Manager over AWS CLI; no password goes into Git,
CloudFormation parameters, Helm history or shell command arguments.
"""
import json
import sys
from urllib.parse import quote


def main() -> None:
    host = sys.argv[1]
    source = json.load(sys.stdin)
    user = source["username"]
    password = source["password"]
    if not host or not user or not password:
        raise SystemExit("incomplete Aurora endpoint or master secret")
    base = (f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
            f"@{host}:5432")
    print(json.dumps({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "pgvs3-aurora", "namespace": "pgvs3"},
        "type": "Opaque",
        "stringData": {"host": host, "user": user, "password": password,
                       "url": f"{base}/pgvs3?sslmode=require",
                       "metastoreUrl": f"{base}/quickwit_metastore?sslmode=require",
                       "sslmode": "require"},
    }))


if __name__ == "__main__":
    main()
