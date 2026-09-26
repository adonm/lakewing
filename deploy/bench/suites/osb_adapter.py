#!/usr/bin/env python3
"""Loopback-only OSB compatibility for Quickwit's incomplete ES API."""

import http.client
import os
import socket
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

PREFIX = "/api/v1/_elastic"
SEARCH = urlsplit(os.environ.get("QUICKWIT_URL", "http://quickwit:7280"))
INGEST = urlsplit(os.environ.get("QUICKWIT_INGEST_URL", "http://quickwit-core:7280"))
MAX_BODY = 10 * 1024 * 1024


class Adapter(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        pass

    def log_error(self, format, *args):
        print(format % args, file=sys.stderr, flush=True)

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def reply(self, status, data, headers=()):
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                self.send_header(name, value)
        if not any(name.lower() == "content-type" for name, _ in headers):
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def handle_request(self):
        path = urlsplit(self.path)
        if path.scheme or not path.path.startswith(PREFIX + "/"):
            self.reply(400, b'{"error":"invalid path"}')
            return
        if self.command == "GET" and path.path == PREFIX + "/_nodes/_all":
            self.reply(200, b'{"nodes":{"quickwit":{"name":"quickwit"}}}')
            return
        if self.command == "GET" and path.path == PREFIX + "/_nodes/stats/_all":
            self.reply(200, b'{"nodes":{}}')
            return
        if self.headers.get("Transfer-Encoding"):
            self.reply(400, b'{"error":"chunked requests are unsupported"}')
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.reply(400, b'{"error":"invalid content length"}')
            return
        if length < 0 or length > MAX_BODY:
            self.reply(413, b'{"error":"request too large"}')
            return
        body = self.rfile.read(length) if length else None
        is_bulk = path.path.endswith("/_bulk")
        target = INGEST if is_bulk else SEARCH
        upstream = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=120)
        try:
            upstream.request(
                self.command, self.path, body=body,
                headers={name: value for name, value in self.headers.items()
                         if name.lower() not in ("host", "connection", "transfer-encoding")},
            )
            response = upstream.getresponse()
            self.reply(response.status, response.read(), response.getheaders())
        except (OSError, http.client.HTTPException) as exc:
            self.log_error("upstream error: %s", exc)
            self.reply(502, b'{"error":"upstream request failed"}')
        finally:
            upstream.close()

    do_GET = handle_request
    do_POST = handle_request
    do_PUT = handle_request
    do_DELETE = handle_request
    do_HEAD = handle_request


if __name__ == "__main__":
    for target in (SEARCH, INGEST):
        if (target.scheme != "http" or not target.hostname or target.username
                or target.path not in ("", "/")):
            raise SystemExit("Quickwit adapter requires internal HTTP service URLs")
    ThreadingHTTPServer(("127.0.0.1", 19200), Adapter).serve_forever()
