FROM rclone/rclone:1.74.3 AS rclone
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/cachebench ./cmd/cachebench
COPY internal ./internal
COPY .deps/duckdb /duckdb
ENV CGO_LDFLAGS=-L/duckdb
RUN --mount=type=cache,target=/root/.cache/go-build go build -tags=duckdb_use_lib,duckdb_arrow -o /cachebench ./cmd/cachebench
RUN --mount=type=cache,target=/root/.cache/go-build go test -c -tags=duckdb_use_lib,duckdb_arrow -o /cachebench.test ./cmd/cachebench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl python3 util-linux linux-perf binutils && rm -rf /var/lib/apt/lists/*
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY --from=build /cachebench /usr/local/bin/cachebench
COPY --from=build /cachebench.test /usr/local/bin/cachebench.test
COPY --from=build /duckdb/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=build /duckdb/duckdb /usr/local/bin/duckdb
ENV LD_LIBRARY_PATH=/usr/local/lib
COPY scripts/cachebench/node.py /usr/local/bin/bench-node
RUN duckdb :memory: -c 'INSTALL ducklake; INSTALL spatial; INSTALL httpfs;'
ENTRYPOINT ["cachebench"]
