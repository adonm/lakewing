# lakewing serve image. Links the pinned prebuilt libduckdb from
# .deps/duckdb (populated by `just setup-duckdb`); the preview binding's
# bundled engine is 1.5.x and cannot open 2.0 catalogs, hence
# -tags=duckdb_use_lib. DuckDB extensions (spatial/ducklake) install at
# runtime, so the pod needs egress unless they are baked.
ARG GO=1.25-bookworm
FROM golang:${GO} AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY .deps/duckdb /duckdb-lib
ENV CGO_LDFLAGS=-L/duckdb-lib
ENV GOFLAGS=-tags=duckdb_use_lib,duckdb_arrow
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /lakewing ./cmd/lakewing

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /lakewing /usr/local/bin/lakewing
COPY --from=build /duckdb-lib/libduckdb.so /usr/local/lib/libduckdb.so
ENV LD_LIBRARY_PATH=/usr/local/lib
EXPOSE 3000 50051
ENTRYPOINT ["lakewing"]
