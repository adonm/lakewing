FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/s3cache ./cmd/s3cache
COPY internal/s3cache ./internal/s3cache
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /s3cache ./cmd/s3cache

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /s3cache /usr/local/bin/s3cache
EXPOSE 8345 8080
ENTRYPOINT ["s3cache"]
