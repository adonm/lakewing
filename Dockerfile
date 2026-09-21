FROM rust:1.98-slim-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler build-essential && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p lakewing && cp target/release/lakewing /usr/local/bin/lakewing

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/lakewing /usr/local/bin/lakewing
ENV TMPDIR=/tmp
ENTRYPOINT ["lakewing"]
