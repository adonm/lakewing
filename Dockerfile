# Static musl binary on scratch: no shell, no libc, no CVE surface. TLS is
# rustls + ring (bundled assembly), so nothing links against OpenSSL; the
# PostgreSQL connection does not verify certificates (sslmode=require), so the
# CA bundle is included only as a convenience for anything else in the image.
#
# Build both architectures with `just image` (docker buildx). The builder
# image's Rust version should match or exceed mise.toml's; CI builds and tests
# natively first, so drift shows up as a failed check before the image builds.
# The Rust version here must be >= mise.toml's; CI runs `just ci` first, so
# drift shows up as a failed check, not a broken image.
FROM rust:alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked --manifest-path crates/pgvs3/Cargo.toml \
 && cp target/release/pgvs3 /pgvs3

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /pgvs3 /usr/local/bin/pgvs3
USER 65532:65532
EXPOSE 8014
ENTRYPOINT ["/usr/local/bin/pgvs3"]
CMD ["serve", "--addr", "0.0.0.0:8014"]
