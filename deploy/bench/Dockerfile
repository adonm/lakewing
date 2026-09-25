# Benchmark image: pgbench (OLTP), duckdb (TPC-H + ClickBench over DuckLake),
# opensearch-benchmark (Quickwit's ES API) and the pgvs3 stress load generator.
# Built by `just kind-bench` on the kind node (image is host-built, then
# `kind load`-ed), so no registry round trip.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# PGDG, not Debian's default client: the charts run postgres:18 and the
# numbers should come from a matching client. pgbench ships in the server
# package, so that comes too — never started (create_main_cluster is off).
RUN . /etc/os-release \
 && install -d /usr/share/keyrings /etc/postgresql-common \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/keyrings/pgdg.asc \
 && echo "deb [signed-by=/usr/share/keyrings/pgdg.asc] http://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      postgresql-client-18 postgresql-18 \
 && rm -rf /var/lib/apt/lists/*

# duckdb matches the harness pins; opensearch-benchmark needs Python <3.14.
RUN pip install --no-cache-dir "duckdb==1.5.2" "opensearch-benchmark"

WORKDIR /bench
# The suite scripts wrap the repo's own harnesses (harness/) so one image
# serves all four without duplicating the DuckLake/PostgreSQL wiring.
COPY suites/ /bench/suites/
COPY harness/ /bench/harness/
# The load generator for the stress suite; staged here by the just recipe.
COPY pgvs3-bin /usr/local/bin/pgvs3
