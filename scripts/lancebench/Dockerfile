FROM python:3.12.14-slim-bookworm
WORKDIR /bench
COPY scripts/lancebench/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "import duckdb; c=duckdb.connect(); [c.execute('INSTALL '+x) for x in ('spatial','ducklake','lance','httpfs')]"
ENV LANCE_CPU_THREADS=4 TOKIO_WORKER_THREADS=4 RAYON_NUM_THREADS=4
COPY scripts/lancebench/bench.py ./bench.py
ENTRYPOINT ["python", "-u", "/bench/bench.py"]
