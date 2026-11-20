# syntax=docker/dockerfile:1.7

FROM python:3.12-slim AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY cmd ./cmd
COPY internal ./internal
COPY api ./api

# hadolint ignore=DL3013
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir .

FROM python:3.12-slim AS runtime

WORKDIR /app

# hadolint ignore=DL3008
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends libjemalloc2; \
    rm -rf /var/lib/apt/lists/*; \
    jemalloc_so="$(find /usr/lib -name 'libjemalloc.so.2' -print -quit)"; \
    test -n "${jemalloc_so}"; \
    ln -sf "${jemalloc_so}" /usr/local/lib/libjemalloc.so; \
    groupadd --gid 10001 tabular; \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin tabular; \
    mkdir -p /var/cache/tabular-worker; \
    chown -R 10001:10001 /var/cache/tabular-worker /app

COPY --from=builder /opt/venv /opt/venv
COPY cmd ./cmd
COPY internal ./internal
COPY api ./api

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LD_PRELOAD=/usr/local/lib/libjemalloc.so \
    TABULAR_WORKER_CACHE_DIR=/var/cache/tabular-worker \
    TABULAR_WORKER_FLIGHT_HOST=0.0.0.0 \
    TABULAR_WORKER_FLIGHT_PORT=8815 \
    TABULAR_WORKER_HTTP_HOST=0.0.0.0 \
    TABULAR_WORKER_HTTP_PORT=8080

USER 10001:10001

EXPOSE 8815 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)"]

CMD ["python", "-m", "cmd.worker.main"]
