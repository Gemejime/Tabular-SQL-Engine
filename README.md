# `rag-tabular-analytics-worker`

Stateless, horizontally autoscaling Python 3.12+ Apache Arrow Flight microservice executing sandboxed DuckDB analytical queries over content-addressed Parquet artifacts.

See [`architecture.md`](architecture.md) for the full 5-layer backend architecture, memory partitioning blueprint, and Architecture Decision Records (`ADR-001`–`ADR-005`).

---

## Core Capabilities

- **Apache Arrow Flight IPC & HTTP Ops Server (`internal/flight/`, `api/`)**:
  - `TabularFlightServer` (`pyarrow.flight.FlightServerBase`, port `8815`) handling `DoGet(Ticket)` analytical queries and `DoAction(Action)` (`cache_warm`, `health_check`), streaming zero-copy Arrow `RecordBatch` frames over HTTP/2 gRPC.
  - `OpsHTTPServer` (port `8080`) exposing `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, and `/docs` (Swagger UI), with graceful `SIGTERM`/`SIGINT` draining.
- **Deterministic SQL AST Security Validator (`internal/validator/`)**:
  - `sqlglot` AST whitelisting (`<= 2 ms` warm steady-state) permitting only read-only `SELECT`, `WITH` (CTEs), `UNION` / `INTERSECT` / `EXCEPT`, and `Subquery` constructs.
  - Strict allowlist for SQL functions, join count limits (`MAX_JOINS=5`), and query length limits (`MAX_SQL_LENGTH=8192`).
  - Blocks DML/DDL, `PRAGMA`, `SET`, `RESET`, `SUMMARIZE`, `DESCRIBE`, `SHOW`, `PIVOT`, `COPY`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`, and DuckDB filesystem/network functions.
- **Tier-1 Disk-Backed NVMe Cache & S3 Range-Read Hydration (`internal/storage/`)**:
  - Content-addressed local cache (`<CACHE_DIR>/{s3_bucket}/{tenant_id}/{table_hash}.parquet`, `10 GiB` ceiling) with single-flight download coalescing, atomic `os.replace` writes, lock-free concurrent readers, and in-memory LRU eviction.
  - Async `aiobotocore` S3 range-read streaming (`8 MiB` chunks) over internal VPC endpoints without monolithic Python heap buffering.
  - `TableResolver` enforces `S3_BUCKET_ALLOWLIST` and canonical `s3_key == artifacts/{tenant_id}/{table_hash}.parquet` tenant isolation.
- **Ephemeral DuckDB Sandbox Runtime (`internal/sandbox/`)**:
  - `apply_sandbox_lock` (`internal/sandbox/security_profile.py`) opens `duckdb.connect(":memory:")`, configures `temp_directory = ''`, per-worker `memory_limit` (`floor(2.5 GiB / POOL_SIZE)` in `MiB`, e.g., `640MiB` at `POOL_SIZE=4`), and `threads = 2`, materializes only the single validated `{hash}.parquet` artifact as a temp table (`CREATE TEMP TABLE "<table_alias>" AS SELECT * FROM read_parquet(...)`), and immediately locks `enable_external_access = false` and `lock_configuration = true`.
  - Streams chunked PyArrow `RecordBatch` frames via `RecordBatchReader` without `fetch_arrow_table()` full materialization.
- **Disposable Subprocess Pool & Circuit Breaker (`internal/pool/`)**:
  - `pebble.ProcessPool` supervisor (`max_workers=POOL_SIZE`, `max_tasks=100`) streaming Arrow IPC `RecordBatch` chunks over an OS pipe / `multiprocessing.connection`.
  - Per-worker `5s` hard execution deadline with isolated child termination (`SIGKILL` on POSIX / `TerminateProcess` on Windows) and automatic worker replacement (`ADR-001`).
- **Observability, Secured Logging & KEDA Autoscaling (`internal/telemetry/`, `deployments/`)**:
  - Structured JSON logs with redaction of authorization headers, bearer tokens, AWS credentials, presigned S3 query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`), and sensitive literals.
  - Prometheus metrics (`tabular_worker_active_queries`, `tabular_worker_query_duration_seconds`, `tabular_worker_cache_hit_ratio`, `tabular_worker_subprocess_recycles_total`, `tabular_worker_flight_stream_bytes_total`, `tabular_worker_ast_rejections_total`), OpenTelemetry tracing, and KEDA `ScaledObject` (`4..24` replicas).

---

## Local Development Setup

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
.venv\Scripts\Activate.ps1

pip install -e ".[dev,test]"
cp .env.example .env
```

Before starting the daemon locally, edit `.env`:

- **S3 bucket allowlist & access**: Add your bucket(s) to `TABULAR_WORKER_S3_BUCKET_ALLOWLIST` (e.g., `["my-tabular-bucket"]`) and configure S3 access (`TABULAR_WORKER_S3_ENDPOINT_URL`, `TABULAR_WORKER_S3_REGION`, and AWS credentials). `TableResolver` fails closed and rejects all buckets when `TABULAR_WORKER_S3_BUCKET_ALLOWLIST` is empty.
- **Writable cache directory**: Set `TABULAR_WORKER_CACHE_DIR` to a writable local path (e.g., `./.cache/tabular-worker`), or create `/var/cache/tabular-worker` and grant write permissions to the user running the daemon before startup.

Start the worker daemon locally:

```bash
python -m cmd.worker.main
```

The Arrow Flight gRPC server listens on `0.0.0.0:8815` and the HTTP Ops server listens on `0.0.0.0:8080` (`/docs` for Swagger UI, `/metrics` for Prometheus).

---

## Configuration

All runtime settings are 12-factor environment variables prefixed with `TABULAR_WORKER_` (defined in [`internal/config/settings.py`](internal/config/settings.py) and documented in [`.env.example`](.env.example)):

| Variable | Default | Description |
| :--- | :--- | :--- |
| `TABULAR_WORKER_POOL_SIZE` | `4` | Number of disposable DuckDB worker subprocesses |
| `TABULAR_WORKER_MAX_TASKS_PER_CHILD` | `100` | Tasks before a worker subprocess is recycled |
| `TABULAR_WORKER_QUERY_TIMEOUT_SECONDS` | `5.0` | Hard per-query subprocess circuit breaker timeout |
| `TABULAR_WORKER_DUCKDB_TOTAL_MEMORY_BYTES` | `2684354560` | Aggregate DuckDB memory ceiling (`2.5 GiB` across pool) |
| `TABULAR_WORKER_CACHE_DIR` | `/var/cache/tabular-worker` | Tier-1 NVMe Parquet cache directory |
| `TABULAR_WORKER_CACHE_MAX_BYTES` | `10737418240` | Tier-1 cache LRU capacity (`10 GiB`) |
| `TABULAR_WORKER_S3_BUCKET_ALLOWLIST` | `[]` | Allowed S3 buckets (JSON array or comma-separated) |
| `TABULAR_WORKER_MAX_CONCURRENT_QUERIES` | `4` | Bounded in-flight query semaphore (`RESOURCE_EXHAUSTED` above cap) |
| `TABULAR_WORKER_OTEL_ENABLED` | `false` | Enable OpenTelemetry OTLP gRPC span export |

> [!NOTE]
> `.env` is for local development only. Production Helm defaults in [`deployments/helm/values.yaml`](deployments/helm/values.yaml) are synced from `.env.prod` via `python scripts/sync_env_to_deployments.py --sync`. See [`deployments/SECRET_MANAGEMENT.md`](deployments/SECRET_MANAGEMENT.md) for secret injection guidelines.

---

## Verification & Quality Gates

Run individual checks or the full `Makefile` gate (`make check`):

```bash
ruff check .
ruff format --check .
mypy cmd internal api
bandit -r cmd internal api -ll
pip-audit
pytest -v test/unit test/security test/chaos test/integration
```

### Extended Soak & SLO Endurance Tests

The default `test/chaos` suite runs a fast 500-stream bounded concurrency soak and burst backpressure test. To run the opt-in 8-hour `jemalloc` and `P95 <= 45ms` endurance suite:

```bash
RUN_SOAK_TESTS=1 pytest -v -m soak test/chaos
```

---

## Container & Kubernetes Deployment

### Multi-Stage Production Docker Image (`jemalloc` Preloaded)

```bash
docker build -f deployments/docker/Dockerfile -t rag-tabular-analytics-worker:0.1.0 .
docker run --rm -p 8815:8815 -p 8080:8080 --read-only \
  -e TABULAR_WORKER_CACHE_MAX_BYTES=805306368 \
  --tmpfs /var/cache/tabular-worker:rw,noexec,nosuid,uid=10001,gid=10001,size=1g \
  --tmpfs /tmp:rw,size=256m \
  rag-tabular-analytics-worker:0.1.0
```

### Helm & ArgoCD Progressive Canary Rollout

- **Staging**: [`deployments/helm/values-staging.yaml`](deployments/helm/values-staging.yaml) and [`deployments/argocd/application-staging.yaml`](deployments/argocd/application-staging.yaml)
- **Production**: [`deployments/helm/values-prod.yaml`](deployments/helm/values-prod.yaml), [`deployments/argocd/application-prod.yaml`](deployments/argocd/application-prod.yaml), and [`deployments/argocd/rollout-canary.yaml`](deployments/argocd/rollout-canary.yaml) (`10% -> 25% -> 50% -> 100%` gated by `AnalysisTemplate` over Prometheus SLIs)

```bash
helm lint deployments/helm
helm template rag-tabular-analytics-worker deployments/helm -f deployments/helm/values-prod.yaml > /dev/null
```

---

## Documentation Index

- [`architecture.md`](architecture.md) — 5-layer backend architecture, request flow, memory partitioning, ADRs, and repository layout
- [`.ai/doc-api.md`](.ai/doc-api.md) — Authoritative HTTP Ops, Arrow Flight IPC (`DoGet`/`DoAction`), and Prometheus metric contracts
- [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml) — OpenAPI 3.1 specification for the HTTP Ops server
- [`.ai/security.md`](.ai/security.md) — Threat model, DuckDB sandbox locks, and secured logging invariants
- [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — Implementation phases, ADRs, and operational runbooks
