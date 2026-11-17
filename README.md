# `rag-tabular-analytics-worker`

Stateless, horizontally autoscaling Python 3.12+ Apache Arrow Flight microservice executing sandboxed DuckDB analytical queries over content-addressed Parquet artifacts.

## Phase 1 Capabilities (Implemented)

- **Deterministic SQL AST Security Validator (`internal/validator/`)**:
  - `sqlglot` AST whitelisting (`<= 2 ms` warm steady-state) permitting only read-only `SELECT`, `WITH` (CTEs), `UNION` / `INTERSECT` / `EXCEPT`, and `Subquery` constructs.
  - Strict allowlist for SQL functions (including `exp.Anonymous` filtering), column/table/CTE alias validation, join count limits (`MAX_JOINS`), and query length limits (`MAX_SQL_LENGTH`).
  - Blocks DML/DDL, `PRAGMA`, `SET`, `RESET`, `SUMMARIZE`, `DESCRIBE`, `SHOW`, `PIVOT`, `COPY`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`, and DuckDB filesystem/network functions.
- **Ephemeral DuckDB Sandbox Runtime (`internal/sandbox/`)**:
  - Materializes only the single validated `{hash}.parquet` artifact (`CREATE TEMP TABLE <alias> AS SELECT * FROM read_parquet(...)`) inside `duckdb.connect(":memory:")`.
  - Immediately locks `enable_external_access = false`, `lock_configuration = true`, `temp_directory = ''`, `threads = 2`, and per-worker `memory_limit` (`floor(2.5 GiB / POOL_SIZE)` in `MiB`, e.g., `640MiB` at `POOL_SIZE=4`).
  - Streams chunked PyArrow `RecordBatch` frames via `RecordBatchReader` without `fetch_arrow_table()` full materialization.
- **Disposable Subprocess Pool & Circuit Breaker (`internal/pool/`)**:
  - `pebble.ProcessPool` supervisor (`max_workers=POOL_SIZE`, `max_tasks=100`) streaming Arrow IPC `RecordBatch` chunks over an OS pipe / `multiprocessing.connection` without full-result `bytes` buffering.
  - Per-worker `5s` hard execution deadline with isolated child termination (`SIGKILL` on POSIX / `TerminateProcess` on Windows) and automatic worker replacement (`ADR-001`).

## Local Development Setup

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -e ".[dev,test]"
```

## Verification Commands

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

## Container & Kubernetes Deployment

### Multi-Stage Production Docker Image (`jemalloc` Preloaded)

```bash
docker build -f deployments/docker/Dockerfile -t rag-tabular-analytics-worker:0.1.0 .
docker run --rm -p 8815:8815 -p 8080:8080 --read-only --tmpfs /tmp:rw,size=256m rag-tabular-analytics-worker:0.1.0
```

### Helm & ArgoCD Progressive Canary Rollout

- **Staging**: `deployments/helm/values-staging.yaml` and `deployments/argocd/application-staging.yaml`
- **Production**: `deployments/helm/values-prod.yaml`, `deployments/argocd/application-prod.yaml`, and `deployments/argocd/rollout-canary.yaml` (`10% -> 25% -> 50% -> 100%` gated by `AnalysisTemplate` over Prometheus SLIs)

```bash
helm lint deployments/helm
helm template rag-tabular-analytics-worker deployments/helm -f deployments/helm/values-prod.yaml > /dev/null
```
