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
mypy internal
bandit -r internal -ll
pip-audit
pytest -v test/unit test/security test/chaos
```
