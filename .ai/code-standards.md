# Code Standards - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

## General Principles

- **Stateless Compute & Subprocess Isolation**: Never execute DuckDB queries inside the main Flight daemon process. Always dispatch to a `pebble.ProcessPool` (or custom `multiprocessing.Process` supervisor) with `max_tasks=100` so native C++ faults, memory spikes, or isolated single-worker `SIGKILL` cancellations never crash the Flight server or break sibling workers (avoiding stdlib `BrokenProcessPool`).
- **Fail-Fast on Corrupt or Unsafe Input**: Reject invalid tickets, malformed schemas, or non-whitelisted SQL ASTs within $\le 2\text{ ms}$ using explicit `ValidationError` exceptions mapped to canonical gRPC `INVALID_ARGUMENT` / `PERMISSION_DENIED` status codes.
- **Root-Cause Focus**: Never mask race conditions or slow queries with arbitrary `time.sleep` calls; use bounded locks, atomic file renames (`os.replace`) in the cache, and deterministic `5s` subprocess circuit breakers.

---

## Python 3.12+ & Code Quality Standards

- **Runtime Target**: Python 3.12+ (64-bit) with `jemalloc` preloaded (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`).
- **Linter, Formatter & Type Checker**:
  - Formatter: `ruff format --check .` (and `ruff format .`)
  - Linter: `ruff check .`
  - Type checker: `mypy cmd internal api` (strict typing on all public and internal boundaries)
  - Security scanner: `bandit -r cmd internal api -ll`
- **Dependencies**:
  - `duckdb` (sandboxed in-process analytical engine)
  - `pyarrow` (Arrow Flight server & zero-copy IPC streaming)
  - `sqlglot` (pure-Python deterministic AST parser & whitelist validator)
  - `aiobotocore` / `s3fs` (S3 range-read hydration)
  - `prometheus-client`, `opentelemetry-api` (observability)
- **Clean Architecture & Dependency Injection**:
  - Wire configuration (`settings.py`), cache manager, S3 streamer, AST validator, process pool supervisor, and telemetry registries explicitly in `cmd/worker/main.py`.
  - Avoid hidden mutable module-level globals; keep functions small, single-purpose, and well-typed with concise docstrings.

---

## DuckDB Sandbox & SQL AST Invariants

1. **Mandatory `sqlglot` AST Validation**:
   - Every incoming SQL string MUST pass `ast_validator.validate(sql)` before reaching the worker pool.
   - Only read-only `Select`, `With` (CTEs), and `Union` (`UNION`/`INTERSECT`/`EXCEPT`) constructs and pre-approved function namespaces are permitted.
   - Block DML/DDL (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`), administrative commands (`PRAGMA`, `SET`, `RESET`, `SUMMARIZE`, `DESCRIBE`, `SHOW`, `PIVOT`), disk/extension commands (`COPY ... TO`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`), and DuckDB file readers/writers (`read_csv`, `read_parquet`, `scan_parquet`, `write_csv`).
2. **Mandatory DuckDB Runtime Locks**:
   - Every child worker must open `duckdb.connect(":memory:")`, bind the single validated local `{hash}.parquet` view (`table_resolver.py`), and immediately enforce before running user SQL:
     - `SET enable_external_access = false`
     - `SET temp_directory = ''`
     - `SET memory_limit = '<floor(2.5GB / POOL_SIZE)>'` (aggregate DuckDB cap $\le 2.5\text{ GiB}$ per pod)
     - `SET threads = 2`
3. **Hard Execution Deadline**:
   - Every query runs under a `5s` timeout monitored by `circuit_breaker.py`; expired child processes are terminated individually via `SIGKILL` and replaced without disrupting sibling workers.

---

## Memory & Tier-1 Cache Invariants

1. **Zero Monolithic Heap Buffering & Single-Flight Hydration**:
   - Never load entire multi-hundred-MB S3 Parquet objects into Python `bytes` on the heap; use per-`table_hash` single-flight coalescing, stream range-reads directly to temporary files on the disk-backed NVMe `emptyDir` mount, and atomically rename via `os.replace` to `{hash}.parquet` tracked in an in-memory LRU index.
   - Never mount a `10 GiB` `tmpfs` (`medium: Memory`) inside a `6.0 GiB` pod (Linux charges `tmpfs` to container RAM); dev/fallback `tmpfs` must be $\le 1.0\text{ GiB}$.
2. **Memory Partitioning**:
   - Pod limit: `6.0 GiB` | Aggregate DuckDB limit (`POOL_SIZE * worker_limit`): `2.5 GiB` | Host Daemon & Arrow headroom: $\ge 2.0\text{ GiB}$ | OS page cache margin: `1.5 GiB`.
3. **Chunked Arrow Flight Egress (No Full Table Materialization)**:
   - Stream PyArrow `RecordBatch` chunks via `RecordBatchReader` / `FlightDataStream` over pipes or Unix sockets (never `/dev/shm`, and never `fetch_arrow_table()` full materialization) without converting rows to Python dicts or JSON.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs must be emitted as machine-parseable JSON lines. Never use `print()` in production code (`cmd/`, `internal/`, `api/`).
- **Mandatory Correlation IDs**: Include `trace_id`, `request_id`, `tenant_id`, `table_hash`, `cache_state`, and `duration_ms` where applicable.
- **Strict Redaction**:
  - Never log AWS credentials (`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), presigned S3 query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`), `Authorization` / `Bearer` tokens, or sensitive tenant PII/PHI.
  - Log only sanitized S3 bucket/key pairs (`s3_bucket`, `s3_key`) or content hashes (`table_hash`).
  - Sanitize internal filesystem paths and stack traces before returning error messages over gRPC/HTTP.

---

## Doc API Mandatory Maintenance

- **Continuous Synchronization**: Whenever any HTTP endpoint, Arrow Flight ticket/action schema, error mapping, Prometheus metric, or config contract is added or modified, [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated immediately in the same change step, no matter how small the modification is, if need be.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
