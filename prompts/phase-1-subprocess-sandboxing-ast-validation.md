# Implementation Plan: Phase 1 — Subprocess Sandboxing & AST Validation

## 1. Goal & Scope
Implement **Phase 1: Subprocess Sandboxing & AST Validation** for `rag-tabular-analytics-worker` (`GEMINI.MD`, `.ai/progress-tracker.md`, `tasks/Tabular Analytics Worker Implementation Plan.pdf`):
- Deterministic `sqlglot` AST security validator ($\le 2\text{ ms}$ warm steady-state) enforcing strict AST node and pre-approved function allowlisting (`SELECT` / `WITH` / `UNION` / `INTERSECT` / `EXCEPT`), column alias checks, join limits, and blocking DML/DDL, `PRAGMA`/`SET`/`RESET`/`SUMMARIZE`/`DESCRIBE`/`SHOW`/`PIVOT`, `COPY`/`ATTACH`/`DETACH`/`LOAD`/`INSTALL`, and DuckDB filesystem/network functions.
- Ephemeral DuckDB `:memory:` sandbox (`duckdb >= 1.2.0`) materializing only the single validated `{hash}.parquet` artifact (`CREATE TEMP TABLE <alias> AS SELECT * FROM read_parquet(...)`, since lazy `TEMP VIEW`s fail at query time once external access is disabled) before locking `enable_external_access=false`, `lock_configuration=true`, `temp_directory=''`, `threads=2`, and `memory_limit=floor(2.5GiB / POOL_SIZE)` (e.g., `640MiB` at `POOL_SIZE=4`).
- Disposable worker subprocess pool (`pebble.ProcessPool` with `max_tasks=100`) streaming chunked Arrow IPC `RecordBatch` frames over an OS pipe / `multiprocessing.connection` or Unix socket (never pickling full-result `bytes` or calling `fetch_arrow_table()`), paired with a `5s` per-worker circuit breaker (`SIGKILL` on POSIX / `TerminateProcess` on Windows + respawn) keeping sibling workers serviceable (`ADR-001`).
- Project scaffolding (`pyproject.toml`, `Makefile`, `internal/config/settings.py`, structured JSON logging) and Phase 1 test suites (`test/unit/`, `test/security/`, `test/chaos/`, `test/fixtures/`).

---

## 2. Files to Create / Modify

### Project Scaffolding & Config
- `pyproject.toml` — Python `>=3.12` package metadata, runtime deps (`duckdb>=1.2.0`, `pyarrow`, `sqlglot`, `pebble`, `pydantic`, `pydantic-settings`), dev/test deps (`ruff`, `mypy`, `bandit`, `pip-audit`, `pytest`, `pytest-asyncio`, `pytest-cov`), and `mypy` overrides for untyped third-party packages (e.g., `pebble`) if needed.
- `Makefile` — Local targets for `format`, `lint`, `typecheck`, `security`, `test-unit`, `test-security`, `test-chaos`, `test`, and `check`.
- `README.md` — Concise service overview, local dev setup, and verification commands.
- `internal/__init__.py`, `internal/config/__init__.py`, `internal/config/settings.py` — Typed `Settings` (`POOL_SIZE=4`, `MAX_TASKS_PER_CHILD=100`, `DUCKDB_TOTAL_MEMORY_BYTES=2.5 GiB`, per-worker `memory_limit` formatted explicitly in `MiB`, `QUERY_TIMEOUT_SECONDS=5.0`, `MAX_SQL_LENGTH`, `MAX_JOINS`, `DUCKDB_THREADS=2`).
- `internal/telemetry/__init__.py`, `internal/telemetry/logging.py` — Structured JSON logger with strict redaction of credentials, bearer tokens, presigned S3 params, and raw SQL literals (no `print()`, and keep redaction key literals outside `logger.<level>(...)` calls to satisfy CI static regex audits).

### SQL AST Security Validator (`internal/validator/`)
- `internal/validator/__init__.py`
- `internal/validator/exceptions.py` — `ValidationError` hierarchy (`ASTSyntaxError`, `DisallowedStatementError`, `DisallowedFunctionError`, `DisallowedTableReferenceError`, `DisallowedAliasError`, `JoinLimitExceededError`, `QueryLengthExceededError`) with `rule` codes for metrics/logging.
- `internal/validator/rules.py` — Strict read-only AST node allowlist (`Select`, `With`, `Union`, `Intersect`, `Except`, `Subquery`), explicit rejection of `exp.Command` and banned statements (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `PRAGMA`, `SET`, `RESET`, `SUMMARIZE`, `DESCRIBE`, `SHOW`, `PIVOT`, `COPY`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`), banned DuckDB I/O & system functions (`read_csv`, `read_parquet`, `scan_parquet`, `write_csv`, `glob`, `httpfs_*`, `duckdb_*`, `getenv`, etc.), pre-approved function allowlist (including strict filtering of `exp.Anonymous`), column alias checks, join count limit (`MAX_JOINS`), and table/CTE alias validation.
- `internal/validator/ast_validator.py` — `ASTValidator.validate(sql, allowed_table_alias)` using `sqlglot.parse(..., read="duckdb")`, walking the full AST against the allowlist, rejecting multi-statement queries, and enforcing $\le 2\text{ ms}$ warm steady-state validation.

### DuckDB Sandbox Runtime (`internal/sandbox/`)
- `internal/sandbox/__init__.py`
- `internal/sandbox/security_profile.py` — Binds the single validated `{hash}.parquet` artifact via `CREATE TEMP TABLE <alias> AS SELECT * FROM read_parquet(...)` (avoiding lazy `TEMP VIEW` failures after lockdown and never setting `enable_external_access=true`), then immediately locks `enable_external_access = false`, `lock_configuration = true`, `temp_directory = ''`, `memory_limit` (`MiB`), and `threads = 2`.
- `internal/sandbox/duckdb_runtime.py` — Ephemeral `duckdb.connect(":memory:")` lifecycle and chunked Arrow `RecordBatchReader` IPC streaming (no `fetch_arrow_table()` full materialization).

### Disposable Subprocess Pool & Circuit Breaker (`internal/pool/`)
- `internal/pool/__init__.py`
- `internal/pool/worker_task.py` — Top-level picklable (`spawn`-safe) child subprocess entrypoint running `duckdb_runtime` + `security_profile` and streaming Arrow IPC `RecordBatch` chunks over a dedicated OS pipe / `multiprocessing.connection` (or Unix socket) without buffering the full result set on the heap.
- `internal/pool/circuit_breaker.py` — `5s` execution deadline enforcement with cross-platform isolated child termination (`SIGKILL` on POSIX / `TerminateProcess` via `pebble` on Windows) and recycle reason tracking (`TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`).
- `internal/pool/process_manager.py` — `pebble.ProcessPool` supervisor (`max_workers=POOL_SIZE`, `max_tasks=100`), pre-allocated worker pool, and automatic worker replacement without `BrokenProcessPool`.

### Tests & Fixtures (`test/`)
- `test/fixtures/sample_data.py` (or generated `.parquet` fixture) — Normalized test Parquet dataset.
- `test/unit/test_settings.py` — Memory budget (`floor(2.5GiB / POOL_SIZE)` in `MiB`) and config validation tests.
- `test/unit/test_ast_validator.py` — Valid `SELECT`/`WITH`/`UNION`, column/table/CTE alias checks, join limit checks, and $\le 2\text{ ms}$ warm steady-state latency tests (post parser warmup).
- `test/unit/test_duckdb_sandbox.py` — Ephemeral `:memory:` execution, `TEMP TABLE` binding + `enable_external_access=false` verification, and chunked Arrow IPC batch streaming tests.
- `test/security/test_sql_injection_fuzz.py` — DML/DDL, `PRAGMA`, `SET`, `RESET`, `SUMMARIZE`, `DESCRIBE`, `SHOW`, `PIVOT`, `COPY`, `ATTACH`, `LOAD`/`INSTALL`, DuckDB file/system functions (`exp.Anonymous` bypass attempts), multi-statement injection, and sandbox escape tests (`enable_external_access=false`).
- `test/chaos/test_subprocess_isolation.py` — Cross-platform forced child termination/panic isolation (without hardcoding POSIX-only `signal.SIGKILL` on Windows), `5s` timeout circuit breaker (cross-join), and `max_tasks=100` recycling tests.

### Documentation Sync
- `.ai/progress-tracker.md` — Update Phase 1 status and session notes upon completion.
- `.ai/doc-api.md` — Verify all AST rules, error mappings, and config invariants remain synchronized.

---

## 3. Verification Plan
1. `ruff format --check .` and `ruff check .`
2. `mypy cmd internal api` (targeting `internal` in Phase 1)
3. `bandit -r internal -ll` and `pip-audit`
4. Secured logging & DuckDB sandbox audit (no `print()`, no unredacted secrets in logger calls, no `enable_external_access=true`)
5. `pytest -v test/unit test/security test/chaos`
6. At least 2 conventional git commits (`chore:`, `feat:`, `tests:`, `docs:`) as Phase 1 is built.

---

## 4. Open Questions for Human Confirmation
1. **Function Allowlist & Join Limit Defaults**: Confirm strict `exp.Anonymous` allowlisting (standard analytical, math, string, date/time, window, cast/conditional functions) and a default `MAX_JOINS` ceiling (e.g., `5`) in `internal/validator/rules.py`.
2. **Phase 1 Cross-Process IPC Transport**: Confirm using a dedicated OS pipe / `multiprocessing.connection` (or Unix socket on POSIX) to stream Arrow IPC `RecordBatch` chunks from `worker_task.py` to the parent process without full-result `bytes` buffering.
3. **Proceed**: Please confirm if we should proceed with implementing Phase 1 according to this updated plan.
