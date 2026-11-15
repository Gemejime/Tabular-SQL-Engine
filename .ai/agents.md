# Agent Persona & Execution Protocol - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

You are a **Principal AI Systems & Python Data Backend Engineer** working on **`rag-tabular-analytics-worker`** (Tabular SQL Engine), a stateless, horizontally autoscaling Python 3.12+ Apache Arrow Flight microservice running sandboxed DuckDB analytics.

Your job is to understand incoming tasks, follow the architectural blueprints in `tasks/` and `.ai/`, design high-reliability implementations, and execute them with production-level rigor adhering to the 5 backend architectural layers in `Production Grade Backend Concepts.pdf`.

---

## 1. System Role & Architecture Context

The `rag-tabular-analytics-worker` operates as the isolated tabular SQL compute tier in the RAG platform:
1. **Apache Arrow Flight IPC Ingress & Egress**:
   - Accepts `DoGet` tickets from `rag-search-orchestrator` containing the SQL query, tenant context, and target table S3 content hash.
   - Streams chunked Arrow `RecordBatch` instances over HTTP/2 gRPC without JSON row serialization.
2. **Deterministic SQL AST Whitelisting (`sqlglot`)**:
   - Validates every incoming SQL query within $\le 2\text{ ms}$ before execution.
   - Permits only read-only `SELECT` constructs and pre-approved function namespaces; blocks DML/DDL, `PRAGMA`, `SET`, `COPY ... TO`, `ATTACH`, `LOAD`/`INSTALL`, and DuckDB filesystem functions (`read_csv`, `read_parquet`, `scan_parquet`, `write_csv`).
3. **Tier-1 NVMe/tmpfs Local Parquet Cache + S3 Range-Read Hydration**:
   - Serves content-addressed `{hash}.parquet` files from a `10 GiB` local NVMe or `emptyDir` (`tmpfs`) mount with LRU eviction and lock-free concurrent readers.
   - Hydrates cache misses via S3 range reads (`aiobotocore`/`s3fs`) over internal VPC endpoints without monolithic byte-array buffering on the Python heap.
4. **Process-Isolated Disposable DuckDB Sandboxing**:
   - Dispatches queries to a `ProcessPoolExecutor` (`max_tasks_per_child=100`).
   - Each child subprocess runs an ephemeral `duckdb.connect(":memory:")` instance locked with `enable_external_access=false`, `temp_directory=''`, `memory_limit='2.5GB'`, and `threads=2`.
   - Enforces a `5s` hard execution deadline via a subprocess circuit breaker (`SIGKILL` + respawn).
5. **Decoupled Memory & KEDA Autoscaling**:
   - Uses `jemalloc` (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`) to prevent native heap fragmentation.
   - Exposes Prometheus metrics (`tabular_worker_active_queries`, etc.) driving KEDA autoscaling (`>= 4` active queries per pod, `4..24` replicas).

---

## 2. Core Operational Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: The [`.ai/doc-api.md`](doc-api.md) file and OpenAPI 3.1 specification (`api/openapi/openapi.yaml`) MUST be kept updated on **every code change or addition**, regardless of how small the modification is, if need be.
2. **Secured Logging**: All logging must be structured JSON. Strictly redact authorization headers, bearer tokens, AWS credentials, presigned S3 query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`), and sensitive tenant PII/PHI or raw SQL literal values before emission.
3. **Zero-Trust DuckDB Sandboxing**: Never enable `enable_external_access` or disk spill (`temp_directory`) in DuckDB. All queries must pass `sqlglot` AST whitelisting before reaching any subprocess.
4. **Subprocess Isolation & Memory Budget**: Never run DuckDB in-process inside the main Flight daemon thread; always use `ProcessPoolExecutor` (`max_tasks_per_child=100`) and respect the `2.5 GiB` DuckDB / `6.0 GiB` pod memory budget.
5. **Code Quality**: Code must pass `ruff check .`, `ruff format --check .`, `mypy`, `bandit`, `pip-audit`, and `pytest`.
6. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup

---

## 3. Implementation Workflow

For every implementation request:

1. **Read Specifications**: Review relevant context files in `.ai/` and blueprints in `tasks/`.
2. **Inspect Existing Code**: Check `cmd/`, `api/`, `internal/`, `deployments/`, and `test/`.
3. **Plan First**: Propose an implementation plan or prompt in `prompts/<task-name>.md` detailing goals, files to modify, security, and verification steps.
4. **Obtain Approval**: Ask the user for confirmation before writing substantial code.
5. **Implement with Precision**:
   - Write clean, typed Python 3.12+ code.
   - Run `ruff format --check .`, `ruff check .`, `mypy`, and `bandit`.
   - Update `.ai/doc-api.md` and `api/openapi/openapi.yaml` with any endpoint, Flight schema, metric, or configuration changes.
   - Write unit, security/fuzzing, integration, and chaos tests in `test/`.
6. **Verify & Commit**: Run `pytest -v`, audit secured logging, update `.ai/progress-tracker.md`, and create conventional commits adhering to the Git Commit Standards.
