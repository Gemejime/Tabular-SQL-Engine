# Architecture Context - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Python 3.12+ (64-bit) | Host Arrow Flight daemon, `sqlglot` AST validation, async S3 hydration, metrics |
| **Memory Allocator** | `jemalloc` (`LD_PRELOAD`) | Mitigates native heap fragmentation across rapid Arrow IPC allocation cycles |
| **IPC & Transport** | Apache Arrow Flight (`pyarrow.flight`) | HTTP/2 gRPC server streaming zero-copy Arrow `RecordBatch` results (`DoGet`, `DoAction`) |
| **SQL Engine** | DuckDB (`duckdb.connect(":memory:")`) | Ephemeral in-memory C++ vectorized analytical engine inside isolated child processes |
| **Worker Pool** | `ProcessPoolExecutor` (`max_tasks_per_child=100`) | Disposable subprocess sandboxing with `5s` circuit-breaker cancellation (`SIGKILL` + respawn) |
| **SQL Security** | `sqlglot` | Pure-Python deterministic AST whitelisting ($\le 2\text{ ms}$) before subprocess dispatch |
| **Tier-1 Cache** | Local `10 GiB` NVMe / `tmpfs` `emptyDir` | Content-addressed `{hash}.parquet` cache with lock-free concurrent readers & LRU eviction |
| **Object Storage** | AWS S3 (`aiobotocore` / `s3fs`) | Range-read streaming over internal VPC endpoints on cache miss (no monolithic heap buffering) |
| **Ops & Doc API** | HTTP Ops Server + OpenAPI 3.1 | `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs` |
| **Autoscaling** | Kubernetes + KEDA `ScaledObject` | Scales `4..24` replicas on `tabular_worker_active_queries >= 4` per pod |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with `Production Grade Backend Concepts.pdf`:

### Layer 1: API & Routing
- **Entry Points (`cmd/worker/main.py`)**:
  - Initializes 12-factor environment configuration (`internal/config/settings.py`), preloads `ProcessPoolExecutor` child workers, warms/verifies the NVMe/tmpfs cache directory, starts the Apache Arrow Flight gRPC server (`FlightServerBase`) and HTTP operational/OpenAPI server (`/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs`), and handles `SIGINT`/`SIGTERM` for graceful connection draining.
- **Endpoints & Flight RPCs (`api/flight/`, `internal/flight/`)**:
  - **Arrow Flight `DoGet(Ticket)`**: Accepts a JSON/Protobuf-encoded ticket (`sql`, `tenant_id`, `table_hash`, `table_alias`, `request_id`), validates the SQL AST, resolves/hydrates the Parquet table, dispatches to the subprocess pool, and streams zero-copy `RecordBatch` frames.
  - **Arrow Flight `DoAction(Action)`**: Supports `cache_warm` (pre-fetch `{hash}.parquet` from S3 into NVMe/tmpfs) and `health_check`.
  - **HTTP Operational & OpenAPI 3.1 Endpoints**: `GET /healthz`, `GET /ready`, `GET /metrics`, `GET /openapi.yaml`, `GET /docs`.
- **Middleware & Interception**:
  - Request/correlation ID extraction (`X-Request-ID` / Flight call metadata), structured JSON logging with strict secret/PII redaction, and OpenTelemetry span propagation.
- **Input Validation & Sanitization**:
  - Pydantic/schema validation on Flight tickets (non-empty `tenant_id`, valid SHA-256 `table_hash`, bounded SQL length) + deterministic `sqlglot` AST whitelisting ($\le 2\text{ ms}$).
- **Rate Limiting & Backpressure**:
  - Bounded in-flight concurrency semaphore aligned with KEDA scaling target (`tabular_worker_active_queries`) and `5s` execution deadline.
- **Idempotency**:
  - Content-addressed `{hash}.parquet` local cache keys and read-only `SELECT` execution guarantee idempotent retries from `rag-search-orchestrator`.

### Layer 2: Data Persistence & State
- **Stateless Compute Tier (No Transactional DB)**:
  - `rag-tabular-analytics-worker` owns no relational database state; permanent schema cataloging and metadata belong to `rag-kb-catalog-service`.
- **Tier-1 Local Artifact Caching (`internal/storage/cache_manager.py`)**:
  - Stores content-addressed `{hash}.parquet` files on a `10 GiB` NVMe or `tmpfs` `emptyDir` mount.
  - Supports lock-free concurrent readers, atomic temp-file-then-rename writes on cache hydration, and LRU eviction under disk pressure.
- **S3 Range-Read Streaming (`internal/storage/s3_streamer.py`)**:
  - Fetches un-cached Parquet artifacts from S3 over internal VPC endpoints using ranged reads (`aiobotocore`/`s3fs`), avoiding monolithic byte-array buffering on the Python heap.
- **Query Bounds**:
  - DuckDB `memory_limit = '2.5GB'`, `threads = 2`, and `5s` hard timeout prevent runaway joins or Cartesian explosions from exhausting memory.

### Layer 3: Architecture & Scale
- **Process-Isolated Disposable Sandboxing (`internal/pool/`)**:
  - `ProcessPoolExecutor` with `max_tasks_per_child=100` isolates native DuckDB C++ memory and panics from the host Flight daemon (`ADR-001`).
  - `circuit_breaker.py` monitors execution deadlines (`5s`), `SIGKILL`ing runaway child processes and immediately respawning replacements (`ADR-001`).
- **Memory Partitioning Blueprint (`ADR-003`)**:
  - **Pod Cgroup Limit**: `6.0 GiB` (`4.0 GiB` request); CPU `2,000m` request / `4,000m` limit.
  - **DuckDB Engine Ceiling**: `2.5 GiB` (`SET memory_limit = '2.5GB'`).
  - **Host Daemon & Arrow Headroom**: $\ge 2.0\text{ GiB}$ reserved for the Python Flight daemon, PyArrow IPC buffers, AST syntax trees, and gRPC TCP frames.
  - **OS Page Cache & Buffer Overhead**: `1.5 GiB` margin below the `6.0 GiB` cgroup limit to prevent Linux OOM kills (`SIGKILL` exit 137).
- **High Availability & KEDA Autoscaling (`ADR-004`)**:
  - KEDA `ScaledObject` scales `4..24` replicas based on `tabular_worker_active_queries >= 4` per pod across compute-optimized nodes (`c6i.xlarge` / `c7g.xlarge`).
- **Creational Design Patterns**:
  - Explicit dependency wiring in `cmd/worker/main.py` for config, cache manager, S3 streamer, AST validator, process pool supervisor, and telemetry registries.

### Layer 4: Security & Identity
- **Tenant Isolation**:
  - Every Flight ticket carries `tenant_id`; S3 object keys and local table aliases are strictly scoped and validated (`table_resolver.py`) to prevent cross-tenant table access.
- **Deterministic SQL AST Whitelisting (`ADR-005`, `internal/validator/`)**:
  - `sqlglot` permits only `Select` constructs and pre-approved functions; blocks DML/DDL, `PRAGMA`, `SET`, `RESET`, `COPY ... TO`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`, and DuckDB filesystem functions (`read_csv`, `read_parquet`, `scan_parquet`, `write_csv`).
- **DuckDB Sandbox Locks (`internal/sandbox/security_profile.py`)**:
  - `duckdb.connect(":memory:")` with `enable_external_access=false`, `temp_directory=''`, `threads=2`, `memory_limit='2.5GB'`.
- **Secured Logging & Container Hardening**:
  - Structured JSON logs with redaction of credentials, tokens, presigned S3 query params, and PII; non-root container with `jemalloc` preloaded.

### Layer 5: Observability & Quality
- **Structured Logging**: Machine-parseable JSON logs with `trace_id`, `request_id`, `tenant_id`, `table_hash`, `cache_state`, and `duration_ms`.
- **Prometheus Observability Matrix (`internal/telemetry/metrics.py`)**:
  - `tabular_worker_active_queries` (Gauge — primary KEDA metric)
  - `tabular_worker_query_duration_seconds` (Histogram partitioned by `warm_cache` vs `cold_s3`)
  - `tabular_worker_cache_hit_ratio` (Gauge)
  - `tabular_worker_cache_bytes_used` (Gauge)
  - `tabular_worker_subprocess_recycles_total` (Counter partitioned by `TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`)
  - `tabular_worker_flight_stream_bytes_total` (Counter)
  - `tabular_worker_ast_rejections_total` (Counter partitioned by violated rule type)
- **Distributed Tracing (`internal/telemetry/tracing.py`)**: OpenTelemetry trace decorators across Flight ingress, AST validation, S3 hydration, and subprocess execution.
- **Health Checks & Graceful Shutdown**: `/healthz` (liveness) and `/ready` (readiness verifying process pool & cache mount availability), plus graceful drain on `SIGTERM`.

---

## Repository Folder Structure

```
rag-tabular-analytics-worker/
├── cmd/
│   └── worker/
│       └── main.py                  # Primary Arrow Flight + Ops HTTP daemon entry point
├── api/
│   ├── flight/
│   │   ├── schema.py                # Flight descriptor schemas, tickets, error mappings
│   │   └── actions.py               # Flight action definitions (cache_warm, health_check)
│   └── openapi/
│       └── openapi.yaml             # OpenAPI 3.1 operational & control API specification
├── internal/
│   ├── config/
│   │   └── settings.py              # Pod configurations, memory ceilings, paths, timeouts
│   ├── flight/
│   │   ├── server.py                # Apache Arrow FlightServerBase implementation
│   │   └── handlers.py              # Ticket resolution and subprocess dispatch handlers
│   ├── pool/
│   │   ├── process_manager.py       # ProcessPoolExecutor supervisor with lifetime recycling
│   │   ├── worker_task.py           # Child process worker task (executes sandboxed DuckDB)
│   │   └── circuit_breaker.py       # 5-second deadline enforcement and process killer
│   ├── sandbox/
│   │   ├── duckdb_runtime.py        # Ephemeral duckdb.connect(':memory:') builder
│   │   └── security_profile.py      # DuckDB setting locks (enable_external_access=false)
│   ├── validator/
│   │   ├── ast_validator.py         # Primary sqlglot validation engine
│   │   ├── rules.py                 # Read-only rules, join limits, banned function sets
│   │   └── exceptions.py            # ValidationError hierarchy
│   ├── storage/
│   │   ├── cache_manager.py         # Local NVMe/tmpfs Parquet file cache with LRU eviction
│   │   ├── s3_streamer.py           # S3 ranged read fetcher for un-cached Parquet tables
│   │   └── table_resolver.py        # Maps query aliases to validated local file descriptors
│   └── telemetry/
│       ├── metrics.py               # Prometheus metrics (tabular_worker_active_queries, etc.)
│       └── tracing.py               # OpenTelemetry distributed trace decorators
├── deployments/
│   ├── docker/
│   │   └── Dockerfile               # Multi-stage image with jemalloc preloaded
│   └── helm/
│       ├── Chart.yaml
│       ├── values.yaml              # 6 GiB RAM limit, NVMe/tmpfs mounts, KEDA config
│       └── templates/
│           ├── deployment.yaml
│           ├── service.yaml
│           └── scaledobject.yaml    # KEDA ScaledObject driven by in-flight queries
├── test/
│   ├── unit/                        # AST validation rules, cache eviction logic
│   ├── integration/                 # Arrow Flight IPC streaming with mock S3 and subprocesses
│   ├── security/                    # SQL injection, sandbox escape, PRAGMA exploit tests
│   ├── chaos/                       # Forced child SIGKILL tests, OOM leak soak tests
│   └── fixtures/                    # Normalized test Parquet datasets and queries
├── pyproject.toml
├── Makefile
└── README.md
```
