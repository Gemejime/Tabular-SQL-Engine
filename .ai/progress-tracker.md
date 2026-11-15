# Progress Tracker & Implementation Roadmap - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

## 1. Calibrated Implementation Phases

This roadmap tracks the implementation milestones detailed in `Tabular Analytics Worker Implementation Plan.pdf`:

| Phase | Duration | Focus Area | Deliverables & Exit Gates | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Subprocess Sandboxing & AST Validation** | Weeks 1–2 | `sqlglot` AST Security & DuckDB Process Pool | - `sqlglot` AST security filters (read-only `SELECT` grammar, statement bans, column alias checks, $\le 2\text{ ms}$).<br>- Disposable worker subprocess pool (`ProcessPoolExecutor`, `max_tasks_per_child=100`) with lifecycle recycling.<br>- Ephemeral DuckDB `:memory:` runtime (`enable_external_access=false`, `memory_limit='2.5GB'`, `temp_directory=''`, `threads=2`). | Pending |
| **Phase 2: NVMe/tmpfs Cache & Arrow Flight Daemon** | Weeks 3–4 | Arrow Flight Server & Tier-1 Storage Hydration | - Apache Arrow Flight server daemon (`FlightServerBase`) handling HTTP/2 gRPC tickets (`DoGet`, `DoAction`) and IPC streams.<br>- Local Parquet cache on NVMe/tmpfs (`10 GiB`) with lock-free concurrent readers and LRU eviction.<br>- S3 range-read ingestion (`aiobotocore`/`s3fs`) avoiding monolithic Python heap buffering.<br>- OpenAPI 3.1 ops endpoints (`/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs`). | Pending |
| **Phase 3: Subprocess Telemetry & KEDA Concurrency Integration** | Weeks 5–6 | Circuit Breaking, Metrics & Autoscaling | - Hard `5s` query execution deadlines with automated child `SIGKILL` and recycling.<br>- Prometheus metrics (`tabular_worker_active_queries`, `tabular_worker_query_duration_seconds`, `tabular_worker_cache_hit_ratio`, `tabular_worker_subprocess_recycles_total`, `tabular_worker_flight_stream_bytes_total`, `tabular_worker_ast_rejections_total`) and OpenTelemetry tracing.<br>- KEDA `ScaledObject` rules bound to `tabular_worker_active_queries >= 4` (`4..24` replicas). | Pending |
| **Phase 4: Chaos Hardening, Fuzzing & Canary Rollout** | Weeks 7–8 | Security Fuzzing, Soak Tests & Production | - SQL injection fuzz testing (AST bypasses, `PRAGMA`, file access, extension loads).<br>- Soak testing under $500$ concurrent query streams and $8$-hour `jemalloc` runs ($0$ OOMKills).<br>- Multi-stage `Dockerfile` (`jemalloc` preloaded), Helm charts, and ArgoCD canary rollout. | Pending |

---

## 2. Architecture Decision Records (ADRs)

- **ADR-001: Subprocess Worker Isolation**: Adopts disposable worker subprocesses (`pebble.ProcessPool` / custom `multiprocessing` supervisor, `max_tasks=100`) over in-process thread pooling or stdlib `ProcessPoolExecutor` to allow isolated per-worker `SIGKILL` without `BrokenProcessPool` and prevent native DuckDB panics from crashing the Arrow Flight daemon.
- **ADR-002: Dedicated NVMe Local Table Caching**: Implements an explicit disk-backed `10 GiB` NVMe `emptyDir` cache (`medium: ""`, avoiding `10 GiB` `tmpfs` inside a `6.0 GiB` pod) for content-addressed `{hash}.parquet` artifacts with single-flight coalescing, eliminating per-query S3 round-trips and meeting warm-cache latency targets ($P95 \le 45\text{ ms}$).
- **ADR-003: Defensive Memory Budgeting**: Rebalances pod allocations to `6.0 GiB` total container RAM (`4.0 GiB` request) with a `2.5 GiB` aggregate DuckDB limit (`floor(2.5GB / POOL_SIZE)` per child), providing $\ge 2.0\text{ GiB}$ headroom for PyArrow IPC buffers and the Python runtime plus `1.5 GiB` OS page cache margin.
- **ADR-004: In-Flight Concurrency Autoscaling**: Bases KEDA autoscaling rules on in-flight query operations (`tabular_worker_active_queries >= 4`) rather than raw HTTP/2 TCP connection counts.
- **ADR-005: Query-Level AST Whitelisting via `sqlglot`**: Uses `sqlglot` to enforce read-only `SELECT` queries before engine execution, guarding against Text-to-SQL prompt injection risks.

---

## 3. Operational Runbooks

### Runbook 1: Spikes in Warm-Cache Latency
1. Check the Prometheus metric `tabular_worker_cache_hit_ratio`.
2. If the hit ratio drops below `80%`, inspect NVMe/tmpfs local cache utilization (`tabular_worker_cache_bytes_used`).
3. Verify that the local cache volume has sufficient capacity to retain active tenant working sets without continuous LRU evictions.
4. If cache thrashing occurs, increase the pod's `emptyDir` or NVMe volume allocation in `deployments/helm/values.yaml`.

### Runbook 2: Elevated Subprocess Recycling Rates
1. Inspect `tabular_worker_subprocess_recycles_total` partitioned by `reason` (`TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`).
2. If `TIMEOUT` dominates, analyze slow queries from OpenTelemetry spans to identify non-selective aggregation queries.
3. If `MEMORY_LIMIT` triggers recycles, verify whether upstream prompts are requesting broad queries across wide tables, and tune prompt constraints in `rag-prompt-registry-service`.

### Runbook 3: AST Rejection Surges
1. Inspect `tabular_worker_ast_rejections_total` grouped by violated `rule`.
2. Determine whether upstream Text-to-SQL generation models are producing unsupported functions or syntax dialects.
3. Update few-shot prompt examples and output schema constraints in `rag-prompt-registry-service` to guide models toward standard DuckDB-compatible read-only syntax.

---

## 4. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
