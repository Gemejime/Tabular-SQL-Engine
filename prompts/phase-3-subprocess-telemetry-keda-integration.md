# Implementation Plan: Phase 3 — Subprocess Telemetry & KEDA Concurrency Integration

## 1. Goal & Scope
Implement **Phase 3: Subprocess Telemetry & KEDA Concurrency Integration** for `rag-tabular-analytics-worker` (per `GEMINI.MD`, `.ai/progress-tracker.md`, `.ai/architecture.md`, `.ai/doc-api.md`, `tasks/Tabular Analytics Worker Implementation Plan.pdf`, and `tasks/Production Grade Backend Concepts.pdf`):

1. **Subprocess Recycle Telemetry & Worker Memory Guardrails (`internal/pool/`, `internal/flight/handlers.py`)**:
   - Carry `RecycleReason` on `WorkerCrashError` (e.g., `reason: RecycleReason`) so `FlightHandlers` increments `tabular_worker_subprocess_recycles_total` with the exact reason (`MEMORY_LIMIT` for exit codes `-9` / `137`, `CRASH` for other unexpected exits, `TIMEOUT` for `5s` circuit-breaker kills) instead of masking `MEMORY_LIMIT` as `CRASH`.
   - Track and emit `TASK_LIMIT` when `pebble.ProcessPool` recycles a child worker after `MAX_TASKS_PER_CHILD` (e.g., via per-worker PID/generation tracking in `ProcessManager` / `worker_task.py`). Standardize the canonical label set (`TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`, `CRASH`) and sync `.ai/doc-api.md`.
   - Enforce DuckDB's `SET memory_limit` (`ADR-003`) + `pebble` `MAX_TASKS_PER_CHILD=100` recycling rather than tight per-task `RLIMIT_AS` / `RLIMIT_DATA` (which breaks DuckDB, PyArrow, and `jemalloc` virtual address reservations). If an OS-level process guard is enabled, apply `WORKER_PROCESS_RLIMIT_BYTES` (optional, disabled by default) once at POSIX worker startup via a pool `initializer`.
   - Investigate and fix the Windows named-pipe startup race in `test/chaos/test_subprocess_isolation.py` (`test_timeout_circuit_breaker_kills_slow_query_and_keeps_siblings_serviceable`) if caused by cold-spawn warm-up timeout or listener cleanup.

2. **OpenTelemetry Distributed Tracing & Log Correlation (`internal/telemetry/tracing.py`, `internal/telemetry/logging.py`, `internal/flight/server.py`, `internal/config/settings.py`)**:
   - Add `opentelemetry-api`, `opentelemetry-sdk`, and `opentelemetry-exporter-otlp-proto-grpc` (or `http`) to `pyproject.toml`.
   - Create `internal/telemetry/tracing.py` providing tracer initialization (enabled/disabled via `TABULAR_WORKER_OTEL_ENABLED`, `TABULAR_WORKER_OTEL_EXPORTER_ENDPOINT`, `TABULAR_WORKER_OTEL_SERVICE_NAME`), W3C `traceparent` context extraction, and span helpers/decorators.
   - Wire a PyArrow Flight `fl.ServerMiddlewareFactory` into `TabularFlightServer` (`internal/flight/server.py`) to extract incoming gRPC `traceparent` metadata and propagate context into `FlightHandlers` (`DoGet`/`DoAction`).
   - Instrument **Flight ingress (`DoGet`/`DoAction`)**, **AST validation (`sqlglot`)**, **Tier-1 cache / S3 hydration**, and **parent-side subprocess DuckDB execution** (around `ProcessManager.stream_query` + `QueryStream` iteration, avoiding OTel exporters inside `pebble` child processes so `SIGKILL` timeouts never drop spans).
   - Ensure active `trace_id` is automatically injected into structured JSON logs (`internal/telemetry/logging.py`) and strictly redact/exclude raw SQL literals, credentials, and presigned S3 URLs from all OTel span attributes and `record_exception` messages.

3. **KEDA `ScaledObject` & Helm Deployment Blueprint (`deployments/helm/`)**:
   - Create `deployments/helm/Chart.yaml`, `deployments/helm/values.yaml`, and templates:
     - `deployments/helm/templates/deployment.yaml` — `6.0 GiB` memory limit / `4.0 GiB` request, `4000m` / `2000m` CPU, `10 GiB` disk-backed `emptyDir` (`medium: ""`, `ADR-002`), non-root `securityContext` (`capabilities: drop: ["ALL"]`), configurable `LD_PRELOAD` for `jemalloc` (disabled or arch-aware via `.Values.jemalloc.enabled` / `.Values.jemalloc.path` so ARM64 `c7g.xlarge` and x86_64 `c6i.xlarge` nodes remain compatible until Phase 4's `Dockerfile`), `terminationGracePeriodSeconds` (`>= 20`s, exceeding `QUERY_TIMEOUT_SECONDS=5.0`) for graceful `SIGTERM` stream draining, `/healthz` & `/ready` probes, and Prometheus scrape annotations.
     - `deployments/helm/templates/service.yaml` — Exposes named ports for Arrow Flight gRPC (`8815`, `grpc`) and Ops HTTP (`8080`, `http`), with configurable `headless` (`clusterIP: None`) support for HTTP/2 gRPC client-side load balancing from `rag-search-orchestrator`.
     - `deployments/helm/templates/scaledobject.yaml` — KEDA `ScaledObject` (`minReplicaCount: 4`, `maxReplicaCount: 24`) with a per-pod Prometheus PromQL trigger (`avg(tabular_worker_active_queries{...})`) and scale-out threshold aligned with `MAX_CONCURRENT_QUERIES` headroom (e.g., `threshold: "3"` when `MAX_CONCURRENT_QUERIES=4`, or raising `MAX_CONCURRENT_QUERIES` above `4`, so pods scale out before shedding `RESOURCE_EXHAUSTED`).

4. **Tests & Documentation Sync**:
   - `test/unit/test_tracing.py` — Span creation, `ServerMiddlewareFactory` `traceparent` extraction, `trace_id` log correlation, SQL/secret redaction on spans, and OTel-disabled no-op behavior.
   - `test/unit/test_helm_manifests.py` — Validate Helm YAML structure, KEDA `ScaledObject` bounds (`4..24`, per-pod `avg(...)` PromQL query, `tabular_worker_active_queries`), memory limits (`6Gi`/`4Gi`), `terminationGracePeriodSeconds`, configurable `jemalloc` `LD_PRELOAD`, and disk-backed `emptyDir` (`medium` not `Memory`).
   - Expand `test/integration/test_flight_server.py` and `test/chaos/test_subprocess_isolation.py` to verify `tabular_worker_subprocess_recycles_total` labels (`TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`, `CRASH`), `tabular_worker_active_queries` gauge behavior, and OpenTelemetry spans on `DoGet`.
   - Sync `.ai/doc-api.md` (add `CRASH` label to `tabular_worker_subprocess_recycles_total`, document `traceparent` header), `api/openapi/openapi.yaml` (if any config/metric descriptions are updated), and `.ai/progress-tracker.md`.

---

## 2. Files to Create / Modify
- `pyproject.toml` — Add `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-grpc` (or `http`).
- `internal/config/settings.py` — Add `OTEL_ENABLED`, `OTEL_EXPORTER_ENDPOINT`, `OTEL_SERVICE_NAME`, and optional `WORKER_PROCESS_RLIMIT_BYTES`.
- `internal/telemetry/tracing.py` — New module for OpenTelemetry tracer provider, `traceparent` propagation/middleware helpers, span decorators/context managers, and `trace_id` helper.
- `internal/telemetry/logging.py` — Inject active `trace_id` from current OTel span when not explicitly passed in `extra`.
- `internal/pool/circuit_breaker.py`, `internal/pool/process_manager.py`, & `internal/pool/worker_task.py` — Carry `RecycleReason` on `WorkerCrashError`, detect and emit `TASK_LIMIT` recycles, and optionally apply `WORKER_PROCESS_RLIMIT_BYTES` once at POSIX worker startup if configured.
- `internal/flight/server.py` & `internal/flight/handlers.py` — Wire Flight `ServerMiddlewareFactory` for `traceparent` extraction, instrument `DoGet` and `DoAction` with OpenTelemetry spans, and report exact `RecycleReason` (`MEMORY_LIMIT` vs `CRASH` vs `TIMEOUT`) to `MetricsRegistry`.
- `cmd/worker/main.py` / `internal/flight/daemon.py` — Initialize and gracefully flush/shut down OTel tracer provider.
- `deployments/helm/Chart.yaml`, `deployments/helm/values.yaml`, `deployments/helm/templates/{deployment,service,scaledobject}.yaml`.
- `test/unit/test_tracing.py`, `test/unit/test_helm_manifests.py`, plus updates to `test/integration/test_flight_server.py` and `test/chaos/test_subprocess_isolation.py`.
- `.ai/doc-api.md`, `.ai/progress-tracker.md`.

---

## 3. Verification Plan
1. `ruff format --check .` and `ruff check .`
2. `mypy cmd internal api`
3. `bandit -r cmd internal api -ll` and `pip-audit`
4. OpenAPI 3.1 validation (`openapi-spec-validator`) on `api/openapi/openapi.yaml`
5. Secured logging & DuckDB sandbox audit (no `print()`, no unredacted secrets/presigned URLs/raw SQL in logs or OTel spans, no `enable_external_access=true`)
6. `pytest -v test/` (all unit, security, integration, and chaos suites)
7. `helm lint deployments/helm` and `helm template` YAML validation
8. At least 2 conventional git commits (`feat:`, `tests:`, `docs:`) as Phase 3 is built.

---

## 4. Open Questions for Confirmation
1. **OpenTelemetry Exporter Protocol**: Should `OTEL_EXPORTER_ENDPOINT` use OTLP/gRPC (`opentelemetry-exporter-otlp-proto-grpc`) with `OTEL_ENABLED=false` by default when no collector endpoint is configured?
2. **KEDA Headroom vs. `MAX_CONCURRENT_QUERIES`**: Should we set the KEDA `ScaledObject` threshold to `3` per pod (keeping `MAX_CONCURRENT_QUERIES=4` so pods scale out before returning `RESOURCE_EXHAUSTED`), or keep the KEDA threshold at `4` and raise `MAX_CONCURRENT_QUERIES` (e.g., `6`)?
3. **KEDA Prometheus Server Address**: In `deployments/helm/values.yaml` & `scaledobject.yaml`, is `http://prometheus-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090` (configurable via `.Values.keda.prometheus.serverAddress`) acceptable as the default placeholder?
4. **Proceed**: Please confirm if you would like me to proceed with implementing Phase 3 according to this updated plan.
