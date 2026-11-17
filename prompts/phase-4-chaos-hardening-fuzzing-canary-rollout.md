# Implementation Plan: Phase 4 — Chaos Hardening, Fuzzing & Canary Rollout

## 1. Goal & Scope
Implement **Phase 4: Chaos Hardening, Fuzzing & Canary Rollout** for `rag-tabular-analytics-worker` (per `GEMINI.MD`, `.ai/progress-tracker.md`, `.ai/architecture.md`, `.ai/security.md`, `.ai/doc-api.md`, and `tasks/Tabular Analytics Worker Implementation Plan.pdf`):

1. **Multi-Stage Production `Dockerfile` with `jemalloc` (`deployments/docker/Dockerfile`, `.dockerignore`, `deployments/helm/values.yaml`)**:
   - Build a minimal, non-root multi-stage Python `3.12-slim` image (`builder` stage installing the package and runtime dependencies into `/opt/venv` + `runtime` stage copying `/opt/venv` and app code).
   - Install `libjemalloc2` (`--no-install-recommends`) and symlink `/usr/lib/*-linux-gnu/libjemalloc.so.2` to `/usr/local/lib/libjemalloc.so` so `LD_PRELOAD=/usr/local/lib/libjemalloc.so` works identically on both `amd64` (`c6i.xlarge`) and `arm64` (`c7g.xlarge`) nodes.
   - Create `/var/cache/tabular-worker` owned by `10001:10001`, run as non-root user `10001:10001` (compatible with `readOnlyRootFilesystem: true` + mounted `/tmp` and `/var/cache/tabular-worker`), expose Flight gRPC (`8815`) and Ops HTTP (`8080`), set `HEALTHCHECK` against `/healthz`, and use JSON `CMD ["python", "-m", "cmd.worker.main"]`.
   - Ensure the `Dockerfile` passes `hadolint/hadolint-action@v3.1.0` in `.github/workflows/ci.yml` (e.g., `--no-install-recommends`, pinned or explicitly documented `hadolint ignore=DL3008`, consolidated `RUN` layers).
   - Update `deployments/helm/values.yaml` to set `jemalloc.enabled: true` and `jemalloc.path: /usr/local/lib/libjemalloc.so`, and update the corresponding assertion in `test/unit/test_helm_manifests.py`.

2. **Expanded SQL Injection & AST Bypass Fuzzing (`test/security/test_sql_injection_fuzz.py`)**:
   - Add deterministic randomized/combinatorial SQL fuzzing covering:
     - Nested CTEs (`WITH ...`), `UNION`/`INTERSECT`/`EXCEPT` branches, and scalar/table subqueries smuggling banned functions (`read_parquet`, `read_csv`, `read_csv_auto`, `scan_parquet`, `glob`, `getenv`, `current_setting`, `duckdb_tables`, `duckdb_settings`, `httpfs_get`, `write_csv`, `copy`).
     - Comment obfuscation (`/* ... */`, `-- ...`), whitespace/newline/case variations, stacked statements (`;`), deeply nested expressions, and disallowed table literals/paths.
     - Warm up `ASTValidator` once in a fixture before timing, then verify every malicious/malformed payload raises `ValidationError` within `<= 2ms` per query and increments `tabular_worker_ast_rejections_total` when routed through `FlightHandlers`.
   - Verify `apply_sandbox_lock` in DuckDB (`enable_external_access = false`, `lock_configuration = true`) directly blocks `read_parquet`, `read_csv`, `PRAGMA`, `SET`, `ATTACH`, `DETACH`, `COPY ... TO`, `INSTALL`, `LOAD`, and `getenv` even if a hypothetical query bypassed the validator.

3. **500-Stream Concurrent Chaos & Memory-Boundary Soak Tests (`test/chaos/test_soak_and_memory.py`)**:
   - **Sustained 500-Query Concurrent Stream Test** (fast ~2–5s default CI run): Execute 500 mixed queries (valid aggregations, AST rejections, and periodic `MAX_TASKS_PER_CHILD` recycles) through a bounded client concurrency pool (`<= MAX_CONCURRENT_QUERIES`) against `FlightServerBase` / `ProcessManager`, asserting:
     - `0` host daemon crashes or unhandled exceptions.
     - `0` leaked child processes after pool shutdown.
     - Memory RSS growth remains bounded across `max_tasks` recycling cycles (measured using stdlib `resource` / `/proc/self/statm` / `tracemalloc` with cross-platform fallback—do **not** require `psutil`, which is not in `pyproject.toml`).
   - **Burst Backpressure Test**: Send an over-limit concurrent burst (`> MAX_CONCURRENT_QUERIES`) to `FlightHandlers` / `FlightServerBase` and verify `RESOURCE_EXHAUSTED` (`ConcurrencyLimitError`) is returned cleanly without leaking semaphore permits or leaving `tabular_worker_active_queries > 0`.
   - **Extended Endurance & SLO Marker (`@pytest.mark.soak`)**: Gate the 8-hour `jemalloc` continuous soak run and large-dataset `250 MB/sec` / `P95 <= 45ms` stress checks behind `@pytest.mark.soak` (skipped unless explicitly enabled, e.g., via `RUN_SOAK_TESTS=1` or `-m soak`) so `pytest -v test/chaos` remains fast in CI.

4. **ArgoCD Canary Rollout & Staging/Production Overlays (`deployments/argocd/`, `deployments/helm/`)**:
   - Add `deployments/helm/values-staging.yaml` (e.g., `minReplicaCount: 1`, `maxReplicaCount: 6`, `OTEL_ENABLED: "true"`) and `deployments/helm/values-prod.yaml` (`minReplicaCount: 4`, `maxReplicaCount: 24`, `4Gi/6Gi` RAM, `2000m/4000m` CPU, `10Gi` NVMe cache, `jemalloc.enabled: true`).
   - Add ArgoCD manifests under `deployments/argocd/`:
     - `application-staging.yaml` and `application-prod.yaml` (ArgoCD `Application` resources pointing to `deployments/helm` with their respective values files).
     - `rollout-canary.yaml` containing an Argo Rollouts `Rollout` (using `spec.workloadRef` to the Helm `Deployment` so existing `deployments/helm/templates/deployment.yaml` and KEDA `ScaledObject` remain intact and pass `helm lint`) with progressive canary steps (`10% -> 25% -> 50% -> 100%`) and an `AnalysisTemplate` gated only on real metrics defined in `internal/telemetry/metrics.py`:
       - `tabular_worker_query_duration_seconds` P99 (`<= 0.12s` warm-cache SLO)
       - `tabular_worker_subprocess_recycles_total{reason=~"TIMEOUT|MEMORY_LIMIT|CRASH"}` rate
       - `tabular_worker_cache_hit_ratio` (`>= 0.80` per Runbook 1)
       - `tabular_worker_active_queries` visibility

5. **Documentation & Tracker Sync (`.ai/progress-tracker.md`, `.ai/doc-api.md`, `README.md`)**:
   - Update `.ai/progress-tracker.md` to mark Phase 4 as `Completed` with session verification notes.
   - Verify `.ai/doc-api.md` and `api/openapi/openapi.yaml` remain in sync with any configuration, container, or metric updates.
   - Update `README.md` with Docker build/run, fuzzing/soak test (`@pytest.mark.soak`), and ArgoCD canary deployment instructions.

---

## 2. Files to Create / Modify
- `deployments/docker/Dockerfile` (new) & `.dockerignore` (new)
- `deployments/helm/values.yaml` (enable `jemalloc` & set `/usr/local/lib/libjemalloc.so`)
- `deployments/helm/values-staging.yaml` & `deployments/helm/values-prod.yaml` (new)
- `deployments/argocd/application-staging.yaml`, `deployments/argocd/application-prod.yaml`, & `deployments/argocd/rollout-canary.yaml` (new)
- `test/security/test_sql_injection_fuzz.py` (expand AST bypass & DuckDB sandbox fuzzing with warm `<= 2ms` budget)
- `test/chaos/test_soak_and_memory.py` (new — 500-stream bounded concurrency soak, burst backpressure, & `@pytest.mark.soak` endurance suite)
- `test/unit/test_helm_manifests.py` (update `jemalloc.enabled` assertion; validate `Dockerfile`, staging/prod Helm values, and ArgoCD manifests)
- `README.md`, `.ai/progress-tracker.md` (and `.ai/doc-api.md` / `api/openapi/openapi.yaml` if any contracts change)

---

## 3. Verification Plan
1. `ruff format --check .` and `ruff check .`
2. `mypy cmd internal api`
3. `bandit -r cmd internal api -ll` and `pip-audit`
4. OpenAPI 3.1 validation (`openapi-spec-validator`) on `api/openapi/openapi.yaml`
5. Secured logging & DuckDB sandbox audit (no `print()`, no unredacted secrets/presigned URLs, no `enable_external_access=true`)
6. `pytest -v test/` (unit, security/fuzz, integration, and chaos/500-stream soak tests)
7. `helm lint` / `helm template` (with default, `values-staging.yaml`, and `values-prod.yaml`) and `Dockerfile` lint checks (`hadolint` where available, plus unit tests covering both)
8. At least 2 conventional git commits (`feat:`, `tests:`, `docs:`) as Phase 4 is built.

---

## 4. Confirmed Architectural Decisions
1. **Canary Rollout Format**: Argo Rollouts `Rollout` (with `spec.workloadRef` referencing the Helm `Deployment`) + `AnalysisTemplate` (progressive steps `10% -> 25% -> 50% -> 100%`) alongside ArgoCD `Application` manifests (`application-staging.yaml`, `application-prod.yaml`).
2. **`jemalloc` Path**: `Dockerfile` symlinks `/usr/lib/*-linux-gnu/libjemalloc.so.2` to `/usr/local/lib/libjemalloc.so` for both `amd64` (`c6i.xlarge`) and `arm64` (`c7g.xlarge`), with `jemalloc.enabled: true` in `deployments/helm/values.yaml`.
3. **500-Stream Soak Test in CI**: Fast (~2–5s) 500-query bounded stream + burst backpressure test runs by default in `test/chaos/`, while the 8-hour `jemalloc` endurance run is gated behind `@pytest.mark.soak`.
