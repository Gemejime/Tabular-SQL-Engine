# Implementation Plan: Phase 2 — Disk-Backed NVMe Cache & Arrow Flight Daemon

## 1. Goal & Scope
Implement **Phase 2: Disk-Backed NVMe Cache & Arrow Flight Daemon** for `rag-tabular-analytics-worker` (`GEMINI.MD`, `.ai/progress-tracker.md`, `.ai/doc-api.md`, `tasks/Tabular Analytics Worker Implementation Plan.pdf`):
- **Tier-1 Disk-Backed NVMe Parquet Cache (`internal/storage/cache_manager.py`)**: Content-addressed `<CACHE_DIR>/{s3_bucket}/{tenant_id}/{table_hash}.parquet` local cache (`10 GiB` default ceiling) with startup disk-state reconciliation + `.tmp` sweep, `threading.Lock`-protected `OrderedDict` LRU eviction + active-reader refcounts (independent of `noatime` mounts), per-artifact single-flight download coalescing, atomic `os.replace` writes, pre-commit Parquet `PAR1`/metadata verification, and safe eviction of corrupted artifacts.
- **S3 Streaming & Tenant Isolation (`internal/storage/s3_streamer.py`, `internal/storage/table_resolver.py`)**: Async S3 range/chunked streaming (`aiobotocore`) over internal VPC endpoints on a dedicated `asyncio` event-loop thread, streaming chunks directly to `.tmp` files on disk (zero monolithic Python heap buffering) with bounded connect/read timeouts, plus strict `s3_bucket` allowlist and canonical `s3_key == f"artifacts/{tenant_id}/{table_hash}.parquet"` tenant isolation (`PERMISSION_DENIED`).
- **Apache Arrow Flight Daemon (`api/flight/`, `internal/flight/`)**: `FlightServerBase` handling HTTP/2 gRPC `DoGet` (streaming zero-copy `RecordBatch` chunks from `ProcessManager.stream_query()` via `GeneratorStream` / `RecordBatchReader` without `fetch_arrow_table()` or JSON row conversion) and `DoAction` (`cache_warm`, `health_check`), bounded by `MAX_CONCURRENT_QUERIES` (`4`, matching `POOL_SIZE` for immediate `RESOURCE_EXHAUSTED` backpressure without hidden pool queuing).
- **Telemetry, OpenAPI 3.1 Ops HTTP Server & Entrypoint (`internal/telemetry/metrics.py`, `api/openapi/openapi.yaml`, `cmd/worker/main.py`)**: Prometheus metrics registry + HTTP ops listener on port `8080` serving `/healthz`, `/ready` (verifying `ProcessManager.is_healthy()` + writable cache mount + drain state), `/metrics`, `/openapi.yaml`, and `/docs` (Swagger UI), with `SIGINT`/`SIGTERM` graceful drain.

---

## 2. Files to Create / Modify

### Configuration (`internal/config/settings.py`, `pyproject.toml`)
- `pyproject.toml` — Add Phase 2 runtime dependencies (`aiobotocore`, `pyyaml`) and dev/test dependency (`openapi-spec-validator`), plus `mypy` `ignore_missing_imports` overrides for `aiobotocore` / `aiobotocore.*`.
- `internal/config/settings.py` — Add `FLIGHT_HOST`, `FLIGHT_PORT` (`8815`), `HTTP_HOST`, `HTTP_PORT` (`8080`), `CACHE_DIR`, `CACHE_MAX_BYTES` (`10 GiB`), `S3_BUCKET_ALLOWLIST`, `S3_ENDPOINT_URL`, `S3_REGION`, `S3_CHUNK_SIZE_BYTES` (`8 MiB`), `S3_TIMEOUT_SECONDS` (`4.0`), and `MAX_CONCURRENT_QUERIES` (default `4`, aligned with `POOL_SIZE=4` and KEDA `tabular_worker_active_queries >= 4`).

### Tier-1 Storage & Tenant Isolation (`internal/storage/`)
- `internal/storage/__init__.py`
- `internal/storage/table_resolver.py` — `TableResolver` validating `tenant_id`, 64-hex SHA-256 `table_hash`, `s3_bucket` allowlist (fail-closed if empty), and canonical `s3_key`; raises `TenantIsolationError` (`PERMISSION_DENIED`) or `ValidationError` (`INVALID_ARGUMENT`).
- `internal/storage/s3_streamer.py` — `S3Streamer` whose `aiobotocore` `AioSession`/client lifecycle is created and closed inside the dedicated `asyncio` hydration loop thread; streams `GetObject` / bounded range reads (`Range: bytes=...` / `iter_chunks`) directly to a temp file in `CACHE_DIR` with explicit connect/read timeouts; maps `NoSuchKey`/`404` to `ArtifactNotFoundError` (`NOT_FOUND`), `403` to `TenantIsolationError`/`S3AccessError`, and never logs presigned URLs, `X-Amz-*` parameters, or AWS credentials.
- `internal/storage/cache_manager.py` — `CacheManager` managing `<CACHE_DIR>/{s3_bucket}/{tenant_id}/{table_hash}.parquet`:
  - Scans `CACHE_DIR` on startup to seed LRU size tracking from existing `.parquet` files and remove orphaned `.tmp` files.
  - Uses a `threading.Lock` for `OrderedDict` LRU state + per-artifact reader `refcount`s (safe across gRPC threads and the `asyncio` loop thread) and per-key single-flight coalescing on the `asyncio` loop thread.
  - Verifies `PAR1` header/footer magic bytes and `pyarrow.parquet.ParquetFile` metadata before `os.replace`, and evicts corrupted entries on read failure.
  - If `CACHE_MAX_BYTES` is exhausted and all cached entries are pinned (`refcount > 0`), fails new cold hydrations with `CacheCapacityError` (`RESOURCE_EXHAUSTED`) rather than exceeding the `10 GiB` volume limit.

### Arrow Flight Contracts, Telemetry & Server (`api/flight/`, `internal/flight/`, `internal/telemetry/`)
- `api/flight/__init__.py`
- `api/flight/schema.py` — Pydantic `FlightQueryTicket` model (`request_id`, `tenant_id`, `sql`, `table_alias`, `table_hash`, `s3_bucket`, `s3_key`) and sanitized gRPC/Flight error mapping (`INVALID_ARGUMENT`, `PERMISSION_DENIED`, `DEADLINE_EXCEEDED`, `INTERNAL`, `RESOURCE_EXHAUSTED`, `NOT_FOUND`) that never leaks stack traces, bucket names, or S3 keys to clients.
- `api/flight/actions.py` — `CacheWarmRequest`/`CacheWarmResponse` and `HealthCheckResponse` schemas for `DoAction`.
- `internal/telemetry/metrics.py` — Thread-safe Prometheus metrics registry & text exposition helper (`tabular_worker_active_queries`, `tabular_worker_cache_hits_total`, `tabular_worker_cache_misses_total`, `tabular_worker_cache_hit_ratio`, `tabular_worker_cache_bytes_used`).
- `internal/flight/__init__.py`
- `internal/flight/handlers.py` — `FlightHandlers` coordinating ticket validation, `request_id` structured log correlation, `ASTValidator`, `TableResolver` (enforced on both `DoGet` and `DoAction("cache_warm")`), `CacheManager`/`S3Streamer` via `asyncio.run_coroutine_threadsafe`, `MAX_CONCURRENT_QUERIES` semaphore, and `ProcessManager.stream_query(...)`:
  - Adapts `ProcessManager.stream_query()` (`Iterator[pa.RecordBatch]`) to `GeneratorStream` / `RecordBatchReader` without materializing a `pa.Table`.
  - Releases the `MAX_CONCURRENT_QUERIES` semaphore, decrements `tabular_worker_active_queries`, and unpins the `CacheManager` reader refcount inside the **stream generator's `finally:` block** (not when `do_get` returns).
  - Catches `QueryTimeoutError` → `FlightTimedOutError` (`DEADLINE_EXCEEDED`) and `WorkerCrashError` → `FlightInternalError` (`INTERNAL`) inside the stream wrapper.
- `internal/flight/server.py` — `TabularFlightServer(pyarrow.flight.FlightServerBase)` implementing `do_get`, `do_action` (`cache_warm` idempotent, `health_check`), and `list_actions`.

### OpenAPI 3.1 & Ops HTTP Server (`api/openapi/`, `internal/flight/ops_server.py`, `cmd/worker/main.py`)
- `api/__init__.py`, `api/openapi/openapi.yaml` — OpenAPI 3.1.0 specification for `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, and `/docs` (validated by `openapi-spec-validator`).
- `internal/flight/ops_server.py` — `http.server.ThreadingHTTPServer` serving `/healthz`, `/ready` (returning `503` if draining, `!ProcessManager.is_healthy()`, or `CACHE_DIR` not writable), `/metrics` (from `internal/telemetry/metrics.py`), `/openapi.yaml`, and `/docs`.
- `cmd/__init__.py`, `cmd/worker/__init__.py`, `cmd/worker/main.py` — Daemon entrypoint starting the `asyncio` hydration loop thread (with `S3Streamer` client lifecycle), `ProcessManager`, `TabularFlightServer`, and Ops HTTP server, and handling `SIGINT`/`SIGTERM` graceful drain (`503` on `/ready` → `TabularFlightServer.shutdown()` → `ProcessManager.close()` → close S3 client & stop `asyncio` loop).

### Tests (`test/unit/`, `test/integration/`)
- `test/unit/test_table_resolver.py` — Tenant isolation, `s3_bucket` allowlist, canonical `s3_key`, and `table_hash` validation tests.
- `test/unit/test_cache_manager.py` — Startup `.parquet` reconciliation & `.tmp` sweep, single-flight coalescing, LRU eviction under capacity pressure, active-reader pinning + all-pinned `RESOURCE_EXHAUSTED`, atomic `os.replace`, and corrupted Parquet handling tests.
- `test/unit/test_flight_schemas.py` — Ticket/action Pydantic validation, sanitized gRPC error mapping, and `api/openapi/openapi.yaml` OpenAPI 3.1 validation tests.
- `test/integration/test_flight_server.py` — End-to-end `FlightClient` `DoGet` & `DoAction` (`cache_warm`, `health_check`) against `TabularFlightServer` with mocked S3 streaming, subprocess DuckDB execution, mid-stream timeout/crash error mapping, HTTP ops endpoints (`/healthz`, `/ready` healthy/unhealthy/draining `503`, `/metrics`, `/openapi.yaml`, `/docs`), and `RESOURCE_EXHAUSTED` backpressure.

### Documentation Sync
- `.ai/doc-api.md` — Keep HTTP ops endpoints, Arrow Flight `DoGet`/`DoAction` schemas, error mappings, Prometheus metrics, and cache conventions synchronized.
- `.ai/progress-tracker.md` — Update Phase 2 status and session notes upon completion.

---

## 3. Verification Plan
1. `ruff format --check .` and `ruff check .`
2. `mypy cmd internal api`
3. `bandit -r cmd internal api -ll` and `pip-audit`
4. OpenAPI 3.1 validation (`openapi-spec-validator`) on `api/openapi/openapi.yaml`
5. Secured logging & DuckDB sandbox audit (no `print()`, no unredacted secrets/presigned URLs in logger calls, no `enable_external_access=true`)
6. `pytest -v test/unit test/security test/integration test/chaos`
7. At least 2 conventional git commits (`feat:`, `tests:`, `docs:`) as Phase 2 is built.

---

## 4. Confirmed Decisions & Proceed Check
1. **S3 Bucket Allowlist & Endpoint Defaults (Confirmed)**: `S3_BUCKET_ALLOWLIST` defaults to an empty list (`[]`, fail-closed unless explicitly configured via `TABULAR_WORKER_S3_BUCKET_ALLOWLIST`).
2. **Flight Authentication Claims (Confirmed)**: For Phase 2, `DoGet`/`DoAction` accept local/VPC gRPC tickets carrying `tenant_id` per `.ai/doc-api.md`, with mTLS/signed-token middleware deferred to Phase 3/4.
3. **HTTP Ops Server Implementation (Confirmed)**: Use Python's built-in `http.server.ThreadingHTTPServer` for `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, and `/docs` (zero extra runtime dependencies, clean `503` drain support).
4. **Proceed**: Awaiting explicit confirmation to begin implementing Phase 2.
