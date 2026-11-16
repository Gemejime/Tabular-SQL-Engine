# Documented APIs & Contracts - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

This document is the authoritative specification for all public and internal interfaces of the `rag-tabular-analytics-worker`.

> [!IMPORTANT]
> **Continuous Doc API & OpenAPI Updates**: This file and `api/openapi/openapi.yaml` MUST be kept synchronized on **every code change or addition**, regardless of how small the modification is, if need be.

---

## 1. OpenAPI 3.1 HTTP Operational & Control Specification

### Base URL
`http://localhost:8080` (Internal Service / Kubernetes Pod Ops Listener)

### Endpoints Overview

| Method | Endpoint | Description | Auth Scope |
| :--- | :--- | :--- | :--- |
| `GET` | `/healthz` | Kubernetes Liveness Probe (event loop & daemon alive) | Public |
| `GET` | `/ready` | Kubernetes Readiness Probe (checks worker pool & NVMe/tmpfs cache mount) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint | Public / Ops |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification (`api/openapi/openapi.yaml`) | Public |
| `GET` | `/docs` | Swagger UI documentation interface | Public |

---

### Detailed HTTP Endpoint Contracts

#### 1. `GET /healthz`
Verifies the Python host daemon process and async event loop are alive.
- **Response `200 OK`**:
```json
{
  "status": "ok",
  "service": "rag-tabular-analytics-worker"
}
```

#### 2. `GET /ready`
Verifies the `ProcessPoolExecutor` supervisor has active pre-allocated worker capacity and the Tier-1 NVMe/tmpfs cache directory is mounted and writable.
- **Response `200 OK`**:
```json
{
  "status": "ready",
  "checks": {
    "process_pool": "healthy",
    "cache_mount": "writable"
  }
}
```
- **Response `503 Service Unavailable`** (draining or unhealthy):
```json
{
  "status": "draining",
  "checks": {
    "drain": "shutting_down"
  }
}
```
or when a dependency check fails:
```json
{
  "status": "not_ready",
  "checks": {
    "process_pool": "unhealthy",
    "cache_mount": "writable"
  }
}
```

#### 3. `GET /metrics`
Exposes Prometheus text exposition metrics (`text/plain; version=0.0.4`).

---

## 2. Apache Arrow Flight IPC Contract (`api/flight/`)

### Transport
- **Protocol**: Apache Arrow Flight over HTTP/2 gRPC (`FlightServerBase`).
- **Primary Caller**: `rag-search-orchestrator` (Go).
- **Egress Format**: Zero-copy chunked Arrow `RecordBatch` stream (no JSON row serialization).

### `DoGet(context, ticket)` — Execute Sandboxed Analytical Query
- **Ticket Payload (`api/flight/schema.py`)**:
```json
{
  "request_id": "req_01JXYZ1234567890",
  "tenant_id": "tenant-corp-01",
  "sql": "SELECT region, SUM(revenue) AS total_rev FROM tbl GROUP BY region",
  "table_alias": "tbl",
  "table_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "s3_bucket": "rag-tabular-artifacts",
  "s3_key": "artifacts/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.parquet"
}
```
- **Error Mappings**:
  - `INVALID_ARGUMENT` (gRPC): Malformed ticket payload or rejected `sqlglot` AST rule violation (`ValidationError`).
  - `PERMISSION_DENIED` (gRPC): Cross-tenant `tenant_id`, unallowlisted `s3_bucket`, or canonical `s3_key` mismatch (`TenantIsolationError`).
  - `DEADLINE_EXCEEDED` (gRPC): Query exceeded `5s` circuit breaker (`SIGKILL`ed by `circuit_breaker.py`).
  - `INTERNAL` (gRPC): Unexpected child subprocess crash/OOM/panic (`SIGKILL`ed) or IPC failure.
  - `RESOURCE_EXHAUSTED` (gRPC): Pod concurrency cap (`MAX_CONCURRENT_QUERIES`) saturated (backpressure).
  - `NOT_FOUND` (gRPC): Target `{hash}.parquet` artifact missing in both local cache and S3.
- **Tenant & S3 Path Validation**:
  - `s3_bucket` MUST match the configured allowlist, and `s3_key` MUST match `artifacts/{tenant_id}/{table_hash}.parquet`.

### `DoAction(context, action)` (`api/flight/actions.py`)
| Action Type | Request Body (JSON) | Response |
| :--- | :--- | :--- |
| `cache_warm` | `{"tenant_id": "...", "table_hash": "...", "s3_bucket": "...", "s3_key": "..."}` | `{"status": "warmed", "bytes": 1048576}` |
| `health_check` | `{}` | `{"status": "ok", "active_workers": 4}` |

---

## 3. Tier-1 Cache & S3 Storage Conventions

- **Local Disk-Backed NVMe Path**: `<CACHE_DIR>/{s3_bucket}/{tenant_id}/{table_hash}.parquet` (`10 GiB` ceiling, LRU eviction). Cache hits are strictly scoped to the requesting `tenant_id` and `s3_bucket` (or verified against S3 object access before serving) so an artifact warmed by another tenant is never served and missing S3 objects preserve `NOT_FOUND`.
- **S3 Source Path**: `s3://<bucket>/artifacts/<tenant_id>/<table_hash>.parquet` (hydrated via S3 range reads over internal VPC endpoints).

---

## 4. Prometheus Observability Contracts (`internal/telemetry/metrics.py`)

| Metric Name | Type | Labels | Purpose |
| :--- | :--- | :--- | :--- |
| `tabular_worker_active_queries` | Gauge | — | Concurrent in-flight queries (primary KEDA metric, target `>= 4` per pod) |
| `tabular_worker_query_duration_seconds` | Histogram | `cache_state` (`warm_cache`, `cold_s3`) | End-to-end query latency distribution |
| `tabular_worker_cache_hit_ratio` | Gauge | — | Local NVMe Parquet cache hit ratio |
| `tabular_worker_cache_hits_total` | Counter | — | Total local Parquet cache hits (for multi-pod PromQL aggregation) |
| `tabular_worker_cache_misses_total` | Counter | — | Total local Parquet cache misses (for multi-pod PromQL aggregation) |
| `tabular_worker_cache_bytes_used` | Gauge | — | Current local NVMe cache disk utilization |
| `tabular_worker_subprocess_recycles_total` | Counter | `reason` (`TIMEOUT`, `MEMORY_LIMIT`, `TASK_LIMIT`) | Child worker recycling events |
| `tabular_worker_flight_stream_bytes_total` | Counter | — | Total zero-copy Arrow Flight bytes streamed |
| `tabular_worker_ast_rejections_total` | Counter | `rule` | Rejected SQL statements by violated AST rule |
