# Tabular Analytics Worker (`rag-tabular-analytics-worker`) - Project Overview

## Overview

The **`rag-tabular-analytics-worker`** is a stateless, horizontally autoscaling Python 3.12+ execution pool within the horizontal compute tier. It executes structured analytical SQL queries over tabular Parquet artifacts extracted by upstream Document AI pools (`rag-document-ai-worker`), running inside sandboxed DuckDB engines with deterministic `sqlglot` AST whitelisting, hard memory ceilings, disabled external/disk access, and zero-copy Apache Arrow Flight IPC streaming to `rag-search-orchestrator`.

```
GO SEARCH ORCHESTRATOR (rag-search-orchestrator)
  │
  │ 1. Arrow Flight DoGet(Ticket: validated SQL, tenant_id, table_s3_hash)
  ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                  rag-tabular-analytics-worker (Host Daemon)                   │
│  - Apache Arrow FlightServerBase (HTTP/2 gRPC) + Ops HTTP (/healthz, /ready,  │
│    /metrics, /openapi.yaml, /docs)                                            │
│  - Deterministic sqlglot AST Validator (SELECT-only whitelist, <= 2ms)        │
│  - Tier-1 Local NVMe/tmpfs Parquet Cache ({hash}.parquet, 10 GiB, LRU)        │
│  - ProcessPoolExecutor Supervisor (max_tasks_per_child=100, 5s Circuit Breaker│
└───────┬───────────────────────────────────────────────────────┬───────────────┘
        │ 2. Cache Miss: S3 Range Read (VPC Endpoint)           │ 3. Subprocess Dispatch
        ▼                                                       ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│   S3 / Cloud Object Storage   │               │ Ephemeral Worker Subprocess   │
│ - Content-addressed Parquet   │               │ - duckdb.connect(':memory:')  │
│   artifacts ({hash}.parquet)  │               │ - enable_external_access=false│
└───────────────────────────────┘               │ - memory_limit='2.5GB'        │
                                                │ - temp_directory='', threads=2│
                                                └───────────────┬───────────────┘
                                                                │ 4. Arrow RecordBatch
                                                                ▼
                                                Streamed via Flight IPC (Zero-Copy)
```

---

## Strategic Objectives

1. **Process-Isolated Disposable Sandboxing**: Isolate DuckDB execution within dedicated ephemeral subprocesses (`max_tasks_per_child=100`) running ephemeral in-memory databases (`:memory:`), guaranteeing that native C++ faults, memory spikes, or query cancellations never crash the primary Flight server daemon.
2. **Deterministic SQL AST Whitelisting**: Enforce pre-execution AST validation via `sqlglot`, blocking DML, DDL, `PRAGMA`s, filesystem extensions, and administrative statements while relying on hard runtime timeouts (`5s`) for join and Cartesian explosion protection.
3. **Tiered Ingestion & Zero-Copy Streaming**: Stream query results directly to Go orchestrators (`rag-search-orchestrator`) via Apache Arrow Flight IPC without JSON conversion, sourcing data from local NVMe/tmpfs caches with S3 range reads.
4. **Decoupled Memory Envelopes**: Enforce explicit memory partitioning across container cgroups (`6.0 GiB` limit), the Python host daemon (`>= 2.0 GiB` headroom), and DuckDB native instances (`2.5 GiB` hard limit) to completely eliminate Linux `SIGKILL` (exit 137) evictions.
5. **Continuous Documentation & Contract Synchronization**: Keep `.ai/doc-api.md` and OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`) continuously synchronized with every code change or addition.

---

## Calibrated Quantitative SLOs & SLIs

- **Warm-Cache Analytical Query Latency**: $P95 \le 45\text{ ms}$, $P99 \le 120\text{ ms}$ for aggregation, projection, and filter operations over cached tables $\le 1,000,000\text{ rows}$.
- **Cold-Fetch S3 Hydration Query Latency**: $P95 \le 250\text{ ms}$, $P99 \le 400\text{ ms}$ for un-cached Parquet artifacts requiring S3 retrieval over internal VPC endpoints.
- **AST Validation Latency**: All non-whitelisted SQL expressions rejected within $\le 2\text{ ms}$.
- **Arrow Flight Egress Throughput**: Minimum sustained streaming rate of $250\text{ MB/sec}$ per pod for active result-set record batches.
- **Fleet Availability**: $99.9\%$ cluster availability, delegating individual query retries and semantic fallbacks to the upstream Go Search Orchestrator (`rag-search-orchestrator`).
- **Container Stability**: $0$ container `OOMKill`s under sustained maximum concurrency (soak-tested under $500$ concurrent query streams and $8$-hour `jemalloc` runs).

---

## Scope & Exclusions

### In-Scope
- **Process-Isolated DuckDB Session Pool**: Lifecycle management of ephemeral child worker processes (`pebble.ProcessPool` / `multiprocessing` supervisor, `max_tasks=100`), binding isolated in-memory instances (`duckdb.connect(":memory:")`) locked with `enable_external_access=false`, `temp_directory=''`, aggregate `memory_limit <= 2.5GB` across `POOL_SIZE` workers, and `threads=2`.
- **SQL AST Security Validator**: Deterministic pre-execution AST checks via `sqlglot` permitting only read-only `Select`, `With` (CTEs), and `Union` constructs and pre-approved function namespaces.
- **Tier-1 Artifact Local Storage Cache**: Local caching of content-addressed Parquet artifacts (`{hash}.parquet`) on disk-backed `10 GiB` NVMe `emptyDir` mounts with single-flight coalescing, in-memory LRU eviction, lock-free concurrent readers, and S3 HTTP range-read streaming on cache miss.
- **Arrow Flight IPC Ingress & Egress**: Apache Arrow Flight server daemon accepting tickets, coordinating subprocess dispatch, and streaming chunked Arrow `RecordBatch` instances.
- **Subprocess Circuit Breaking**: Cancellation monitors that `SIGKILL` and respawn child worker subprocesses exceeding execution deadlines (`5s`).
- **Operational HTTP & Telemetry Endpoints**: `/healthz`, `/ready`, `/metrics` (Prometheus), `/openapi.yaml`, and `/docs` documented via OpenAPI 3.1.

### Exclusions (Explicitly Out-of-Scope)
- **Document Layout & Parquet Extraction**: Document parsing and tabular boundary extraction from raw spreadsheets/PDFs (owned by `rag-document-ai-worker`).
- **Natural Language to SQL Generation**: Generative Text-to-SQL translation and prompt tuning (owned by upstream LLM pipelines via `rag-prompt-registry-service`).
- **Transactional Writes & Long-Term Cataloging**: Permanent schema storage and document metadata updates (owned by `rag-kb-catalog-service` and PostgreSQL).
- **Vector Search & RRF Merging**: Ranking chunk embeddings and merging vector scores (owned by `rag-search-orchestrator` and `rag-inference-serving`).
