# Security Context - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

## Threat Model & Trust Boundaries

- **Untrusted Text-to-SQL Payloads**: Queries arrive from `rag-search-orchestrator` after upstream LLM Text-to-SQL generation (`rag-prompt-registry-service`). Prompt injection or malformed SQL must be treated as hostile and blocked by deterministic `sqlglot` AST whitelisting ($\le 2\text{ ms}$) before reaching DuckDB.
- **Native Engine Isolation**: DuckDB C++ memory corruption, panics, or runaway cross-joins are isolated inside ephemeral child processes (`pebble.ProcessPool` / `multiprocessing` supervisor, `max_tasks=100`) with an isolated per-worker `5s` `SIGKILL` circuit breaker.
- **Disabled External & Disk Access**: Child DuckDB instances run strictly `:memory:`, bind only the single validated `{hash}.parquet` view, and immediately lock `enable_external_access=false` and `temp_directory=''` before executing user SQL, preventing arbitrary file reads, network exfiltration, or extension loading.
- **Tenant & S3 Path Isolation**: Every Flight ticket includes `tenant_id`, `table_hash`, `s3_bucket`, and `s3_key`. `table_resolver.py` enforces a `s3_bucket` allowlist and canonical `s3_key == f"artifacts/{tenant_id}/{table_hash}.parquet"` to prevent cross-tenant confused-deputy reads (raising gRPC `PERMISSION_DENIED`).
- **Service-to-Service Auth & S3 Read-Only VPC Access**: Arrow Flight connections from `rag-search-orchestrator` use mTLS (or signed service tokens) in addition to `NetworkPolicy`, and worker pods hold least-privilege IAM permissions (`s3:GetObject` only) routed exclusively through internal VPC S3 endpoints.

---

## Secured Logging Invariants

1. **Structured JSON Telemetry Only**:
   - Emit machine-parseable JSON logs containing correlation fields (`trace_id`, `request_id`, `tenant_id`, `table_hash`, `cache_state`, `duration_ms`, `status`).
   - Raw `print()` statements are prohibited in `cmd/`, `internal/`, and `api/`.
2. **Credentials, Tokens & Presigned URL Redaction**:
   - Strictly redact `Authorization` headers, `Bearer` tokens, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and presigned S3 query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`).
   - Log only sanitized bucket and object keys (`s3_bucket`, `s3_key`) or `{hash}.parquet` identifiers.
3. **PII/PHI & Error Trace Sanitization**:
   - Never log raw query result rows, tenant PII/PHI, or unredacted SQL string literals containing sensitive values.
   - Error responses returned via Arrow Flight (`gRPC INTERNAL` / `INVALID_ARGUMENT`) or HTTP must sanitize internal stack traces, local mount paths, and cluster hostnames.

---

## Network & Container Hardening

1. **Multi-Stage Non-Root Container with `jemalloc`**:
   - Preload `jemalloc` globally (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`).
   - Run as a non-root user, drop all Linux capabilities (`capabilities: drop: ["ALL"]`), and mount only the `10 GiB` NVMe/`tmpfs` `emptyDir` cache path as writable.
2. **Kubernetes Network Policies**:
   - **Ingress**: Restricted to `rag-search-orchestrator` on the Arrow Flight HTTP/2 gRPC port and cluster monitoring/probes on the Ops HTTP port (`/healthz`, `/ready`, `/metrics`).
   - **Egress**: Restricted to internal VPC S3 endpoints (HTTPS `443`), OpenTelemetry collector, and cluster DNS (`kube-dns`/`CoreDNS` port `53`). General external internet egress is strictly denied.

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
