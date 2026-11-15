# GEMINI.MD — Tabular Analytics Worker (`rag-tabular-analytics-worker`)

This repository implements **`rag-tabular-analytics-worker`**, a stateless, horizontally autoscaling Python 3.12+ Apache Arrow Flight microservice executing sandboxed DuckDB analytical queries over Parquet artifacts.

---

## Mandatory Context & Reading Order

Before planning, modifying code, or making any architectural decision, **always read and follow**:

1. [`.ai/agents.md`](.ai/agents.md) — **Primary AI Agent Persona, System Role, and Execution Protocol**
2. [`.ai.md`](.ai.md) — **Root Application Building Context & Non-Negotiable Invariants**
3. [`.ai/project-overview.md`](.ai/project-overview.md) — Product definition, strategic objectives, SLOs/SLIs, and scope boundaries
4. [`.ai/architecture.md`](.ai/architecture.md) — 5-layer backend architecture, memory partitioning (`6.0 GiB` pod / `2.5 GiB` DuckDB), and folder structure
5. [`.ai/doc-api.md`](.ai/doc-api.md) — Authoritative OpenAPI 3.1 HTTP ops endpoints, Arrow Flight IPC (`DoGet`/`DoAction`) contracts, and Prometheus metrics
6. [`.ai/code-standards.md`](.ai/code-standards.md) — Python 3.12+, `ruff`, `mypy`, `pebble.ProcessPool` / `multiprocessing` supervisor (`max_tasks=100`, keeping sibling queries serviceable after a worker timeout per `ADR-001`), `sqlglot` AST checks, and `jemalloc` hygiene
7. [`.ai/security.md`](.ai/security.md) — Threat model, DuckDB sandbox locks (`enable_external_access=false`, `temp_directory=''`), and Secured Logging invariants
8. [`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md) — Spec-driven workflow, human-in-the-loop rules, and pre-commit verification checklist
9. [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — 4-phase roadmap (Weeks 1–8), `ADR-001`–`ADR-005`, and operational runbooks

---

## Non-Negotiable Engineering Rules

1. **Continuous Doc API & OpenAPI 3.1 Synchronization**:
   - [`.ai/doc-api.md`](.ai/doc-api.md) and `api/openapi/openapi.yaml` **MUST** be kept updated on **every code change or addition**, no matter how small, whenever endpoints, Flight tickets/actions, error mappings, metrics, or schemas are touched.
2. **Secured Logging**:
   - Emit structured JSON logs only (never raw `print()` in `cmd/`, `internal/`, or `api/`).
   - Strictly redact authorization headers, bearer tokens, AWS credentials, presigned S3 query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`), and sensitive tenant PII/PHI or raw SQL literals before emission.
3. **Process-Isolated DuckDB Sandboxing**:
   - Never run DuckDB in the main Arrow Flight daemon process. Always execute inside an isolated worker pool (`pebble.ProcessPool` / `multiprocessing` supervisor, `max_tasks=100`) with `duckdb.connect(":memory:")`, `enable_external_access=false` (applied after binding the single validated `{hash}.parquet` view), `temp_directory=''`, aggregate `memory_limit <= 2.5GB` across `POOL_SIZE` workers, `threads=2`, and a `5s` per-worker `SIGKILL` circuit breaker.
   - Validate all SQL queries with `sqlglot` AST whitelisting ($\le 2\text{ ms}$) prior to subprocess dispatch.
4. **CI & Quality Gates**:
   - All changes must pass the GitHub Actions gates in [`.github/workflows/ci.yml`](.github/workflows/ci.yml): `lint-and-typecheck`, `security-and-logging-audit`, `OpenAPI 3.1 & Doc API Contract Sync`, `test-suites`, and `container-and-helm-lint`.

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
