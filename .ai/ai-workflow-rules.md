# AI Workflow Rules - Tabular Analytics Worker (`rag-tabular-analytics-worker`)

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in `tasks/` (including `Tabular Analytics Worker Implementation Plan.pdf` and `Production Grade Backend Concepts.pdf`) and context files in [`.ai/`](.) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the Python 3.12+ Arrow Flight + Process-Isolated DuckDB architecture and the 5 backend architectural layers documented in [`.ai/architecture.md`](architecture.md).
- Implement against these specs without inferring undocumented dependencies or deviating from Architectural Decision Records (`ADR-001` through `ADR-005`).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan before making substantial code changes, and await human confirmation when scope or contracts are non-trivial.
- **Ambiguity Escalation**: If S3 bucket/endpoint policies, Flight ticket auth claims, `sqlglot` function allowlists, or KEDA thresholds are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, deleting cached artifacts, or breaking published Flight/OpenAPI contracts.
- **Demonstrate & Verify**: Provide clear verification steps, `pytest` output, linting/typechecking results, and security audit checks after completing any unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: The [`.ai/doc-api.md`](doc-api.md) file and OpenAPI 3.1 specification (`api/openapi/openapi.yaml`) MUST be updated on **every code change or addition**, regardless of how small the modification is, if need be.
2. **Secured Logging**: All logs emitted by the Flight daemon or worker subprocesses must be structured JSON. Strictly redact presigned S3 URLs (`X-Amz-Signature`), AWS credentials, authorization tokens, bearer headers, and tenant PII/PHI before emission.
3. **Process-Isolated DuckDB Sandboxing**: Never execute DuckDB queries in the main Flight server process; dispatch to `ProcessPoolExecutor` (`max_tasks_per_child=100`) with `enable_external_access=false`, `temp_directory=''`, `memory_limit='2.5GB'`, and `5s` circuit breaking.
4. **Deterministic SQL AST Whitelisting**: Reject non-whitelisted SQL statements via `sqlglot` within $\le 2\text{ ms}$ prior to subprocess dispatch.
5. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
6. **Quality Gates**: `ruff check`, `ruff format --check`, `mypy`, `bandit`, and `pytest` must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting & Linting**: Run `ruff format --check .` and `ruff check .`.
2. **Static Type Checking**: Run `mypy cmd internal api`.
3. **Security & Dependency Audit**: Run `bandit -r cmd internal api -ll` and `pip-audit`.
4. **Automated Tests**: Ensure unit, security, integration, and chaos tests pass (`pytest -v test/`).
5. **Secured Logging Audit**: Verify no raw presigned URLs, AWS credentials, bearer tokens, `print()` calls, or PII are logged.
6. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any new or modified endpoints, Flight tickets/actions, metrics, or schemas.
7. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.
