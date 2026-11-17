"""Security and fuzzing tests for SQL AST validation, DuckDB sandbox escapes, and logging."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from pathlib import Path

import duckdb
import pyarrow.flight as fl
import pytest

from internal.config.settings import Settings
from internal.flight.handlers import FlightHandlers
from internal.pool.process_manager import ProcessManager
from internal.sandbox.security_profile import SandboxProfile, apply_sandbox_lock
from internal.storage.cache_manager import CacheManager
from internal.storage.s3_streamer import S3Streamer
from internal.storage.table_resolver import TableResolver
from internal.telemetry.logging import JsonLogFormatter, redact_mapping, redact_text
from internal.telemetry.metrics import MetricsRegistry
from internal.validator import (
    ASTSyntaxError,
    ASTValidator,
    DisallowedFunctionError,
    DisallowedStatementError,
    DisallowedTableReferenceError,
    ValidationError,
)
from test.fixtures.sample_data import write_sample_parquet


@pytest.fixture(scope="module")
def validator() -> ASTValidator:
    instance = ASTValidator(max_sql_length=8192, max_joins=5)
    # Warm up sqlglot DuckDB parser/caches before steady-state latency checks.
    instance.validate("SELECT COUNT(*) FROM tbl", allowed_table_alias="tbl")
    return instance


@pytest.mark.parametrize(
    "malicious_sql",
    [
        "INSERT INTO tbl VALUES (1, 'a', 'b', 1.0, 1)",
        "UPDATE tbl SET revenue = 0",
        "DELETE FROM tbl WHERE id = 1",
        "DROP TABLE tbl",
        "ALTER TABLE tbl ADD COLUMN x INT",
        "CREATE TABLE evil (id INT)",
        "PRAGMA table_info('tbl')",
        "PRAGMA version",
        "SET memory_limit = '10GB'",
        "RESET memory_limit",
        "SUMMARIZE tbl",
        "DESCRIBE tbl",
        "SHOW TABLES",
        "PIVOT tbl ON region USING SUM(revenue)",
        "COPY tbl TO '/tmp/exfil.parquet'",
        "ATTACH '/tmp/other.db' AS other",
        "DETACH other",
        "LOAD httpfs",
        "INSTALL httpfs",
    ],
)
def test_blocks_banned_dml_ddl_and_admin_statements(
    validator: ASTValidator,
    malicious_sql: str,
) -> None:
    with pytest.raises((DisallowedStatementError, ValidationError)):
        validator.validate(malicious_sql, allowed_table_alias="tbl")


@pytest.mark.parametrize(
    "malicious_sql",
    [
        "SELECT * FROM read_parquet('/etc/passwd')",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM read_csv_auto('/tmp/secret.csv')",
        "SELECT * FROM scan_parquet('s3://bucket/key.parquet')",
        "SELECT glob('*') FROM tbl",
        "SELECT getenv('AWS_SECRET_ACCESS_KEY') FROM tbl",
        "SELECT current_setting('memory_limit') FROM tbl",
        "SELECT duckdb_tables() FROM tbl",
        "SELECT duckdb_settings() FROM tbl",
        "SELECT httpfs_get('https://evil.example.com') FROM tbl",
        "SELECT write_csv(tbl, '/tmp/out.csv') FROM tbl",
        "SELECT custom_unapproved_udf(revenue) FROM tbl",
    ],
)
def test_blocks_duckdb_io_system_and_unapproved_anonymous_functions(
    validator: ASTValidator,
    malicious_sql: str,
) -> None:
    with pytest.raises((DisallowedFunctionError, DisallowedTableReferenceError, ValidationError)):
        validator.validate(malicious_sql, allowed_table_alias="tbl")


def test_blocks_file_literal_table_and_multi_statement_injection(
    validator: ASTValidator,
    tmp_path: Path,
) -> None:
    parquet = write_sample_parquet(tmp_path / "secret.parquet", num_rows=10)
    with pytest.raises(DisallowedTableReferenceError):
        validator.validate(f"SELECT * FROM '{parquet}'", allowed_table_alias="tbl")

    with pytest.raises(ASTSyntaxError):
        validator.validate("SELECT COUNT(*) FROM tbl; DROP TABLE tbl;", allowed_table_alias="tbl")


def test_structured_logging_redacts_secrets_and_presigned_urls() -> None:
    raw = (
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.secret.sig "
        "url=https://s3.amazonaws.com/b/k?X-Amz-Signature=deadbeef&X-Amz-Credential=abc "
        "key=AKIAIOSFODNN7EXAMPLE"
    )
    sanitized = redact_text(raw)
    assert "eyJhbGciOiJIUzI1NiJ9" not in sanitized
    assert "deadbeef" not in sanitized
    assert "AKIAIOSFODNN7EXAMPLE" not in sanitized
    assert "[REDACTED]" in sanitized

    redacted_map = redact_mapping(
        {
            "tenant_id": "t-1",
            "aws_secret_access_key": "supersecret",
            "sql": "SELECT * FROM tbl WHERE ssn = '123-45-6789'",
        }
    )
    assert redacted_map["tenant_id"] == "t-1"
    assert redacted_map["aws_secret_access_key"] == "[REDACTED]"
    assert redacted_map["sql"] == "[REDACTED]"

    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request token Bearer abc123xyz",
        args=(),
        exc_info=None,
    )
    parsed = json.loads(formatter.format(record))
    assert parsed["level"] == "INFO"
    assert "abc123xyz" not in parsed["message"]


def _generate_fuzz_payloads(seed: int = 42, count: int = 120) -> list[str]:
    """Generate deterministic obfuscated SQL injection and AST bypass payloads."""
    rng = random.Random(seed)  # noqa: S311
    banned_calls = [
        "read_parquet('/etc/passwd')",
        "read_csv('/etc/passwd')",
        "read_csv_auto('/tmp/secret.csv')",
        "scan_parquet('s3://evil-bucket/key.parquet')",
        "glob('/var/run/*')",
        "getenv('AWS_SECRET_ACCESS_KEY')",
        "current_setting('memory_limit')",
        "duckdb_tables()",
        "duckdb_settings()",
        "httpfs_get('https://evil.example.com')",
        "write_csv(tbl, '/tmp/out.csv')",
        "copy(tbl)",
    ]
    wrappers = [
        lambda call: f"WITH cte AS (SELECT {call} AS x FROM tbl) SELECT x FROM cte",
        lambda call: f"SELECT COUNT(*) FROM tbl UNION ALL SELECT 1 FROM {call}",
        lambda call: f"SELECT COUNT(*) FROM tbl INTERSECT SELECT 1 WHERE {call} IS NOT NULL",
        lambda call: f"SELECT COUNT(*) FROM tbl EXCEPT SELECT 1 FROM {call}",
        lambda call: f"SELECT (SELECT {call} FROM tbl LIMIT 1) AS leaked FROM tbl",
        lambda call: f"SELECT * FROM (SELECT * FROM {call}) sub",
        lambda call: f"SELECT /* bypass */ COUNT(*) FROM tbl; {call}",
        lambda call: f"SELECT * FROM '/tmp/{rng.randint(1, 999)}.parquet'",
        lambda call: f"SELECT * FROM tbl WHERE id = {'(' * 12}{call}{')' * 12}",
    ]
    comments = ["/*+ hint */ ", "/* fuzz */\n", "-- line comment\n", "  \t\n "]

    payloads: list[str] = []
    for i in range(count):
        call = banned_calls[i % len(banned_calls)]
        if rng.random() < 0.5:
            call = "".join(c.upper() if rng.random() < 0.5 else c.lower() for c in call)
        prefix = rng.choice(comments)
        wrapper = wrappers[i % len(wrappers)]
        payloads.append(f"{prefix}{wrapper(call)}")
    return payloads


def test_deterministic_sql_fuzzing_blocks_all_bypasses_within_2ms(
    validator: ASTValidator,
) -> None:
    payloads = _generate_fuzz_payloads(seed=42, count=120)
    total_ms = 0.0
    for payload in payloads:
        start = time.perf_counter()
        with pytest.raises(ValidationError):
            validator.validate(payload, allowed_table_alias="tbl")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        total_ms += elapsed_ms

    avg_ms = total_ms / len(payloads)
    assert avg_ms <= 2.0, f"Average AST validation latency {avg_ms:.3f}ms exceeded 2.0ms budget"


def test_flight_handlers_increment_ast_rejection_metric_on_fuzz_payloads(
    tmp_path: Path,
) -> None:
    settings = Settings(
        CACHE_DIR=tmp_path / "cache",
        S3_BUCKET_ALLOWLIST=["rag-tabular-artifacts"],
        POOL_SIZE=1,
        MAX_CONCURRENT_QUERIES=2,
    )
    metrics = MetricsRegistry()
    ast_validator = ASTValidator(max_sql_length=8192, max_joins=5)
    table_resolver = TableResolver(settings.S3_BUCKET_ALLOWLIST)
    cache_manager = CacheManager(tmp_path / "cache", max_bytes=1024 * 1024, metrics=metrics)
    s3_streamer = S3Streamer(client=object())
    process_manager = ProcessManager(settings, metrics=metrics)
    loop = asyncio.new_event_loop()
    try:
        handlers = FlightHandlers(
            settings=settings,
            ast_validator=ast_validator,
            table_resolver=table_resolver,
            cache_manager=cache_manager,
            s3_streamer=s3_streamer,
            process_manager=process_manager,
            hydration_loop=loop,
            metrics=metrics,
        )
        payloads = _generate_fuzz_payloads(seed=7, count=25)
        table_hash = "a" * 64
        for idx, sql in enumerate(payloads):
            ticket_bytes = json.dumps(
                {
                    "request_id": f"req-fuzz-{idx}",
                    "tenant_id": "tenant-fuzz",
                    "sql": sql,
                    "table_alias": "tbl",
                    "table_hash": table_hash,
                    "s3_bucket": "rag-tabular-artifacts",
                    "s3_key": f"artifacts/tenant-fuzz/{table_hash}.parquet",
                }
            ).encode("utf-8")
            with pytest.raises(fl.FlightError):
                handlers.handle_do_get(ticket_bytes)

        rendered = metrics.render_prometheus().decode("utf-8")
        assert "tabular_worker_ast_rejections_total{" in rendered
        total_rejections = sum(
            sample.value
            for metric in metrics.registry.collect()
            if metric.name == "tabular_worker_ast_rejections"
            for sample in metric.samples
            if sample.name == "tabular_worker_ast_rejections_total"
        )
        assert total_rejections == len(payloads)
    finally:
        loop.close()


@pytest.mark.parametrize(
    "escape_sql",
    [
        "SELECT * FROM read_parquet('/etc/passwd')",
        "SELECT * FROM read_csv('/etc/passwd')",
        "PRAGMA memory_limit='10GB'",
        "SET memory_limit = '10GB'",
        "ATTACH '/tmp/other.db' AS extra_db",
        "DETACH memory",
        "COPY tbl TO '/tmp/exfil.parquet'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SELECT getenv('PATH')",
    ],
)
def test_duckdb_sandbox_lock_blocks_direct_engine_escapes(
    tmp_path: Path,
    escape_sql: str,
) -> None:
    parquet_path = write_sample_parquet(tmp_path / "sandbox.parquet", num_rows=10)
    conn = duckdb.connect(":memory:")
    try:
        apply_sandbox_lock(conn, parquet_path, "tbl", SandboxProfile())
        with pytest.raises(duckdb.Error):
            conn.execute(escape_sql).fetchall()
    finally:
        conn.close()
