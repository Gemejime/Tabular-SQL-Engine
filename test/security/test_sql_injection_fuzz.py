"""Security and fuzzing tests for SQL AST validation, DuckDB sandbox escapes, and logging."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from internal.telemetry.logging import JsonLogFormatter, redact_mapping, redact_text
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
    return ASTValidator(max_sql_length=8192, max_joins=5)


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
