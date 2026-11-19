"""Unit tests for typed 12-factor Settings, .env loading, and DuckDB memory budgets."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

from internal.config.settings import (
    DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES,
    MIB_BYTES,
    Settings,
    get_settings,
)
from internal.telemetry.logging import JsonLogFormatter, configure_logging, get_logger

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _clear_tabular_worker_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import os

    for key in list(os.environ):
        if key.startswith("TABULAR_WORKER_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("internal.config.settings._PROJECT_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()


def test_default_memory_budget_per_worker() -> None:
    settings = Settings(_env_file=None)
    assert settings.POOL_SIZE == 4
    assert settings.MAX_TASKS_PER_CHILD == 100
    assert settings.DUCKDB_TOTAL_MEMORY_BYTES == DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES
    assert settings.worker_memory_limit_mib == 640
    assert settings.worker_memory_limit_str == "640MiB"


def test_floored_mib_budget_for_non_power_of_two_pool_size() -> None:
    settings = Settings(_env_file=None, POOL_SIZE=3)
    expected_mib = int(DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES / 3 // MIB_BYTES)
    assert settings.worker_memory_limit_mib == expected_mib
    assert settings.worker_memory_limit_str == f"{expected_mib}MiB"
    assert settings.worker_memory_limit_mib * settings.POOL_SIZE * MIB_BYTES <= (
        DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES
    )


def test_rejects_excessive_aggregate_duckdb_memory() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, DUCKDB_TOTAL_MEMORY_BYTES=DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES + 1)


def test_dotenv_values_affect_settings(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "TABULAR_WORKER_SERVICE_NAME=custom-analytics-worker",
                "TABULAR_WORKER_LOG_LEVEL=DEBUG",
                "TABULAR_WORKER_POOL_SIZE=2",
                "TABULAR_WORKER_FLIGHT_PORT=9915",
                "TABULAR_WORKER_S3_BUCKET_ALLOWLIST=bucket-a,bucket-b",
                "TABULAR_WORKER_S3_ENDPOINT_URL=",
                "TABULAR_WORKER_WORKER_PROCESS_RLIMIT_BYTES=",
                "TABULAR_WORKER_OTEL_ENABLED=true",
            ]
        ),
        encoding="utf-8",
    )

    first = get_settings()
    assert first.SERVICE_NAME == "custom-analytics-worker"
    assert first.LOG_LEVEL == "DEBUG"
    assert first.POOL_SIZE == 2
    assert first.FLIGHT_PORT == 9915
    assert first.S3_BUCKET_ALLOWLIST == ["bucket-a", "bucket-b"]
    assert first.S3_ENDPOINT_URL is None
    assert first.WORKER_PROCESS_RLIMIT_BYTES is None
    assert first.OTEL_ENABLED is True
    assert first.worker_memory_limit_mib == 1280

    # Updating .env changes subsequent Settings loads
    env_path.write_text(
        "\n".join(
            [
                "TABULAR_WORKER_POOL_SIZE=8",
                'TABULAR_WORKER_S3_BUCKET_ALLOWLIST=["bucket-c"]',
            ]
        ),
        encoding="utf-8",
    )
    get_settings.cache_clear()
    updated = get_settings()
    assert updated.POOL_SIZE == 8
    assert updated.S3_BUCKET_ALLOWLIST == ["bucket-c"]
    assert updated.worker_memory_limit_mib == 320


def test_os_env_overrides_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("TABULAR_WORKER_POOL_SIZE=2\n", encoding="utf-8")
    monkeypatch.setenv("TABULAR_WORKER_POOL_SIZE", "6")

    settings = Settings()
    assert settings.POOL_SIZE == 6


def test_env_example_covers_all_settings_fields_and_loads_cleanly() -> None:
    env_example_path = _REPO_ROOT / ".env.example"
    assert env_example_path.is_file()

    defined_keys: set[str] = set()
    for raw_line in env_example_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        defined_keys.add(key)

    expected_keys = {f"TABULAR_WORKER_{field}" for field in Settings.model_fields}
    missing = expected_keys - defined_keys
    assert not missing, f"Missing variables in .env.example: {sorted(missing)}"

    loaded = Settings(_env_file=env_example_path)
    assert loaded.POOL_SIZE == 4
    assert loaded.S3_BUCKET_ALLOWLIST == []
    assert loaded.S3_ENDPOINT_URL is None


def test_configure_logging_applies_level_and_service_name() -> None:
    logger = get_logger("internal.test_env_logging")
    try:
        configure_logging(level="DEBUG", service_name="custom-worker")
        assert logger.level == logging.DEBUG
        assert any(
            isinstance(handler.formatter, JsonLogFormatter)
            and handler.formatter._service_name == "custom-worker"
            for handler in logger.handlers
        )
    finally:
        configure_logging(level="INFO", service_name="rag-tabular-analytics-worker")
