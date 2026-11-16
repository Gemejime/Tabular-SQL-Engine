"""Unit tests for typed 12-factor Settings and DuckDB memory budget calculations."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from internal.config.settings import DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES, MIB_BYTES, Settings


def test_default_memory_budget_per_worker() -> None:
    settings = Settings()
    assert settings.POOL_SIZE == 4
    assert settings.MAX_TASKS_PER_CHILD == 100
    assert settings.DUCKDB_TOTAL_MEMORY_BYTES == DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES
    assert settings.worker_memory_limit_mib == 640
    assert settings.worker_memory_limit_str == "640MiB"


def test_floored_mib_budget_for_non_power_of_two_pool_size() -> None:
    settings = Settings(POOL_SIZE=3)
    expected_mib = int(DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES / 3 // MIB_BYTES)
    assert settings.worker_memory_limit_mib == expected_mib
    assert settings.worker_memory_limit_str == f"{expected_mib}MiB"
    assert settings.worker_memory_limit_mib * settings.POOL_SIZE * MIB_BYTES <= (
        DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES
    )


def test_rejects_excessive_aggregate_duckdb_memory() -> None:
    with pytest.raises(ValidationError):
        Settings(DUCKDB_TOTAL_MEMORY_BYTES=DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES + 1)
