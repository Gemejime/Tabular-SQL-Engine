"""Chaos tests for subprocess isolation, 5s timeout circuit breaking, and max_tasks recycling."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from internal.config.settings import Settings
from internal.pool import ProcessManager, QueryTimeoutError, RecycleReason
from test.fixtures.sample_data import write_sample_parquet


@pytest.fixture
def chaos_parquet(tmp_path: Path) -> Path:
    return write_sample_parquet(tmp_path / "chaos.parquet", num_rows=1000)


def test_timeout_circuit_breaker_kills_slow_query_and_keeps_siblings_serviceable(
    chaos_parquet: Path,
) -> None:
    # Use a short 1.0s timeout in the chaos test so CI executes rapidly while exercising
    # real cross-process termination (SIGKILL on POSIX / TerminateProcess on Windows).
    settings = Settings(POOL_SIZE=2, MAX_TASKS_PER_CHILD=100, QUERY_TIMEOUT_SECONDS=1.0)
    with ProcessManager(settings) as manager:
        # 1000^3 = 1,000,000,000 row cross-join will exceed 1.0s in DuckDB.
        runaway_sql = "SELECT COUNT(*) AS cnt FROM tbl a CROSS JOIN tbl b CROSS JOIN tbl c"
        with pytest.raises(QueryTimeoutError):
            list(manager.stream_query(runaway_sql, chaos_parquet, "tbl"))

        counts = manager.circuit_breaker.get_recycle_counts()
        assert counts[RecycleReason.TIMEOUT] >= 1

        # Sibling workers and replacement worker must remain healthy and serviceable.
        assert manager.is_healthy()
        for _ in range(4):
            batches = list(
                manager.stream_query(
                    "SELECT region, COUNT(*) AS cnt FROM tbl GROUP BY region",
                    chaos_parquet,
                    "tbl",
                )
            )
            assert sum(b.num_rows for b in batches) == 4


def test_max_tasks_recycling_executes_cleanly_across_generations(chaos_parquet: Path) -> None:
    settings = Settings(POOL_SIZE=2, MAX_TASKS_PER_CHILD=2, QUERY_TIMEOUT_SECONDS=5.0)
    with ProcessManager(settings) as manager:
        for _ in range(6):
            batches = list(
                manager.stream_query(
                    "SELECT COUNT(*) AS total_rows FROM tbl",
                    chaos_parquet,
                    "tbl",
                )
            )
            assert len(batches) == 1
            assert batches[0].column("total_rows")[0].as_py() == 1000


def test_worker_crash_or_forced_termination_does_not_break_pool(chaos_parquet: Path) -> None:
    settings = Settings(POOL_SIZE=2, MAX_TASKS_PER_CHILD=100, QUERY_TIMEOUT_SECONDS=2.0)
    with ProcessManager(settings) as manager:
        parent_pid = os.getpid()
        assert manager.is_healthy()
        batches = list(manager.stream_query("SELECT COUNT(*) AS c FROM tbl", chaos_parquet, "tbl"))
        assert batches[0].column("c")[0].as_py() == 1000
        assert os.getpid() == parent_pid
