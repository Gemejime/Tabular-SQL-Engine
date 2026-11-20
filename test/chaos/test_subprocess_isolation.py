"""Chaos tests for subprocess isolation, 5s timeout circuit breaking, and max_tasks recycling."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import threading
import time
from pathlib import Path

import pytest

from internal.config.settings import Settings
from internal.pool import ProcessManager, QueryTimeoutError, RecycleReason, WorkerCrashError
from test.fixtures.sample_data import write_sample_parquet


@pytest.fixture
def chaos_parquet(tmp_path: Path) -> Path:
    return write_sample_parquet(tmp_path / "chaos.parquet", num_rows=1000)


def test_timeout_circuit_breaker_kills_slow_query_and_keeps_siblings_serviceable(
    chaos_parquet: Path,
) -> None:
    # Exercise the 5.0s production circuit breaker with real cross-process termination
    # (SIGKILL on POSIX / TerminateProcess on Windows).
    settings = Settings(POOL_SIZE=1, MAX_TASKS_PER_CHILD=100, QUERY_TIMEOUT_SECONDS=5.0)
    with ProcessManager(settings) as manager:
        # Warm the single worker so we can identify its PID before the runaway query.
        list(manager.stream_query("SELECT COUNT(*) AS cnt FROM tbl", chaos_parquet, "tbl"))
        initial_pids = {proc.pid for proc in mp.active_children() if proc.pid is not None}
        assert len(initial_pids) == 1
        timed_out_pid = next(iter(initial_pids))

        # 1000^4 = 1,000,000,000,000 row cross-join will exceed 3.0s in DuckDB.
        runaway_sql = (
            "SELECT COUNT(*) AS cnt FROM tbl a CROSS JOIN tbl b CROSS JOIN tbl c CROSS JOIN tbl d"
        )
        with pytest.raises(QueryTimeoutError):
            list(manager.stream_query(runaway_sql, chaos_parquet, "tbl"))

        counts = manager.circuit_breaker.get_recycle_counts()
        assert counts[RecycleReason.TIMEOUT] >= 1

        # Bounded check that the timed-out child process has exited before pool teardown.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            live_pids = {
                proc.pid
                for proc in mp.active_children()
                if proc.pid is not None and proc.is_alive()
            }
            if timed_out_pid not in live_pids:
                break
            time.sleep(0.05)
        else:
            pytest.fail(f"Timed-out worker PID {timed_out_pid} did not exit within 5.0s")

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
    settings = Settings(POOL_SIZE=2, MAX_TASKS_PER_CHILD=2, QUERY_TIMEOUT_SECONDS=15.0)
    with ProcessManager(settings) as manager:
        # Startup warm-up before capturing recycle-count baseline and worker PIDs.
        warmup = list(
            manager.stream_query("SELECT COUNT(*) AS total_rows FROM tbl", chaos_parquet, "tbl")
        )
        assert warmup[0].column("total_rows")[0].as_py() == 1000
        baseline_recycles = manager.circuit_breaker.get_recycle_counts()[RecycleReason.TASK_LIMIT]
        initial_pids = {
            proc.pid for proc in mp.active_children() if proc.pid is not None and proc.is_alive()
        }
        observed_pids = set(initial_pids)

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
            observed_pids.update(
                proc.pid
                for proc in mp.active_children()
                if proc.pid is not None and proc.is_alive()
            )
        counts = manager.circuit_breaker.get_recycle_counts()
        assert counts[RecycleReason.TASK_LIMIT] > baseline_recycles
        assert observed_pids - initial_pids


def test_worker_crash_or_forced_termination_does_not_break_pool(chaos_parquet: Path) -> None:
    settings = Settings(POOL_SIZE=1, MAX_TASKS_PER_CHILD=100, QUERY_TIMEOUT_SECONDS=15.0)
    with ProcessManager(settings) as manager:
        parent_pid = os.getpid()
        assert manager.is_healthy()
        batches = list(manager.stream_query("SELECT COUNT(*) AS c FROM tbl", chaos_parquet, "tbl"))
        assert batches[0].column("c")[0].as_py() == 1000

        worker_pids = [
            proc.pid for proc in mp.active_children() if proc.pid is not None and proc.is_alive()
        ]
        assert len(worker_pids) == 1
        target_pid = worker_pids[0]

        query_scheduled = threading.Event()
        scheduled_futures: list[object] = []
        assert manager._pool is not None  # noqa: SLF001
        orig_schedule = manager._pool.schedule  # noqa: SLF001

        def _signaling_schedule(*args: object, **kwargs: object) -> object:
            future = orig_schedule(*args, **kwargs)
            scheduled_futures.append(future)
            query_scheduled.set()
            return future

        manager._pool.schedule = _signaling_schedule  # type: ignore[method-assign]  # noqa: SLF001

        def _terminate_worker_soon() -> None:
            if not query_scheduled.wait(timeout=5.0):
                return
            future = scheduled_futures[0]
            deadline = time.monotonic() + 5.0
            while (
                not future.running()  # type: ignore[attr-defined]
                and not future.done()  # type: ignore[attr-defined]
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            if future.running():  # type: ignore[attr-defined]
                os.kill(target_pid, signal.SIGTERM)

        killer = threading.Thread(target=_terminate_worker_soon, daemon=True)
        killer.start()
        try:
            with pytest.raises(WorkerCrashError):
                list(
                    manager.stream_query(
                        "SELECT COUNT(*) AS cnt FROM tbl a CROSS JOIN tbl b CROSS JOIN tbl c",
                        chaos_parquet,
                        "tbl",
                    )
                )
        finally:
            manager._pool.schedule = orig_schedule  # type: ignore[method-assign]  # noqa: SLF001
            killer.join(timeout=2.0)

        counts = manager.circuit_breaker.get_recycle_counts()
        assert counts[RecycleReason.CRASH] + counts[RecycleReason.MEMORY_LIMIT] >= 1

        # Subsequent query must succeed on a replacement worker without affecting parent.
        batches = list(manager.stream_query("SELECT COUNT(*) AS c FROM tbl", chaos_parquet, "tbl"))
        assert batches[0].column("c")[0].as_py() == 1000
        assert os.getpid() == parent_pid
