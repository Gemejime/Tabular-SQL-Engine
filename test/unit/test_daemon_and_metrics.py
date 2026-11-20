"""Unit tests for MetricsRegistry recycle initialization, S3Streamer reset, and daemon shutdown."""

from __future__ import annotations

import asyncio
import gc
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.flight as fl
import pytest
from prometheus_client import generate_latest

from internal.config.settings import Settings
from internal.flight.daemon import WorkerDaemon
from internal.flight.handlers import FlightHandlers, _OwnedFlightBatchIterator
from internal.storage.s3_streamer import S3Streamer
from internal.telemetry.metrics import MetricsRegistry


def test_metrics_registry_initializes_abnormal_recycle_series_to_zero() -> None:
    metrics = MetricsRegistry()
    exposition = generate_latest(metrics.registry).decode("utf-8")

    for reason in ("TIMEOUT", "MEMORY_LIMIT", "CRASH"):
        assert f'tabular_worker_subprocess_recycles_total{{reason="{reason}"}} 0.0' in exposition


def test_s3_streamer_close_failure_and_reset_allow_fresh_restart() -> None:
    streamer = S3Streamer()
    ctx = MagicMock()

    async def _failing_aexit(*_args: object) -> None:
        raise RuntimeError("close failed")

    ctx.__aexit__ = _failing_aexit
    streamer._client_ctx = ctx
    streamer._client = object()

    with pytest.raises(RuntimeError, match="close failed"):
        asyncio.run(streamer.close())

    assert streamer._client_ctx is None
    assert streamer._client is None

    streamer._client_ctx = object()
    streamer._client = object()
    streamer.reset_client_state()
    assert streamer._client_ctx is None
    assert streamer._client is None


def test_daemon_shutdown_bounds_flight_drain_and_completes_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    daemon = WorkerDaemon(Settings(_env_file=None))
    daemon.hydration_thread.start()

    unblock_flight = threading.Event()
    stalled_server = MagicMock()
    stalled_server.shutdown.side_effect = lambda: unblock_flight.wait(timeout=2.0)
    daemon.flight_server = stalled_server

    closed_streams_called = threading.Event()

    def _close_active_streams() -> int:
        closed_streams_called.set()
        unblock_flight.set()
        return 1

    daemon.handlers.close_active_streams = _close_active_streams  # type: ignore[method-assign]
    daemon.process_manager.close = MagicMock()  # type: ignore[method-assign]
    daemon.ops_server = SimpleNamespace(close=MagicMock())  # type: ignore[assignment]

    async def _slow_s3_close() -> None:
        await asyncio.sleep(2.0)

    daemon.s3_streamer.close = _slow_s3_close  # type: ignore[method-assign]
    daemon.s3_streamer._client_ctx = object()
    daemon.s3_streamer._client = object()

    tracing_stopped = threading.Event()
    monkeypatch.setattr("internal.flight.daemon.shutdown_tracing", tracing_stopped.set)

    # Speed up the test by using a short Flight drain timeout via _shutdown_flight_server
    orig_shutdown_flight = daemon._shutdown_flight_server
    monkeypatch.setattr(
        daemon,
        "_shutdown_flight_server",
        lambda drain_timeout_seconds=0.05: orig_shutdown_flight(0.05),
    )

    daemon.shutdown()

    assert closed_streams_called.is_set()
    daemon.process_manager.close.assert_called_once()
    assert daemon.ops_server is None
    assert not daemon.hydration_thread.loop.is_running()
    assert daemon.s3_streamer._client_ctx is None
    assert daemon.s3_streamer._client is None
    assert tracing_stopped.is_set()


def test_owned_flight_batch_iterator_distinguishes_exhaustion_from_abort() -> None:
    batch = pa.RecordBatch.from_pydict({"x": [1, 2]})

    # Normal exhaustion raises StopIteration.
    normal_stream = MagicMock()
    normal_stream.__iter__.return_value = iter([batch])
    normal_pinned = MagicMock(cache_state="hit")
    normal_sem = threading.BoundedSemaphore(1)
    normal_sem.acquire()
    normal_metrics = MetricsRegistry()
    normal_metrics.inc_active_queries()
    normal_iter = _OwnedFlightBatchIterator(
        query_stream=normal_stream,
        pinned=normal_pinned,
        metrics=normal_metrics,
        semaphore=normal_sem,
        start_time=0.0,
        request_id="req-ok",
        tenant_id="tenant-1",
        table_hash="a" * 64,
    )
    assert next(normal_iter) == batch
    with pytest.raises(StopIteration):
        next(normal_iter)
    assert normal_iter._stream_status == "ok"
    normal_pinned.release.assert_called_once()

    # Forced close aborts the stream and raises FlightCancelledError on next().
    aborted_stream = MagicMock()
    aborted_stream.__iter__.return_value = iter([batch])
    aborted_pinned = MagicMock(cache_state="hit")
    aborted_sem = threading.BoundedSemaphore(1)
    aborted_sem.acquire()
    aborted_metrics = MetricsRegistry()
    aborted_metrics.inc_active_queries()
    aborted_iter = _OwnedFlightBatchIterator(
        query_stream=aborted_stream,
        pinned=aborted_pinned,
        metrics=aborted_metrics,
        semaphore=aborted_sem,
        start_time=0.0,
        request_id="req-abort",
        tenant_id="tenant-1",
        table_hash="a" * 64,
    )
    aborted_iter.close()
    assert aborted_iter._stream_status == "closed"
    with pytest.raises(fl.FlightCancelledError):
        next(aborted_iter)
    aborted_pinned.release.assert_called_once()


def test_active_streams_registry_does_not_pin_abandoned_iterators() -> None:
    handlers = FlightHandlers(
        settings=Settings(_env_file=None),
        ast_validator=MagicMock(),
        table_resolver=MagicMock(),
        cache_manager=MagicMock(),
        s3_streamer=MagicMock(),
        process_manager=MagicMock(),
        hydration_loop=MagicMock(),
        metrics=MetricsRegistry(),
    )
    stream = MagicMock()
    stream.__iter__.return_value = iter([])
    pinned = MagicMock(cache_state="hit")
    sem = threading.BoundedSemaphore(1)
    sem.acquire()
    handlers._metrics.inc_active_queries()

    owned_iter: _OwnedFlightBatchIterator | None = _OwnedFlightBatchIterator(
        query_stream=stream,
        pinned=pinned,
        metrics=handlers._metrics,
        semaphore=sem,
        start_time=0.0,
        request_id="req-gc",
        tenant_id="tenant-1",
        table_hash="a" * 64,
        on_cleanup=handlers._unregister_stream,
    )
    with handlers._active_streams_lock:
        handlers._active_streams.add(owned_iter)

    assert len(handlers._active_streams) == 1
    owned_iter = None
    gc.collect()

    assert len(handlers._active_streams) == 0
    pinned.release.assert_called_once()
    assert handlers.close_active_streams() == 0
