"""Unit tests for MetricsRegistry recycle initialization, S3Streamer reset, and daemon shutdown."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from prometheus_client import generate_latest

from internal.config.settings import Settings
from internal.flight.daemon import WorkerDaemon
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
