"""Disposable pebble.ProcessPool supervisor with chunked Arrow IPC pipe streaming."""

from __future__ import annotations

import contextlib
import io
import os
from collections.abc import Iterator
from concurrent.futures import Future
from multiprocessing.connection import Listener
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
from pebble import ProcessPool

from internal.config.settings import Settings
from internal.pool.circuit_breaker import CircuitBreaker, QueryTimeoutError, WorkerCrashError
from internal.pool.worker_task import run_sandboxed_query_task
from internal.telemetry.logging import get_logger

_logger = get_logger("internal.pool.process_manager")


class _IpcRecvConn(Protocol):
    """Structural protocol for Connection / PipeConnection receive side."""

    def recv_bytes(self, maxlength: int | None = None, /) -> bytes: ...
    def close(self) -> None: ...


class _ConnectionRawReader(io.RawIOBase):
    """Readable binary stream backed by a multiprocessing Connection."""

    def __init__(self, conn: _IpcRecvConn) -> None:
        super().__init__()
        self._conn = conn
        self._buffer = bytearray()
        self._eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        while not self._buffer and not self._eof:
            try:
                chunk = self._conn.recv_bytes()
            except EOFError:
                self._eof = True
                break
            if not chunk:
                self._eof = True
                break
            self._buffer.extend(chunk)

        if not self._buffer:
            return 0

        n = min(len(b), len(self._buffer))
        b[:n] = self._buffer[:n]
        del self._buffer[:n]
        return n


class ProcessManager:
    """Supervises disposable DuckDB worker subprocesses via pebble.ProcessPool."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._circuit_breaker = CircuitBreaker(timeout_seconds=settings.QUERY_TIMEOUT_SECONDS)
        self._pool: ProcessPool | None = None

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Return the circuit breaker instance."""
        return self._circuit_breaker

    def start(self) -> None:
        """Initialize the disposable subprocess pool."""
        if self._pool is None:
            self._pool = ProcessPool(
                max_workers=self._settings.POOL_SIZE,
                max_tasks=self._settings.MAX_TASKS_PER_CHILD,
            )
            _logger.info(
                "Started disposable DuckDB worker pool",
                extra={
                    "extra_fields": {
                        "pool_size": self._settings.POOL_SIZE,
                        "max_tasks_per_child": self._settings.MAX_TASKS_PER_CHILD,
                        "worker_memory_limit": self._settings.worker_memory_limit_str,
                    }
                },
            )

    def close(self) -> None:
        """Shut down the subprocess pool and wait for workers to exit."""
        if self._pool is not None:
            self._pool.close()  # type: ignore[no-untyped-call]
            self._pool.join()
            self._pool = None

    def __enter__(self) -> ProcessManager:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def is_healthy(self) -> bool:
        """Return True if the worker pool is initialized and active."""
        return self._pool is not None and self._pool.active

    def stream_query(
        self,
        sql: str,
        parquet_path: Path,
        table_alias: str,
    ) -> Iterator[pa.RecordBatch]:
        """Dispatch a validated SQL query to a worker and yield Arrow RecordBatch chunks."""
        if self._pool is None:
            raise RuntimeError("ProcessManager must be started before executing queries.")

        authkey = os.urandom(32)
        listener = Listener(authkey=authkey)
        conn: _IpcRecvConn | None = None
        future: Future[int] = self._pool.schedule(
            run_sandboxed_query_task,
            args=(
                sql,
                str(parquet_path),
                table_alias,
                self._settings.worker_memory_limit_str,
                self._settings.DUCKDB_THREADS,
                "",
                self._settings.ARROW_BATCH_SIZE,
                str(listener.address),
                authkey,
            ),
            timeout=self._settings.QUERY_TIMEOUT_SECONDS,
        )

        # Close the listener if the worker fails before connecting so accept() unblocks.
        def _unblock_listener(_done_future: Future[int]) -> None:
            if _done_future.exception() is not None:
                with contextlib.suppress(OSError):
                    listener.close()

        future.add_done_callback(_unblock_listener)

        try:
            try:
                conn = listener.accept()
            except OSError as exc:
                self._circuit_breaker.await_result(future)
                raise WorkerCrashError(f"IPC accept failed: {exc}") from exc

            raw_reader = _ConnectionRawReader(conn)
            buffered_reader = io.BufferedReader(raw_reader)
            try:
                with pa.ipc.open_stream(buffered_reader) as stream_reader:
                    yield from stream_reader
            except (EOFError, OSError, pa.ArrowInvalid) as exc:
                # Re-check future first so timeouts/crashes surface as canonical errors.
                self._circuit_breaker.await_result(future)
                raise WorkerCrashError(f"Arrow IPC stream interrupted: {exc}") from exc

            self._circuit_breaker.await_result(future)
        except (QueryTimeoutError, WorkerCrashError):
            raise
        finally:
            if conn is not None:
                conn.close()
            with contextlib.suppress(OSError):
                listener.close()
