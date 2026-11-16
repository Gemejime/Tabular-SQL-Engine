"""Disposable pebble.ProcessPool supervisor with chunked Arrow IPC pipe streaming."""

from __future__ import annotations

import contextlib
import io
import os
from collections.abc import Iterator
from concurrent.futures import Future
from multiprocessing.connection import Client, Listener
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
from pebble import ProcessPool

from internal.config.settings import Settings
from internal.pool.circuit_breaker import (
    CircuitBreaker,
    QueryTimeoutError,
    RecycleReason,
    WorkerCrashError,
)
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


class QueryStream:
    """Iterable Arrow RecordBatch stream carrying the IPC schema and owning IPC cleanup."""

    def __init__(
        self,
        *,
        schema: pa.Schema,
        stream_reader: pa.ipc.RecordBatchStreamReader,
        conn: _IpcRecvConn,
        listener: Listener,
        future: Future[int],
        circuit_breaker: CircuitBreaker,
    ) -> None:
        self.schema = schema
        self._stream_reader = stream_reader
        self._conn = conn
        self._listener = listener
        self._future = future
        self._circuit_breaker = circuit_breaker
        self._closed = False
        self._iterated = False

    def close(self) -> None:
        """Close the Arrow stream reader, IPC connection, and listener idempotently."""
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            self._stream_reader.close()
        with contextlib.suppress(OSError):
            self._conn.close()
        with contextlib.suppress(OSError):
            self._listener.close()

    def __del__(self) -> None:
        self.close()

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        if self._iterated:
            raise RuntimeError("QueryStream can only be iterated once.")
        self._iterated = True
        try:
            try:
                yield from self._stream_reader
            except (EOFError, OSError, pa.ArrowInvalid) as exc:
                self._circuit_breaker.await_result(self._future)
                raise WorkerCrashError(f"Arrow IPC stream interrupted: {exc}") from exc
            self._circuit_breaker.await_result(self._future)
        finally:
            self.close()


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
    ) -> QueryStream:
        """Dispatch a validated SQL query to a worker and return a schema-aware batch stream."""
        if self._pool is None:
            raise RuntimeError("ProcessManager must be started before executing queries.")

        import threading

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

        accept_done = threading.Event()
        aborted = threading.Event()
        accepted_conn: list[_IpcRecvConn | None] = [None]
        accept_error: list[BaseException | None] = [None]

        def _accept_worker() -> None:
            try:
                incoming = listener.accept()
                if aborted.is_set():
                    with contextlib.suppress(OSError):
                        incoming.close()
                    return
                accepted_conn[0] = incoming
            except BaseException as exc:
                accept_error[0] = exc
            finally:
                accept_done.set()

        accept_thread = threading.Thread(
            target=_accept_worker,
            name="ipc-listener-accept",
            daemon=True,
        )
        accept_thread.start()

        def _wake_listener() -> None:
            aborted.set()
            with contextlib.suppress(OSError, EOFError, Exception):
                wake_conn = Client(listener.address, authkey=authkey)
                wake_conn.close()
            with contextlib.suppress(OSError):
                listener.close()

        def _on_future_done(_done_future: Future[int]) -> None:
            if not accept_done.is_set() and (
                _done_future.cancelled() or _done_future.exception() is not None
            ):
                _wake_listener()

        future.add_done_callback(_on_future_done)

        handed_off = False
        try:
            if not accept_done.wait(timeout=self._settings.QUERY_TIMEOUT_SECONDS):
                _wake_listener()
                accept_thread.join(timeout=1.0)
                try:
                    self._circuit_breaker.await_result(future)
                except QueryTimeoutError:
                    raise
                except (OSError, EOFError, WorkerCrashError) as exc:
                    self._circuit_breaker.record_recycle(RecycleReason.TIMEOUT)
                    raise QueryTimeoutError(
                        "Worker failed to connect within "
                        f"{self._settings.QUERY_TIMEOUT_SECONDS:.1f}s."
                    ) from exc
                self._circuit_breaker.record_recycle(RecycleReason.TIMEOUT)
                raise QueryTimeoutError(
                    f"Worker failed to connect within {self._settings.QUERY_TIMEOUT_SECONDS:.1f}s."
                )

            if accept_error[0] is not None:
                self._circuit_breaker.await_result(future)
                raise WorkerCrashError(f"IPC accept failed: {accept_error[0]}") from accept_error[0]

            conn = accepted_conn[0]
            if conn is None:
                self._circuit_breaker.await_result(future)
                raise WorkerCrashError("IPC accept aborted before worker connected.")

            raw_reader = _ConnectionRawReader(conn)
            buffered_reader = io.BufferedReader(raw_reader)
            try:
                stream_reader = pa.ipc.open_stream(buffered_reader)
            except (EOFError, OSError, pa.ArrowInvalid) as exc:
                self._circuit_breaker.await_result(future)
                raise WorkerCrashError(f"Arrow IPC stream interrupted: {exc}") from exc

            handed_off = True
            return QueryStream(
                schema=stream_reader.schema,
                stream_reader=stream_reader,
                conn=conn,
                listener=listener,
                future=future,
                circuit_breaker=self._circuit_breaker,
            )
        except (QueryTimeoutError, WorkerCrashError):
            raise
        finally:
            if not handed_off:
                if conn is not None:
                    with contextlib.suppress(OSError):
                        conn.close()
                with contextlib.suppress(OSError):
                    listener.close()
