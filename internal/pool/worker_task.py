"""Top-level picklable worker task executed inside disposable pebble subprocesses."""

from __future__ import annotations

import io
from multiprocessing.connection import Client
from pathlib import Path
from typing import Protocol

from internal.sandbox.duckdb_runtime import stream_query_to_ipc
from internal.sandbox.security_profile import SandboxProfile


class _IpcSendConn(Protocol):
    """Structural protocol for Connection / PipeConnection send side."""

    def send_bytes(self, buf: bytes, /) -> None: ...
    def close(self) -> None: ...


class _ConnectionBinarySink(io.RawIOBase):
    """Streams Arrow IPC bytes chunk-by-chunk over a multiprocessing Connection."""

    def __init__(self, conn: _IpcSendConn) -> None:
        super().__init__()
        self._conn = conn

    def writable(self) -> bool:
        return True

    def write(self, b: bytes | bytearray | memoryview) -> int:  # type: ignore[override]
        data = bytes(b)
        if data:
            self._conn.send_bytes(data)
        return len(data)


def run_sandboxed_query_task(
    sql: str,
    parquet_path_str: str,
    table_alias: str,
    memory_limit: str,
    threads: int,
    temp_directory: str,
    batch_size: int,
    ipc_address: str,
    authkey: bytes,
) -> int:
    """Execute a validated SQL query in an ephemeral DuckDB sandbox and stream Arrow IPC.

    This function is top-level so it is safe under ``spawn`` multiprocessing.
    """
    profile = SandboxProfile(
        memory_limit=memory_limit,
        threads=threads,
        temp_directory=temp_directory,
    )
    conn = Client(ipc_address, authkey=authkey)
    try:
        sink = _ConnectionBinarySink(conn)
        total_rows = stream_query_to_ipc(
            sql=sql,
            parquet_path=Path(parquet_path_str),
            table_alias=table_alias,
            profile=profile,
            sink=sink,
            batch_size=batch_size,
        )
        # Signal clean end-of-stream to parent reader.
        conn.send_bytes(b"")
        return total_rows
    finally:
        conn.close()
