"""Ephemeral DuckDB :memory: runtime with chunked PyArrow RecordBatch streaming."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from internal.sandbox.security_profile import SandboxProfile, apply_sandbox_lock


def _get_arrow_reader(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    batch_size: int,
) -> pa.RecordBatchReader:
    """Return a streaming PyArrow RecordBatchReader without full-table copies."""
    reader: pa.RecordBatchReader = conn.sql(sql).to_arrow_reader(batch_size=batch_size)
    return reader


def execute_sandboxed_query(
    sql: str,
    parquet_path: Path,
    table_alias: str,
    profile: SandboxProfile,
    *,
    batch_size: int = 8192,
) -> Iterator[pa.RecordBatch]:
    """Execute validated SQL inside an ephemeral locked DuckDB :memory: connection.

    Yields PyArrow ``RecordBatch`` chunks without full-table materialization.
    """
    conn = duckdb.connect(":memory:")
    try:
        apply_sandbox_lock(conn, parquet_path, table_alias, profile)
        reader = _get_arrow_reader(conn, sql, batch_size)
        yield from reader
    finally:
        conn.close()


def stream_query_to_ipc(
    sql: str,
    parquet_path: Path,
    table_alias: str,
    profile: SandboxProfile,
    sink: Any,
    *,
    batch_size: int = 8192,
) -> int:
    """Execute validated SQL and write Arrow IPC stream frames directly to ``sink``.

    Returns:
        Total number of rows written across all RecordBatches.
    """
    conn = duckdb.connect(":memory:")
    total_rows = 0
    try:
        apply_sandbox_lock(conn, parquet_path, table_alias, profile)
        reader = _get_arrow_reader(conn, sql, batch_size)
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)
                total_rows += batch.num_rows
        return total_rows
    finally:
        conn.close()
