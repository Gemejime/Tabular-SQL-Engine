"""Unit tests for ephemeral DuckDB sandbox execution and chunked Arrow IPC streaming."""

from __future__ import annotations

import io
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from internal.sandbox import (
    SandboxProfile,
    apply_sandbox_lock,
    execute_sandboxed_query,
    stream_query_to_ipc,
)
from test.fixtures.sample_data import write_sample_parquet


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    return write_sample_parquet(tmp_path / "sample.parquet", num_rows=500)


def test_execute_sandboxed_query_streams_record_batches(sample_parquet: Path) -> None:
    profile = SandboxProfile(memory_limit="640MiB", threads=2, temp_directory="")
    batches = list(
        execute_sandboxed_query(
            sql="SELECT region, COUNT(*) AS cnt, SUM(revenue) AS total FROM tbl GROUP BY region",
            parquet_path=sample_parquet,
            table_alias="tbl",
            profile=profile,
            batch_size=2,
        )
    )
    assert len(batches) >= 2
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    total_rows = sum(batch.num_rows for batch in batches)
    assert total_rows == 4


def test_stream_query_to_ipc_writes_valid_arrow_stream(sample_parquet: Path) -> None:
    profile = SandboxProfile(memory_limit="640MiB", threads=2, temp_directory="")
    buf = io.BytesIO()
    rows = stream_query_to_ipc(
        sql="SELECT id, region, revenue FROM tbl ORDER BY id",
        parquet_path=sample_parquet,
        table_alias="tbl",
        profile=profile,
        sink=buf,
        batch_size=128,
    )
    assert rows == 500
    buf.seek(0)
    with pa.ipc.open_stream(buf) as reader:
        streamed_batches = list(reader)
    assert len(streamed_batches) == 4  # 128 * 3 + 116
    assert sum(b.num_rows for b in streamed_batches) == 500


def test_sandbox_locks_external_access_and_configuration(sample_parquet: Path) -> None:
    profile = SandboxProfile(memory_limit="640MiB", threads=2, temp_directory="")
    conn = duckdb.connect(":memory:")
    try:
        apply_sandbox_lock(conn, sample_parquet, "tbl", profile)

        # Bound TEMP TABLE must remain readable after external access is disabled.
        count_row = conn.execute("SELECT COUNT(*) FROM tbl").fetchone()
        assert count_row == (500,)

        # Direct read_parquet must fail once enable_external_access=false.
        escaped = str(sample_parquet).replace("'", "''")
        with pytest.raises(duckdb.PermissionException):
            conn.execute(f"SELECT * FROM read_parquet('{escaped}')").fetchall()

        # Re-enabling external access must fail once lock_configuration=true.
        with pytest.raises(duckdb.InvalidInputException):
            conn.execute("SET enable_external_access = true")
    finally:
        conn.close()
