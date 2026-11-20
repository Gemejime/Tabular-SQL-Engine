"""Test fixtures for rag-tabular-analytics-worker."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_sample_parquet(path: Path, num_rows: int = 1000) -> Path:
    """Write a deterministic normalized Parquet dataset for unit, security, and chaos tests."""
    regions = ["north", "south", "east", "west"]
    categories = ["electronics", "apparel", "grocery", "home"]

    table = pa.table(
        {
            "id": pa.array(range(1, num_rows + 1), type=pa.int64()),
            "region": pa.array([regions[i % len(regions)] for i in range(num_rows)]),
            "category": pa.array([categories[i % len(categories)] for i in range(num_rows)]),
            "revenue": pa.array([float((i % 100) + 1) * 10.5 for i in range(num_rows)]),
            "quantity": pa.array([(i % 10) + 1 for i in range(num_rows)], type=pa.int32()),
        }
    )
    pq.write_table(table, path)
    return path
