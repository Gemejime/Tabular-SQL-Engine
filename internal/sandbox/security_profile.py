"""DuckDB sandbox security profile: single-artifact materialization and hard locks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from internal.validator.rules import SAFE_IDENTIFIER_RE


@dataclass(frozen=True, slots=True)
class SandboxProfile:
    """Immutable DuckDB sandbox configuration applied to every child worker connection."""

    memory_limit: str = "640MiB"
    threads: int = 2
    temp_directory: str = ""


def apply_sandbox_lock(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    table_alias: str,
    profile: SandboxProfile,
) -> None:
    """Materialize a single validated Parquet artifact and lock down DuckDB external access.

    The artifact is materialized via ``CREATE TEMP TABLE`` before disabling external access
    so read-only queries execute entirely in memory without filesystem/network capabilities.
    """
    if not SAFE_IDENTIFIER_RE.match(table_alias):
        raise ValueError(f"Unsafe table alias: {table_alias!r}")

    resolved_path = parquet_path.resolve(strict=True)
    if not resolved_path.is_file():
        raise FileNotFoundError(f"Parquet artifact is not a regular file: {resolved_path}")

    escaped_tmp = profile.temp_directory.replace("'", "''")
    conn.execute(f"SET temp_directory = '{escaped_tmp}'")
    escaped_mem = profile.memory_limit.replace("'", "''")
    conn.execute(f"SET memory_limit = '{escaped_mem}'")
    conn.execute(f"SET threads = {int(profile.threads)}")

    escaped_path = str(resolved_path).replace("'", "''")
    conn.execute(
        f'CREATE TEMP TABLE "{table_alias}" '  # noqa: S608  # nosec B608
        f"AS SELECT * FROM read_parquet('{escaped_path}')"
    )

    # Lock external access and configuration immediately after materializing the single artifact.
    conn.execute("SET enable_external_access = false")
    conn.execute("SET lock_configuration = true")
