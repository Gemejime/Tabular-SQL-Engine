"""12-factor typed configuration for rag-tabular-analytics-worker."""

from __future__ import annotations

import math
from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

GIB_BYTES = 1024 * 1024 * 1024
MIB_BYTES = 1024 * 1024
DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES = int(2.5 * GIB_BYTES)  # 2,684,354,560 bytes (2.5 GiB)


class Settings(BaseSettings):
    """Runtime settings for the tabular analytics worker."""

    model_config = SettingsConfigDict(
        env_prefix="TABULAR_WORKER_",
        case_sensitive=True,
        extra="ignore",
    )

    SERVICE_NAME: str = Field(default="rag-tabular-analytics-worker")
    LOG_LEVEL: str = Field(default="INFO")

    # Subprocess pool & circuit breaker (ADR-001)
    POOL_SIZE: int = Field(default=4, ge=1, le=32)
    MAX_TASKS_PER_CHILD: int = Field(default=100, ge=1, le=10000)
    QUERY_TIMEOUT_SECONDS: float = Field(default=5.0, gt=0.0, le=30.0)

    # DuckDB sandbox memory & concurrency limits (ADR-003)
    DUCKDB_TOTAL_MEMORY_BYTES: int = Field(
        default=DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES,
        ge=64 * MIB_BYTES,
        le=DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES,
    )
    DUCKDB_THREADS: int = Field(default=2, ge=1, le=8)
    ARROW_BATCH_SIZE: int = Field(default=8192, ge=128, le=65536)

    # SQL AST validator limits (ADR-005)
    MAX_SQL_LENGTH: int = Field(default=8192, ge=64, le=65536)
    MAX_JOINS: int = Field(default=5, ge=0, le=20)

    @model_validator(mode="after")
    def _validate_worker_memory_budget(self) -> Settings:
        """Ensure each worker receives at least 1 MiB within the aggregate DuckDB budget."""
        if self.worker_memory_limit_mib < 1:
            raise ValueError(
                "DUCKDB_TOTAL_MEMORY_BYTES divided by POOL_SIZE must be at least 1 MiB"
            )
        return self

    @property
    def worker_memory_limit_mib(self) -> int:
        """Return per-worker DuckDB memory limit floored to whole MiB."""
        return math.floor(self.DUCKDB_TOTAL_MEMORY_BYTES / self.POOL_SIZE / MIB_BYTES)

    @property
    def worker_memory_limit_str(self) -> str:
        """Return per-worker DuckDB memory_limit pragma string (e.g. '640MiB')."""
        return f"{self.worker_memory_limit_mib}MiB"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached singleton settings instance."""
    return Settings()
