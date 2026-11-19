"""12-factor typed configuration for rag-tabular-analytics-worker."""

from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

GIB_BYTES = 1024 * 1024 * 1024
MIB_BYTES = 1024 * 1024
DEFAULT_DUCKDB_TOTAL_MEMORY_BYTES = int(2.5 * GIB_BYTES)  # 2,684,354,560 bytes (2.5 GiB)
DEFAULT_CACHE_MAX_BYTES = 10 * GIB_BYTES  # 10 GiB NVMe emptyDir ceiling (ADR-002)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Runtime settings for the tabular analytics worker."""

    model_config = SettingsConfigDict(
        env_prefix="TABULAR_WORKER_",
        env_file=(_PROJECT_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
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

    # Arrow Flight & HTTP Ops Server endpoints
    FLIGHT_HOST: str = Field(default="0.0.0.0")  # noqa: S104  # nosec B104
    FLIGHT_PORT: int = Field(default=8815, ge=0, le=65535)
    HTTP_HOST: str = Field(default="0.0.0.0")  # noqa: S104  # nosec B104
    HTTP_PORT: int = Field(default=8080, ge=0, le=65535)

    # Tier-1 Disk-Backed NVMe Parquet Cache (ADR-002)
    CACHE_DIR: Path = Field(default=Path("/var/cache/tabular-worker"))
    CACHE_MAX_BYTES: int = Field(default=DEFAULT_CACHE_MAX_BYTES, ge=MIB_BYTES)

    # S3 VPC Endpoint Streaming & Tenant Allowlist
    S3_BUCKET_ALLOWLIST: Annotated[list[str], NoDecode] = Field(default_factory=list)
    S3_ENDPOINT_URL: str | None = Field(default=None)
    S3_REGION: str = Field(default="us-east-1")
    S3_CHUNK_SIZE_BYTES: int = Field(default=8 * MIB_BYTES, ge=64 * 1024, le=128 * MIB_BYTES)
    S3_TIMEOUT_SECONDS: float = Field(default=4.0, gt=0.0, le=60.0)

    # Concurrency & KEDA Backpressure (ADR-004)
    MAX_CONCURRENT_QUERIES: int = Field(default=4, ge=1, le=64)

    # Optional POSIX worker address-space guardrail (disabled by default; ADR-003 uses DuckDB limit)
    WORKER_PROCESS_RLIMIT_BYTES: int | None = Field(default=None, ge=64 * MIB_BYTES)

    # OpenTelemetry Distributed Tracing
    OTEL_ENABLED: bool = Field(default=False)
    OTEL_EXPORTER_ENDPOINT: str | None = Field(default=None)
    OTEL_SERVICE_NAME: str = Field(default="rag-tabular-analytics-worker")

    @field_validator(
        "S3_ENDPOINT_URL",
        "WORKER_PROCESS_RLIMIT_BYTES",
        "OTEL_EXPORTER_ENDPOINT",
        mode="before",
    )
    @classmethod
    def _empty_str_to_none(cls, value: Any) -> Any:
        """Treat blank .env values for optional settings as None."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("S3_BUCKET_ALLOWLIST", mode="before")
    @classmethod
    def _parse_bucket_allowlist(cls, value: Any) -> list[str]:
        """Parse S3_BUCKET_ALLOWLIST from JSON array, comma-separated string, or list."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                parsed = json.loads(stripped)
                if not isinstance(parsed, list):
                    raise ValueError("S3_BUCKET_ALLOWLIST JSON value must be a list of strings")
                return [str(item).strip() for item in parsed if str(item).strip()]
            return [item.strip() for item in stripped.split(",") if item.strip()]
        raise ValueError("S3_BUCKET_ALLOWLIST must be a list or string")

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
