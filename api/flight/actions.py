"""Apache Arrow Flight DoAction request and response schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CacheWarmRequest(BaseModel):
    """Request payload for Arrow Flight `DoAction("cache_warm")`."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_id: str = Field(..., min_length=1, max_length=128)
    table_hash: str = Field(..., min_length=64, max_length=64)
    s3_bucket: str = Field(..., min_length=3, max_length=63)
    s3_key: str = Field(..., min_length=1, max_length=512)


class CacheWarmResponse(BaseModel):
    """Response payload for Arrow Flight `DoAction("cache_warm")`."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["warmed"] = "warmed"
    bytes: int = Field(..., ge=0)


class HealthCheckResponse(BaseModel):
    """Response payload for Arrow Flight `DoAction("health_check")`."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "degraded"]
    active_workers: int = Field(..., ge=0)
