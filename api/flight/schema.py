"""Arrow Flight DoGet ticket schema and sanitized gRPC/Flight error mapping."""

from __future__ import annotations

from enum import StrEnum

import pyarrow.flight as fl
from pydantic import BaseModel, ConfigDict, Field
from pydantic import ValidationError as PydanticValidationError

from internal.storage.cache_manager import CacheCapacityError, CorruptedArtifactError
from internal.storage.s3_streamer import ArtifactNotFoundError, S3AccessError
from internal.storage.table_resolver import TableValidationError, TenantIsolationError
from internal.validator.exceptions import ValidationError as SQLValidationError


class FlightErrorCode(StrEnum):
    """Canonical gRPC/Flight error codes documented in .ai/doc-api.md."""

    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    INTERNAL = "INTERNAL"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    NOT_FOUND = "NOT_FOUND"


class FlightQueryTicket(BaseModel):
    """Validated JSON payload for Arrow Flight `DoGet(Ticket)`."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    request_id: str = Field(..., min_length=1, max_length=128)
    tenant_id: str = Field(..., min_length=1, max_length=128)
    sql: str = Field(..., min_length=1, max_length=65536)
    table_alias: str = Field(..., min_length=1, max_length=128)
    table_hash: str = Field(..., min_length=64, max_length=64)
    s3_bucket: str = Field(..., min_length=3, max_length=63)
    s3_key: str = Field(..., min_length=1, max_length=512)


class ConcurrencyLimitError(RuntimeError):
    """Raised when MAX_CONCURRENT_QUERIES is saturated for immediate backpressure."""


def sanitize_client_message(code: FlightErrorCode, detail: str) -> str:
    """Format a client-safe error message without internal paths, buckets, or S3 keys."""
    return f"{code.value}: {detail}"


def map_exception_to_flight_error(exc: BaseException) -> fl.FlightError:
    """Map internal domain exceptions to sanitized PyArrow Flight exceptions."""
    if isinstance(exc, fl.FlightError):
        return exc

    if isinstance(exc, TenantIsolationError):
        return fl.FlightUnauthorizedError(
            sanitize_client_message(
                FlightErrorCode.PERMISSION_DENIED,
                "Tenant or artifact access is not permitted.",
            )
        )

    if isinstance(exc, (PydanticValidationError, TableValidationError, SQLValidationError)):
        if isinstance(exc, SQLValidationError):
            detail = f"SQL validation failed ({exc.rule})."
        elif isinstance(exc, TableValidationError):
            detail = "Malformed table or tenant identifier."
        else:
            detail = "Malformed Flight ticket payload."
        return fl.FlightServerError(
            sanitize_client_message(FlightErrorCode.INVALID_ARGUMENT, detail)
        )

    if isinstance(exc, ArtifactNotFoundError):
        return fl.FlightServerError(
            sanitize_client_message(
                FlightErrorCode.NOT_FOUND,
                "Requested Parquet artifact was not found.",
            )
        )

    if isinstance(exc, (ConcurrencyLimitError, CacheCapacityError)):
        return fl.FlightServerError(
            sanitize_client_message(
                FlightErrorCode.RESOURCE_EXHAUSTED,
                "Worker concurrency or cache capacity is saturated; retry later.",
            )
        )

    if isinstance(exc, TimeoutError):
        return fl.FlightTimedOutError(
            sanitize_client_message(
                FlightErrorCode.DEADLINE_EXCEEDED,
                "Query execution exceeded the 5s deadline.",
            )
        )

    if isinstance(exc, (CorruptedArtifactError, S3AccessError)):
        return fl.FlightInternalError(
            sanitize_client_message(
                FlightErrorCode.INTERNAL,
                "Upstream artifact retrieval or verification failed.",
            )
        )

    return fl.FlightInternalError(
        sanitize_client_message(
            FlightErrorCode.INTERNAL,
            "Internal query execution failure.",
        )
    )
