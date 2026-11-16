"""Unit tests for Arrow Flight schemas, sanitized error mappings, and OpenAPI 3.1 spec."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.flight as fl
import pytest
import yaml
from pydantic import ValidationError as PydanticValidationError

from api.flight import (
    CacheWarmRequest,
    CacheWarmResponse,
    ConcurrencyLimitError,
    FlightQueryTicket,
    HealthCheckResponse,
    map_exception_to_flight_error,
)
from internal.storage import (
    ArtifactNotFoundError,
    CacheCapacityError,
    S3AccessError,
    TenantIsolationError,
)
from internal.validator.exceptions import DisallowedStatementError

VALID_HASH = hashlib.sha256(b"flight-schema-test").hexdigest()


def test_flight_query_ticket_valid_and_rejects_extra_fields() -> None:
    ticket = FlightQueryTicket(
        request_id="req_01",
        tenant_id="tenant-corp-01",
        sql="SELECT COUNT(*) FROM tbl",
        table_alias="tbl",
        table_hash=VALID_HASH,
        s3_bucket="rag-tabular-artifacts",
        s3_key=f"artifacts/tenant-corp-01/{VALID_HASH}.parquet",
    )
    assert ticket.tenant_id == "tenant-corp-01"

    with pytest.raises(PydanticValidationError):
        FlightQueryTicket(
            request_id="req_01",
            tenant_id="tenant-corp-01",
            sql="SELECT 1",
            table_alias="tbl",
            table_hash=VALID_HASH,
            s3_bucket="rag-tabular-artifacts",
            s3_key=f"artifacts/tenant-corp-01/{VALID_HASH}.parquet",
            unexpected="boom",  # type: ignore[call-arg]
        )


def test_action_models() -> None:
    req = CacheWarmRequest(
        tenant_id="tenant-1",
        table_hash=VALID_HASH,
        s3_bucket="rag-tabular-artifacts",
        s3_key=f"artifacts/tenant-1/{VALID_HASH}.parquet",
    )
    resp = CacheWarmResponse(bytes=2048)
    health = HealthCheckResponse(status="ok", active_workers=4)
    assert req.tenant_id == "tenant-1"
    assert resp.status == "warmed"
    assert health.active_workers == 4


def test_sanitized_flight_error_mappings() -> None:
    err_perm = map_exception_to_flight_error(
        TenantIsolationError("secret-bucket/artifacts/tenant-x/secret.parquet")
    )
    assert isinstance(err_perm, fl.FlightUnauthorizedError)
    assert "PERMISSION_DENIED" in str(err_perm)
    assert "secret-bucket" not in str(err_perm)

    err_ast = map_exception_to_flight_error(DisallowedStatementError("PRAGMA show_tables"))
    assert isinstance(err_ast, fl.FlightServerError)
    assert "INVALID_ARGUMENT" in str(err_ast)

    err_timeout = map_exception_to_flight_error(TimeoutError("deadline"))
    assert isinstance(err_timeout, fl.FlightTimedOutError)
    assert "DEADLINE_EXCEEDED" in str(err_timeout)

    err_not_found = map_exception_to_flight_error(ArtifactNotFoundError("s3://bucket/key"))
    assert "NOT_FOUND" in str(err_not_found)
    assert "s3://" not in str(err_not_found)

    err_capacity = map_exception_to_flight_error(CacheCapacityError("full"))
    assert "RESOURCE_EXHAUSTED" in str(err_capacity)

    err_concurrency = map_exception_to_flight_error(ConcurrencyLimitError("busy"))
    assert "RESOURCE_EXHAUSTED" in str(err_concurrency)

    err_internal = map_exception_to_flight_error(S3AccessError("internal trace /var/cache/secret"))
    assert isinstance(err_internal, fl.FlightInternalError)
    assert "INTERNAL" in str(err_internal)
    assert "/var/cache/secret" not in str(err_internal)


def test_openapi_31_spec_is_valid() -> None:
    from openapi_spec_validator import validate

    spec_path = Path(__file__).resolve().parents[2] / "api" / "openapi" / "openapi.yaml"
    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    assert str(spec.get("openapi", "")).startswith("3.1")
    validate(spec)
    for endpoint in ("/healthz", "/ready", "/metrics", "/openapi.yaml", "/docs"):
        assert endpoint in spec["paths"]
