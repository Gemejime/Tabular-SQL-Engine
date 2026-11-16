"""Unit tests for TableResolver tenant isolation, bucket allowlist, and path validation."""

from __future__ import annotations

import hashlib

import pytest

from internal.storage import TableResolver, TableValidationError, TenantIsolationError

VALID_HASH = hashlib.sha256(b"sample-parquet").hexdigest()


def test_resolve_valid_canonical_artifact() -> None:
    resolver = TableResolver(["rag-tabular-artifacts"])
    resolved = resolver.resolve(
        tenant_id="tenant-corp-01",
        table_hash=VALID_HASH,
        s3_bucket="rag-tabular-artifacts",
        s3_key=f"artifacts/tenant-corp-01/{VALID_HASH}.parquet",
    )
    assert resolved.tenant_id == "tenant-corp-01"
    assert resolved.table_hash == VALID_HASH
    assert resolved.s3_bucket == "rag-tabular-artifacts"
    assert resolved.relative_cache_path.parts == (
        "rag-tabular-artifacts",
        "tenant-corp-01",
        f"{VALID_HASH}.parquet",
    )


def test_empty_allowlist_fails_closed() -> None:
    resolver = TableResolver([])
    with pytest.raises(TenantIsolationError):
        resolver.resolve(
            tenant_id="tenant-corp-01",
            table_hash=VALID_HASH,
            s3_bucket="rag-tabular-artifacts",
            s3_key=f"artifacts/tenant-corp-01/{VALID_HASH}.parquet",
        )


def test_unallowlisted_bucket_raises_tenant_isolation_error() -> None:
    resolver = TableResolver(["allowed-bucket"])
    with pytest.raises(TenantIsolationError):
        resolver.resolve(
            tenant_id="tenant-corp-01",
            table_hash=VALID_HASH,
            s3_bucket="other-bucket",
            s3_key=f"artifacts/tenant-corp-01/{VALID_HASH}.parquet",
        )


def test_cross_tenant_s3_key_raises_tenant_isolation_error() -> None:
    resolver = TableResolver(["rag-tabular-artifacts"])
    with pytest.raises(TenantIsolationError):
        resolver.resolve(
            tenant_id="tenant-a",
            table_hash=VALID_HASH,
            s3_bucket="rag-tabular-artifacts",
            s3_key=f"artifacts/tenant-b/{VALID_HASH}.parquet",
        )


@pytest.mark.parametrize(
    ("tenant_id", "table_hash"),
    [
        ("../escape", VALID_HASH),
        ("tenant/sub", VALID_HASH),
        ("", VALID_HASH),
        ("tenant-a", "not-a-sha256"),
        ("tenant-a", VALID_HASH.upper()),
    ],
)
def test_malformed_identifiers_raise_table_validation_error(
    tenant_id: str,
    table_hash: str,
) -> None:
    resolver = TableResolver(["rag-tabular-artifacts"])
    with pytest.raises(TableValidationError):
        resolver.resolve(
            tenant_id=tenant_id,
            table_hash=table_hash,
            s3_bucket="rag-tabular-artifacts",
            s3_key=f"artifacts/{tenant_id}/{table_hash}.parquet",
        )
