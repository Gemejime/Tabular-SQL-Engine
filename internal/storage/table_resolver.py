"""Tenant isolation, S3 bucket allowlist, and canonical artifact path resolution."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from internal.validator.exceptions import ValidationError

_TENANT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TABLE_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_S3_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


class TenantIsolationError(PermissionError):
    """Raised when a request violates tenant isolation, bucket allowlist, or canonical S3 key."""


class TableValidationError(ValidationError):
    """Raised when tenant_id, table_hash, s3_bucket, or s3_key format is malformed."""

    rule = "INVALID_TABLE_SPEC"


@dataclass(frozen=True, slots=True)
class ResolvedTable:
    """Validated artifact coordinates for cache lookup and S3 hydration."""

    tenant_id: str
    table_hash: str
    s3_bucket: str
    s3_key: str
    relative_cache_path: Path

    @property
    def cache_key(self) -> tuple[str, str, str]:
        """Return content-addressed cache key scoped by bucket and tenant."""
        return (self.s3_bucket, self.tenant_id, self.table_hash)


class TableResolver:
    """Validates tenant, hash, bucket allowlist, and canonical S3 key invariants."""

    def __init__(self, s3_bucket_allowlist: Iterable[str]) -> None:
        self._allowlist: frozenset[str] = frozenset(
            bucket.strip() for bucket in s3_bucket_allowlist if bucket.strip()
        )

    @property
    def allowlist(self) -> frozenset[str]:
        """Return configured S3 bucket allowlist."""
        return self._allowlist

    def resolve(
        self,
        *,
        tenant_id: str,
        table_hash: str,
        s3_bucket: str,
        s3_key: str,
    ) -> ResolvedTable:
        """Validate artifact coordinates and return a ResolvedTable.

        Raises:
            TableValidationError: For malformed tenant_id, table_hash, s3_bucket, or s3_key.
            TenantIsolationError: If allowlist is empty, bucket is unallowlisted,
                or s3_key does not match canonical `artifacts/{tenant_id}/{table_hash}.parquet`.
        """
        if not tenant_id or not _TENANT_ID_RE.match(tenant_id) or ".." in tenant_id:
            raise TableValidationError("Invalid tenant_id format.")

        if not table_hash or not _TABLE_HASH_RE.match(table_hash):
            raise TableValidationError("table_hash must be a 64-character lowercase SHA-256 hex.")

        if not s3_bucket or not _S3_BUCKET_RE.match(s3_bucket) or ".." in s3_bucket:
            raise TableValidationError("Invalid s3_bucket format.")

        if not s3_key or not s3_key.strip():
            raise TableValidationError("s3_key must not be empty.")

        # Fail-closed when S3_BUCKET_ALLOWLIST is empty or does not contain the requested bucket.
        if not self._allowlist or s3_bucket not in self._allowlist:
            raise TenantIsolationError("Requested S3 bucket is not permitted.")

        expected_key = f"artifacts/{tenant_id}/{table_hash}.parquet"
        if s3_key != expected_key:
            raise TenantIsolationError("s3_key does not match canonical tenant artifact path.")

        return ResolvedTable(
            tenant_id=tenant_id,
            table_hash=table_hash,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            relative_cache_path=Path(s3_bucket) / tenant_id / f"{table_hash}.parquet",
        )
