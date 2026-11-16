"""Async S3 streaming hydration over internal VPC endpoints without heap buffering."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from internal.storage.table_resolver import TenantIsolationError
from internal.telemetry.logging import get_logger

_logger = get_logger("internal.storage.s3_streamer")


class ArtifactNotFoundError(FileNotFoundError):
    """Raised when the requested Parquet artifact does not exist in S3."""


class S3AccessError(RuntimeError):
    """Raised when S3 retrieval fails due to upstream transport or service errors."""


class S3Streamer:
    """Streams Parquet artifacts from S3 directly to disk inside the hydration asyncio loop."""

    def __init__(
        self,
        *,
        endpoint_url: str | None = None,
        region_name: str = "us-east-1",
        chunk_size_bytes: int = 8 * 1024 * 1024,
        timeout_seconds: float = 4.0,
        client: Any | None = None,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._chunk_size_bytes = chunk_size_bytes
        self._timeout_seconds = timeout_seconds
        self._injected_client = client
        self._client_ctx: Any | None = None
        self._client: Any | None = client

    async def start(self) -> None:
        """Initialize the aiobotocore S3 client inside the active asyncio event loop."""
        if self._client is not None:
            return
        session = get_session()
        boto_config = Config(
            region_name=self._region_name,
            connect_timeout=self._timeout_seconds,
            read_timeout=self._timeout_seconds,
            retries={"max_attempts": 2, "mode": "standard"},
        )
        self._client_ctx = session.create_client(
            "s3",
            endpoint_url=self._endpoint_url,
            region_name=self._region_name,
            config=boto_config,
        )
        self._client = await self._client_ctx.__aenter__()

    async def close(self) -> None:
        """Close the aiobotocore S3 client inside the active asyncio event loop."""
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
        if self._injected_client is None:
            self._client = None

    async def stream_to_file(self, bucket: str, key: str, dest_tmp_path: Path) -> int:
        """Stream an S3 Parquet object chunk-by-chunk directly to a local temp file."""
        if self._client is None:
            await self.start()
        assert self._client is not None  # noqa: S101

        dest_tmp_path.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = 0

        try:
            async with asyncio.timeout(self._timeout_seconds):
                response = await self._client.get_object(Bucket=bucket, Key=key)
                body = response["Body"]
                try:
                    with dest_tmp_path.open("wb") as fh:
                        if hasattr(body, "iter_chunks"):
                            async for chunk in body.iter_chunks(chunk_size=self._chunk_size_bytes):
                                if chunk:
                                    fh.write(chunk)
                                    total_bytes += len(chunk)
                        else:
                            while True:
                                chunk = await body.read(self._chunk_size_bytes)
                                if not chunk:
                                    break
                                fh.write(chunk)
                                total_bytes += len(chunk)
                        fh.flush()
                finally:
                    close_fn = getattr(body, "close", None)
                    if callable(close_fn):
                        res = close_fn()
                        if asyncio.iscoroutine(res):
                            await res
        except TimeoutError as exc:
            _logger.warning(
                "S3 artifact stream timed out",
                extra={"extra_fields": {"s3_bucket": bucket, "s3_key": key}},
            )
            raise S3AccessError("S3 artifact stream timed out.") from exc
        except ClientError as exc:
            error_info = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
            meta = exc.response.get("ResponseMetadata", {}) if hasattr(exc, "response") else {}
            code = str(error_info.get("Code", ""))
            status_code = int(meta.get("HTTPStatusCode", 0) or 0)

            if code in ("NoSuchKey", "404", "NotFound") or status_code == 404:
                _logger.info(
                    "Requested Parquet artifact not found in S3",
                    extra={"extra_fields": {"s3_bucket": bucket, "s3_key": key}},
                )
                raise ArtifactNotFoundError("Target Parquet artifact not found.") from exc

            if code in ("AccessDenied", "403", "Forbidden") or status_code == 403:
                _logger.warning(
                    "S3 access denied for artifact",
                    extra={"extra_fields": {"s3_bucket": bucket, "s3_key": key}},
                )
                raise TenantIsolationError("Access denied to requested S3 artifact.") from exc

            _logger.error(
                "S3 client error during artifact hydration",
                extra={
                    "extra_fields": {
                        "s3_bucket": bucket,
                        "s3_key": key,
                        "s3_error_code": code,
                        "http_status": status_code,
                    }
                },
            )
            raise S3AccessError(f"S3 retrieval failed with code {code or status_code}.") from exc
        except BotoCoreError as exc:
            _logger.error(
                "S3 transport error during artifact hydration",
                extra={"extra_fields": {"s3_bucket": bucket, "s3_key": key}},
            )
            raise S3AccessError("S3 transport failure during artifact hydration.") from exc

        _logger.info(
            "Streamed S3 Parquet artifact to local temp file",
            extra={
                "extra_fields": {
                    "s3_bucket": bucket,
                    "s3_key": key,
                    "bytes": total_bytes,
                }
            },
        )
        return total_bytes
