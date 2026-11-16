"""Arrow Flight DoGet and DoAction handlers coordinating validation, cache, and DuckDB pool."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.flight as fl
from pydantic import ValidationError as PydanticValidationError

from api.flight.actions import CacheWarmRequest, CacheWarmResponse, HealthCheckResponse
from api.flight.schema import (
    ConcurrencyLimitError,
    FlightQueryTicket,
    map_exception_to_flight_error,
)
from internal.config.settings import Settings
from internal.pool.circuit_breaker import QueryTimeoutError, RecycleReason, WorkerCrashError
from internal.pool.process_manager import ProcessManager
from internal.storage.cache_manager import CacheManager, PinnedArtifact
from internal.storage.s3_streamer import S3Streamer
from internal.storage.table_resolver import TableResolver
from internal.telemetry.logging import get_logger
from internal.telemetry.metrics import MetricsRegistry
from internal.validator.ast_validator import ASTValidator
from internal.validator.exceptions import ValidationError as SQLValidationError

_logger = get_logger("internal.flight.handlers")


class FlightHandlers:
    """Coordinates Arrow Flight RPCs with AST validation, NVMe cache, S3, and ProcessManager."""

    def __init__(
        self,
        *,
        settings: Settings,
        ast_validator: ASTValidator,
        table_resolver: TableResolver,
        cache_manager: CacheManager,
        s3_streamer: S3Streamer,
        process_manager: ProcessManager,
        hydration_loop: asyncio.AbstractEventLoop,
        metrics: MetricsRegistry,
    ) -> None:
        self._settings = settings
        self._ast_validator = ast_validator
        self._table_resolver = table_resolver
        self._cache_manager = cache_manager
        self._s3_streamer = s3_streamer
        self._process_manager = process_manager
        self._hydration_loop = hydration_loop
        self._metrics = metrics
        self._concurrency_semaphore = threading.BoundedSemaphore(settings.MAX_CONCURRENT_QUERIES)

    def _hydrate_sync(
        self,
        tenant_id: str,
        table_hash: str,
        s3_bucket: str,
        s3_key: str,
    ) -> PinnedArtifact:
        """Resolve tenant/S3 invariants and hydrate/pin artifact on the asyncio loop."""
        resolved = self._table_resolver.resolve(
            tenant_id=tenant_id,
            table_hash=table_hash,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
        )
        future = asyncio.run_coroutine_threadsafe(
            self._cache_manager.get_or_hydrate(resolved, self._s3_streamer),
            self._hydration_loop,
        )
        return future.result(timeout=self._settings.S3_TIMEOUT_SECONDS + 2.0)

    def handle_do_get(self, ticket_bytes: bytes) -> fl.FlightDataStream:
        """Validate ticket and SQL AST, hydrate Parquet, and stream RecordBatch chunks."""
        start_time = time.perf_counter()

        try:
            ticket = FlightQueryTicket.model_validate_json(ticket_bytes)
        except (PydanticValidationError, ValueError) as exc:
            _logger.warning("Rejected malformed Flight ticket payload")
            raise map_exception_to_flight_error(exc) from exc

        try:
            resolved = self._table_resolver.resolve(
                tenant_id=ticket.tenant_id,
                table_hash=ticket.table_hash,
                s3_bucket=ticket.s3_bucket,
                s3_key=ticket.s3_key,
            )
            self._ast_validator.validate(ticket.sql, ticket.table_alias)
        except SQLValidationError as exc:
            self._metrics.inc_ast_rejection(exc.rule)
            _logger.warning(
                "Rejected SQL query during AST validation",
                extra={
                    "request_id": ticket.request_id,
                    "tenant_id": ticket.tenant_id,
                    "table_hash": ticket.table_hash,
                    "rule": exc.rule,
                },
            )
            raise map_exception_to_flight_error(exc) from exc
        except Exception as exc:
            _logger.warning(
                "Rejected Flight ticket during table resolution",
                extra={
                    "request_id": ticket.request_id,
                    "tenant_id": ticket.tenant_id,
                    "table_hash": ticket.table_hash,
                },
            )
            raise map_exception_to_flight_error(exc) from exc

        if not self._concurrency_semaphore.acquire(blocking=False):
            _logger.warning(
                "Pod concurrency limit saturated; returning RESOURCE_EXHAUSTED",
                extra={
                    "request_id": ticket.request_id,
                    "tenant_id": ticket.tenant_id,
                    "table_hash": ticket.table_hash,
                },
            )
            raise map_exception_to_flight_error(ConcurrencyLimitError())

        pinned: PinnedArtifact | None = None
        active_incremented = False
        handed_off = False

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._cache_manager.get_or_hydrate(resolved, self._s3_streamer),
                self._hydration_loop,
            )
            pinned = future.result(timeout=self._settings.S3_TIMEOUT_SECONDS + 2.0)

            self._metrics.inc_active_queries()
            active_incremented = True

            batch_iter = iter(
                self._process_manager.stream_query(
                    ticket.sql,
                    pinned.path,
                    ticket.table_alias,
                )
            )
            first_batch = next(batch_iter, None)
            schema = first_batch.schema if first_batch is not None else pa.schema([])

            def _stream_batches() -> Iterator[pa.RecordBatch]:
                assert pinned is not None  # noqa: S101
                stream_status = "ok"
                stream_bytes = 0
                try:
                    if first_batch is not None:
                        stream_bytes += first_batch.nbytes
                        self._metrics.add_stream_bytes(first_batch.nbytes)
                        yield first_batch
                    for batch in batch_iter:
                        stream_bytes += batch.nbytes
                        self._metrics.add_stream_bytes(batch.nbytes)
                        yield batch
                except QueryTimeoutError as exc:
                    stream_status = "timeout"
                    self._metrics.inc_subprocess_recycle(RecycleReason.TIMEOUT.value)
                    raise map_exception_to_flight_error(exc) from exc
                except WorkerCrashError as exc:
                    stream_status = "worker_crash"
                    self._metrics.inc_subprocess_recycle(RecycleReason.CRASH.value)
                    raise map_exception_to_flight_error(exc) from exc
                except Exception as exc:
                    stream_status = "error"
                    raise map_exception_to_flight_error(exc) from exc
                finally:
                    duration_s = time.perf_counter() - start_time
                    self._metrics.observe_query_duration(pinned.cache_state, duration_s)
                    self._metrics.dec_active_queries()
                    pinned.release()
                    self._concurrency_semaphore.release()
                    _logger.info(
                        "Completed Flight DoGet query stream",
                        extra={
                            "request_id": ticket.request_id,
                            "tenant_id": ticket.tenant_id,
                            "table_hash": ticket.table_hash,
                            "cache_state": pinned.cache_state,
                            "duration_ms": round(duration_s * 1000.0, 2),
                            "status": stream_status,
                            "extra_fields": {"stream_bytes": stream_bytes},
                        },
                    )

            handed_off = True
            return fl.GeneratorStream(schema, _stream_batches())
        except QueryTimeoutError as exc:
            self._metrics.inc_subprocess_recycle(RecycleReason.TIMEOUT.value)
            raise map_exception_to_flight_error(exc) from exc
        except WorkerCrashError as exc:
            self._metrics.inc_subprocess_recycle(RecycleReason.CRASH.value)
            raise map_exception_to_flight_error(exc) from exc
        except Exception as exc:
            raise map_exception_to_flight_error(exc) from exc
        finally:
            if not handed_off:
                if active_incremented:
                    self._metrics.dec_active_queries()
                if pinned is not None:
                    pinned.release()
                self._concurrency_semaphore.release()

    def handle_cache_warm(self, body: bytes) -> bytes:
        """Idempotently hydrate a tenant-validated Parquet artifact into the NVMe cache."""
        try:
            req = CacheWarmRequest.model_validate_json(body)
        except (PydanticValidationError, ValueError) as exc:
            raise map_exception_to_flight_error(exc) from exc

        pinned: PinnedArtifact | None = None
        try:
            pinned = self._hydrate_sync(
                tenant_id=req.tenant_id,
                table_hash=req.table_hash,
                s3_bucket=req.s3_bucket,
                s3_key=req.s3_key,
            )
            resp = CacheWarmResponse(status="warmed", bytes=pinned.size_bytes)
            _logger.info(
                "Warmed Parquet artifact in Tier-1 cache",
                extra={
                    "tenant_id": req.tenant_id,
                    "table_hash": req.table_hash,
                    "cache_state": pinned.cache_state,
                    "extra_fields": {"bytes": pinned.size_bytes},
                },
            )
            return json.dumps(resp.model_dump()).encode("utf-8")
        except Exception as exc:
            raise map_exception_to_flight_error(exc) from exc
        finally:
            if pinned is not None:
                pinned.release()

    def handle_health_check(self) -> bytes:
        """Return worker pool health status for Flight DoAction('health_check')."""
        healthy = self._process_manager.is_healthy()
        resp = HealthCheckResponse(
            status="ok" if healthy else "degraded",
            active_workers=self._settings.POOL_SIZE if healthy else 0,
        )
        return json.dumps(resp.model_dump()).encode("utf-8")
