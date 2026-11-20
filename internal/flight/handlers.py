"""Arrow Flight DoGet and DoAction handlers coordinating validation, cache, and DuckDB pool."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future

import pyarrow as pa
import pyarrow.flight as fl
from opentelemetry.trace import Span
from pydantic import ValidationError as PydanticValidationError

from api.flight.actions import CacheWarmRequest, CacheWarmResponse, HealthCheckResponse
from api.flight.schema import (
    ConcurrencyLimitError,
    FlightQueryTicket,
    map_exception_to_flight_error,
)
from internal.config.settings import Settings
from internal.pool.circuit_breaker import QueryTimeoutError, RecycleReason, WorkerCrashError
from internal.pool.process_manager import ProcessManager, QueryStream
from internal.storage.cache_manager import CacheManager, PinnedArtifact
from internal.storage.s3_streamer import S3Streamer
from internal.storage.table_resolver import ResolvedTable, TableResolver
from internal.telemetry.logging import get_logger
from internal.telemetry.metrics import MetricsRegistry
from internal.telemetry.tracing import (
    get_current_trace_id,
    get_tracer,
    record_sanitized_exception,
    sanitize_span_attributes,
    start_span,
)
from internal.validator.ast_validator import ASTValidator
from internal.validator.exceptions import ValidationError as SQLValidationError

_logger = get_logger("internal.flight.handlers")


class _OwnedFlightBatchIterator:
    """Iterator owning Flight query stream cleanup across iteration, close, and GC."""

    def __init__(
        self,
        *,
        query_stream: QueryStream,
        pinned: PinnedArtifact,
        metrics: MetricsRegistry,
        semaphore: threading.BoundedSemaphore,
        start_time: float,
        request_id: str,
        tenant_id: str,
        table_hash: str,
        exec_span: Span | None = None,
        trace_id: str | None = None,
        on_cleanup: Callable[[_OwnedFlightBatchIterator], None] | None = None,
    ) -> None:
        self._query_stream = query_stream
        self._batch_iter = iter(query_stream)
        self._pinned = pinned
        self._metrics = metrics
        self._semaphore = semaphore
        self._start_time = start_time
        self._request_id = request_id
        self._tenant_id = tenant_id
        self._table_hash = table_hash
        self._exec_span = exec_span
        self._trace_id = trace_id
        self._on_cleanup = on_cleanup
        self._stream_status = "ok"
        self._stream_bytes = 0
        self._cleanup_lock = threading.Lock()
        self._cleaned_up = False

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return self

    def __next__(self) -> pa.RecordBatch:
        with self._cleanup_lock:
            if self._cleaned_up:
                raise StopIteration
        try:
            batch = next(self._batch_iter)
        except StopIteration:
            self._cleanup()
            raise
        except QueryTimeoutError as exc:
            with self._cleanup_lock:
                if self._cleaned_up:
                    raise StopIteration from None
            self._stream_status = "timeout"
            self._metrics.inc_subprocess_recycle(RecycleReason.TIMEOUT.value)
            if self._exec_span is not None:
                record_sanitized_exception(self._exec_span, exc)
            self._cleanup()
            raise map_exception_to_flight_error(exc) from exc
        except WorkerCrashError as exc:
            with self._cleanup_lock:
                if self._cleaned_up:
                    raise StopIteration from None
            self._stream_status = "worker_crash"
            self._metrics.inc_subprocess_recycle(exc.reason.value)
            if self._exec_span is not None:
                record_sanitized_exception(self._exec_span, exc)
            self._cleanup()
            raise map_exception_to_flight_error(exc) from exc
        except Exception as exc:
            with self._cleanup_lock:
                if self._cleaned_up:
                    raise StopIteration from None
            self._stream_status = "error"
            if self._exec_span is not None:
                record_sanitized_exception(self._exec_span, exc)
            self._cleanup()
            raise map_exception_to_flight_error(exc) from exc

        self._stream_bytes += batch.nbytes
        self._metrics.add_stream_bytes(batch.nbytes)
        return batch

    def close(self) -> None:
        """Close underlying stream and release owned resources idempotently."""
        if not self._cleaned_up and self._stream_status == "ok":
            self._stream_status = "closed"
        self._cleanup()

    def __del__(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self._cleanup_lock:
            if self._cleaned_up:
                return
            self._cleaned_up = True

        self._query_stream.close()
        duration_s = time.perf_counter() - self._start_time
        self._metrics.observe_query_duration(self._pinned.cache_state, duration_s)
        self._metrics.dec_active_queries()
        self._pinned.release()
        self._semaphore.release()
        if self._on_cleanup is not None:
            self._on_cleanup(self)
        if self._exec_span is not None:
            self._exec_span.set_attributes(
                sanitize_span_attributes(
                    {
                        "cache_state": self._pinned.cache_state,
                        "stream_bytes": self._stream_bytes,
                        "status": self._stream_status,
                    }
                )
            )
            self._exec_span.end()
        log_extra: dict[str, object] = {
            "request_id": self._request_id,
            "tenant_id": self._tenant_id,
            "table_hash": self._table_hash,
            "cache_state": self._pinned.cache_state,
            "duration_ms": round(duration_s * 1000.0, 2),
            "status": self._stream_status,
            "extra_fields": {"stream_bytes": self._stream_bytes},
        }
        if self._trace_id is not None:
            log_extra["trace_id"] = self._trace_id
        _logger.info("Completed Flight DoGet query stream", extra=log_extra)


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
        self._active_streams_lock = threading.Lock()
        self._active_streams: set[_OwnedFlightBatchIterator] = set()

    def _unregister_stream(self, stream: _OwnedFlightBatchIterator) -> None:
        with self._active_streams_lock:
            self._active_streams.discard(stream)

    def close_active_streams(self) -> int:
        """Force-close any active GeneratorStream iterators during shutdown drain timeout."""
        with self._active_streams_lock:
            streams = list(self._active_streams)
        for stream in streams:
            stream.close()
        return len(streams)

    def set_hydration_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Update the asyncio event loop used for S3 hydration."""
        self._hydration_loop = loop

    def _await_hydration(self, resolved: ResolvedTable) -> PinnedArtifact:
        """Await hydration on the asyncio loop, releasing any artifact if the caller abandons."""
        future: Future[PinnedArtifact] = asyncio.run_coroutine_threadsafe(
            self._cache_manager.get_or_hydrate(resolved, self._s3_streamer),
            self._hydration_loop,
        )
        lock = threading.Lock()
        caller_took_ownership = False
        caller_abandoned = False
        released = False

        def _release_once(done_fut: Future[PinnedArtifact]) -> None:
            nonlocal released
            if done_fut.cancelled():
                return
            try:
                artifact = done_fut.result()
            except BaseException:
                return
            with lock:
                if caller_took_ownership or not caller_abandoned or released:
                    return
                released = True
            artifact.release()

        future.add_done_callback(_release_once)
        try:
            artifact = future.result(timeout=self._settings.S3_TIMEOUT_SECONDS + 2.0)
            with lock:
                caller_took_ownership = True
            return artifact
        except BaseException:
            with lock:
                caller_abandoned = True
            if future.done():
                _release_once(future)
            raise

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
        return self._await_hydration(resolved)

    def handle_do_get(self, ticket_bytes: bytes) -> fl.FlightDataStream:
        """Validate ticket and SQL AST, hydrate Parquet, and stream RecordBatch chunks."""
        start_time = time.perf_counter()

        with start_span("flight.do_get") as do_get_span:
            try:
                ticket = FlightQueryTicket.model_validate_json(ticket_bytes)
            except (PydanticValidationError, ValueError) as exc:
                _logger.warning("Rejected malformed Flight ticket payload")
                raise map_exception_to_flight_error(exc) from exc

            do_get_span.set_attributes(
                sanitize_span_attributes(
                    {
                        "request_id": ticket.request_id,
                        "tenant_id": ticket.tenant_id,
                        "table_hash": ticket.table_hash,
                        "table_alias": ticket.table_alias,
                    }
                )
            )

            try:
                resolved = self._table_resolver.resolve(
                    tenant_id=ticket.tenant_id,
                    table_hash=ticket.table_hash,
                    s3_bucket=ticket.s3_bucket,
                    s3_key=ticket.s3_key,
                )
                with start_span(
                    "sqlglot.validate",
                    attributes={
                        "request_id": ticket.request_id,
                        "tenant_id": ticket.tenant_id,
                        "table_hash": ticket.table_hash,
                    },
                ):
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
            exec_span: Span | None = None

            try:
                self._metrics.inc_active_queries()
                active_incremented = True

                with start_span(
                    "storage.get_or_hydrate",
                    attributes={
                        "request_id": ticket.request_id,
                        "tenant_id": ticket.tenant_id,
                        "table_hash": ticket.table_hash,
                    },
                ) as hydrate_span:
                    pinned = self._await_hydration(resolved)
                    hydrate_span.set_attribute("cache_state", pinned.cache_state)

                exec_span = get_tracer().start_span(
                    "duckdb.subprocess_execute",
                    attributes=sanitize_span_attributes(
                        {
                            "request_id": ticket.request_id,
                            "tenant_id": ticket.tenant_id,
                            "table_hash": ticket.table_hash,
                            "cache_state": pinned.cache_state,
                        }
                    ),
                )
                query_stream = self._process_manager.stream_query(
                    ticket.sql,
                    pinned.path,
                    ticket.table_alias,
                )
                owned_iter = _OwnedFlightBatchIterator(
                    query_stream=query_stream,
                    pinned=pinned,
                    metrics=self._metrics,
                    semaphore=self._concurrency_semaphore,
                    start_time=start_time,
                    request_id=ticket.request_id,
                    tenant_id=ticket.tenant_id,
                    table_hash=ticket.table_hash,
                    exec_span=exec_span,
                    trace_id=get_current_trace_id(),
                    on_cleanup=self._unregister_stream,
                )
                with self._active_streams_lock:
                    self._active_streams.add(owned_iter)
                handed_off = True
                return fl.GeneratorStream(query_stream.schema, owned_iter)
            except QueryTimeoutError as exc:
                self._metrics.inc_subprocess_recycle(RecycleReason.TIMEOUT.value)
                if exec_span is not None:
                    record_sanitized_exception(exec_span, exc)
                raise map_exception_to_flight_error(exc) from exc
            except WorkerCrashError as exc:
                self._metrics.inc_subprocess_recycle(exc.reason.value)
                if exec_span is not None:
                    record_sanitized_exception(exec_span, exc)
                raise map_exception_to_flight_error(exc) from exc
            except Exception as exc:
                if exec_span is not None:
                    record_sanitized_exception(exec_span, exc)
                raise map_exception_to_flight_error(exc) from exc
            finally:
                if not handed_off:
                    if exec_span is not None:
                        exec_span.end()
                    if active_incremented:
                        self._metrics.dec_active_queries()
                    if pinned is not None:
                        pinned.release()
                    self._concurrency_semaphore.release()

    def handle_cache_warm(self, body: bytes) -> bytes:
        """Idempotently hydrate a tenant-validated Parquet artifact into the NVMe cache."""
        with start_span("flight.do_action.cache_warm") as span:
            try:
                req = CacheWarmRequest.model_validate_json(body)
            except (PydanticValidationError, ValueError) as exc:
                raise map_exception_to_flight_error(exc) from exc

            span.set_attributes(
                sanitize_span_attributes(
                    {
                        "tenant_id": req.tenant_id,
                        "table_hash": req.table_hash,
                    }
                )
            )
            pinned: PinnedArtifact | None = None
            try:
                pinned = self._hydrate_sync(
                    tenant_id=req.tenant_id,
                    table_hash=req.table_hash,
                    s3_bucket=req.s3_bucket,
                    s3_key=req.s3_key,
                )
                span.set_attribute("cache_state", pinned.cache_state)
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
        with start_span("flight.do_action.health_check"):
            healthy = self._process_manager.is_healthy()
            resp = HealthCheckResponse(
                status="ok" if healthy else "degraded",
                active_workers=self._settings.POOL_SIZE if healthy else 0,
            )
            return json.dumps(resp.model_dump()).encode("utf-8")
