"""OpenTelemetry distributed tracing, W3C traceparent extraction, and safe span helpers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

import pyarrow.flight as fl
from opentelemetry import trace
from opentelemetry.context import Context, attach, detach
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from internal.config.settings import Settings
from internal.telemetry.logging import REDACTED, get_logger, redact_text

_logger = get_logger("internal.telemetry.tracing")
_PROPAGATOR = TraceContextTextMapPropagator()

_SENSITIVE_SPAN_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "bearer",
        "token",
        "access_token",
        "secret",
        "password",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "presigned_url",
        "raw_sql",
        "sql",
        "sql_literals",
    }
)

_ACTIVE_PROVIDER: TracerProvider | None = None


def init_tracing(settings: Settings) -> TracerProvider | None:
    """Initialize OpenTelemetry TracerProvider when OTEL_ENABLED is True."""
    global _ACTIVE_PROVIDER
    if not settings.OTEL_ENABLED:
        return None

    if _ACTIVE_PROVIDER is not None:
        _ACTIVE_PROVIDER.shutdown()
        _ACTIVE_PROVIDER = None

    resource = Resource.create({"service.name": settings.OTEL_SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    if settings.OTEL_EXPORTER_ENDPOINT:
        exporter = OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_ENDPOINT)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    if isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        trace.set_tracer_provider(provider)
    _ACTIVE_PROVIDER = provider
    _logger.info(
        "Initialized OpenTelemetry tracing",
        extra={
            "extra_fields": {
                "otel_service_name": settings.OTEL_SERVICE_NAME,
                "exporter_configured": bool(settings.OTEL_EXPORTER_ENDPOINT),
            }
        },
    )
    return provider


def shutdown_tracing() -> None:
    """Flush and shut down the active TracerProvider if initialized."""
    global _ACTIVE_PROVIDER
    if _ACTIVE_PROVIDER is not None:
        _ACTIVE_PROVIDER.shutdown()
        _ACTIVE_PROVIDER = None


def get_tracer(name: str = "rag-tabular-analytics-worker") -> trace.Tracer:
    """Return an OpenTelemetry tracer from the currently owned provider or global fallback."""
    if _ACTIVE_PROVIDER is not None:
        return _ACTIVE_PROVIDER.get_tracer(name)
    return trace.get_tracer(name)


def get_current_trace_id() -> str | None:
    """Return the active 32-char hex trace_id if a valid span is active."""
    span = trace.get_current_span()
    span_ctx = span.get_span_context()
    if span_ctx.is_valid:
        return format(span_ctx.trace_id, "032x")
    return None


def sanitize_span_attributes(
    attributes: Mapping[str, Any] | None,
) -> dict[str, str | int | float | bool]:
    """Filter and redact span attributes so raw SQL, credentials, and presigned URLs never leak."""
    if not attributes:
        return {}
    sanitized: dict[str, str | int | float | bool] = {}
    for key, val in attributes.items():
        if key.lower() in _SENSITIVE_SPAN_KEYS:
            sanitized[key] = REDACTED
        elif isinstance(val, str):
            sanitized[key] = redact_text(val)
        elif isinstance(val, bool | int | float):
            sanitized[key] = val
        elif val is not None:
            sanitized[key] = redact_text(str(val))
    return sanitized


def record_sanitized_exception(span: Span, exc: BaseException) -> None:
    """Record allowlisted error-type and rule metadata without leaking raw SQL/secrets."""
    exc_type = type(exc).__name__
    span.set_status(Status(StatusCode.ERROR, exc_type))
    span.set_attribute("exception.type", exc_type)
    rule = getattr(exc, "rule", None)
    if isinstance(rule, str) and rule:
        span.set_attribute("error.rule", rule)
        span.set_attribute("exception.message", f"{exc_type}:{rule}")
    else:
        span.set_attribute("exception.message", exc_type)


def extract_trace_context(headers: Mapping[str, Any] | None) -> Context:
    """Extract W3C traceparent/tracestate context from gRPC or HTTP metadata headers."""
    if not headers:
        return Context()
    carrier: dict[str, str] = {}
    for key, val in headers.items():
        lower_key = str(key).lower()
        if lower_key in ("traceparent", "tracestate"):
            if isinstance(val, list | tuple) and val:
                carrier[lower_key] = str(val[0])
            elif isinstance(val, bytes):
                carrier[lower_key] = val.decode("utf-8", errors="ignore")
            elif isinstance(val, str):
                carrier[lower_key] = val
    return _PROPAGATOR.extract(carrier=carrier)


@contextmanager
def start_span(
    name: str,
    *,
    tracer_name: str = "rag-tabular-analytics-worker",
    context: Context | None = None,
    attributes: Mapping[str, Any] | None = None,
) -> Iterator[Span]:
    """Start an OpenTelemetry span with sanitized attributes and safe error recording."""
    tracer = get_tracer(tracer_name)
    safe_attrs = sanitize_span_attributes(attributes)
    with tracer.start_as_current_span(
        name,
        context=context,
        attributes=safe_attrs,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield span
        except BaseException as exc:
            record_sanitized_exception(span, exc)
            raise


class FlightTracingMiddleware(fl.ServerMiddleware):  # type: ignore[misc]
    """Per-RPC PyArrow Flight middleware carrying extracted W3C trace context."""

    def __init__(self, trace_context: Context) -> None:
        super().__init__()
        self.trace_context = trace_context

    def sending_headers(self) -> dict[str, str]:
        return {}

    def call_completed(self, exception: Exception | None) -> None:
        return None


class FlightTracingMiddlewareFactory(fl.ServerMiddlewareFactory):  # type: ignore[misc]
    """PyArrow Flight ServerMiddlewareFactory extracting W3C traceparent headers."""

    def start_call(
        self,
        info: fl.CallInfo,
        headers: dict[str, list[str]],
    ) -> FlightTracingMiddleware:
        ctx = extract_trace_context(headers)
        return FlightTracingMiddleware(ctx)


@contextmanager
def attach_flight_trace_context(context: fl.ServerCallContext | None) -> Iterator[None]:
    """Attach the extracted W3C trace context from Flight ServerCallContext if present."""
    if context is None:
        yield
        return
    middleware = context.get_middleware("otel_tracing")
    if isinstance(middleware, FlightTracingMiddleware):
        token = attach(middleware.trace_context)
        try:
            yield
        finally:
            detach(token)
        return
    yield
