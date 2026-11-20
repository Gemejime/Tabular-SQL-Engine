"""Unit tests for OpenTelemetry tracing, W3C traceparent extraction, and log correlation."""

from __future__ import annotations

import json
import logging

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from internal.config.settings import Settings
from internal.telemetry.logging import REDACTED, JsonLogFormatter
from internal.telemetry.tracing import (
    extract_trace_context,
    get_current_trace_id,
    get_tracer,
    init_tracing,
    record_sanitized_exception,
    sanitize_span_attributes,
    shutdown_tracing,
    start_span,
)
from internal.validator.exceptions import DisallowedFunctionError


def test_init_tracing_disabled_is_noop() -> None:
    provider = init_tracing(Settings(OTEL_ENABLED=False))
    assert provider is None
    with start_span("noop.span", attributes={"request_id": "req_noop"}):
        assert get_current_trace_id() is None


def test_traceparent_extraction_and_log_correlation(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("internal.telemetry.tracing.get_tracer", provider.get_tracer)
    try:
        expected_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        traceparent = f"00-{expected_trace_id}-00f067aa0ba902b7-01"
        ctx = extract_trace_context({"traceparent": [traceparent]})

        formatter = JsonLogFormatter()
        with start_span("flight.do_get", context=ctx, attributes={"request_id": "req_01"}):
            assert get_current_trace_id() == expected_trace_id
            record = logging.LogRecord(
                name="test.logger",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="Query executed",
                args=(),
                exc_info=None,
            )
            payload = json.loads(formatter.format(record))
            assert payload["trace_id"] == expected_trace_id

            explicit_record = logging.LogRecord(
                name="test.logger",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="Explicit trace",
                args=(),
                exc_info=None,
            )
            explicit_record.trace_id = "custom-trace-id"
            explicit_payload = json.loads(formatter.format(explicit_record))
            assert explicit_payload["trace_id"] == "custom-trace-id"

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert format(spans[0].context.trace_id, "032x") == expected_trace_id
        assert spans[0].attributes is not None
        assert spans[0].attributes["request_id"] == "req_01"
    finally:
        provider.shutdown()
        shutdown_tracing()


def test_span_attributes_and_exceptions_redact_sql_and_secrets() -> None:
    attrs = sanitize_span_attributes(
        {
            "request_id": "req_02",
            "sql": "SELECT * FROM tbl WHERE ssn = '123-45-6789'",
            "raw_sql": "SELECT secret FROM tbl",
            "authorization": "Bearer supersecrettoken123",
            "url": "https://s3.amazonaws.com/b/k?X-Amz-Signature=deadbeef&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE",
        }
    )
    assert attrs["request_id"] == "req_02"
    assert attrs["sql"] == REDACTED
    assert attrs["raw_sql"] == REDACTED
    assert attrs["authorization"] == REDACTED
    assert "deadbeef" not in str(attrs["url"])
    assert "AKIAIOSFODNN7EXAMPLE" not in str(attrs["url"])
    assert REDACTED in str(attrs["url"])

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    try:
        with tracer.start_as_current_span("err.span") as span:
            record_sanitized_exception(
                span,
                RuntimeError("Failed with Bearer mytoken123 and ?X-Amz-Signature=abc123"),
            )
        with tracer.start_as_current_span("rule.span") as rule_span:
            record_sanitized_exception(
                rule_span,
                DisallowedFunctionError("read_csv_auto('secret') is banned"),
            )
        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        msg = str(spans[0].attributes["exception.message"])  # type: ignore[index]
        assert msg == "RuntimeError"
        assert "mytoken123" not in msg
        assert "abc123" not in msg

        rule_attrs = spans[1].attributes
        assert rule_attrs is not None
        assert rule_attrs["exception.type"] == "DisallowedFunctionError"
        assert rule_attrs["error.rule"] == "DISALLOWED_FUNCTION"
        assert rule_attrs["exception.message"] == "DisallowedFunctionError:DISALLOWED_FUNCTION"
        assert "read_csv_auto" not in str(rule_attrs["exception.message"])
    finally:
        provider.shutdown()


def test_start_span_disables_automatic_exception_events_with_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("internal.telemetry.tracing.get_tracer", provider.get_tracer)
    try:
        secret_msg = (
            "Auth failed: Bearer supersecrettoken999 "
            "https://s3.amazonaws.com/b/k?X-Amz-Signature=deadbeef&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE"
        )
        with pytest.raises(RuntimeError), start_span("flight.do_get"):
            raise RuntimeError(secret_msg)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].events == ()
        attrs = spans[0].attributes
        assert attrs is not None
        assert attrs["exception.type"] == "RuntimeError"
        assert attrs["exception.message"] == "RuntimeError"
        assert "supersecrettoken999" not in str(attrs)
        assert "deadbeef" not in str(attrs)
    finally:
        provider.shutdown()


def test_init_tracing_enabled_start_stop_start_lifecycle() -> None:
    settings = Settings(
        OTEL_ENABLED=True,
        OTEL_EXPORTER_ENDPOINT=None,
        OTEL_SERVICE_NAME="tabular-worker-test",
    )
    try:
        provider_1 = init_tracing(settings)
        assert provider_1 is not None
        exporter_1 = InMemorySpanExporter()
        provider_1.add_span_processor(SimpleSpanProcessor(exporter_1))

        with start_span("lifecycle.first", attributes={"request_id": "req_1"}):
            assert get_current_trace_id() is not None
        assert len(exporter_1.get_finished_spans()) == 1

        shutdown_tracing()

        provider_2 = init_tracing(settings)
        assert provider_2 is not None
        assert provider_2 is not provider_1
        exporter_2 = InMemorySpanExporter()
        provider_2.add_span_processor(SimpleSpanProcessor(exporter_2))

        tracer_2 = get_tracer("test.lifecycle")
        assert tracer_2 is not None
        with start_span("lifecycle.second", attributes={"request_id": "req_2"}):
            assert get_current_trace_id() is not None

        assert len(exporter_2.get_finished_spans()) == 1
        assert len(exporter_1.get_finished_spans()) == 1
    finally:
        shutdown_tracing()
