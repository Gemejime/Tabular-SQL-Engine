"""Structured JSON logging with strict secret, credential, and SQL literal redaction."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace as otel_trace

REDACTED = "[REDACTED]"

_BEARER_RE = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9\-._~+/]+=*")
_AMZ_QUERY_RE = re.compile(
    r"(?i)([?&](?:X-Amz-Signature|X-Amz-Credential|X-Amz-Security-Token"
    r"|X-Amz-Algorithm|X-Amz-Date|X-Amz-Expires|X-Amz-SignedHeaders)=)[^&\s\"']+"
)
_AWS_KEY_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")

_SENSITIVE_KEYS: frozenset[str] = frozenset(
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

_CORRELATION_FIELDS: tuple[str, ...] = (
    "trace_id",
    "request_id",
    "tenant_id",
    "table_hash",
    "cache_state",
    "duration_ms",
    "status",
    "rule",
    "reason",
    "worker_pid",
)


def redact_text(value: str) -> str:
    """Redact bearer tokens, presigned S3 query params, and AWS access key IDs from text."""
    sanitized = _BEARER_RE.sub(rf"\1{REDACTED}", value)
    sanitized = _AMZ_QUERY_RE.sub(rf"\1{REDACTED}", sanitized)
    sanitized = _AWS_KEY_RE.sub(REDACTED, sanitized)
    return sanitized


def _redact_value(val: Any) -> Any:
    """Recursively sanitize nested mappings, sequences, and strings."""
    if isinstance(val, dict):
        return redact_mapping(val)
    if isinstance(val, str):
        return redact_text(val)
    if isinstance(val, list | tuple):
        return [_redact_value(item) for item in val]
    return val


def redact_mapping(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively redact sensitive keys and string values in a structured log mapping."""
    result: dict[str, Any] = {}
    for key, val in data.items():
        if key.lower() in _SENSITIVE_KEYS:
            result[key] = REDACTED
        else:
            result[key] = _redact_value(val)
    return result


class JsonLogFormatter(logging.Formatter):
    """Formats log records as single-line structured JSON with redaction."""

    def __init__(self, service_name: str = "rag-tabular-analytics-worker") -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "message": redact_text(record.getMessage()),
        }

        for field_name in _CORRELATION_FIELDS:
            field_val = getattr(record, field_name, None)
            if field_val is not None:
                payload[field_name] = _redact_value(field_val)

        if "trace_id" not in payload:
            span_ctx = otel_trace.get_current_span().get_span_context()
            if span_ctx.is_valid:
                payload["trace_id"] = format(span_ctx.trace_id, "032x")

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            payload.update(redact_mapping(extra_fields))

        if record.exc_info and record.exc_info[1] is not None:
            payload["error_type"] = type(record.exc_info[1]).__name__
            payload["error_message"] = redact_text(str(record.exc_info[1]))

        return json.dumps(payload, default=str, separators=(",", ":"))


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a configured structured JSON logger without duplicate stream handlers."""
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)

    return logger
