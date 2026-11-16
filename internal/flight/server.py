"""Apache Arrow FlightServerBase implementation for rag-tabular-analytics-worker."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow.flight as fl

from api.flight.schema import FlightErrorCode, sanitize_client_message
from internal.flight.handlers import FlightHandlers


class TabularFlightServer(fl.FlightServerBase):  # type: ignore[misc]
    """HTTP/2 gRPC Arrow Flight server handling DoGet and DoAction RPCs."""

    def __init__(
        self,
        location: str,
        handlers: FlightHandlers,
        **kwargs: Any,
    ) -> None:
        super().__init__(location, **kwargs)
        self._handlers = handlers

    def do_get(
        self,
        context: fl.ServerCallContext,
        ticket: fl.Ticket,
    ) -> fl.FlightDataStream:
        """Execute a validated SQL query and stream zero-copy Arrow RecordBatch chunks."""
        return self._handlers.handle_do_get(ticket.ticket)

    def list_actions(self, context: fl.ServerCallContext) -> list[tuple[str, str]]:
        """Advertise supported Flight DoAction operations."""
        return [
            ("cache_warm", "Pre-hydrate a content-addressed Parquet artifact into Tier-1 cache."),
            ("health_check", "Check worker pool health and active worker capacity."),
        ]

    def do_action(
        self,
        context: fl.ServerCallContext,
        action: fl.Action,
    ) -> Iterator[fl.Result]:
        """Handle `cache_warm` and `health_check` Flight actions."""
        action_type = action.type
        if action_type == "cache_warm":
            payload = self._handlers.handle_cache_warm(action.body.to_pybytes())
            yield fl.Result(payload)
            return
        if action_type == "health_check":
            payload = self._handlers.handle_health_check()
            yield fl.Result(payload)
            return

        raise fl.FlightServerError(
            sanitize_client_message(
                FlightErrorCode.INVALID_ARGUMENT,
                f"Unsupported Flight action: {action_type}",
            )
        )
