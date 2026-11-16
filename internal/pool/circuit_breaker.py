"""Per-worker 5s execution circuit breaker and recycle reason tracking."""

from __future__ import annotations

from concurrent.futures import Future
from enum import StrEnum
from threading import Lock
from typing import Any

from pebble import ProcessExpired

from internal.telemetry.logging import get_logger

_logger = get_logger("internal.pool.circuit_breaker")


class RecycleReason(StrEnum):
    """Canonical worker subprocess recycle reasons (matches Prometheus label values)."""

    TIMEOUT = "TIMEOUT"
    MEMORY_LIMIT = "MEMORY_LIMIT"
    TASK_LIMIT = "TASK_LIMIT"
    CRASH = "CRASH"


class QueryTimeoutError(TimeoutError):
    """Raised when a child worker exceeds the hard execution deadline."""


class WorkerCrashError(RuntimeError):
    """Raised when a child worker terminates unexpectedly (SIGKILL, OOM, or panic)."""


class CircuitBreaker:
    """Monitors subprocess futures against hard deadlines and tracks recycle reasons."""

    def __init__(self, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = timeout_seconds
        self._lock = Lock()
        self._recycle_counts: dict[RecycleReason, int] = {reason: 0 for reason in RecycleReason}

    @property
    def timeout_seconds(self) -> float:
        """Return configured hard execution deadline in seconds."""
        return self._timeout_seconds

    def record_recycle(self, reason: RecycleReason) -> None:
        """Increment counter for a worker recycle reason."""
        with self._lock:
            self._recycle_counts[reason] += 1

    def get_recycle_counts(self) -> dict[RecycleReason, int]:
        """Return a snapshot of worker recycle counts by reason."""
        with self._lock:
            return dict(self._recycle_counts)

    def await_result(self, future: Future[Any]) -> Any:
        """Wait for a pebble subprocess future, translating timeouts and crashes."""
        try:
            return future.result(timeout=self._timeout_seconds)
        except TimeoutError as exc:
            self.record_recycle(RecycleReason.TIMEOUT)
            _logger.warning(
                "Worker subprocess exceeded execution deadline and was terminated",
                extra={"reason": RecycleReason.TIMEOUT.value},
            )
            raise QueryTimeoutError(
                f"Query exceeded {self._timeout_seconds:.1f}s execution deadline."
            ) from exc
        except ProcessExpired as exc:
            exit_code = getattr(exc, "exitcode", None)
            reason = RecycleReason.MEMORY_LIMIT if exit_code in (-9, 137) else RecycleReason.CRASH
            self.record_recycle(reason)
            _logger.error(
                "Worker subprocess terminated unexpectedly",
                extra={
                    "reason": reason.value,
                    "extra_fields": {"exit_code": exit_code},
                },
            )
            raise WorkerCrashError(
                f"Worker subprocess exited unexpectedly (exit_code={exit_code})."
            ) from exc
