"""Thread-safe Prometheus metrics registry and text exposition helper."""

from __future__ import annotations

import threading

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class MetricsRegistry:
    """Isolated Prometheus metrics registry for rag-tabular-analytics-worker."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._registry = registry or CollectorRegistry(auto_describe=True)
        self._ratio_lock = threading.Lock()
        self._hits_count = 0
        self._misses_count = 0

        self.active_queries = Gauge(
            "tabular_worker_active_queries",
            "Concurrent in-flight queries (primary KEDA autoscaling metric)",
            registry=self._registry,
        )
        self.query_duration_seconds = Histogram(
            "tabular_worker_query_duration_seconds",
            "End-to-end query execution latency in seconds",
            labelnames=("cache_state",),
            buckets=(0.005, 0.01, 0.025, 0.045, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            registry=self._registry,
        )
        self.cache_hit_ratio = Gauge(
            "tabular_worker_cache_hit_ratio",
            "Local NVMe Parquet cache hit ratio",
            registry=self._registry,
        )
        self.cache_hits_total = Counter(
            "tabular_worker_cache_hits_total",
            "Total local Parquet cache hits",
            registry=self._registry,
        )
        self.cache_misses_total = Counter(
            "tabular_worker_cache_misses_total",
            "Total local Parquet cache misses",
            registry=self._registry,
        )
        self.cache_bytes_used = Gauge(
            "tabular_worker_cache_bytes_used",
            "Current local NVMe cache disk utilization in bytes",
            registry=self._registry,
        )
        self.subprocess_recycles_total = Counter(
            "tabular_worker_subprocess_recycles_total",
            "Child worker subprocess recycling events",
            labelnames=("reason",),
            registry=self._registry,
        )
        self.flight_stream_bytes_total = Counter(
            "tabular_worker_flight_stream_bytes_total",
            "Total zero-copy Arrow Flight bytes streamed",
            registry=self._registry,
        )
        self.ast_rejections_total = Counter(
            "tabular_worker_ast_rejections_total",
            "Rejected SQL statements by violated AST rule",
            labelnames=("rule",),
            registry=self._registry,
        )

    @property
    def registry(self) -> CollectorRegistry:
        """Return underlying Prometheus CollectorRegistry."""
        return self._registry

    def inc_active_queries(self) -> None:
        """Increment active in-flight queries gauge."""
        self.active_queries.inc()

    def dec_active_queries(self) -> None:
        """Decrement active in-flight queries gauge."""
        self.active_queries.dec()

    def observe_query_duration(self, cache_state: str, duration_seconds: float) -> None:
        """Record query execution duration partitioned by cache_state."""
        self.query_duration_seconds.labels(cache_state=cache_state).observe(
            max(0.0, duration_seconds)
        )

    def record_cache_hit(self) -> None:
        """Increment cache hit counter and update hit ratio gauge."""
        self.cache_hits_total.inc()
        with self._ratio_lock:
            self._hits_count += 1
            total = self._hits_count + self._misses_count
            self.cache_hit_ratio.set(self._hits_count / total if total > 0 else 0.0)

    def record_cache_miss(self) -> None:
        """Increment cache miss counter and update hit ratio gauge."""
        self.cache_misses_total.inc()
        with self._ratio_lock:
            self._misses_count += 1
            total = self._hits_count + self._misses_count
            self.cache_hit_ratio.set(self._hits_count / total if total > 0 else 0.0)

    def set_cache_bytes_used(self, num_bytes: int) -> None:
        """Update current local cache disk utilization gauge."""
        self.cache_bytes_used.set(max(0, num_bytes))

    def inc_subprocess_recycle(self, reason: str) -> None:
        """Increment subprocess recycle counter for a given reason."""
        self.subprocess_recycles_total.labels(reason=reason).inc()

    def add_stream_bytes(self, num_bytes: int) -> None:
        """Add streamed Arrow IPC bytes to the Flight egress counter."""
        if num_bytes > 0:
            self.flight_stream_bytes_total.inc(num_bytes)

    def inc_ast_rejection(self, rule: str) -> None:
        """Increment AST rejection counter for a violated rule."""
        self.ast_rejections_total.labels(rule=rule).inc()

    def render_prometheus(self) -> bytes:
        """Render all registered metrics in Prometheus text exposition format 0.0.4."""
        result: bytes = generate_latest(self._registry)
        return result
