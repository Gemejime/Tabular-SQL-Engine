"""Chaos soak, bounded 500-stream concurrency, and backpressure tests."""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import sys
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pyarrow.flight as fl
import pytest

from internal.config.settings import Settings
from internal.flight.daemon import WorkerDaemon
from internal.pool.circuit_breaker import RecycleReason
from test.fixtures.sample_data import write_sample_parquet


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def iter_chunks(self, chunk_size: int = 8192) -> Any:
        for idx in range(0, len(self._data), chunk_size):
            yield self._data[idx : idx + chunk_size]

    def close(self) -> None:
        return None


class _FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self._objects = objects

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": _FakeBody(self._objects[(Bucket, Key)])}


def _get_rss_bytes() -> int:
    """Return current process RSS in bytes using stdlib-only cross-platform fallbacks."""
    statm = Path("/proc/self/statm")
    if statm.exists():
        pages = int(statm.read_text(encoding="utf-8").split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")

    if sys.platform != "win32":
        import resource  # pragma: no cover

        usage = resource.getrusage(resource.RUSAGE_SELF)
        scale = 1 if sys.platform == "darwin" else 1024
        return int(usage.ru_maxrss) * scale

    current, _peak = tracemalloc.get_traced_memory()
    return int(current)


def _make_ticket(
    *,
    request_id: str,
    tenant_id: str,
    sql: str,
    table_hash: str,
    bucket: str,
    s3_key: str,
) -> fl.Ticket:
    payload = json.dumps(
        {
            "request_id": request_id,
            "tenant_id": tenant_id,
            "sql": sql,
            "table_alias": "tbl",
            "table_hash": table_hash,
            "s3_bucket": bucket,
            "s3_key": s3_key,
        }
    ).encode("utf-8")
    return fl.Ticket(payload)


def test_500_stream_bounded_concurrency_soak_and_memory_stability(tmp_path: Path) -> None:
    """Execute 500 mixed streams across worker recycles with 0 leaks or daemon crashes."""
    sample_path = write_sample_parquet(tmp_path / "soak.parquet", num_rows=200)
    parquet_bytes = sample_path.read_bytes()
    table_hash = hashlib.sha256(parquet_bytes).hexdigest()
    bucket = "rag-tabular-artifacts"
    tenant_id = "tenant-soak-01"
    s3_key = f"artifacts/{tenant_id}/{table_hash}.parquet"

    fake_s3 = _FakeS3Client({(bucket, s3_key): parquet_bytes})
    settings = Settings(
        FLIGHT_HOST="127.0.0.1",
        FLIGHT_PORT=0,
        HTTP_HOST="127.0.0.1",
        HTTP_PORT=0,
        CACHE_DIR=tmp_path / "nvme_cache",
        CACHE_MAX_BYTES=64 * 1024 * 1024,
        S3_BUCKET_ALLOWLIST=[bucket],
        POOL_SIZE=4,
        MAX_TASKS_PER_CHILD=50,
        MAX_CONCURRENT_QUERIES=4,
        QUERY_TIMEOUT_SECONDS=15.0,
    )

    tracemalloc.start()
    daemon = WorkerDaemon(settings=settings, s3_client=fake_s3)
    daemon.start()
    try:
        assert daemon.flight_server is not None
        port = daemon.flight_server.port

        # Warm the cache once so the 500-stream soak exercises warm-cache Flight + DuckDB.
        warm_client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
        warm_ticket = _make_ticket(
            request_id="req-warm",
            tenant_id=tenant_id,
            sql="SELECT COUNT(*) AS cnt FROM tbl",
            table_hash=table_hash,
            bucket=bucket,
            s3_key=s3_key,
        )
        assert warm_client.do_get(warm_ticket).read_all().column("cnt")[0].as_py() == 200
        baseline_rss = _get_rss_bytes()

        thread_local = threading.local()

        def _run_one(idx: int) -> str:
            client: fl.FlightClient | None = getattr(thread_local, "client", None)
            if client is None:
                client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
                thread_local.client = client

            # Mix valid analytical aggregations (90%) with AST-rejected SQL (10%).
            if idx % 10 == 9:
                bad_ticket = _make_ticket(
                    request_id=f"req-soak-{idx}",
                    tenant_id=tenant_id,
                    sql="SELECT * FROM read_parquet('/etc/passwd')",
                    table_hash=table_hash,
                    bucket=bucket,
                    s3_key=s3_key,
                )
                with pytest.raises(fl.FlightError):
                    client.do_get(bad_ticket).read_all()
                return "rejected"

            sql = (
                "SELECT region, COUNT(*) AS cnt, SUM(revenue) AS rev FROM tbl GROUP BY region"
                if idx % 2 == 0
                else "SELECT COUNT(*) AS cnt FROM tbl"
            )
            ticket = _make_ticket(
                request_id=f"req-soak-{idx}",
                tenant_id=tenant_id,
                sql=sql,
                table_hash=table_hash,
                bucket=bucket,
                s3_key=s3_key,
            )
            table = client.do_get(ticket).read_all()
            assert table.num_rows >= 1
            return "ok"

        total_streams = 500
        results: list[str] = []
        with ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_QUERIES) as executor:
            futures = [executor.submit(_run_one, i) for i in range(total_streams)]
            for fut in as_completed(futures):
                results.append(fut.result())

        assert results.count("ok") == 450
        assert results.count("rejected") == 50

        # Verify task-limit recycling occurred across 450 valid queries with MAX_TASKS_PER_CHILD=50.
        assert daemon.process_manager is not None
        recycles = daemon.process_manager.circuit_breaker.get_recycle_counts()
        assert recycles[RecycleReason.TASK_LIMIT] >= 1

        # Verify active queries returned to 0 and RSS growth stayed bounded.
        active_val = daemon.metrics.active_queries._value.get()  # noqa: SLF001
        assert active_val == 0.0
        final_rss = _get_rss_bytes()
        assert (final_rss - baseline_rss) < 128 * 1024 * 1024
    finally:
        daemon.shutdown()
        tracemalloc.stop()

    # Ensure zero child processes leaked after daemon shutdown.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        alive = [p for p in mp.active_children() if p.is_alive()]
        if not alive:
            break
        time.sleep(0.05)
    assert not [p for p in mp.active_children() if p.is_alive()]


def test_burst_backpressure_returns_resource_exhausted_without_permit_leak(
    tmp_path: Path,
) -> None:
    """Over-limit concurrent burst returns RESOURCE_EXHAUSTED and leaks 0 permits."""
    sample_path = write_sample_parquet(tmp_path / "burst.parquet", num_rows=500)
    parquet_bytes = sample_path.read_bytes()
    table_hash = hashlib.sha256(parquet_bytes).hexdigest()
    bucket = "rag-tabular-artifacts"
    tenant_id = "tenant-burst-01"
    s3_key = f"artifacts/{tenant_id}/{table_hash}.parquet"

    fake_s3 = _FakeS3Client({(bucket, s3_key): parquet_bytes})
    settings = Settings(
        FLIGHT_HOST="127.0.0.1",
        FLIGHT_PORT=0,
        HTTP_HOST="127.0.0.1",
        HTTP_PORT=0,
        CACHE_DIR=tmp_path / "nvme_cache",
        CACHE_MAX_BYTES=64 * 1024 * 1024,
        S3_BUCKET_ALLOWLIST=[bucket],
        POOL_SIZE=1,
        MAX_CONCURRENT_QUERIES=1,
        QUERY_TIMEOUT_SECONDS=15.0,
    )
    daemon = WorkerDaemon(settings=settings, s3_client=fake_s3)
    daemon.start()
    try:
        assert daemon.flight_server is not None
        port = daemon.flight_server.port

        # Warm cache first.
        warm_client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
        warm_client.do_get(
            _make_ticket(
                request_id="req-burst-warm",
                tenant_id=tenant_id,
                sql="SELECT COUNT(*) AS cnt FROM tbl",
                table_hash=table_hash,
                bucket=bucket,
                s3_key=s3_key,
            )
        ).read_all()

        # Hold the single concurrency permit on the server until the burst finishes.
        permit_acquired = threading.Event()
        release_holder = threading.Event()
        holder_done = threading.Event()
        orig_stream_query = daemon.process_manager.stream_query

        def _synchronized_stream_query(sql: str, parquet_path: Path, table_alias: str):  # type: ignore[no-untyped-def]
            if "CROSS JOIN" in sql:
                permit_acquired.set()
                release_holder.wait(timeout=10.0)
            return orig_stream_query(sql, parquet_path, table_alias)

        daemon.process_manager.stream_query = _synchronized_stream_query  # type: ignore[method-assign]

        def _hold_slot() -> None:
            client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
            client.do_get(
                _make_ticket(
                    request_id="req-burst-holder",
                    tenant_id=tenant_id,
                    sql="SELECT COUNT(*) AS cnt FROM tbl a CROSS JOIN tbl b",
                    table_hash=table_hash,
                    bucket=bucket,
                    s3_key=s3_key,
                )
            ).read_all()
            holder_done.set()

        holder = threading.Thread(target=_hold_slot, daemon=True)
        burst_client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
        rejected_exhausted = 0
        try:
            holder.start()
            assert permit_acquired.wait(timeout=5.0)
            assert daemon.metrics.active_queries._value.get() == 1.0  # noqa: SLF001

            for i in range(5):
                try:
                    burst_client.do_get(
                        _make_ticket(
                            request_id=f"req-burst-{i}",
                            tenant_id=tenant_id,
                            sql="SELECT COUNT(*) AS cnt FROM tbl",
                            table_hash=table_hash,
                            bucket=bucket,
                            s3_key=s3_key,
                        )
                    ).read_all()
                except fl.FlightError as exc:
                    if "MAX_CONCURRENT_QUERIES" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc):
                        rejected_exhausted += 1

            assert rejected_exhausted >= 1
        finally:
            release_holder.set()
            holder.join(timeout=10.0)
            daemon.process_manager.stream_query = orig_stream_query  # type: ignore[method-assign]

        assert holder_done.is_set()

        # Verify permit was cleanly released and subsequent query succeeds.
        assert daemon.metrics.active_queries._value.get() == 0.0  # noqa: SLF001
        follow_up = burst_client.do_get(
            _make_ticket(
                request_id="req-burst-followup",
                tenant_id=tenant_id,
                sql="SELECT COUNT(*) AS cnt FROM tbl",
                table_hash=table_hash,
                bucket=bucket,
                s3_key=s3_key,
            )
        ).read_all()
        assert follow_up.column("cnt")[0].as_py() == 500
        assert daemon.metrics.active_queries._value.get() == 0.0  # noqa: SLF001
    finally:
        daemon.shutdown()


@pytest.mark.soak
@pytest.mark.skipif(
    os.environ.get("RUN_SOAK_TESTS") != "1",
    reason="Extended 8-hour jemalloc/SLO endurance test; set RUN_SOAK_TESTS=1 to execute.",
)
def test_extended_8h_jemalloc_endurance_and_slo(tmp_path: Path) -> None:  # pragma: no cover
    """8-hour continuous jemalloc endurance and P95 <= 45ms warm-cache SLO check."""
    duration_seconds = float(os.environ.get("SOAK_DURATION_SECONDS", "28800"))
    sample_path = write_sample_parquet(tmp_path / "endurance.parquet", num_rows=50_000)
    parquet_bytes = sample_path.read_bytes()
    table_hash = hashlib.sha256(parquet_bytes).hexdigest()
    bucket = "rag-tabular-artifacts"
    tenant_id = "tenant-endurance"
    s3_key = f"artifacts/{tenant_id}/{table_hash}.parquet"

    fake_s3 = _FakeS3Client({(bucket, s3_key): parquet_bytes})
    settings = Settings(
        FLIGHT_HOST="127.0.0.1",
        FLIGHT_PORT=0,
        HTTP_HOST="127.0.0.1",
        HTTP_PORT=0,
        CACHE_DIR=tmp_path / "nvme_cache",
        S3_BUCKET_ALLOWLIST=[bucket],
        POOL_SIZE=4,
        MAX_TASKS_PER_CHILD=100,
        MAX_CONCURRENT_QUERIES=4,
    )
    daemon = WorkerDaemon(settings=settings, s3_client=fake_s3)
    daemon.start()
    try:
        assert daemon.flight_server is not None
        client = fl.FlightClient(f"grpc://127.0.0.1:{daemon.flight_server.port}")
        ticket = _make_ticket(
            request_id="req-endurance",
            tenant_id=tenant_id,
            sql="SELECT region, SUM(revenue) AS rev FROM tbl GROUP BY region",
            table_hash=table_hash,
            bucket=bucket,
            s3_key=s3_key,
        )
        latencies_ms: list[float] = []
        deadline = time.monotonic() + duration_seconds
        while time.monotonic() < deadline:
            t0 = time.perf_counter()
            table = client.do_get(ticket).read_all()
            assert table.num_rows == 4
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        latencies_ms.sort()
        p95_idx = int(0.95 * (len(latencies_ms) - 1))
        assert latencies_ms[p95_idx] <= 45.0
    finally:
        daemon.shutdown()
