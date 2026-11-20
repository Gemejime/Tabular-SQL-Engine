"""Integration tests for Arrow Flight server, S3 hydration, DuckDB pool, and HTTP ops."""

from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow.flight as fl
import pytest

from internal.config.settings import Settings
from internal.flight.daemon import WorkerDaemon
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
        self.get_calls = 0

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.get_calls += 1
        key = (Bucket, Key)
        if key not in self._objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "GetObject",
            )
        return {"Body": _FakeBody(self._objects[key])}


@pytest.fixture
def running_daemon(tmp_path: Path) -> Iterator[tuple[WorkerDaemon, _FakeS3Client, str, str]]:
    sample_path = write_sample_parquet(tmp_path / "source.parquet", num_rows=200)
    parquet_bytes = sample_path.read_bytes()
    table_hash = hashlib.sha256(parquet_bytes).hexdigest()
    bucket = "rag-tabular-artifacts"
    s3_key = f"artifacts/tenant-corp-01/{table_hash}.parquet"

    fake_s3 = _FakeS3Client({(bucket, s3_key): parquet_bytes})
    settings = Settings(
        FLIGHT_HOST="127.0.0.1",
        FLIGHT_PORT=0,
        HTTP_HOST="127.0.0.1",
        HTTP_PORT=0,
        CACHE_DIR=tmp_path / "nvme_cache",
        CACHE_MAX_BYTES=64 * 1024 * 1024,
        S3_BUCKET_ALLOWLIST=[bucket],
        POOL_SIZE=2,
        MAX_CONCURRENT_QUERIES=2,
        QUERY_TIMEOUT_SECONDS=20.0,
    )
    daemon = WorkerDaemon(settings=settings, s3_client=fake_s3)
    daemon.start()
    try:
        yield daemon, fake_s3, table_hash, s3_key
    finally:
        daemon.shutdown()


def _http_get(port: int, path: str) -> tuple[int, str]:
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5.0) as resp:  # noqa: S310
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def test_flight_do_get_do_action_and_ops_endpoints(
    running_daemon: tuple[WorkerDaemon, _FakeS3Client, str, str],
) -> None:
    daemon, fake_s3, table_hash, s3_key = running_daemon
    assert daemon.flight_server is not None
    assert daemon.ops_server is not None

    client = fl.FlightClient(f"grpc://127.0.0.1:{daemon.flight_server.port}")

    # 1. DoAction health_check
    health_results = list(client.do_action(fl.Action("health_check", b"{}")))
    health_payload = json.loads(health_results[0].body.to_pybytes().decode("utf-8"))
    assert health_payload == {"status": "ok", "active_workers": 2}

    # 2. DoAction cache_warm (cold S3 hydration)
    warm_req = json.dumps(
        {
            "tenant_id": "tenant-corp-01",
            "table_hash": table_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": s3_key,
        }
    ).encode("utf-8")
    warm_results = list(client.do_action(fl.Action("cache_warm", warm_req)))
    warm_payload = json.loads(warm_results[0].body.to_pybytes().decode("utf-8"))
    assert warm_payload["status"] == "warmed"
    assert warm_payload["bytes"] > 0
    assert fake_s3.get_calls == 1

    # 3. DoGet (warm cache hit -> zero-copy RecordBatch stream)
    ticket_bytes = json.dumps(
        {
            "request_id": "req_01",
            "tenant_id": "tenant-corp-01",
            "sql": (
                "SELECT region, SUM(revenue) AS total_rev FROM tbl GROUP BY region ORDER BY region"
            ),
            "table_alias": "tbl",
            "table_hash": table_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": s3_key,
        }
    ).encode("utf-8")
    reader = client.do_get(fl.Ticket(ticket_bytes))
    table = reader.read_all()
    assert table.num_rows == 4
    assert fake_s3.get_calls == 1  # Still 1: served from warm NVMe cache

    # 3b. DoGet empty result must preserve typed schema columns
    empty_ticket = json.dumps(
        {
            "request_id": "req_01_empty",
            "tenant_id": "tenant-corp-01",
            "sql": "SELECT id, region, revenue FROM tbl WHERE 1 = 0",
            "table_alias": "tbl",
            "table_hash": table_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": s3_key,
        }
    ).encode("utf-8")
    empty_table = client.do_get(fl.Ticket(empty_ticket)).read_all()
    assert empty_table.num_rows == 0
    assert empty_table.schema.names == ["id", "region", "revenue"]

    # 4. Error mapping checks: AST rejection, cross-tenant isolation, missing S3 object
    bad_sql_ticket = json.dumps(
        {
            "request_id": "req_02",
            "tenant_id": "tenant-corp-01",
            "sql": "PRAGMA show_tables",
            "table_alias": "tbl",
            "table_hash": table_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": s3_key,
        }
    ).encode("utf-8")
    with pytest.raises(fl.FlightServerError, match="INVALID_ARGUMENT"):
        client.do_get(fl.Ticket(bad_sql_ticket)).read_all()

    cross_tenant_ticket = json.dumps(
        {
            "request_id": "req_03",
            "tenant_id": "tenant-corp-01",
            "sql": "SELECT COUNT(*) FROM tbl",
            "table_alias": "tbl",
            "table_hash": table_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": f"artifacts/tenant-other/{table_hash}.parquet",
        }
    ).encode("utf-8")
    with pytest.raises(fl.FlightUnauthorizedError, match="PERMISSION_DENIED"):
        client.do_get(fl.Ticket(cross_tenant_ticket)).read_all()

    missing_hash = hashlib.sha256(b"does-not-exist").hexdigest()
    missing_ticket = json.dumps(
        {
            "request_id": "req_04",
            "tenant_id": "tenant-corp-01",
            "sql": "SELECT COUNT(*) FROM tbl",
            "table_alias": "tbl",
            "table_hash": missing_hash,
            "s3_bucket": "rag-tabular-artifacts",
            "s3_key": f"artifacts/tenant-corp-01/{missing_hash}.parquet",
        }
    ).encode("utf-8")
    with pytest.raises(fl.FlightServerError, match="NOT_FOUND"):
        client.do_get(fl.Ticket(missing_ticket)).read_all()

    # 5. Backpressure check: saturate concurrency semaphore
    acquired_1 = daemon.handlers._concurrency_semaphore.acquire(blocking=False)
    acquired_2 = daemon.handlers._concurrency_semaphore.acquire(blocking=False)
    assert acquired_1 and acquired_2
    try:
        with pytest.raises(fl.FlightServerError, match="RESOURCE_EXHAUSTED"):
            client.do_get(fl.Ticket(ticket_bytes))
    finally:
        daemon.handlers._concurrency_semaphore.release()
        daemon.handlers._concurrency_semaphore.release()

    # 6. HTTP ops endpoints (/healthz, /ready, /metrics, /openapi.yaml, /docs)
    http_port = daemon.ops_server.port
    status, body = _http_get(http_port, "/healthz")
    assert status == 200
    assert json.loads(body)["status"] == "ok"

    status, body = _http_get(http_port, "/ready")
    assert status == 200
    assert json.loads(body)["status"] == "ready"

    status, body = _http_get(http_port, "/metrics")
    assert status == 200
    assert "tabular_worker_active_queries" in body
    assert "tabular_worker_cache_hits_total" in body
    assert "tabular_worker_flight_stream_bytes_total" in body

    status, body = _http_get(http_port, "/openapi.yaml")
    assert status == 200
    assert "openapi: 3.1.0" in body

    status, body = _http_get(http_port, "/docs")
    assert status == 200
    assert "SwaggerUIBundle" in body

    # Drain -> /ready must return 503
    daemon.ops_state.mark_draining()
    status, body = _http_get(http_port, "/ready")
    assert status == 503
    assert json.loads(body)["status"] == "draining"


def test_flight_do_get_emits_otel_spans_and_updates_active_queries(
    running_daemon: tuple[WorkerDaemon, _FakeS3Client, str, str],
) -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    daemon, _fake_s3, table_hash, s3_key = running_daemon
    assert daemon.flight_server is not None
    assert daemon.ops_server is not None

    exporter = InMemorySpanExporter()
    current_provider = trace.get_tracer_provider()
    if isinstance(current_provider, TracerProvider):
        provider = current_provider
        owns_provider = False
    else:
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
        owns_provider = True
    processor = SimpleSpanProcessor(exporter)
    provider.add_span_processor(processor)
    try:
        client = fl.FlightClient(f"grpc://127.0.0.1:{daemon.flight_server.port}")
        trace_id = "abcdef0123456789abcdef0123456789"
        call_opts = fl.FlightCallOptions(
            headers=[(b"traceparent", f"00-{trace_id}-00f067aa0ba902b7-01".encode())]
        )
        ticket_bytes = json.dumps(
            {
                "request_id": "req_otel_01",
                "tenant_id": "tenant-corp-01",
                "sql": "SELECT COUNT(*) AS cnt FROM tbl",
                "table_alias": "tbl",
                "table_hash": table_hash,
                "s3_bucket": "rag-tabular-artifacts",
                "s3_key": s3_key,
            }
        ).encode("utf-8")

        table = client.do_get(fl.Ticket(ticket_bytes), options=call_opts).read_all()
        assert table.num_rows == 1

        spans = exporter.get_finished_spans()
        span_names = {s.name for s in spans}
        assert "flight.do_get" in span_names
        assert "sqlglot.validate" in span_names
        assert "storage.get_or_hydrate" in span_names
        assert "duckdb.subprocess_execute" in span_names
        for span in spans:
            assert format(span.context.trace_id, "032x") == trace_id
            if span.attributes:
                assert "sql" not in span.attributes

        status, body = _http_get(daemon.ops_server.port, "/metrics")
        assert status == 200
        assert "tabular_worker_active_queries 0.0" in body
    finally:
        provider.force_flush()
        if owns_provider:
            provider.shutdown()
