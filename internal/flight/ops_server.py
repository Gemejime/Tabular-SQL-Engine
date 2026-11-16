"""HTTP operational & OpenAPI 3.1 server (/healthz, /ready, /metrics, /openapi.yaml, /docs)."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from internal.pool.process_manager import ProcessManager
from internal.storage.cache_manager import CacheManager
from internal.telemetry.logging import get_logger
from internal.telemetry.metrics import MetricsRegistry

_logger = get_logger("internal.flight.ops_server")

_DEFAULT_OPENAPI_PATH = Path(__file__).resolve().parents[2] / "api" / "openapi" / "openapi.yaml"

_SWAGGER_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>rag-tabular-analytics-worker API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
    };
  </script>
</body>
</html>
"""


class OpsState:
    """Shared readiness, drain, metrics, and OpenAPI state for the HTTP ops server."""

    def __init__(
        self,
        *,
        service_name: str,
        process_manager: ProcessManager,
        cache_manager: CacheManager,
        metrics: MetricsRegistry,
        openapi_path: Path = _DEFAULT_OPENAPI_PATH,
    ) -> None:
        self.service_name = service_name
        self.process_manager = process_manager
        self.cache_manager = cache_manager
        self.metrics = metrics
        self.openapi_path = openapi_path
        self._draining = threading.Event()

    def mark_draining(self) -> None:
        """Mark the worker as draining so /ready returns 503 Service Unavailable."""
        self._draining.set()

    def clear_draining(self) -> None:
        """Clear the draining flag when starting or restarting the daemon."""
        self._draining.clear()

    def is_draining(self) -> bool:
        """Return True if graceful shutdown drain has been initiated."""
        return self._draining.is_set()

    def readiness_payload(self) -> tuple[int, dict[str, Any]]:
        """Evaluate readiness checks and return (HTTP status code, JSON body)."""
        if self.is_draining():
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                "status": "draining",
                "checks": {"drain": "shutting_down"},
            }

        pool_ok = self.process_manager.is_healthy()
        cache_ok = self.cache_manager.is_writable()
        if pool_ok and cache_ok:
            return HTTPStatus.OK, {
                "status": "ready",
                "checks": {
                    "process_pool": "healthy",
                    "cache_mount": "writable",
                },
            }

        return HTTPStatus.SERVICE_UNAVAILABLE, {
            "status": "not_ready",
            "checks": {
                "process_pool": "healthy" if pool_ok else "unhealthy",
                "cache_mount": "writable" if cache_ok else "unwritable",
            },
        }


class _OpsHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer carrying typed OpsState."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler_class: type[BaseHTTPRequestHandler],
        ops_state: OpsState,
    ) -> None:
        self.ops_state = ops_state
        super().__init__(server_address, request_handler_class)


class _OpsRequestHandler(BaseHTTPRequestHandler):
    """Handles Kubernetes probes, Prometheus scrape, OpenAPI spec, and Swagger UI."""

    server: _OpsHTTPServer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Suppress default stderr logging in favor of structured JSON logging."""
        _logger.debug(
            "Ops HTTP request",
            extra={"extra_fields": {"path": self.path}},
        )

    def _send_bytes(self, status_code: int, content_type: str, body: bytes) -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        state = self.server.ops_state
        path = self.path.split("?", 1)[0]

        if path == "/healthz":
            body = json.dumps({"status": "ok", "service": state.service_name}).encode("utf-8")
            self._send_bytes(HTTPStatus.OK, "application/json", body)
            return

        if path == "/ready":
            status_code, payload = state.readiness_payload()
            body = json.dumps(payload).encode("utf-8")
            self._send_bytes(status_code, "application/json", body)
            return

        if path == "/metrics":
            body = state.metrics.render_prometheus()
            self._send_bytes(HTTPStatus.OK, "text/plain; version=0.0.4; charset=utf-8", body)
            return

        if path == "/openapi.yaml":
            try:
                spec_bytes = state.openapi_path.read_bytes()
            except OSError:
                self._send_bytes(
                    HTTPStatus.NOT_FOUND,
                    "application/json",
                    b'{"error":"OpenAPI specification not found"}',
                )
                return
            self._send_bytes(HTTPStatus.OK, "application/yaml; charset=utf-8", spec_bytes)
            return

        if path == "/docs":
            self._send_bytes(HTTPStatus.OK, "text/html; charset=utf-8", _SWAGGER_UI_HTML.encode())
            return

        self._send_bytes(HTTPStatus.NOT_FOUND, "application/json", b'{"error":"Not found"}')


class OpsHTTPServer:
    """Lifecycle wrapper around ThreadingHTTPServer running on a background daemon thread."""

    def __init__(self, host: str, port: int, ops_state: OpsState) -> None:
        self._server = _OpsHTTPServer((host, port), _OpsRequestHandler, ops_state)
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """Return bound TCP port (useful when binding to ephemeral port 0 in tests)."""
        return int(self._server.server_address[1])

    def start(self) -> None:
        """Start serving HTTP ops requests on a background thread."""
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._server.serve_forever,
                name="ops-http-server",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        """Shut down the HTTP ops server and join its thread."""
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
