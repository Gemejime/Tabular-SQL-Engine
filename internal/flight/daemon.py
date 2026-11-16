"""Daemon lifecycle orchestrator for Arrow Flight server, Ops HTTP, S3 hydration, and pool."""

from __future__ import annotations

import asyncio
import signal
import threading
from types import FrameType
from typing import Any

from internal.config.settings import Settings, get_settings
from internal.flight.handlers import FlightHandlers
from internal.flight.ops_server import OpsHTTPServer, OpsState
from internal.flight.server import TabularFlightServer
from internal.pool.process_manager import ProcessManager
from internal.storage.cache_manager import CacheManager
from internal.storage.s3_streamer import S3Streamer
from internal.storage.table_resolver import TableResolver
from internal.telemetry.logging import get_logger
from internal.telemetry.metrics import MetricsRegistry
from internal.validator.ast_validator import ASTValidator

_logger = get_logger("internal.flight.daemon")


class HydrationLoopThread:
    """Dedicated background thread running the asyncio event loop for S3 hydration."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="s3-hydration-loop",
            daemon=True,
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the managed asyncio event loop."""
        return self._loop

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._ready.set)
        self._loop.run_forever()
        self._loop.close()

    def start(self) -> None:
        """Start the background asyncio thread and wait until the loop is running."""
        if self._thread.is_alive():
            return
        if self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._ready = threading.Event()
            self._thread = threading.Thread(
                target=self._run,
                name="s3-hydration-loop",
                daemon=True,
            )
        self._thread.start()
        self._ready.wait(timeout=5.0)

    def stop(self) -> None:
        """Stop the asyncio event loop and join its thread."""
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)


class WorkerDaemon:
    """Lifecycle orchestrator for the Arrow Flight daemon and Ops HTTP server."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        s3_client: Any | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.metrics = MetricsRegistry()
        self.hydration_thread = HydrationLoopThread()
        self.ast_validator = ASTValidator(
            max_sql_length=self.settings.MAX_SQL_LENGTH,
            max_joins=self.settings.MAX_JOINS,
        )
        self.table_resolver = TableResolver(self.settings.S3_BUCKET_ALLOWLIST)
        self.cache_manager = CacheManager(
            cache_dir=self.settings.CACHE_DIR,
            max_bytes=self.settings.CACHE_MAX_BYTES,
            metrics=self.metrics,
        )
        self.s3_streamer = S3Streamer(
            endpoint_url=self.settings.S3_ENDPOINT_URL,
            region_name=self.settings.S3_REGION,
            chunk_size_bytes=self.settings.S3_CHUNK_SIZE_BYTES,
            timeout_seconds=self.settings.S3_TIMEOUT_SECONDS,
            client=s3_client,
        )
        self.process_manager = ProcessManager(self.settings)
        self.handlers = FlightHandlers(
            settings=self.settings,
            ast_validator=self.ast_validator,
            table_resolver=self.table_resolver,
            cache_manager=self.cache_manager,
            s3_streamer=self.s3_streamer,
            process_manager=self.process_manager,
            hydration_loop=self.hydration_thread.loop,
            metrics=self.metrics,
        )
        self.ops_state = OpsState(
            service_name=self.settings.SERVICE_NAME,
            process_manager=self.process_manager,
            cache_manager=self.cache_manager,
            metrics=self.metrics,
        )
        self.flight_server: TabularFlightServer | None = None
        self.ops_server: OpsHTTPServer | None = None
        self._stop_event = threading.Event()
        self._flight_thread: threading.Thread | None = None
        self._is_shut_down = False

    def start(self) -> None:
        """Start hydration loop, S3 client, cache, worker pool, Flight server, and Ops HTTP."""
        self._is_shut_down = False
        self._stop_event.clear()
        self.ops_state.clear_draining()

        try:
            self.hydration_thread.start()
            self.handlers.set_hydration_loop(self.hydration_thread.loop)
            asyncio.run_coroutine_threadsafe(
                self.s3_streamer.start(),
                self.hydration_thread.loop,
            ).result(timeout=5.0)

            self.cache_manager.initialize()
            self.process_manager.start()

            location = f"grpc://{self.settings.FLIGHT_HOST}:{self.settings.FLIGHT_PORT}"
            self.flight_server = TabularFlightServer(location, self.handlers)
            self._flight_thread = threading.Thread(
                target=self.flight_server.serve,
                name="arrow-flight-server",
                daemon=True,
            )
            self._flight_thread.start()

            self.ops_server = OpsHTTPServer(
                self.settings.HTTP_HOST,
                self.settings.HTTP_PORT,
                self.ops_state,
            )
            self.ops_server.start()

            _logger.info(
                "Started rag-tabular-analytics-worker daemon",
                extra={
                    "extra_fields": {
                        "flight_port": self.flight_server.port,
                        "http_port": self.ops_server.port,
                    }
                },
            )
        except BaseException:
            self.shutdown()
            raise

    def shutdown(self) -> None:
        """Gracefully drain readiness, shut down Flight server, pool, S3 client, and loop."""
        if self._is_shut_down:
            return
        self._is_shut_down = True
        self._stop_event.set()
        self.ops_state.mark_draining()

        _logger.info("Initiating graceful shutdown drain")

        if self.flight_server is not None:
            self.flight_server.shutdown()
            self.flight_server.wait()
            self.flight_server = None
        if self._flight_thread is not None:
            if self._flight_thread.is_alive():
                self._flight_thread.join(timeout=5.0)
            self._flight_thread = None

        self.process_manager.close()

        if self.ops_server is not None:
            self.ops_server.close()
            self.ops_server = None

        if self.hydration_thread.loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self.s3_streamer.close(),
                self.hydration_thread.loop,
            ).result(timeout=5.0)
        self.hydration_thread.stop()
        _logger.info("Shutdown complete")

    def run_forever(self) -> None:
        """Start daemon and block until SIGINT or SIGTERM triggers graceful shutdown."""

        def _handle_signal(signum: int, _frame: FrameType | None) -> None:
            _logger.info(
                "Received termination signal",
                extra={"extra_fields": {"signal": signum}},
            )
            self._stop_event.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        try:
            self.start()
            while not self._stop_event.is_set():
                self._stop_event.wait(timeout=0.5)
        finally:
            self.shutdown()
