"""CLI entrypoint for rag-tabular-analytics-worker."""

from __future__ import annotations

from internal.flight.daemon import WorkerDaemon


def main() -> None:
    """Start the rag-tabular-analytics-worker daemon and block until termination."""
    daemon = WorkerDaemon()
    daemon.run_forever()


if __name__ == "__main__":
    main()
