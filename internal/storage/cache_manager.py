"""Tier-1 disk-backed NVMe Parquet cache with LRU eviction, refcounts, and single-flight."""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from internal.storage.s3_streamer import S3Streamer
from internal.storage.table_resolver import ResolvedTable
from internal.telemetry.logging import get_logger
from internal.telemetry.metrics import MetricsRegistry

_logger = get_logger("internal.storage.cache_manager")

CacheKey = tuple[str, str, str]
CacheState = Literal["warm_cache", "cold_s3"]
_PARQUET_MAGIC = b"PAR1"


class CacheCapacityError(RuntimeError):
    """Raised when CACHE_MAX_BYTES is exhausted and no unpinned entries can be evicted."""


class CorruptedArtifactError(ValueError):
    """Raised when a hydrated Parquet file fails PAR1 magic or metadata verification."""


@dataclass(slots=True)
class _CacheEntry:
    path: Path
    size_bytes: int
    refcount: int = 0
    pending_evict: bool = False


@dataclass(slots=True)
class PinnedArtifact:
    """Reader-pinned cached Parquet artifact handle; must call release() when done."""

    key: CacheKey
    path: Path
    size_bytes: int
    cache_state: CacheState
    _manager: CacheManager
    _entry: _CacheEntry | None = None
    _released: bool = False

    def release(self) -> None:
        """Decrement the active reader refcount (idempotent)."""
        if not self._released:
            self._released = True
            self._manager.release(self.key, entry=self._entry)


def _validate_parquet_file(path: Path) -> int:
    """Verify PAR1 header/footer magic bytes and PyArrow ParquetFile metadata."""
    try:
        size_bytes = path.stat().st_size
    except OSError as exc:
        raise CorruptedArtifactError(f"Cannot stat Parquet artifact: {exc}") from exc

    if size_bytes < 12:
        raise CorruptedArtifactError("Parquet artifact is smaller than minimum 12-byte format.")

    try:
        with path.open("rb") as fh:
            header = fh.read(4)
            fh.seek(-4, os.SEEK_END)
            footer = fh.read(4)
    except OSError as exc:
        raise CorruptedArtifactError(f"Failed reading Parquet magic bytes: {exc}") from exc

    if header != _PARQUET_MAGIC or footer != _PARQUET_MAGIC:
        raise CorruptedArtifactError("Artifact failed Parquet PAR1 header/footer validation.")

    try:
        pf = pq.ParquetFile(path)
        _ = pf.metadata
        del pf
    except (pa.ArrowException, OSError, ValueError) as exc:
        raise CorruptedArtifactError(f"Artifact failed Parquet metadata validation: {exc}") from exc

    return size_bytes


class CacheManager:
    """Thread-safe disk-backed LRU Parquet cache with single-flight S3 hydration."""

    def __init__(
        self,
        cache_dir: Path,
        max_bytes: int,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._cache_dir = cache_dir
        self._max_bytes = max_bytes
        self._metrics = metrics
        self._lock = threading.Lock()
        self._entries: OrderedDict[CacheKey, _CacheEntry] = OrderedDict()
        self._total_bytes = 0
        self._reserved_bytes = 0
        self._inflight: dict[CacheKey, asyncio.Future[tuple[Path, int]]] = {}

    @property
    def cache_dir(self) -> Path:
        """Return root cache directory."""
        return self._cache_dir

    @property
    def total_bytes(self) -> int:
        """Return total tracked cached bytes."""
        with self._lock:
            return self._total_bytes

    def initialize(self) -> None:
        """Create cache_dir, sweep orphaned .tmp files, and reconcile existing .parquet files."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        entries: OrderedDict[CacheKey, _CacheEntry] = OrderedDict()
        total_bytes = 0

        for path in self._cache_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.name.endswith(".tmp") or ".tmp." in path.name:
                with contextlib.suppress(OSError):
                    path.unlink(missing_ok=True)
                continue
            if path.suffix != ".parquet":
                continue

            try:
                rel = path.relative_to(self._cache_dir)
            except ValueError:
                continue
            if len(rel.parts) != 3:
                continue
            bucket, tenant_id, filename = rel.parts
            table_hash = filename.removesuffix(".parquet")
            key: CacheKey = (bucket, tenant_id, table_hash)
            try:
                size_bytes = path.stat().st_size
            except OSError:
                continue
            entries[key] = _CacheEntry(path=path, size_bytes=size_bytes)
            total_bytes += size_bytes

        with self._lock:
            self._entries = entries
            self._total_bytes = total_bytes

        if self._metrics is not None:
            self._metrics.set_cache_bytes_used(total_bytes)

        _logger.info(
            "Initialized Tier-1 Parquet cache",
            extra={
                "extra_fields": {
                    "cached_artifacts": len(entries),
                    "cache_bytes_used": total_bytes,
                    "cache_max_bytes": self._max_bytes,
                }
            },
        )

    def is_writable(self) -> bool:
        """Return True if cache_dir exists and is writable."""
        return self._cache_dir.is_dir() and os.access(self._cache_dir, os.W_OK | os.R_OK)

    def _try_pin_warm(self, key: CacheKey) -> PinnedArtifact | None:
        """Attempt to pin an already-cached artifact under lock."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.pending_evict:
                if entry.refcount == 0:
                    self._remove_entry_locked(key, unlink_file=True)
                    return None
                raise CorruptedArtifactError(
                    "Artifact is pending eviction while active readers finish."
                )
            if not entry.path.exists():
                if entry.refcount == 0:
                    self._remove_entry_locked(key, unlink_file=False)
                    return None
                entry.pending_evict = True
                raise CorruptedArtifactError("Cached artifact missing while active readers finish.")
            self._entries.move_to_end(key)
            entry.refcount += 1
            return PinnedArtifact(
                key=key,
                path=entry.path,
                size_bytes=entry.size_bytes,
                cache_state="warm_cache",
                _manager=self,
                _entry=entry,
            )

    def _remove_entry_locked(self, key: CacheKey, *, unlink_file: bool) -> None:
        """Remove an entry from LRU tracking (caller must hold self._lock)."""
        entry = self._entries.pop(key, None)
        if entry is None:
            return
        self._total_bytes = max(0, self._total_bytes - entry.size_bytes)
        if unlink_file:
            with contextlib.suppress(OSError):
                entry.path.unlink(missing_ok=True)
        if self._metrics is not None:
            self._metrics.set_cache_bytes_used(self._total_bytes)

    def release(self, key: CacheKey, *, entry: _CacheEntry | None = None) -> None:
        """Decrement active reader refcount and unlink if pending eviction."""
        with self._lock:
            tracked = self._entries.get(key)
            target = entry if entry is not None else tracked
            if target is None:
                return
            target.refcount = max(0, target.refcount - 1)
            if target.refcount == 0 and target.pending_evict:
                if tracked is target:
                    self._remove_entry_locked(key, unlink_file=True)
                else:
                    with contextlib.suppress(OSError):
                        target.path.unlink(missing_ok=True)

    def evict_corrupted(self, key: CacheKey) -> None:
        """Evict a corrupted artifact immediately or once active readers finish."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            if entry.refcount == 0:
                self._remove_entry_locked(key, unlink_file=True)
            else:
                entry.pending_evict = True

    def _commit_tmp_file(
        self,
        key: CacheKey,
        tmp_path: Path,
        dest_path: Path,
        size_bytes: int,
        *,
        reserved_bytes: int = 0,
    ) -> None:
        """Evict unpinned LRU entries if needed and atomically rename tmp_path to dest_path."""
        with self._lock:
            if reserved_bytes > 0:
                self._reserved_bytes = max(0, self._reserved_bytes - reserved_bytes)

            if size_bytes > self._max_bytes:
                with contextlib.suppress(OSError):
                    tmp_path.unlink(missing_ok=True)
                raise CacheCapacityError(
                    "Artifact size exceeds configured CACHE_MAX_BYTES ceiling."
                )

            # Remove stale entry for same key if present and unpinned.
            existing = self._entries.get(key)
            if existing is not None:
                if existing.refcount == 0:
                    self._remove_entry_locked(key, unlink_file=True)
                else:
                    with contextlib.suppress(OSError):
                        tmp_path.unlink(missing_ok=True)
                    raise CorruptedArtifactError(
                        "Existing cache entry is still pinned; cannot replace."
                    )

            for candidate_key, candidate_entry in list(self._entries.items()):
                if self._total_bytes + self._reserved_bytes + size_bytes <= self._max_bytes:
                    break
                if candidate_entry.refcount > 0 or candidate_entry.pending_evict:
                    continue
                self._remove_entry_locked(candidate_key, unlink_file=True)

            if self._total_bytes + self._reserved_bytes + size_bytes > self._max_bytes:
                with contextlib.suppress(OSError):
                    tmp_path.unlink(missing_ok=True)
                raise CacheCapacityError(
                    "Cache capacity exhausted with all cached entries pinned by active readers."
                )

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_path, dest_path)
            self._entries[key] = _CacheEntry(path=dest_path, size_bytes=size_bytes, refcount=0)
            self._entries.move_to_end(key)
            self._total_bytes += size_bytes
            if self._metrics is not None:
                self._metrics.set_cache_bytes_used(self._total_bytes)

    def _pin_after_hydration(self, key: CacheKey) -> PinnedArtifact:
        """Pin a newly hydrated entry for the caller."""
        pinned = self._try_pin_after_hydration(key)
        if pinned is None:
            raise RuntimeError("Hydrated artifact missing from cache immediately after commit.")
        return pinned

    def _try_pin_after_hydration(self, key: CacheKey) -> PinnedArtifact | None:
        """Pin a newly hydrated entry if it is still present in the cache."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.pending_evict or not entry.path.exists():
                return None
            self._entries.move_to_end(key)
            entry.refcount += 1
            return PinnedArtifact(
                key=key,
                path=entry.path,
                size_bytes=entry.size_bytes,
                cache_state="cold_s3",
                _manager=self,
                _entry=entry,
            )

    async def get_or_hydrate(
        self,
        resolved: ResolvedTable,
        s3_streamer: S3Streamer,
    ) -> PinnedArtifact:
        """Return a reader-pinned Parquet artifact, hydrating from S3 on cache miss."""
        key = resolved.cache_key
        recorded_miss = False

        while True:
            warm = self._try_pin_warm(key)
            if warm is not None:
                if not recorded_miss and self._metrics is not None:
                    self._metrics.record_cache_hit()
                return warm

            if not recorded_miss:
                if self._metrics is not None:
                    self._metrics.record_cache_miss()
                recorded_miss = True

            inflight_fut = self._inflight.get(key)
            if inflight_fut is not None:
                await asyncio.shield(inflight_fut)
                pinned = self._try_pin_after_hydration(key)
                if pinned is not None:
                    return pinned
                continue

            loop = asyncio.get_running_loop()
            leader_fut: asyncio.Future[tuple[Path, int]] = loop.create_future()
            self._inflight[key] = leader_fut

            tmp_path: Path | None = None
            reserved_bytes = 0
            reservation_released = False

            def _reserve_chunk(nbytes: int) -> None:
                nonlocal reserved_bytes
                with self._lock:
                    if reserved_bytes + nbytes > self._max_bytes:
                        raise CacheCapacityError(
                            "Artifact size exceeds configured CACHE_MAX_BYTES ceiling."
                        )
                    for candidate_key, candidate_entry in list(self._entries.items()):
                        if self._total_bytes + self._reserved_bytes + nbytes <= self._max_bytes:
                            break
                        if candidate_entry.refcount > 0 or candidate_entry.pending_evict:
                            continue
                        self._remove_entry_locked(candidate_key, unlink_file=True)
                    if self._total_bytes + self._reserved_bytes + nbytes > self._max_bytes:
                        raise CacheCapacityError(
                            "Cache capacity exhausted with all cached entries pinned "
                            "by active readers."
                        )
                    self._reserved_bytes += nbytes
                    reserved_bytes += nbytes

            try:
                dest_path = self._cache_dir / resolved.relative_cache_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                fd, tmp_name = tempfile.mkstemp(
                    prefix=f".{resolved.table_hash[:12]}.",
                    suffix=".tmp",
                    dir=dest_path.parent,
                )
                os.close(fd)
                tmp_path = Path(tmp_name)

                await s3_streamer.stream_to_file(
                    resolved.s3_bucket,
                    resolved.s3_key,
                    tmp_path,
                    on_chunk=_reserve_chunk,
                )
                size_bytes = _validate_parquet_file(tmp_path)
                self._commit_tmp_file(
                    key,
                    tmp_path,
                    dest_path,
                    size_bytes,
                    reserved_bytes=reserved_bytes,
                )
                reservation_released = True
                pinned = self._pin_after_hydration(key)
                if not leader_fut.done():
                    leader_fut.set_result((dest_path, size_bytes))
                return pinned
            except BaseException as exc:
                if not reservation_released and reserved_bytes > 0:
                    with self._lock:
                        self._reserved_bytes = max(0, self._reserved_bytes - reserved_bytes)
                    reservation_released = True
                if tmp_path is not None:
                    with contextlib.suppress(OSError):
                        tmp_path.unlink(missing_ok=True)
                if not leader_fut.done():
                    leader_fut.set_exception(exc)
                    # Mark exception retrieved in case there are no concurrent waiters.
                    leader_fut.exception()
                raise
            finally:
                self._inflight.pop(key, None)
