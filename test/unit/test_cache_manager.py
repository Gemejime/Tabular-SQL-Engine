"""Unit tests for Tier-1 NVMe CacheManager LRU, refcounting, single-flight, and Parquet checks."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

import pytest

from internal.storage import (
    ArtifactNotFoundError,
    CacheCapacityError,
    CacheManager,
    CorruptedArtifactError,
    S3Streamer,
    TableResolver,
)
from internal.telemetry.metrics import MetricsRegistry
from test.fixtures.sample_data import write_sample_parquet


class _FakeBody:
    def __init__(self, data: bytes, delay: float = 0.0) -> None:
        self._data = data
        self._delay = delay

    async def iter_chunks(self, chunk_size: int = 1024) -> Any:
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        for idx in range(0, len(self._data), chunk_size):
            yield self._data[idx : idx + chunk_size]

    def close(self) -> None:
        return None


class _FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes], delay: float = 0.0) -> None:
        self._objects = objects
        self._delay = delay
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
        return {"Body": _FakeBody(self._objects[key], delay=self._delay)}


def _make_hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def test_startup_reconciliation_and_tmp_sweep(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    h1 = _make_hash("table-1")
    valid_path = cache_dir / "bucket-a" / "tenant-1" / f"{h1}.parquet"
    valid_path.parent.mkdir(parents=True, exist_ok=True)
    write_sample_parquet(valid_path, num_rows=20)
    expected_size = valid_path.stat().st_size

    orphan_tmp = valid_path.parent / ".orphan.tmp"
    orphan_tmp.write_bytes(b"partial")

    metrics = MetricsRegistry()
    manager = CacheManager(cache_dir=cache_dir, max_bytes=1024 * 1024, metrics=metrics)
    manager.initialize()

    assert not orphan_tmp.exists()
    assert manager.total_bytes == expected_size
    assert metrics.cache_bytes_used._value.get() == expected_size


@pytest.mark.asyncio
async def test_single_flight_coalescing_hydrates_once(tmp_path: Path) -> None:
    sample_file = write_sample_parquet(tmp_path / "src.parquet", num_rows=50)
    parquet_bytes = sample_file.read_bytes()
    h1 = _make_hash("singleflight")
    s3_key = f"artifacts/tenant-1/{h1}.parquet"

    fake_client = _FakeS3Client({("bucket-a", s3_key): parquet_bytes}, delay=0.05)
    streamer = S3Streamer(chunk_size_bytes=4096, timeout_seconds=4.0, client=fake_client)
    metrics = MetricsRegistry()
    manager = CacheManager(
        cache_dir=tmp_path / "cache", max_bytes=10 * 1024 * 1024, metrics=metrics
    )
    manager.initialize()

    resolver = TableResolver(["bucket-a"])
    resolved = resolver.resolve(
        tenant_id="tenant-1",
        table_hash=h1,
        s3_bucket="bucket-a",
        s3_key=s3_key,
    )

    pinned_list = await asyncio.gather(
        manager.get_or_hydrate(resolved, streamer),
        manager.get_or_hydrate(resolved, streamer),
        manager.get_or_hydrate(resolved, streamer),
    )
    try:
        assert fake_client.get_calls == 1
        assert all(p.path.exists() for p in pinned_list)
        assert manager.total_bytes == len(parquet_bytes)
    finally:
        for p in pinned_list:
            p.release()


@pytest.mark.asyncio
async def test_lru_eviction_and_all_pinned_capacity_error(tmp_path: Path) -> None:
    src = write_sample_parquet(tmp_path / "src.parquet", num_rows=40)
    data = src.read_bytes()
    size = len(data)

    h1 = _make_hash("t1")
    h2 = _make_hash("t2")
    h3 = _make_hash("t3")
    k1 = f"artifacts/tenant-1/{h1}.parquet"
    k2 = f"artifacts/tenant-1/{h2}.parquet"
    k3 = f"artifacts/tenant-1/{h3}.parquet"

    fake_client = _FakeS3Client(
        {("bucket-a", k1): data, ("bucket-a", k2): data, ("bucket-a", k3): data}
    )
    streamer = S3Streamer(client=fake_client)
    # Capacity fits exactly 2 artifacts, not 3.
    manager = CacheManager(cache_dir=tmp_path / "cache", max_bytes=size * 2 + 10)
    manager.initialize()
    resolver = TableResolver(["bucket-a"])

    r1 = resolver.resolve(tenant_id="tenant-1", table_hash=h1, s3_bucket="bucket-a", s3_key=k1)
    r2 = resolver.resolve(tenant_id="tenant-1", table_hash=h2, s3_bucket="bucket-a", s3_key=k2)
    r3 = resolver.resolve(tenant_id="tenant-1", table_hash=h3, s3_bucket="bucket-a", s3_key=k3)

    p1 = await manager.get_or_hydrate(r1, streamer)
    p2 = await manager.get_or_hydrate(r2, streamer)

    # Both entries are pinned -> 3rd hydration must fail with CacheCapacityError.
    with pytest.raises(CacheCapacityError):
        await manager.get_or_hydrate(r3, streamer)

    # Release p1 (oldest), keep p2 pinned -> 3rd hydration evicts r1 and succeeds.
    p1.release()
    p3 = await manager.get_or_hydrate(r3, streamer)
    try:
        assert not (tmp_path / "cache" / r1.relative_cache_path).exists()
        assert (tmp_path / "cache" / r2.relative_cache_path).exists()
        assert (tmp_path / "cache" / r3.relative_cache_path).exists()
    finally:
        p2.release()
        p3.release()


@pytest.mark.asyncio
async def test_corrupted_parquet_rejected_and_not_committed(tmp_path: Path) -> None:
    h1 = _make_hash("corrupt")
    s3_key = f"artifacts/tenant-1/{h1}.parquet"
    fake_client = _FakeS3Client({("bucket-a", s3_key): b"NOT_A_PARQUET_FILE_AT_ALL"})
    streamer = S3Streamer(client=fake_client)
    manager = CacheManager(cache_dir=tmp_path / "cache", max_bytes=1024 * 1024)
    manager.initialize()
    resolver = TableResolver(["bucket-a"])
    resolved = resolver.resolve(
        tenant_id="tenant-1",
        table_hash=h1,
        s3_bucket="bucket-a",
        s3_key=s3_key,
    )

    with pytest.raises(CorruptedArtifactError):
        await manager.get_or_hydrate(resolved, streamer)
    assert manager.total_bytes == 0
    assert not (tmp_path / "cache" / resolved.relative_cache_path).exists()


@pytest.mark.asyncio
async def test_missing_s3_object_raises_artifact_not_found(tmp_path: Path) -> None:
    h1 = _make_hash("missing")
    s3_key = f"artifacts/tenant-1/{h1}.parquet"
    fake_client = _FakeS3Client({})
    streamer = S3Streamer(client=fake_client)
    manager = CacheManager(cache_dir=tmp_path / "cache", max_bytes=1024 * 1024)
    manager.initialize()
    resolver = TableResolver(["bucket-a"])
    resolved = resolver.resolve(
        tenant_id="tenant-1",
        table_hash=h1,
        s3_bucket="bucket-a",
        s3_key=s3_key,
    )

    with pytest.raises(ArtifactNotFoundError):
        await manager.get_or_hydrate(resolved, streamer)
