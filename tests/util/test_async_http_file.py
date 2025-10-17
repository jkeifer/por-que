import asyncio
import os

import pytest

from por_que.util.async_http_file import AsyncHttpFile


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_async_http_file_basic(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=80,
        prefetch_bytes=100,
    ) as hf:
        # Test initial state
        assert hf.tell() == 0

        # Test reading a chunk
        data = await hf.read(100)
        assert len(data) == 100
        assert hf.tell() == 100

        # Test reading another chunk
        data2 = await hf.read(50)
        assert len(data2) == 50
        assert hf.tell() == 150

        # Test seeking from the beginning of the file (SEEK_SET)
        hf.seek(0, os.SEEK_SET)
        assert hf.tell() == 0
        data3 = await hf.read(10)
        assert len(data3) == 10
        assert hf.tell() == 10

        # Test seeking from the current position (SEEK_CUR)
        hf.seek(20, os.SEEK_CUR)
        assert hf.tell() == 30
        data4 = await hf.read(20)
        assert len(data4) == 20
        assert hf.tell() == 50

        # Test seeking from the end of the file (SEEK_END)
        hf.seek(-50, os.SEEK_END)
        file_size = hf.size
        assert hf.tell() == file_size - 50
        data5 = await hf.read(50)
        assert len(data5) == 50
        assert hf.tell() == file_size

        # Test reading past the end of the file
        assert await hf.read() == b''
        assert hf.tell() == file_size

        # Test seeking past the end of the file
        hf.seek(1000, os.SEEK_END)
        assert hf.tell() == file_size  # Position should be clamped
        assert await hf.read() == b''

        # Test seeking before the beginning of the file
        hf.seek(-file_size - 100, os.SEEK_END)
        assert hf.tell() == 0  # Position should be clamped


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_concurrent_reads_same_range(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=100,
        prefetch_bytes=0,  # Disable prefetch to control exactly what's fetched
    ) as hf:
        # Create cursors for concurrent reads
        c1 = hf.clone()
        c2 = hf.clone()

        # Issue 3 concurrent reads of the same range from position 0
        # All should share the same HTTP fetch
        results = await asyncio.gather(
            hf.read(100),  # Root cursor at position 0
            c1.read(100),  # Cursor 1 at position 0
            c2.read(100),  # Cursor 2 at position 0
        )

        # All should return the same data
        assert results[0] == results[1] == results[2]
        assert len(results[0]) == 100

        # Verify cache has the data
        cache = hf.cursor.ohf.cache
        cached_intervals = list(cache.cache.find_overlapping(0, 100))
        assert len(cached_intervals) > 0
        # Should be bytes, not a Task anymore
        assert isinstance(cached_intervals[0].data, bytes)


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_concurrent_reads_different_ranges(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=50,
        prefetch_bytes=0,
    ) as hf:
        # Create cursors for concurrent reads
        c1 = hf.clone()
        c2 = hf.clone()

        # Position each file handle differently
        hf.seek(0)
        c1.seek(200)
        c2.seek(400)

        # Issue concurrent reads from different positions
        results = await asyncio.gather(
            hf.read(50),  # Read bytes 0-50
            c1.read(50),  # Read bytes 200-250
            c2.read(50),  # Read bytes 400-450
        )

        # All should return different data (since they're from different positions)
        assert len(results[0]) == 50
        assert len(results[1]) == 50
        assert len(results[2]) == 50

        # The data should be different (different file positions)
        assert results[0] != results[1]
        assert results[1] != results[2]
        assert results[0] != results[2]


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_overlapping_concurrent_reads(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=50,
        prefetch_bytes=0,
    ) as hf:
        # First read to populate cache
        await hf.read(100)  # Bytes 0-100 now cached

        # Reset position
        hf.seek(0)

        # Issue overlapping reads: 0-150 and 50-200
        # First should use cache for 0-100, fetch 100-150
        # Second should use cache for 50-100, fetch 100-200
        c1 = hf.clone()
        c1.seek(50)

        results = await asyncio.gather(
            hf.read(150),  # Read bytes 0-150
            c1.read(150),  # Read bytes 50-200
        )

        assert len(results[0]) == 150
        assert len(results[1]) == 150

        # Verify the overlapping portion matches
        # results[0][50:150] should equal results[1][0:100]
        assert results[0][50:150] == results[1][0:100]


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_async_http_file_context_manager(parquet_url: str) -> None:
    hf = AsyncHttpFile(parquet_url, prefetch_bytes=0)

    # Should not be opened initially
    assert hf._cursor is None

    async with hf as f:
        # Should be opened
        assert f._cursor is not None
        await f.read(10)

    # Should be closed after context exits
    assert hf._cursor is None


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_read_without_open_raises(parquet_url: str) -> None:
    hf = AsyncHttpFile(parquet_url)

    with pytest.raises(ValueError, match='I/O operation on closed file'):
        await hf.read(10)


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_cache_deduplication_under_load(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=50,
        prefetch_bytes=0,
    ) as hf:
        # Create many cursors for concurrent reads of the same range
        # Only one fetch should occur
        num_reads = 20
        cursors = [hf.clone() for _ in range(num_reads - 1)]
        tasks = [hf.read(100)] + [c.read(100) for c in cursors]

        results = await asyncio.gather(*tasks)

        # All should return the same data
        assert all(r == results[0] for r in results)
        assert len(results[0]) == 100

        # Verify cache has the data and it's bytes (not a Task)
        cache = hf.cursor.ohf.cache
        cached_intervals = list(cache.cache.find_overlapping(0, 100))
        assert len(cached_intervals) > 0
        assert isinstance(cached_intervals[0].data, bytes)


@pytest.mark.asyncio
@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
async def test_clone_user_after_close(parquet_url: str) -> None:
    async with AsyncHttpFile(
        parquet_url,
        minimum_range_request_bytes=50,
        prefetch_bytes=0,
    ) as hf:
        cursor = hf.clone()
    with pytest.raises(RuntimeError, match='Session is closed'):
        await cursor.read(100)
