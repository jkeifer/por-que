"""Tests for AsyncReadableSeekableAdapter."""

import asyncio
import io

from collections.abc import Iterator

import pytest

from por_que.protocols import AsyncReadableSeekable
from por_que.util.async_adapter import AsyncReadableSeekableAdapter


@pytest.fixture
def sample_file() -> Iterator[io.BytesIO]:
    data = b'Hello, World! This is a test file with some content.'
    file = io.BytesIO(data)
    yield file
    file.close()


@pytest.mark.asyncio
async def test_async_read(sample_file: io.BytesIO) -> None:
    """Test async read operation."""
    adapter = AsyncReadableSeekableAdapter(sample_file)
    data = await adapter.read(13)
    assert data == b'Hello, World!'
    assert adapter.tell() == 13


@pytest.mark.asyncio
async def test_async_read_all(sample_file: io.BytesIO) -> None:
    adapter = AsyncReadableSeekableAdapter(sample_file)
    data = await adapter.read()
    assert data == b'Hello, World! This is a test file with some content.'
    assert adapter.tell() == len(data)


@pytest.mark.asyncio
async def test_seek_and_tell(sample_file: io.BytesIO) -> None:
    adapter = AsyncReadableSeekableAdapter(sample_file)

    # Initial position
    assert adapter.tell() == 0

    # Seek to position 7
    adapter.seek(7)
    assert adapter.tell() == 7

    # Read from new position
    data = await adapter.read(5)
    assert data == b'World'
    assert adapter.tell() == 12


@pytest.mark.asyncio
async def test_seek_relative(sample_file: io.BytesIO) -> None:
    adapter = AsyncReadableSeekableAdapter(sample_file)

    # Seek to position 7
    adapter.seek(7)
    assert adapter.tell() == 7

    # Seek forward 6 bytes (relative)
    adapter.seek(6, 1)
    assert adapter.tell() == 13

    # Seek backward 5 bytes (relative)
    adapter.seek(-5, 1)
    assert adapter.tell() == 8


@pytest.mark.asyncio
async def test_seek_from_end(sample_file: io.BytesIO) -> None:
    adapter = AsyncReadableSeekableAdapter(sample_file)

    # Seek to 10 bytes before end
    adapter.seek(-10, 2)
    data = await adapter.read()
    assert data == b'e content.'


@pytest.mark.asyncio
async def test_concurrent_reads() -> None:
    data = b'0123456789' * 100  # 1000 bytes
    file = io.BytesIO(data)
    adapter = AsyncReadableSeekableAdapter(file)

    # Create multiple concurrent read tasks
    async def read_chunk(offset: int, size: int) -> bytes:
        adapter.seek(offset)
        return await adapter.read(size)

    results = await asyncio.gather(
        read_chunk(0, 10),
        read_chunk(100, 10),
        read_chunk(500, 10),
    )

    assert results[0] == b'0123456789'
    assert results[1] == b'0123456789'
    assert results[2] == b'0123456789'


@pytest.mark.asyncio
async def test_protocol_compliance(sample_file: io.BytesIO) -> None:
    """Test that AsyncReadableAdapter implements AsyncReadableSeekable protocol."""
    adapter = AsyncReadableSeekableAdapter(sample_file)

    # Runtime check that adapter implements the protocol
    assert isinstance(adapter, AsyncReadableSeekable)


@pytest.mark.asyncio
async def test_repr(sample_file: io.BytesIO) -> None:
    adapter = AsyncReadableSeekableAdapter(sample_file)
    repr_str = repr(adapter)

    assert 'AsyncReadableSeekableAdapter' in repr_str
    assert 'BytesIO' in repr_str
