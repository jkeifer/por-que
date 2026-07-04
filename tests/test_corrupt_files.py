"""Corruption harness: students will feed this library broken bytes.

Every public entry point must fail with a helpful ``ParquetFormatError`` that
names what was being parsed, never a raw low-level traceback. These tests take
real fixture files (fetched once into memory), corrupt copies of them in
memory, and drive every async entry point over the corrupted bytes.

Nothing here touches a fixture file on disk: base bytes are fetched over HTTP
into memory and every corruption is a fresh ``bytes``/``BytesIO``.
"""

from __future__ import annotations

import io
import struct

from collections.abc import Callable
from urllib.request import urlopen

import pytest

from por_que import FileMetadata, ParquetFile
from por_que.constants import PARQUET_MAGIC
from por_que.exceptions import BufferExhaustedError, ParquetFormatError
from por_que.pages import Page
from por_que.protocols import AsyncReadableSeekable
from por_que.statistics import BloomFilter, ColumnIndex, OffsetIndex
from por_que.util.async_adapter import AsyncReadableSeekableAdapter

BASE_URL = 'https://raw.githubusercontent.com/apache/parquet-testing/master/data'

# A tiny single-column file (324 bytes) for the file-level corruption matrix,
# and a richer file carrying column/offset indexes and a bloom filter for the
# on-demand sub-structure entry points.
TINY_FILE = 'byte_array_decimal'
INDEXED_FILE = 'data_index_bloom_encoding_stats'

# The raw, low-level exception types that must NEVER escape a public entry
# point. ``ParquetFormatError`` is a ``ValueError`` but is not a subclass of any
# of these, so a genuine format error passes the guard while a raw leak fails
# it. ``pytest.raises(ParquetFormatError)`` already enforces this (a raw leak
# propagates straight past it), but we assert it explicitly to document intent.
RAW_LOW_LEVEL = (
    IndexError,
    KeyError,
    struct.error,
    UnicodeDecodeError,
    BufferExhaustedError,
)


# --------------------------------------------------------------------------- #
# In-memory base bytes and corruption helpers
# --------------------------------------------------------------------------- #
def _fetch(name: str) -> bytes:
    with urlopen(f'{BASE_URL}/{name}.parquet') as response:  # noqa: S310
        return response.read()


@pytest.fixture(scope='session')
def tiny_bytes() -> bytes:
    return _fetch(TINY_FILE)


@pytest.fixture(scope='session')
def indexed_bytes() -> bytes:
    return _fetch(INDEXED_FILE)


def _reader(data: bytes) -> AsyncReadableSeekable:
    """Wrap in-memory bytes as the async reader the entry points expect."""
    return AsyncReadableSeekableAdapter(io.BytesIO(data))


def _metadata_start(data: bytes) -> int:
    size = struct.unpack('<I', data[-8:-4])[0]
    return len(data) - len(PARQUET_MAGIC) - 4 - size


def _corrupt(data: bytes, start: int, length: int, fill: int = 0xFF) -> bytes:
    """Overwrite ``length`` bytes at ``start`` with ``fill`` in a fresh copy."""
    buf = bytearray(data)
    buf[start : start + length] = bytes([fill]) * length
    return bytes(buf)


def _assert_format_error(
    excinfo: pytest.ExceptionInfo[ParquetFormatError],
    *needles: str,
) -> None:
    err = excinfo.value
    # The key regression guard: nothing raw slipped out.
    assert not isinstance(err, RAW_LOW_LEVEL), f'raw low-level error escaped: {err!r}'
    message = str(err)
    assert message, 'format error carried no message'
    for needle in needles:
        assert needle in message, f'expected {needle!r} in {message!r}'


# --------------------------------------------------------------------------- #
# File-level corruption matrix (FileMetadata.from_reader)
# --------------------------------------------------------------------------- #
# Each builder takes the tiny base bytes and returns a corrupted copy. The
# second element is a substring the error message must contain (or ``None`` to
# only assert a clean, typed failure with no raw leak).
FileCase = tuple[Callable[[bytes], bytes], str | None]

FILE_LEVEL_CASES: dict[str, FileCase] = {
    'empty_file': (lambda d: b'', 'too small'),
    'under_12_bytes': (lambda d: d[:4], 'too small'),
    'zeroed_magic_footer': (lambda d: d[:-4] + bytes(4), 'magic footer'),
    'footer_cut_off': (lambda d: d[:-6], 'magic footer'),
    'metadata_size_zero': (
        lambda d: d[:-8] + struct.pack('<I', 0) + PARQUET_MAGIC,
        'out of bounds',
    ),
    'metadata_size_larger_than_file': (
        lambda d: d[:-8] + struct.pack('<I', 0xFFFFFFFF) + PARQUET_MAGIC,
        'out of bounds',
    ),
    # An unsigned size equal to the whole file drives metadata_start negative:
    # the "negative-equivalent garbage" case.
    'metadata_size_equals_filesize': (
        lambda d: d[:-8] + struct.pack('<I', len(d)) + PARQUET_MAGIC,
        'out of bounds',
    ),
    # metadata_size in the footer is left intact but bytes are removed from the
    # middle of the metadata region, so the declared span no longer lines up.
    'metadata_region_partially_cut': (
        lambda d: d[: _metadata_start(d) + 30] + d[-8:],
        None,
    ),
}


@pytest.mark.parametrize('case', FILE_LEVEL_CASES, ids=list(FILE_LEVEL_CASES))
@pytest.mark.asyncio
async def test_file_metadata_from_reader_rejects_corruption(
    tiny_bytes: bytes,
    case: str,
) -> None:
    builder, needle = FILE_LEVEL_CASES[case]
    corrupted = builder(tiny_bytes)

    with pytest.raises(ParquetFormatError) as excinfo:
        await FileMetadata.from_reader(_reader(corrupted))

    _assert_format_error(excinfo, *([] if needle is None else [needle]))


@pytest.mark.parametrize('case', FILE_LEVEL_CASES, ids=list(FILE_LEVEL_CASES))
@pytest.mark.asyncio
async def test_parquet_file_from_reader_rejects_corruption(
    tiny_bytes: bytes,
    case: str,
) -> None:
    builder, needle = FILE_LEVEL_CASES[case]
    corrupted = builder(tiny_bytes)

    with pytest.raises(ParquetFormatError) as excinfo:
        await ParquetFile.from_reader(_reader(corrupted), TINY_FILE)

    _assert_format_error(excinfo, *([] if needle is None else [needle]))


@pytest.mark.asyncio
async def test_parquet_file_rejects_zeroed_magic_header(tiny_bytes: bytes) -> None:
    # A footer-only truncation often leaves a plausible footer, so the leading
    # magic is validated too. FileMetadata alone never reads the header, so this
    # case is specific to ParquetFile.from_reader.
    corrupted = bytes(len(PARQUET_MAGIC)) + tiny_bytes[len(PARQUET_MAGIC) :]

    with pytest.raises(ParquetFormatError) as excinfo:
        await ParquetFile.from_reader(_reader(corrupted), TINY_FILE)

    _assert_format_error(excinfo, 'magic header')


@pytest.mark.asyncio
async def test_parquet_file_rejects_corrupt_data_region(tiny_bytes: bytes) -> None:
    # Metadata and footer are intact, but the data page bytes are garbage. The
    # footer parses cleanly; the failure surfaces while materializing pages.
    corrupted = _corrupt(tiny_bytes, start=4, length=32)

    # Metadata still parses on its own...
    await FileMetadata.from_reader(_reader(corrupted))

    # ...but building the full file (which reads the pages) must fail cleanly.
    with pytest.raises(ParquetFormatError) as excinfo:
        await ParquetFile.from_reader(_reader(corrupted), TINY_FILE)

    _assert_format_error(excinfo)


# --------------------------------------------------------------------------- #
# Thrift garbage inside the metadata region
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize('flip_offset', [2, 6, 12, 24, 48, 80])
@pytest.mark.asyncio
async def test_metadata_thrift_garbage_byte_flips(
    tiny_bytes: bytes,
    flip_offset: int,
) -> None:
    """Flipping bytes deep in the metadata region must fail cleanly.

    A single 0xFF byte can become a bad thrift field type, an absurd length, or
    corrupt an enum/int; whatever it becomes, the escaping error must be a typed
    ``ParquetFormatError`` with no raw low-level leak.
    """
    start = _metadata_start(tiny_bytes)
    corrupted = _corrupt(tiny_bytes, start=start + flip_offset, length=1)

    with pytest.raises(ParquetFormatError) as excinfo:
        await FileMetadata.from_reader(_reader(corrupted))

    _assert_format_error(excinfo)


@pytest.mark.asyncio
async def test_metadata_invalid_utf8_string(tiny_bytes: bytes) -> None:
    """Invalid UTF-8 in the created_by string must not leak UnicodeDecodeError."""
    created_by = b'HVR'  # start of this fixture's created_by value
    idx = tiny_bytes.index(created_by)
    corrupted = _corrupt(tiny_bytes, start=idx, length=1)

    with pytest.raises(ParquetFormatError) as excinfo:
        await FileMetadata.from_reader(_reader(corrupted))

    _assert_format_error(excinfo, 'file metadata')


# --------------------------------------------------------------------------- #
# On-demand sub-structure entry points (indexed fixture)
# --------------------------------------------------------------------------- #
# Offsets confirmed against the clean fixture:
#   String column: data_page_offset=4,
#   column_index_offset=156 (len 25), offset_index_offset=181 (len 11),
#   bloom_filter_offset=192 (length unrecorded).
DATA_PAGE_OFFSET = 4
COLUMN_INDEX_OFFSET, COLUMN_INDEX_LENGTH = 156, 25
OFFSET_INDEX_OFFSET, OFFSET_INDEX_LENGTH = 181, 11
BLOOM_FILTER_OFFSET = 192


@pytest.fixture(scope='session')
def indexed_metadata(indexed_bytes: bytes) -> FileMetadata:
    """Parse the clean fixture once to borrow real schema/chunk objects."""
    import asyncio

    return asyncio.run(FileMetadata.from_reader(_reader(indexed_bytes)))


def _string_chunk(metadata: FileMetadata):
    return metadata.row_groups[0].column_chunks['String']


@pytest.mark.asyncio
async def test_page_from_reader_truncated(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    schema_element = _string_chunk(indexed_metadata).metadata.schema_element
    # Truncate right after the page starts: the header cannot fit.
    truncated = indexed_bytes[: DATA_PAGE_OFFSET + 2]

    with pytest.raises(ParquetFormatError) as excinfo:
        await Page.from_reader(_reader(truncated), DATA_PAGE_OFFSET, schema_element)

    _assert_format_error(excinfo, 'truncated', 'page header', 'offset 4')


@pytest.mark.asyncio
async def test_page_from_reader_garbage(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    schema_element = _string_chunk(indexed_metadata).metadata.schema_element
    corrupted = _corrupt(indexed_bytes, start=DATA_PAGE_OFFSET, length=48)

    with pytest.raises(ParquetFormatError) as excinfo:
        await Page.from_reader(_reader(corrupted), DATA_PAGE_OFFSET, schema_element)

    _assert_format_error(excinfo)


@pytest.mark.asyncio
async def test_column_index_from_reader_truncated(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    schema_element = _string_chunk(indexed_metadata).metadata.schema_element
    truncated = indexed_bytes[: COLUMN_INDEX_OFFSET + 4]

    with pytest.raises(ParquetFormatError) as excinfo:
        await ColumnIndex.from_reader(
            _reader(truncated),
            COLUMN_INDEX_OFFSET,
            schema_element,
            COLUMN_INDEX_LENGTH,
        )

    _assert_format_error(excinfo, 'column index', 'offset 156')


@pytest.mark.asyncio
async def test_column_index_from_reader_garbage(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    schema_element = _string_chunk(indexed_metadata).metadata.schema_element
    corrupted = _corrupt(indexed_bytes, COLUMN_INDEX_OFFSET, COLUMN_INDEX_LENGTH)

    with pytest.raises(ParquetFormatError) as excinfo:
        await ColumnIndex.from_reader(
            _reader(corrupted),
            COLUMN_INDEX_OFFSET,
            schema_element,
            COLUMN_INDEX_LENGTH,
        )

    _assert_format_error(excinfo)


@pytest.mark.asyncio
async def test_offset_index_from_reader_truncated(indexed_bytes: bytes) -> None:
    truncated = indexed_bytes[: OFFSET_INDEX_OFFSET + 4]

    with pytest.raises(ParquetFormatError) as excinfo:
        await OffsetIndex.from_reader(
            _reader(truncated),
            OFFSET_INDEX_OFFSET,
            OFFSET_INDEX_LENGTH,
        )

    _assert_format_error(excinfo, 'offset index', 'offset 181')


@pytest.mark.asyncio
async def test_offset_index_from_reader_garbage(indexed_bytes: bytes) -> None:
    corrupted = _corrupt(indexed_bytes, OFFSET_INDEX_OFFSET, OFFSET_INDEX_LENGTH)

    with pytest.raises(ParquetFormatError) as excinfo:
        await OffsetIndex.from_reader(
            _reader(corrupted),
            OFFSET_INDEX_OFFSET,
            OFFSET_INDEX_LENGTH,
        )

    _assert_format_error(excinfo)


@pytest.mark.asyncio
async def test_bloom_filter_from_reader_truncated(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    chunk = _string_chunk(indexed_metadata)
    truncated = indexed_bytes[: BLOOM_FILTER_OFFSET + 3]

    with pytest.raises(ParquetFormatError) as excinfo:
        await BloomFilter.from_reader(_reader(truncated), chunk)

    _assert_format_error(excinfo, 'truncated', 'bloom filter', 'offset 192')


@pytest.mark.asyncio
async def test_bloom_filter_from_reader_garbage(
    indexed_bytes: bytes,
    indexed_metadata: FileMetadata,
) -> None:
    chunk = _string_chunk(indexed_metadata)
    corrupted = _corrupt(indexed_bytes, BLOOM_FILTER_OFFSET, 48)

    with pytest.raises(ParquetFormatError) as excinfo:
        await BloomFilter.from_reader(_reader(corrupted), chunk)

    _assert_format_error(excinfo)


@pytest.mark.asyncio
async def test_bloom_filter_missing_raises(tiny_bytes: bytes) -> None:
    # The tiny fixture's only column has no bloom filter; asking for one is a
    # clean, explicit ParquetFormatError rather than a surprise.
    metadata = await FileMetadata.from_reader(_reader(tiny_bytes))
    chunk = metadata.row_groups[0].column_chunks['value']

    with pytest.raises(ParquetFormatError) as excinfo:
        await BloomFilter.from_reader(_reader(tiny_bytes), chunk)

    _assert_format_error(excinfo, 'no bloom filter')
