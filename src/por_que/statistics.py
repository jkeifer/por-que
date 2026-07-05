from __future__ import annotations

import struct

from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel, Field

from .enums import (
    BoundaryOrderName,
    EncodingName,
    GeospatialTypeName,
    PageTypeName,
    Type,
    TypeName,
)
from .exceptions import ParquetFormatError, parse_context
from .protocols import AsyncReadableSeekable
from .schema import SchemaLeaf, SchemaLinked
from .util.spans import read_thrift_span

if TYPE_CHECKING:
    from .file_metadata import ColumnChunk, ColumnMetadata


class ColumnStatistics(
    SchemaLinked,
    frozen=True,
    ser_json_bytes='base64',
    val_json_bytes='base64',
):
    min_: bytes | None = Field(None, alias='min')
    max_: bytes | None = Field(None, alias='max')
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: bytes | None = None
    max_value: bytes | None = None
    is_min_value_exact: bool | None = None
    is_max_value_exact: bool | None = None
    schema_path: str

    def _converted_value(
        self,
        value: bytes | None,
        deprecated_value: bytes | None,
    ) -> Any:
        if value is None:
            value = deprecated_value

        if value is None:
            return None

        return self.schema_element.bytes_to_logical_type(value)

    @property
    def converted_min_value(self) -> Any:
        return self._converted_value(self.min_value, self.min_)

    @property
    def converted_max_value(self) -> Any:
        return self._converted_value(self.max_value, self.max_)


class SizeStatistics(BaseModel, frozen=True):
    """Size statistics for BYTE_ARRAY columns."""

    unencoded_byte_array_data_bytes: int | None = None
    repetition_level_histogram: list[int] | None = None
    definition_level_histogram: list[int] | None = None


class BoundingBox(BaseModel, frozen=True):
    """Bounding box for GEOMETRY or GEOGRAPHY types."""

    xmin: float
    xmax: float
    ymin: float
    ymax: float
    zmin: float | None = None
    zmax: float | None = None
    mmin: float | None = None
    mmax: float | None = None


class GeospatialStatistics(BaseModel, frozen=True):
    """Statistics specific to Geometry and Geography logical types."""

    bbox: BoundingBox | None = None
    geospatial_types: list[GeospatialTypeName] | None = None


class PageEncodingStats(BaseModel, frozen=True):
    """Statistics of a given page type and encoding."""

    page_type: PageTypeName
    encoding: EncodingName
    count: int


class PageLocation(BaseModel, frozen=True):
    """Location information for a page within a column chunk."""

    offset: int  # File offset of the page
    compressed_page_size: int  # Compressed size of the page
    first_row_index: int  # First row index of the page


class OffsetIndex(BaseModel, frozen=True):
    """Index containing page locations and sizes for efficient seeking."""

    start_offset: int
    byte_length: int
    page_locations: list[PageLocation]
    unencoded_byte_array_data_bytes: list[int] | None = None

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        start_offset: int,
        length: int | None = None,
    ) -> Self:
        """Parse Page Index data from file location.

        When ``length`` is known the exact span is fetched in one read;
        otherwise a speculative span is fetched and grown as needed.
        """
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        def parse(data: memoryview, offset: int) -> tuple[dict[str, Any], int]:
            parser = ThriftCompactParser(data, offset)
            props = PageIndexParser(parser).read_offset_index()
            return props, parser.pos - offset

        with parse_context(f'offset index at offset {start_offset}'):
            if length is not None:
                reader.seek(start_offset)
                data = await reader.read(length)
                props, byte_length = parse(memoryview(data), start_offset)
            else:
                props, byte_length = await read_thrift_span(
                    reader,
                    start_offset,
                    parse,
                )

            return cls(
                start_offset=start_offset,
                byte_length=byte_length,
                **props,
            )


class ColumnIndex(
    SchemaLinked,
    frozen=True,
    ser_json_bytes='base64',
    val_json_bytes='base64',
):
    """Index containing min/max statistics and null information for pages."""

    start_offset: int
    byte_length: int
    null_pages: list[bool]  # Which pages are all null
    min_values: list[bytes]  # Raw min values for each page
    max_values: list[bytes]  # Raw max values for each page
    boundary_order: BoundaryOrderName  # Whether min/max values are ordered
    null_counts: list[int] | None = None  # Null count per page
    repetition_level_histograms: list[int] | None = None
    definition_level_histograms: list[int] | None = None
    schema_path: str

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        start_offset: int,
        schema_element: SchemaLeaf,
        length: int | None = None,
    ) -> Self:
        """Parse Page Index data from file location.

        When ``length`` is known the exact span is fetched in one read;
        otherwise a speculative span is fetched and grown as needed.
        """
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        def parse(data: memoryview, offset: int) -> tuple[dict[str, Any], int]:
            parser = ThriftCompactParser(data, offset)
            props = PageIndexParser(parser).read_column_index()
            return props, parser.pos - offset

        with parse_context(f'column index at offset {start_offset}'):
            if length is not None:
                reader.seek(start_offset)
                data = await reader.read(length)
                props, byte_length = parse(memoryview(data), start_offset)
            else:
                props, byte_length = await read_thrift_span(
                    reader,
                    start_offset,
                    parse,
                )

            return cls(
                start_offset=start_offset,
                byte_length=byte_length,
                schema_path=schema_element.full_path,
                **props,
            )._link(schema_element)

    def _converted_values(self, values: list[bytes]) -> list[Any]:
        # min/max bytes for all-null pages are meaningless placeholders
        return [
            None if null_page else self.schema_element.bytes_to_logical_type(value)
            for value, null_page in zip(values, self.null_pages, strict=True)
        ]

    @property
    def converted_min_values(self) -> list[Any]:
        return self._converted_values(self.min_values)

    @property
    def converted_max_values(self) -> list[Any]:
        return self._converted_values(self.max_values)


# The eight salt constants that define the split-block bloom filter's mask.
# Each block has eight 32-bit words; word ``i`` is probed at the bit chosen by
# multiplying the lower 32 bits of the hash by ``SBBF_SALT[i]``. These exact
# values are mandated by the Parquet spec so that every reader and writer
# agrees on which bits a value touches.
SBBF_SALT = (
    0x47B6137B,
    0x44974D91,
    0x8824AD5B,
    0xA2B7289D,
    0x705495C7,
    0x2DF1424B,
    0x9EFC4947,
    0x5C6BFB31,
)

# A block is 256 bits: eight contiguous 32-bit words = 32 bytes. That is also a
# typical CPU cache line, which is the whole point of the design (see the
# BloomFilter docstring).
_SBBF_BLOCK_BYTES = 32
_SBBF_WORDS_PER_BLOCK = 8
_U32_MASK = 0xFFFFFFFF


class BloomFilter(BaseModel, frozen=True):
    """A split-block bloom filter (SBBF) for one column chunk.

    A bloom filter answers "might this value be present?" cheaply and without
    reading the column's data pages. Its answer has a deliberate asymmetry:

    * A ``False`` is exact -- the value is *definitely absent*. There are no
      false negatives, which is what makes a bloom filter safe for skipping
      row groups: if it says no, you can skip without risking wrong results.
    * A ``True`` is a *maybe* -- the value is probably present, but could be a
      false positive. The false-positive rate is tunable at write time by
      sizing the bitset relative to the number of distinct values.

    Parquet uses the *split-block* variant. Instead of setting bits spread
    across the whole bitset (which would touch many cache lines per lookup),
    every value is confined to a single 256-bit *block* sized to a CPU cache
    line. A lookup reads one block and probes eight bits within it, so a query
    is one cache-line access rather than eight scattered ones.

    This model is standalone: it is not part of the ``ParquetFile`` tree and is
    loaded on demand via :meth:`from_reader`, so serialized files are
    unaffected by its existence.
    """

    start_offset: int
    byte_length: int
    num_bytes: int
    header_length: int
    bitset: bytes = Field(repr=False)
    physical_type: TypeName

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnMetadata | ColumnChunk,
    ) -> Self:
        """Load and parse a column chunk's bloom filter from the file.

        The filter lives at ``bloom_filter_offset``: a thrift-compact
        ``BloomFilterHeader`` followed immediately by the raw bitset. When
        ``bloom_filter_length`` is recorded we fetch that exact span in one
        read; otherwise (older writers) we parse the header from a speculative
        span, learn the bitset size from it, then read the bitset.

        Raises:
            ParquetFormatError: If the chunk has no bloom filter, or the filter
                uses an unsupported algorithm/hash/compression.
        """
        from .parsers.parquet.bloom_filter import BloomFilterHeaderParser
        from .parsers.thrift.parser import ThriftCompactParser

        offset = chunk_metadata.bloom_filter_offset
        if offset is None:
            raise ParquetFormatError(
                f'Column chunk {chunk_metadata.path_in_schema!r} has no bloom filter',
            )
        length = chunk_metadata.bloom_filter_length
        physical_type = chunk_metadata.type

        def parse_header(data: memoryview, off: int) -> tuple[dict[str, Any], int]:
            parser = ThriftCompactParser(data, off)
            props = BloomFilterHeaderParser(parser).read_header()
            return props, parser.pos - off

        with parse_context(f'bloom filter at offset {offset}'):
            if length is not None:
                reader.seek(offset)
                data = await reader.read(length)
                header_props, header_length = parse_header(memoryview(data), offset)
                num_bytes = header_props['num_bytes']
                bitset = bytes(data[header_length : header_length + num_bytes])
                byte_length = length
            else:
                header_props, header_length = await read_thrift_span(
                    reader,
                    offset,
                    parse_header,
                )
                num_bytes = header_props['num_bytes']
                reader.seek(offset + header_length)
                bitset = await reader.read(num_bytes)
                byte_length = header_length + num_bytes

        if len(bitset) != num_bytes:
            raise ParquetFormatError(
                f'Bloom filter bitset is truncated: expected {num_bytes} '
                f'bytes, got {len(bitset)}',
            )

        return cls(
            start_offset=offset,
            byte_length=byte_length,
            num_bytes=num_bytes,
            header_length=header_length,
            bitset=bitset,
            physical_type=physical_type,
        )

    @property
    def num_blocks(self) -> int:
        """Number of 256-bit blocks in the bitset."""
        return self.num_bytes // _SBBF_BLOCK_BYTES

    def _plain_encode(self, value: Any) -> bytes:
        """PLAIN-encode a python value into the bytes the hash is fed.

        Parquet hashes the PLAIN encoding of the *physical* value: fixed-width
        little-endian bytes for the numeric types, and the raw value bytes
        (no length prefix) for byte arrays. This mirrors the reverse decoding
        in ``parsers/physical_types.py``.
        """
        match self.physical_type:
            case Type.INT32:
                return struct.pack('<i', value)
            case Type.INT64:
                return struct.pack('<q', value)
            case Type.FLOAT:
                return struct.pack('<f', value)
            case Type.DOUBLE:
                return struct.pack('<d', value)
            case Type.BYTE_ARRAY | Type.FIXED_LEN_BYTE_ARRAY:
                if isinstance(value, str):
                    return value.encode('utf-8')
                return bytes(value)
            case _:
                raise ParquetFormatError(
                    f'Cannot query a bloom filter for physical type '
                    f'{self.physical_type.name}',
                )

    def might_contain(self, value: Any) -> bool:
        """Whether ``value`` might be present in the column chunk.

        ``False`` is exact (the value is definitely absent). ``True`` means the
        value is probably present but may be a false positive. See the class
        docstring for why this asymmetry is the useful part.
        """
        from .util.xxhash import xxh64

        hash_ = xxh64(self._plain_encode(value))
        return self._block_check(hash_)

    def _block_check(self, hash_: int) -> bool:
        """Check the eight mask bits for ``hash_`` in its selected block.

        The top 32 bits of the hash pick the block (scaled into range so the
        distribution stays uniform without a modulo); the low 32 bits pick,
        via each salt, one bit per word. The value might be present only if
        every one of those eight bits is already set.
        """
        block_index = ((hash_ >> 32) * self.num_blocks) >> 32
        low = hash_ & _U32_MASK
        base = block_index * _SBBF_BLOCK_BYTES
        for i in range(_SBBF_WORDS_PER_BLOCK):
            start = base + i * 4
            word = int.from_bytes(self.bitset[start : start + 4], 'little')
            bit = ((low * SBBF_SALT[i]) & _U32_MASK) >> 27
            if not (word >> bit) & 1:
                return False
        return True
