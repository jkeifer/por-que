from __future__ import annotations

from typing import Any, Self

from pydantic import BaseModel, Field

from .enums import (
    BoundaryOrder,
    Encoding,
    GeospatialType,
    PageType,
)
from .protocols import AsyncReadableSeekable
from .schema import SchemaLeaf, SchemaLinked
from .util.spans import read_thrift_span


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
    geospatial_types: list[GeospatialType] | None = None


class PageEncodingStats(BaseModel, frozen=True):
    """Statistics of a given page type and encoding."""

    page_type: PageType
    encoding: Encoding
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

        if length is not None:
            reader.seek(start_offset)
            data = await reader.read(length)
            props, byte_length = parse(memoryview(data), start_offset)
        else:
            props, byte_length = await read_thrift_span(reader, start_offset, parse)

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
    boundary_order: BoundaryOrder  # Whether min/max values are ordered
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

        if length is not None:
            reader.seek(start_offset)
            data = await reader.read(length)
            props, byte_length = parse(memoryview(data), start_offset)
        else:
            props, byte_length = await read_thrift_span(reader, start_offset, parse)

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
