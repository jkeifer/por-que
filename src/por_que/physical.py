from __future__ import annotations

import asyncio
import inspect
import json
import struct

from enum import StrEnum
from io import SEEK_END
from pathlib import Path
from typing import Any, Literal, Self, TypeGuard, assert_never

from pydantic import BaseModel, Field, model_validator

from ._version import get_version
from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import Compression
from .exceptions import ParquetFormatError
from .file_metadata import (
    ColumnChunk,
    ColumnIndex,
    FileMetadata,
    OffsetIndex,
    SchemaRoot,
)
from .pages import (
    AnyDataPage,
    DataPageV1,
    DataPageV2,
    DictionaryPage,
    IndexPage,
    Page,
)
from .parsers.page_content import DictType, PageDataType
from .parsers.parquet.metadata import MetadataParser
from .protocols import (
    AsyncCursableReadableSeekable,
    AsyncReadableSeekable,
    ReadableSeekable,
)
from .util.async_adapter import AsyncReadableSeekableAdapter
from .util.models import get_item_or_attr


def is_async_reader(
    reader: ReadableSeekable | AsyncReadableSeekable,
) -> TypeGuard[AsyncReadableSeekable]:
    return hasattr(reader, 'read') and inspect.iscoroutinefunction(reader.read)


def ensure_async_reader(
    reader: ReadableSeekable | AsyncReadableSeekable,
) -> AsyncReadableSeekable:
    """Convert ReadableSeekable to AsyncReadableSeekable if needed."""
    if is_async_reader(reader):
        return reader
    # erroneous type checking error ignored
    return AsyncReadableSeekableAdapter(reader)  # type: ignore


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


class PorQueMeta(BaseModel, frozen=True):
    format_version: Literal[0] = 0
    por_que_version: str = get_version()


class PhysicalColumnIndex(BaseModel, frozen=True):
    """Physical location and parsed content of Column Index data."""

    column_index_offset: int
    column_index_length: int
    column_index: ColumnIndex

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        column_index_offset: int,
    ) -> Self:
        """Parse Page Index data from file location."""
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        reader.seek(column_index_offset)
        start_pos = reader.tell()

        # Parse page index data directly from file
        parser = ThriftCompactParser(reader, column_index_offset)
        column_index = await PageIndexParser(parser).read_column_index()

        end_pos = reader.tell()
        byte_length = end_pos - start_pos

        return cls(
            column_index_offset=column_index_offset,
            column_index_length=byte_length,
            column_index=column_index,
        )


class PhysicalOffsetIndex(BaseModel, frozen=True):
    """Physical location and parsed content of Offset Index data."""

    offset_index_offset: int
    offset_index_length: int
    offset_index: OffsetIndex

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        offset_index_offset: int,
    ) -> Self:
        """Parse Page Index data from file location."""
        from .parsers.parquet.page_index import PageIndexParser
        from .parsers.thrift.parser import ThriftCompactParser

        reader.seek(offset_index_offset)
        start_pos = reader.tell()

        # Parse page index data directly from file
        parser = ThriftCompactParser(reader, offset_index_offset)
        offset_index = await PageIndexParser(parser).read_offset_index()

        end_pos = reader.tell()
        byte_length = end_pos - start_pos

        return cls(
            offset_index_offset=offset_index_offset,
            offset_index_length=byte_length,
            offset_index=offset_index,
        )


class PhysicalColumnChunk(BaseModel, frozen=True):
    """A container for all the data for a single column within a row group."""

    path_in_schema: str
    start_offset: int
    total_byte_size: int
    codec: Compression
    num_values: int
    data_pages: list[AnyDataPage]
    index_pages: list[IndexPage]
    dictionary_page: DictionaryPage | None
    metadata: ColumnChunk = Field(exclude=True)
    column_index: PhysicalColumnIndex | None = None
    offset_index: PhysicalOffsetIndex | None = None
    row_group: int

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnChunk,
        schema_root: SchemaRoot,
        row_group: int,
    ) -> Self:
        """Parses all pages within a column chunk from a reader."""
        data_pages = []
        index_pages = []
        dictionary_page = None

        # The file_offset on the ColumnChunk struct can be misleading.
        # The actual start of the page data is the minimum of the page offsets.
        start_offset = chunk_metadata.data_page_offset
        if chunk_metadata.dictionary_page_offset is not None:
            start_offset = min(start_offset, chunk_metadata.dictionary_page_offset)

        current_offset = start_offset
        # The total_compressed_size is for all pages in the chunk.
        chunk_end_offset = start_offset + chunk_metadata.total_compressed_size

        # Read all pages sequentially within the column chunk's byte range
        while current_offset < chunk_end_offset:
            page = await Page.from_reader(
                reader,
                current_offset,
                schema_root,
                chunk_metadata,
            )

            # Sort pages by type
            if isinstance(page, DictionaryPage):
                if dictionary_page is not None:
                    raise ValueError('Multiple dictionary pages found in column chunk')
                dictionary_page = page
            elif isinstance(
                page,
                DataPageV1 | DataPageV2,
            ):
                data_pages.append(page)
            elif isinstance(page, IndexPage):
                index_pages.append(page)

            # Move to next page using the page size information
            current_offset = (
                page.start_offset + page.header_size + page.compressed_page_size
            )

        column_index = None
        if chunk_metadata.column_index_offset is not None:
            column_index = await PhysicalColumnIndex.from_reader(
                reader,
                chunk_metadata.column_index_offset,
            )

        offset_index = None
        if chunk_metadata.offset_index_offset is not None:
            offset_index = await PhysicalOffsetIndex.from_reader(
                reader,
                chunk_metadata.offset_index_offset,
            )

        return cls(
            path_in_schema=chunk_metadata.path_in_schema,
            start_offset=start_offset,
            total_byte_size=chunk_metadata.total_compressed_size,
            codec=chunk_metadata.codec,
            num_values=chunk_metadata.num_values,
            data_pages=data_pages,
            index_pages=index_pages,
            dictionary_page=dictionary_page,
            metadata=chunk_metadata,
            column_index=column_index,
            offset_index=offset_index,
            row_group=row_group,
        )

    async def _parse_dictionary(self, reader: AsyncReadableSeekable) -> DictType:
        """Parse dictionary content if dictionary page exists.

        Args:
            reader: File-like object to read from

        Returns:
            List of dictionary values as Python objects,
            or empty list if no dictionary page
        """
        if self.dictionary_page is None:
            return []

        return await self.dictionary_page.parse_content(
            reader=reader,
            physical_type=self.metadata.type,
            compression_codec=self.codec,
            schema_element=self.metadata.schema_element,
        )

    async def parse_data_page(
        self,
        page_index: int,
        reader: ReadableSeekable | AsyncReadableSeekable,
        dictionary_values: DictType | None = None,
    ) -> PageDataType:
        """Parse a data page in this column chunk.

        Args:
            page_index: Index in self.data_pages to parse
            reader: File-like object to read from
            dictionary_values: List of values from column chunk
                               dictionary page (optional)

        Returns:
            List of data values
        """
        reader = ensure_async_reader(reader)

        try:
            data_page = self.data_pages[page_index]
        except IndexError:
            raise ValueError(
                f'Data page index {page_index} is out of range '
                f'(page count: {len(self.data_pages)}',
            ) from None

        if dictionary_values is None:
            dictionary_values = await self._parse_dictionary(reader)

        return await data_page.parse_content(
            reader=reader,
            physical_type=self.metadata.type,
            compression_codec=self.codec,
            schema_element=self.metadata.schema_element,
            dictionary_values=dictionary_values if dictionary_values else None,
        )

    async def parse_all_data_pages(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
    ) -> PageDataType:
        """Parse all data from all pages in this column chunk into a single list.

        Args:
            reader: File-like object to read from

        Returns:
            Flattened list of all data values from all pages in this column
        """
        reader = ensure_async_reader(reader)

        dictionary_values = await self._parse_dictionary(reader)

        data: PageDataType = []
        for page_index in range(len(self.data_pages)):
            data.extend(
                await self.parse_data_page(
                    page_index,
                    reader,
                    dictionary_values=dictionary_values,
                ),
            )

        return data


class PhysicalMetadata(BaseModel, frozen=True):
    """The physical layout of the file metadata within the file."""

    start_offset: int
    total_byte_size: int
    metadata: FileMetadata

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
    ) -> Self:
        reader.seek(-FOOTER_SIZE, SEEK_END)
        footer_start = reader.tell()
        footer_bytes = await reader.read(FOOTER_SIZE)
        magic_footer = footer_bytes[4:8]

        if magic_footer != PARQUET_MAGIC:
            raise ParquetFormatError(
                'Invalid magic footer: expected '
                f'{PARQUET_MAGIC!r}, got {magic_footer!r}',
            )

        metadata_size = struct.unpack('<I', footer_bytes[:4])[0]

        # Parse metadata directly from file
        metadata_start = footer_start - metadata_size
        reader.seek(metadata_start)
        metadata = await MetadataParser(reader, metadata_start).parse()

        return cls(
            start_offset=metadata_start,
            total_byte_size=metadata_size,
            metadata=metadata,
        )


class ParquetFile(
    BaseModel,
    frozen=True,
    ser_json_bytes='base64',
    val_json_bytes='base64',
):
    """The root object representing the entire physical file structure."""

    source: str
    filesize: int
    column_chunks: list[PhysicalColumnChunk]
    metadata: PhysicalMetadata
    magic_header: str = PARQUET_MAGIC.decode()
    magic_footer: str = PARQUET_MAGIC.decode()
    meta_info: PorQueMeta = Field(
        default_factory=PorQueMeta,
        alias='_meta',
        description='Metadata about the por-que serialization format',
    )

    @model_validator(mode='before')
    @classmethod
    def inject_metadata_references(cls, data: Any) -> Any:
        """Inject metadata references into column chunks during validation."""
        if not isinstance(data, dict):
            return data

        try:
            physical_metadata = data['metadata']
            column_chunks = data['column_chunks']
        except KeyError:
            return data

        if not column_chunks:
            return data

        try:
            metadata: FileMetadata | dict = get_item_or_attr(
                physical_metadata,
                'metadata',
            )
        except ValueError:
            return data

        if not isinstance(metadata, FileMetadata):
            metadata = FileMetadata(**metadata)
            physical_metadata['metadata'] = metadata

        # Process each column chunk to add metadata reference
        updated_chunks = []
        for chunk_data in column_chunks:
            try:
                row_group: int = get_item_or_attr(
                    chunk_data,
                    'row_group',
                )
                path: str = get_item_or_attr(
                    chunk_data,
                    'path_in_schema',
                )
            except ValueError:
                return data

            # Find and inject the logical metadata reference
            try:
                column_chunk: ColumnChunk = metadata.row_groups[
                    row_group
                ].column_chunks[path]
            except (IndexError, KeyError):
                return data

            if hasattr(chunk_data, 'metadata') and chunk_data.metadata is column_chunk:
                updated_chunks.append(chunk_data)
            else:
                _chunk = (
                    chunk_data if isinstance(chunk_data, dict) else chunk_data.__dict__
                )
                _chunk['metadata'] = column_chunk
                updated_chunks.append(_chunk)

        # Update the data with injected metadata
        return {**data, 'column_chunks': updated_chunks}

    @classmethod
    async def from_reader(
        cls,
        reader: ReadableSeekable | AsyncReadableSeekable,
        source: Path | str,
    ) -> Self:
        reader = ensure_async_reader(reader)

        reader.seek(0, SEEK_END)
        filesize = reader.tell()

        if filesize < 12:
            raise ParquetFormatError('Parquet file is too small to be valid')

        phy_metadata = await PhysicalMetadata.from_reader(reader)
        column_chunks = await cls._parse_column_chunks(reader, phy_metadata.metadata)

        return cls(
            source=str(source),
            filesize=filesize,
            column_chunks=column_chunks,
            metadata=phy_metadata,
        )

    @classmethod
    async def _parse_column_chunks(
        cls,
        reader: AsyncReadableSeekable,
        metadata: FileMetadata,
    ) -> list[PhysicalColumnChunk]:
        # build list of coroutines to read each column chunk of every row group
        # not wrapping in tasks here to ensure we can control scheduling order
        # concurrently vs serialially based on support for parallel cursors
        coroutines = [
            PhysicalColumnChunk.from_reader(
                reader=(
                    reader.clone()
                    if isinstance(reader, AsyncCursableReadableSeekable)
                    else reader
                ),
                chunk_metadata=chunk_metadata,
                schema_root=metadata.schema_root,
                row_group=row_group_index,
            )
            for row_group_index, row_group_metadata in enumerate(metadata.row_groups)
            for chunk_metadata in row_group_metadata.column_chunks.values()
        ]

        if isinstance(reader, AsyncCursableReadableSeekable):
            # run all tasks concurrently, but get results in order
            tasks = [asyncio.create_task(coro) for coro in coroutines]
            return [await task for task in tasks]

        # run tasks in serial, awaiting each before starting next
        return [await asyncio.create_task(coro) for coro in coroutines]

    def to_dict(self, target: AsdictTarget = AsdictTarget.DICT) -> dict[str, Any]:
        match target:
            case AsdictTarget.DICT:
                return self.model_dump()
            case AsdictTarget.JSON:
                return self.model_dump(mode='json')
            case _:
                assert_never(target)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        data = json.loads(json_str)
        return cls.from_dict(data)
