from __future__ import annotations

import asyncio
import json

from collections.abc import AsyncIterator, Iterator, Sequence
from enum import StrEnum
from io import SEEK_END
from pathlib import Path
from typing import Any, Literal, Self, assert_never

from pydantic import BaseModel, Field, model_validator

from ._version import get_version
from .constants import PARQUET_MAGIC
from .enums import Compression
from .exceptions import ParquetFormatError
from .file_metadata import (
    ColumnChunk,
    ColumnIndex,
    FileMetadata,
    OffsetIndex,
    SchemaLeaf,
)
from .pages import (
    AnyDataPage,
    DataPageV1,
    DataPageV2,
    DictionaryPage,
    IndexPage,
    Page,
)
from .parsers.page_content import DictType, ValueTuple
from .protocols import (
    AsyncCursableReadableSeekable,
    AsyncReadableSeekable,
    ReadableSeekable,
)
from .structuring import reconstruct as reconstruction
from .util.async_adapter import ensure_async_reader
from .util.iteration import AsyncChain
from .util.models import get_item_or_attr


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


class PorQueMeta(BaseModel, frozen=True):
    format_version: Literal[0] = 0
    por_que_version: str = get_version()


class PhysicalOffsetIndex(BaseModel, frozen=True):
    """Physical location and parsed content of Offset Index data."""

    offset_index_offset: int
    offset_index_length: int
    offset_index: OffsetIndex


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
    column_index: ColumnIndex | None = None
    offset_index: OffsetIndex | None = None
    row_group: int

    @model_validator(mode='before')
    @classmethod
    def inject_schema_element_from_context(cls, data: Any):
        """Inject schema element from context if not provided."""
        if not isinstance(data, dict):
            return data

        try:
            schema_element = get_item_or_attr(data['metadata'], 'schema_element')
        except (KeyError, ValueError):
            return data

        if not (schema_element or isinstance(schema_element, SchemaLeaf)):
            return data

        column_index = data.get('column_index', None)
        if column_index and isinstance(column_index, dict):
            column_index['schema_element'] = schema_element

        data_pages = data.get('data_pages', [])
        for data_page in data_pages:
            if isinstance(data_page, dict):
                data_page['schema_element'] = schema_element

        return data

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnChunk,
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
                chunk_metadata.metadata.schema_element,
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
            column_index = await ColumnIndex.from_reader(
                reader,
                chunk_metadata.column_index_offset,
                chunk_metadata.metadata.schema_element,
            )

        offset_index = None
        if chunk_metadata.offset_index_offset is not None:
            offset_index = await OffsetIndex.from_reader(
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
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> Iterator[ValueTuple]:
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
            dictionary_values=dictionary_values if dictionary_values else None,
            excluded_logical_columns=excluded_logical_columns,
        )

    async def parse_all_data_pages(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> AsyncIterator[ValueTuple]:
        """Parse all data from all pages in this column chunk.

        Args:
            reader: File-like object to read from

        Yields:
            Value tuples from all pages in this column
        """
        reader = ensure_async_reader(reader)

        dictionary_values = await self._parse_dictionary(reader)

        coroutines = [
            self.parse_data_page(
                page_index,
                (
                    reader.clone()
                    if isinstance(reader, AsyncCursableReadableSeekable)
                    else reader
                ),
                dictionary_values=dictionary_values,
                excluded_logical_columns=excluded_logical_columns,
            )
            for page_index in range(len(self.data_pages))
        ]

        if isinstance(reader, AsyncCursableReadableSeekable):
            # run all tasks concurrently, but get results in order
            tasks = [asyncio.create_task(coro) for coro in coroutines]
            for task in tasks:
                for value_tuple in await task:
                    yield value_tuple
            return

        # run tasks in serial, awaiting each before starting next
        for coroutine in coroutines:
            for value_tuple in await coroutine:
                yield value_tuple


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
    metadata: FileMetadata
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
            metadata = data['metadata']
            column_chunks = data['column_chunks']
        except KeyError:
            return data

        if not column_chunks:
            return data

        if not isinstance(metadata, FileMetadata):
            metadata = FileMetadata(**metadata)

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

        metadata = await FileMetadata.from_reader(reader)

        return cls(
            source=str(source),
            filesize=filesize,
            column_chunks=await cls._parse_column_chunks(
                reader,
                metadata,
            ),
            metadata=metadata,
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

    async def read_all_data(
        self,
        reader: AsyncReadableSeekable,
        excluded_logical_columns: Sequence[str] | None = None,
        reconstruct: bool = True,
    ) -> dict[str, Any]:
        # we can have multiple column chunks per column
        # so we need to chain each column chunk iterator together
        # to get a single iterator per column
        column_iters: dict[str, AsyncChain[ValueTuple]] = {}
        for cc in self.column_chunks:
            value_tuples = cc.parse_all_data_pages(
                reader.clone()
                if isinstance(reader, AsyncCursableReadableSeekable)
                else reader,
                excluded_logical_columns=excluded_logical_columns,
            )
            try:
                column_iters[cc.path_in_schema].add(value_tuples)
            except KeyError:
                column_iters[cc.path_in_schema] = AsyncChain(value_tuples)

        # When reconstruct=False, return flat data (tuples) for testing
        if not reconstruct:
            return {
                path: [v async for v in aiter(column_iter)]
                for path, column_iter in column_iters.items()
            }

        return await reconstruction(
            self.metadata.schema_root,
            {path: aiter(iterable) for path, iterable in column_iters.items()},
        )
