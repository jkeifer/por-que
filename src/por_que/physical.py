from __future__ import annotations

import asyncio
import json

from collections.abc import AsyncIterator, Callable, Coroutine, Iterator, Sequence
from enum import StrEnum
from io import SEEK_END
from pathlib import Path
from typing import Any, Literal, Self, assert_never

from pydantic import BaseModel, Field, model_validator

from ._version import get_version
from .constants import FOOTER_SIZE, PARQUET_MAGIC
from .enums import CompressionName, ProgressPhase
from .exceptions import (
    ParquetCorruptedError,
    ParquetMagicError,
    parse_context,
)
from .file_metadata import (
    ColumnChunk,
    FileMetadata,
)
from .pages import (
    AnyDataPage,
    DataPageV1,
    DataPageV2,
    DictionaryPage,
    IndexPage,
    Page,
)
from .parsers.page_content import DictType, PageValue
from .protocols import (
    AsyncCursableReadableSeekable,
    AsyncReadableSeekable,
    ReadableSeekable,
)
from .schema import SchemaLeaf
from .statistics import BloomFilter, ColumnIndex, OffsetIndex
from .structuring import reconstruct as reconstruction
from .util.async_adapter import ensure_async_reader
from .util.iteration import AsyncChain
from .util.models import get_item_or_attr


class AsdictTarget(StrEnum):
    DICT = 'dict'
    JSON = 'json'


class PorQueMeta(BaseModel, frozen=True):
    """Self-identifying envelope stamped on every exported por-que model.

    Subclasses specialize ``model`` (the root-model discriminator a consumer
    dispatches on) and ``format_version`` (bumped per model when its serialized
    shape changes). The shared ``por_que_version`` records the producing tool.
    """

    # default_factory so the volatile vcs version doesn't leak into the
    # emitted JSON schema as a `default`
    por_que_version: str = Field(default_factory=get_version)


class FileMeta(PorQueMeta, frozen=True):
    """Envelope for a full ``ParquetFile`` dump."""

    # format 1: added `schema_path` reference keys and dropped the nested
    # PhysicalMetadata layer from the serialized structure.
    # format 2: added the `model` discriminator to the envelope so every export
    # self-identifies its root model. See git history around this comment for
    # the details of what changed when bumping the format version again.
    # format 3: added the GEOGRAPHY logical type's `algorithm` field and the
    # LZ4_RAW compression codec, both new members of the serialized shape.
    model: Literal['file'] = 'file'
    format_version: Literal[3] = 3


class MetadataMeta(PorQueMeta, frozen=True):
    """Envelope for a metadata-only export (:class:`MetadataExport`)."""

    # format 1: initial metadata-only export envelope.
    # format 2: added the GEOGRAPHY logical type's `algorithm` field and the
    # LZ4_RAW compression codec, both new members of the serialized shape.
    model: Literal['metadata'] = 'metadata'
    format_version: Literal[2] = 2


class PhysicalColumnChunk(BaseModel, frozen=True):
    """A container for all the data for a single column within a row group."""

    path_in_schema: str
    start_offset: int
    total_byte_size: int
    codec: CompressionName
    num_values: int
    data_pages: list[AnyDataPage]
    index_pages: list[IndexPage]
    dictionary_page: DictionaryPage | None
    metadata: ColumnChunk = Field(exclude=True)
    column_index: ColumnIndex | None = None
    offset_index: OffsetIndex | None = None
    row_group: int

    @classmethod
    async def from_reader(
        cls,
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnChunk,
        row_group: int,
    ) -> Self:
        """Parses all pages within a column chunk from a reader."""
        # The file_offset on the ColumnChunk struct can be misleading.
        # The actual start of the page data is the minimum of the page offsets.
        start_offset = chunk_metadata.data_page_offset
        if chunk_metadata.dictionary_page_offset is not None:
            start_offset = min(start_offset, chunk_metadata.dictionary_page_offset)

        # The page indexes live in known byte spans, so when both the offset
        # and length are present the exact span is fetched in one read and
        # parsed from memory. When the length is unknown, from_reader falls
        # back to a speculative span that grows as needed.
        column_index = None
        if chunk_metadata.column_index_offset is not None:
            column_index = await ColumnIndex.from_reader(
                reader,
                chunk_metadata.column_index_offset,
                chunk_metadata.metadata.schema_element,
                chunk_metadata.column_index_length,
            )

        offset_index = None
        if chunk_metadata.offset_index_offset is not None:
            offset_index = await OffsetIndex.from_reader(
                reader,
                chunk_metadata.offset_index_offset,
                chunk_metadata.offset_index_length,
            )

        # When an offset index is present the file tells us where every data
        # page lives, so we can discover structure with independent reads at
        # known locations instead of a dependent sequential walk. If the byte
        # accounting doesn't add up (e.g. index pages not covered by the offset
        # index), we fall back to the sequential walk for correctness.
        pages = None
        if offset_index is not None:
            pages = await cls._discover_pages_via_offset_index(
                reader,
                chunk_metadata,
                offset_index,
                start_offset,
            )

        if pages is None:
            pages = await cls._walk_pages(reader, chunk_metadata, start_offset)

        dictionary_page, data_pages, index_pages = pages

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

    @staticmethod
    async def _walk_pages(
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnChunk,
        start_offset: int,
    ) -> tuple[DictionaryPage | None, list[AnyDataPage], list[IndexPage]]:
        """Discover a chunk's pages by walking headers sequentially.

        Each read depends on the previous parse (parse a header, compute the
        next offset from its size, repeat), so over HTTP this serializes round
        trips. This is the general fallback: it works for any chunk, whether or
        not it has an offset index.
        """
        data_pages: list[AnyDataPage] = []
        index_pages: list[IndexPage] = []
        dictionary_page: DictionaryPage | None = None

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

        return dictionary_page, data_pages, index_pages

    @classmethod
    async def _discover_pages_via_offset_index(
        cls,
        reader: AsyncReadableSeekable,
        chunk_metadata: ColumnChunk,
        offset_index: OffsetIndex,
        start_offset: int,
    ) -> tuple[DictionaryPage | None, list[AnyDataPage], list[IndexPage]] | None:
        """Discover pages using the offset index: the file tells you where
        everything is.

        The offset index already lists every data page's ``offset`` and size,
        so instead of a dependent sequential walk we can parse each page header
        at its known location. Because those reads no longer depend on each
        other, they can run concurrently (when the reader can ``clone()`` into
        independent cursors) rather than serializing round trips.

        The offset index only covers *data* pages. The dictionary page (if any)
        is discovered separately via ``dictionary_page_offset``. Index pages are
        not covered at all: if the parsed pages plus the dictionary page don't
        tile the chunk's byte range exactly, some bytes are unaccounted for and
        we return ``None`` so the caller falls back to the sequential walk.
        """
        schema_element = chunk_metadata.metadata.schema_element

        # Header offsets at known locations: the dictionary page (if present)
        # followed by every data page the offset index points at.
        page_locations = sorted(
            offset_index.page_locations,
            key=lambda location: location.offset,
        )
        header_offsets = [location.offset for location in page_locations]
        dictionary_page_offset = chunk_metadata.dictionary_page_offset
        if dictionary_page_offset is not None:
            header_offsets.insert(0, dictionary_page_offset)

        def cursor() -> AsyncReadableSeekable:
            return (
                reader.clone()
                if isinstance(reader, AsyncCursableReadableSeekable)
                else reader
            )

        coroutines = [
            Page.from_reader(cursor(), offset, schema_element)
            for offset in header_offsets
        ]

        if isinstance(reader, AsyncCursableReadableSeekable):
            # Independent reads at known offsets: run them concurrently.
            tasks = [asyncio.create_task(coro) for coro in coroutines]
            parsed = [await task for task in tasks]
        else:
            # Still known offsets, but serialized without independent cursors.
            parsed = [await coro for coro in coroutines]

        dictionary_page: DictionaryPage | None = None
        pages = parsed
        if dictionary_page_offset is not None:
            first, *pages = parsed
            if not isinstance(first, DictionaryPage):
                return None
            dictionary_page = first

        data_pages: list[AnyDataPage] = []
        for page in pages:
            if not isinstance(page, DataPageV1 | DataPageV2):
                return None
            data_pages.append(page)

        # Gap detection: the parsed pages must tile the chunk's byte range
        # exactly. Any gap means uncovered bytes (e.g. an index page the offset
        # index doesn't describe), so we bail to the sequential walk.
        spans: list[tuple[int, int]] = []
        if dictionary_page is not None:
            spans.append(
                (
                    dictionary_page.start_offset,
                    dictionary_page.start_offset
                    + dictionary_page.header_size
                    + dictionary_page.compressed_page_size,
                ),
            )
        # PageLocation.compressed_page_size includes the page header, so it is
        # the full on-disk span of the page.
        spans.extend(
            (location.offset, location.offset + location.compressed_page_size)
            for location in page_locations
        )

        chunk_end_offset = start_offset + chunk_metadata.total_compressed_size
        if not cls._spans_tile_range(spans, start_offset, chunk_end_offset):
            return None

        return dictionary_page, data_pages, []

    @staticmethod
    def _spans_tile_range(
        spans: list[tuple[int, int]],
        start: int,
        end: int,
    ) -> bool:
        """Whether ``spans`` cover ``[start, end)`` contiguously with no gaps."""
        cursor = start
        for span_start, span_end in sorted(spans):
            if span_start != cursor:
                return False
            cursor = span_end
        return cursor == end

    async def load_bloom_filter(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
    ) -> BloomFilter:
        """Load this column chunk's bloom filter on demand.

        Bloom filters are a "go deeper" capability: they are never read during
        the eager structure parse, only when a caller explicitly wants to probe
        for a value. This is the one obvious entry point, delegating to
        :meth:`BloomFilter.from_reader`.

        Raises:
            ParquetFormatError: If this chunk has no bloom filter.
        """
        reader = ensure_async_reader(reader)
        return await BloomFilter.from_reader(reader, self.metadata)

    async def parse_dictionary(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
        *,
        apply_logical_types: bool = True,
    ) -> DictType:
        """Decode this chunk's dictionary page to its distinct values.

        Mirrors :meth:`parse_data_page` ergonomics: accepts a sync or async
        reader and applies logical types by default. Pass
        ``apply_logical_types=False`` for the raw physical values (the bytes
        the bloom filter hashes, for example).

        Returns:
            The dictionary's values, or ``[]`` if the chunk has no
            dictionary page.
        """
        reader = ensure_async_reader(reader)
        values = await self._parse_dictionary(reader)
        if not apply_logical_types:
            return values
        schema_element = self.metadata.schema_element
        return [schema_element.physical_to_logical_type(value) for value in values]

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
        *,
        dictionary_values: DictType | None = None,
        apply_logical_types: bool = True,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> Iterator[PageValue]:
        """Parse a data page in this column chunk.

        Args:
            page_index: Index in self.data_pages to parse
            reader: File-like object to read from
            dictionary_values: List of values from column chunk
                               dictionary page (optional)
            apply_logical_types: Whether to convert physical values to
                their logical representation.
            excluded_logical_columns: Column paths to exclude from logical
                type conversion, even when `apply_logical_types` is True.

        Returns:
            Iterator of PageValue entries.
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
            apply_logical_types=apply_logical_types,
            excluded_logical_columns=excluded_logical_columns,
        )

    async def parse_all_data_pages(
        self,
        reader: ReadableSeekable | AsyncReadableSeekable,
        *,
        apply_logical_types: bool = True,
        excluded_logical_columns: Sequence[str] | None = None,
    ) -> AsyncIterator[PageValue]:
        """Parse all data from all pages in this column chunk.

        Args:
            reader: File-like object to read from
            apply_logical_types: Whether to convert physical values to
                their logical representation.
            excluded_logical_columns: Column paths to exclude from logical
                type conversion, even when `apply_logical_types` is True.

        Yields:
            PageValue entries from all pages in this column
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
                apply_logical_types=apply_logical_types,
                excluded_logical_columns=excluded_logical_columns,
            )
            for page_index in range(len(self.data_pages))
        ]

        if isinstance(reader, AsyncCursableReadableSeekable):
            # run all tasks concurrently, but get results in order
            tasks = [asyncio.create_task(coro) for coro in coroutines]
            for task in tasks:
                for page_value in await task:
                    yield page_value
            return

        # run tasks in serial, awaiting each before starting next
        for coroutine in coroutines:
            for page_value in await coroutine:
                yield page_value


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
    meta_info: FileMeta = Field(
        default_factory=FileMeta,
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

    @model_validator(mode='after')
    def _relink_schema_references(self) -> Self:
        """Re-link physical page/index models to their schema leaves.

        Mirrors ``FileMetadata._relink_schema_references``, but walks the
        physical side of the file: each column chunk's column index, data
        pages, and per-page statistics, all keyed by their own
        ``schema_path``. Re-linking an already-linked model (the parse
        path) is harmless, so we don't special-case it.
        """
        schema_root = self.metadata.schema_root

        for chunk in self.column_chunks:
            if chunk.column_index is not None:
                leaf = schema_root.find_element(chunk.column_index.schema_path)
                if not isinstance(leaf, SchemaLeaf):
                    raise ValueError(
                        f'Column index path {chunk.column_index.schema_path!r} '
                        'does not resolve to a schema leaf',
                    )
                chunk.column_index.link(leaf)

            for page in chunk.data_pages:
                leaf = schema_root.find_element(page.schema_path)
                if not isinstance(leaf, SchemaLeaf):
                    raise ValueError(
                        f'Data page path {page.schema_path!r} does not '
                        'resolve to a schema leaf',
                    )
                page.link(leaf)
                if page.statistics is not None:
                    page.statistics.link(leaf)

        return self

    @classmethod
    async def from_reader(
        cls,
        reader: ReadableSeekable | AsyncReadableSeekable,
        source: Path | str,
        columns: Sequence[str] | None = None,
        row_groups: Sequence[int] | None = None,
        *,
        progress: Callable[[ProgressPhase, int, int], None] | None = None,
    ) -> Self:
        """Parse a file's structure, optionally materializing a subset.

        Args:
            reader: File-like object to read from.
            source: Identifier for the file (path or URL), stored on the model.
            columns: Full dotted ``path_in_schema`` names to materialize. When
                ``None`` (the default) every column is materialized. Unselected
                columns are simply absent from ``column_chunks`` -- calling
                ``reconstruct``/``parse_all_data_pages`` on an absent column is
                the caller's responsibility to avoid. Names matching nothing
                are ignored.
            row_groups: Row group ordinals to materialize. When ``None`` (the
                default) every row group is materialized. Chunks in unselected
                row groups are absent from ``column_chunks``.
            progress: Optional callback called as
                ``progress(phase, done, total)`` across three phases:
                ``ProgressPhase.METADATA_READ`` (bytes of the metadata
                span fetched), ``ProgressPhase.METADATA_PARSE`` (row
                groups parsed), and ``ProgressPhase.COLUMN_CHUNKS``
                (column chunk structures read). Each phase fires once
                with ``(phase, 0, total)`` before work starts, then once
                per unit of work completed. Exceptions raised by the
                callback propagate to the caller.

        Selection filters only which page structures are read from the file;
        ``metadata`` is always a full parse.
        """
        reader = ensure_async_reader(reader)

        reader.seek(0, SEEK_END)
        filesize = reader.tell()

        # Smallest possible file: header magic + metadata length + footer magic.
        min_size = 2 * len(PARQUET_MAGIC) + FOOTER_SIZE
        if filesize < min_size:
            raise ParquetCorruptedError(
                f'File is too small to be a Parquet file: {filesize} bytes '
                f'(need at least {min_size})',
            )

        # A truncated download or a non-parquet file often still has a plausible
        # footer, so validate the leading magic too before trusting anything.
        reader.seek(0)
        header_magic = await reader.read(len(PARQUET_MAGIC))
        if header_magic != PARQUET_MAGIC:
            raise ParquetMagicError(
                f'Invalid magic header: expected {PARQUET_MAGIC!r}, '
                f'got {header_magic!r}',
            )

        metadata = await FileMetadata.from_reader(reader, progress=progress)

        with parse_context(f'column chunks of {source}'):
            column_chunks = await cls._parse_column_chunks(
                reader,
                metadata,
                columns=columns,
                row_groups=row_groups,
                progress=progress,
            )

        return cls(
            source=str(source),
            filesize=filesize,
            column_chunks=column_chunks,
            metadata=metadata,
        )

    @classmethod
    async def _parse_column_chunks(
        cls,
        reader: AsyncReadableSeekable,
        metadata: FileMetadata,
        columns: Sequence[str] | None = None,
        row_groups: Sequence[int] | None = None,
        progress: Callable[[ProgressPhase, int, int], None] | None = None,
    ) -> list[PhysicalColumnChunk]:
        column_filter = None if columns is None else set(columns)
        row_group_filter = None if row_groups is None else set(row_groups)

        # build list of coroutines to read each selected column chunk of every
        # selected row group; not wrapping in tasks here to ensure we can
        # control scheduling order concurrently vs serially based on support
        # for parallel cursors
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
            if row_group_filter is None or row_group_index in row_group_filter
            for path, chunk_metadata in row_group_metadata.column_chunks.items()
            if column_filter is None or path in column_filter
        ]

        total = len(coroutines)
        if progress is not None:
            progress(ProgressPhase.COLUMN_CHUNKS, 0, total)

        # Tick as each chunk COMPLETES, not as ordered results are collected:
        # awaiting tasks in list order would freeze the count until the first
        # task finished even though later ones were already done, then jump.
        completed = 0

        async def tracked(
            coro: Coroutine[Any, Any, PhysicalColumnChunk],
        ) -> PhysicalColumnChunk:
            nonlocal completed
            chunk = await coro
            completed += 1
            if progress is not None:
                progress(ProgressPhase.COLUMN_CHUNKS, completed, total)
            return chunk

        if isinstance(reader, AsyncCursableReadableSeekable):
            # run all tasks concurrently, but get results in order
            tasks = [asyncio.create_task(tracked(coro)) for coro in coroutines]
            return [await task for task in tasks]

        # run tasks in serial, awaiting each before starting next
        return [await asyncio.create_task(tracked(coro)) for coro in coroutines]

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
        *,
        apply_logical_types: bool = True,
        excluded_logical_columns: Sequence[str] | None = None,
        reconstruct: bool = True,
    ) -> dict[str, Any]:
        # we can have multiple column chunks per column
        # so we need to chain each column chunk iterator together
        # to get a single iterator per column
        column_iters: dict[str, AsyncChain[PageValue]] = {}
        for cc in self.column_chunks:
            page_values = cc.parse_all_data_pages(
                reader.clone()
                if isinstance(reader, AsyncCursableReadableSeekable)
                else reader,
                apply_logical_types=apply_logical_types,
                excluded_logical_columns=excluded_logical_columns,
            )
            try:
                column_iters[cc.path_in_schema].add(page_values)
            except KeyError:
                column_iters[cc.path_in_schema] = AsyncChain(page_values)

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


class MetadataExport(BaseModel, frozen=True):
    """A metadata-only export: the file footer plus enough context to place it.

    This is the ``dump --metadata-only`` payload -- a first-class,
    app-consumable root model. It carries no page structure (no
    ``column_chunks``); everything a consumer needs to lay out the file comes
    from ``filesize`` and the footer ``metadata``.
    """

    source: str
    filesize: int
    metadata: FileMetadata
    meta_info: MetadataMeta = Field(
        default_factory=MetadataMeta,
        alias='_meta',
        description='Metadata about the por-que serialization format',
    )

    @classmethod
    async def from_reader(
        cls,
        reader: ReadableSeekable | AsyncReadableSeekable,
        source: Path | str,
        *,
        progress: Callable[[ProgressPhase, int, int], None] | None = None,
    ) -> Self:
        """Parse just the footer metadata, recording source and file size.

        The file size is known the moment we seek to the end to find the
        footer, so it costs nothing to capture here alongside the metadata.

        Args:
            reader: File-like object to read from.
            source: Identifier for the file (path or URL), stored on the model.
            progress: Optional callback passed through to
                :meth:`FileMetadata.from_reader`, called as
                ``progress(phase, done, total)`` for the
                ``ProgressPhase.METADATA_READ`` and
                ``ProgressPhase.METADATA_PARSE`` phases.
        """
        reader = ensure_async_reader(reader)

        reader.seek(0, SEEK_END)
        filesize = reader.tell()

        metadata = await FileMetadata.from_reader(reader, progress=progress)

        return cls(
            source=str(source),
            filesize=filesize,
            metadata=metadata,
        )

    def to_json(self, **kwargs: Any) -> str:
        return self.model_dump_json(by_alias=True, **kwargs)
