"""Loading parquet sources for the CLI.

Every command accepts either a local file path or an ``http(s)`` URL. Remote
files are read with ``AsyncHttpFile`` (range requests, no full download); local
files are opened normally and adapted to the async reader interface by the
library itself. Parsing is async, so commands cross the sync/async boundary
with ``asyncio.run``.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import cast

from por_que import AsyncHttpFile, FileMetadata, MetadataExport, ParquetFile
from por_que.protocols import ReadableSeekable


def is_url(source: str) -> bool:
    return source.startswith(('http://', 'https://'))


async def load_metadata(source: str) -> FileMetadata:
    """Parse just the file footer metadata (schema, row groups, key-values)."""
    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await FileMetadata.from_reader(reader)

    with Path(source).open('rb') as handle:
        return await FileMetadata.from_reader(cast('ReadableSeekable', handle))


async def load_metadata_export(source: str) -> MetadataExport:
    """Parse the footer metadata into a self-identifying export envelope.

    Unlike :func:`load_metadata`, this captures ``source`` and ``filesize``
    so the result is a first-class, app-consumable ``MetadataExport``.
    """
    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await MetadataExport.from_reader(reader, source)

    with Path(source).open('rb') as handle:
        return await MetadataExport.from_reader(
            cast('ReadableSeekable', handle),
            source,
        )


async def load_file(
    source: str,
    columns: Sequence[str] | None = None,
    row_groups: Sequence[int] | None = None,
) -> ParquetFile:
    """Parse the physical file, optionally materializing only a subset.

    ``columns`` / ``row_groups`` are passed straight through to
    ``ParquetFile.from_reader`` so page structure is read only for what a
    command actually needs.
    """
    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await ParquetFile.from_reader(
                reader,
                source,
                columns=columns,
                row_groups=row_groups,
            )

    with Path(source).open('rb') as handle:
        return await ParquetFile.from_reader(
            cast('ReadableSeekable', handle),
            source,
            columns=columns,
            row_groups=row_groups,
        )
