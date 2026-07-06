"""Loading parquet sources for the CLI.

Every command accepts either a local file path or an ``http(s)`` URL. Remote
files are read with ``AsyncHttpFile`` (range requests, no full download); local
files are opened normally and adapted to the async reader interface by the
library itself. Parsing is async, so commands cross the sync/async boundary
with ``asyncio.run``.

A local source may also be a por-que JSON dump (the output of ``por-que dump``,
metadata-only or full) instead of a parquet file. Such dumps are detected by
content and rehydrated straight into the models -- no re-parsing, no re-fetching
of the original file.
"""

from __future__ import annotations

import json

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

from por_que import AsyncHttpFile, FileMetadata, MetadataExport, ParquetFile
from por_que.enums import ProgressPhase
from por_que.protocols import ReadableSeekable

Progress = Callable[[ProgressPhase, int, int], None] | None


def is_url(source: str) -> bool:
    return source.startswith(('http://', 'https://'))


def load_dump(source: str) -> dict | None:
    """Return a parsed por-que dump if ``source`` is one, else ``None``.

    A dump is a local JSON file carrying the ``_meta`` envelope that
    ``to_json`` writes. Detection sniffs the first non-space byte (``{``) so a
    parquet file (magic ``PAR1``) never gets slurped as text.
    """
    if is_url(source):
        return None  # ponytail: dumps are local artifacts; add URL dumps if asked
    path = Path(source)
    try:
        with path.open('rb') as handle:
            if handle.read(64).lstrip()[:1] != b'{':
                return None
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and '_meta' in data else None


async def load_metadata(source: str, progress: Progress = None) -> FileMetadata:
    """Parse just the file footer metadata (schema, row groups, key-values)."""
    dump = load_dump(source)
    if dump is not None:
        return FileMetadata.model_validate(dump['metadata'])

    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await FileMetadata.from_reader(reader, progress=progress)

    with Path(source).open('rb') as handle:
        return await FileMetadata.from_reader(
            cast('ReadableSeekable', handle),
            progress=progress,
        )


async def load_metadata_export(
    source: str,
    progress: Progress = None,
) -> MetadataExport:
    """Parse the footer metadata into a self-identifying export envelope.

    Unlike :func:`load_metadata`, this captures ``source`` and ``filesize``
    so the result is a first-class, app-consumable ``MetadataExport``.
    """
    dump = load_dump(source)
    if dump is not None:
        # Rebuild from parts so a full dump down-converts cleanly: page fields
        # drop out and the ``_meta`` envelope re-identifies as metadata-only.
        return MetadataExport(
            source=dump['source'],
            filesize=dump['filesize'],
            metadata=FileMetadata.model_validate(dump['metadata']),
        )

    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await MetadataExport.from_reader(reader, source, progress=progress)

    with Path(source).open('rb') as handle:
        return await MetadataExport.from_reader(
            cast('ReadableSeekable', handle),
            source,
            progress=progress,
        )


async def load_file(
    source: str,
    columns: Sequence[str] | None = None,
    row_groups: Sequence[int] | None = None,
    progress: Progress = None,
) -> ParquetFile:
    """Parse the physical file, optionally materializing only a subset.

    ``columns`` / ``row_groups`` are passed straight through to
    ``ParquetFile.from_reader`` so page structure is read only for what a
    command actually needs.
    """
    dump = load_dump(source)
    if dump is not None:
        if 'column_chunks' not in dump:
            raise ValueError(
                f'{source} is a metadata-only dump; page structure needs a '
                f'full dump (por-que dump, without --metadata-only)',
            )
        # ponytail: dumps carry whatever was dumped; columns/row_groups
        # filtering is a read-time optimization the render layer redoes anyway.
        return ParquetFile.from_dict(dump)

    if is_url(source):
        async with AsyncHttpFile(source) as reader:
            return await ParquetFile.from_reader(
                reader,
                source,
                columns=columns,
                row_groups=row_groups,
                progress=progress,
            )

    with Path(source).open('rb') as handle:
        return await ParquetFile.from_reader(
            cast('ReadableSeekable', handle),
            source,
            columns=columns,
            row_groups=row_groups,
            progress=progress,
        )
