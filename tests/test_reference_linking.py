"""Tests for deserialize-time re-linking of schema references.

Parsers link ``SchemaLinked`` models (statistics, column indexes, data
pages) to their ``SchemaLeaf`` at parse time. These tests exercise the
other path: building models from dicts/JSON (``model_validate``), where
``FileMetadata`` and ``ParquetFile`` walk their own structure to re-link
against ``schema_root`` after validation.
"""

from collections.abc import Iterator

import pytest

from pydantic import ValidationError

from por_que import AsyncHttpFile, FileMetadata, ParquetFile
from por_que.file_metadata import ColumnChunk, ColumnStatistics, SchemaLeaf
from por_que.physical import PhysicalColumnChunk


def _find_chunk_with_statistics(metadata: FileMetadata) -> ColumnChunk:
    for row_group in metadata.row_groups:
        for chunk in row_group.column_chunks.values():
            if chunk.statistics is not None:
                return chunk
    raise AssertionError('expected a column chunk with statistics')


def _physical_chunks_with_data_pages(
    pf: ParquetFile,
) -> Iterator[PhysicalColumnChunk]:
    return (cc for cc in pf.column_chunks if cc.data_pages)


@pytest.mark.parametrize('parquet_file_name', ['binary'])
@pytest.mark.asyncio
async def test_metadata_roundtrip_relinks(parquet_url: str) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        metadata = await FileMetadata.from_reader(hf)

    restored = FileMetadata.model_validate(metadata.model_dump())

    chunk = _find_chunk_with_statistics(restored)
    stats = chunk.statistics
    assert stats is not None

    assert isinstance(stats.schema_element, SchemaLeaf)
    assert stats.schema_element is restored.schema_root.find_element(
        stats.schema_path,
    )
    _ = stats.converted_min_value
    _ = stats.converted_max_value


@pytest.mark.parametrize('parquet_file_name', ['binary'])
@pytest.mark.asyncio
async def test_metadata_json_roundtrip_relinks(parquet_url: str) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        metadata = await FileMetadata.from_reader(hf)

    restored = FileMetadata.model_validate_json(metadata.model_dump_json())

    chunk = _find_chunk_with_statistics(restored)
    stats = chunk.statistics
    assert stats is not None

    assert isinstance(stats.schema_element, SchemaLeaf)
    assert stats.schema_element is restored.schema_root.find_element(
        stats.schema_path,
    )
    _ = stats.converted_min_value
    _ = stats.converted_max_value


@pytest.mark.parametrize('parquet_file_name', ['binary_truncated_min_max'])
@pytest.mark.asyncio
async def test_parquet_file_roundtrip_relinks(parquet_url: str) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)

    restored = ParquetFile.from_dict(pf.to_dict())

    chunk = next(_physical_chunks_with_data_pages(restored))
    page = chunk.data_pages[0]

    assert isinstance(page.schema_element, SchemaLeaf)
    if page.statistics is not None:
        assert isinstance(page.statistics.schema_element, SchemaLeaf)

    if chunk.column_index is not None:
        assert isinstance(chunk.column_index.schema_element, SchemaLeaf)
        _ = chunk.column_index.converted_min_values


@pytest.mark.parametrize('parquet_file_name', ['binary'])
@pytest.mark.asyncio
async def test_unresolvable_schema_path_fails(parquet_url: str) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        metadata = await FileMetadata.from_reader(hf)

    data = metadata.model_dump()
    chunk = _find_chunk_with_statistics(metadata)
    bad_path = 'totally.nonexistent.path'

    for row_group in data['row_groups']:
        cc = row_group['column_chunks'].get(chunk.path_in_schema)
        if cc is not None and cc['metadata']['statistics'] is not None:
            cc['metadata']['statistics']['schema_path'] = bad_path

    with pytest.raises(ValidationError, match=bad_path) as exc_info:
        FileMetadata.model_validate(data)

    assert bad_path in str(exc_info.value)


def test_fragment_validates_unlinked() -> None:
    stats = ColumnStatistics.model_validate({'schema_path': 'some.column'})

    with pytest.raises(ValueError, match='not linked'):
        _ = stats.schema_element
