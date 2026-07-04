import pytest

from por_que import AsyncHttpFile, FileMetadata
from por_que.enums import (
    Compression,
    ConvertedType,
    Repetition,
    SchemaElementType,
    Type,
)
from por_que.file_metadata import ColumnStatistics, SchemaLeaf

TEST_FILE = [
    'byte_array_decimal',
]

EXPECTED = {
    'column_count': 1,
    'compression_stats': {
        'compressed_mb': 0.00016021728515625,
        'ratio': 1.0,
        'space_saved_percent': 0.0,
        'total_compressed': 168,
        'total_uncompressed': 168,
        'uncompressed_mb': 0.00016021728515625,
    },
    'created_by': 'HVR 5.3.0/9 (linux_glibc2.5-x64-64bit)',
    'key_value_metadata': [],
    'row_count': 24,
    'row_group_count': 1,
    'row_groups': [
        {
            'byte_length': 37,
            'column_chunks': {
                'value': {
                    'column_index_length': None,
                    'column_index_offset': None,
                    'file_offset': 4,
                    'file_path': None,
                    'metadata': {
                        'bloom_filter_length': None,
                        'bloom_filter_offset': None,
                        'byte_length': 25,
                        'codec': Compression.UNCOMPRESSED,
                        'data_page_offset': 4,
                        'dictionary_page_offset': None,
                        'encoding_stats': None,
                        'encodings': [],
                        'geospatial_statistics': None,
                        'index_page_offset': None,
                        'num_values': 24,
                        'path_in_schema': 'value',
                        'size_statistics': None,
                        'start_offset': 243,
                        'statistics': None,
                        'total_compressed_size': 168,
                        'total_uncompressed_size': 168,
                        'type': Type.BYTE_ARRAY,
                    },
                    'offset_index_length': None,
                    'offset_index_offset': None,
                },
            },
            'compression_stats': {
                'compressed_mb': 0.00016021728515625,
                'ratio': 1.0,
                'space_saved_percent': 0.0,
                'total_compressed': 168,
                'total_uncompressed': 168,
                'uncompressed_mb': 0.00016021728515625,
            },
            'file_offset': None,
            'ordinal': None,
            'row_count': 24,
            'sorting_columns': None,
            'start_offset': 238,
            'total_byte_size': 168,
            'total_compressed_size': None,
        },
    ],
    'schema_root': {
        'byte_length': 13,
        'children': {
            'value': {
                'byte_length': 20,
                'converted_type': ConvertedType.DECIMAL,
                'definition_level': 1,
                'element_type': SchemaElementType.COLUMN,
                'field_id': 6,
                'full_path': 'value',
                'list_semantics': None,
                'logical_type': None,
                'name': 'value',
                'precision': 4,
                'repetition': Repetition.OPTIONAL,
                'repetition_level': 0,
                'scale': 2,
                'start_offset': 214,
                'type': Type.BYTE_ARRAY,
                'type_length': None,
            },
        },
        'definition_level': 0,
        'element_type': SchemaElementType.ROOT,
        'full_path': '',
        'name': 'schema',
        'num_children': 1,
        'repetition': Repetition.REQUIRED,
        'repetition_level': 0,
        'start_offset': 201,
    },
    'start_offset': 197,
    'total_byte_size': 119,
    'version': 1,
}


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILE,
)
@pytest.mark.asyncio
async def test_file_metadata_from_reader(
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        metadata = await FileMetadata.from_reader(hf)

        assert metadata.model_dump() == EXPECTED


@pytest.mark.parametrize(
    'parquet_file_name',
    ['binary'],
)
@pytest.mark.asyncio
async def test_statistics_linked_at_parse_time(
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        metadata = await FileMetadata.from_reader(hf)

    stats = None
    for row_group in metadata.row_groups:
        for chunk in row_group.column_chunks.values():
            if chunk.statistics is not None:
                stats = chunk.statistics
                break
        if stats is not None:
            break

    assert stats is not None, 'expected a column chunk with statistics'
    assert isinstance(stats.schema_element, SchemaLeaf)
    assert stats.schema_path == stats.schema_element.full_path
    # Accessing converted values must not raise now that we are linked.
    _ = stats.converted_min_value
    _ = stats.converted_max_value


def test_unlinked_statistics_raises() -> None:
    stats = ColumnStatistics(schema_path='some.column')

    with pytest.raises(ValueError, match='not linked'):
        _ = stats.schema_element
