"""Tests for FileMetadata using Apache Parquet test files."""

import pytest

from por_que.types import FileMetadata


@pytest.fixture
def base_url():
    """Base URL for Apache Parquet test files."""
    return 'https://raw.githubusercontent.com/apache/parquet-testing/master/data'


@pytest.fixture
def alltypes_plain_metadata(base_url):
    """Load metadata from alltypes_plain.parquet file."""
    return FileMetadata.from_url(f'{base_url}/alltypes_plain.parquet')


@pytest.fixture
def nested_structs_metadata(base_url):
    """Load metadata from nested_structs.rust.parquet file."""
    return FileMetadata.from_url(f'{base_url}/nested_structs.rust.parquet')


@pytest.fixture
def delta_encoding_metadata(base_url):
    """Load metadata from delta_encoding_optional_column.parquet file."""
    return FileMetadata.from_url(f'{base_url}/delta_encoding_optional_column.parquet')


@pytest.mark.parametrize(
    'metadata_fixture',
    [
        'alltypes_plain_metadata',
        'nested_structs_metadata',
        'delta_encoding_metadata',
    ],
)
def test_file_metadata_comprehensive(metadata_fixture, request):
    """Comprehensive test of file metadata parsing for all test files."""
    metadata = request.getfixturevalue(metadata_fixture)

    # Basic metadata structure
    assert metadata is not None
    assert metadata.version > 0
    assert len(metadata.schema) > 0
    assert metadata.num_rows >= 0
    assert len(metadata.row_groups) >= 0

    # Schema elements validation
    for element in metadata.schema:
        assert hasattr(element, 'name')
        assert hasattr(element, 'type')
        assert element.name is not None

    # Row groups validation
    for row_group in metadata.row_groups:
        assert hasattr(row_group, 'columns')
        assert hasattr(row_group, 'total_byte_size')
        assert hasattr(row_group, 'num_rows')
        assert len(row_group.columns) > 0

        # Column metadata validation
        for column in row_group.columns:
            assert hasattr(column, 'meta_data')
            if column.meta_data:
                col_meta = column.meta_data
                assert hasattr(col_meta, 'type')
                assert hasattr(col_meta, 'path_in_schema')
                assert hasattr(col_meta, 'codec')
                assert hasattr(col_meta, 'num_values')


def test_specific_file_features(nested_structs_metadata, alltypes_plain_metadata):
    """Test specific features of individual files."""
    # Nested structs should have complex schema
    assert len(nested_structs_metadata.schema) > 1

    # Test utility methods on alltypes_plain
    summary = alltypes_plain_metadata.summary()
    assert 'Parquet File Metadata' in summary
    assert str(alltypes_plain_metadata.version) in summary

    detailed = alltypes_plain_metadata.detailed_dump()
    assert 'PARQUET FILE DETAILED METADATA DUMP' in detailed

    # Test row group column names
    if alltypes_plain_metadata.row_groups:
        column_names = alltypes_plain_metadata.row_groups[0].column_names()
        assert isinstance(column_names, list)
        assert all(isinstance(name, str) for name in column_names)
