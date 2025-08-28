"""Tests for FileMetadata using Apache Parquet test files."""

import pytest

from .expected_schemas import EXPECTED_SCHEMAS


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

    # Validate schema structure
    schema_str = str(metadata.schema)
    assert schema_str == EXPECTED_SCHEMAS[metadata_fixture]

    # Basic validation
    assert metadata.num_rows >= 0
    assert len(metadata.row_groups) >= 0

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
    # Nested structs should have complex schema with many groups
    assert len(nested_structs_metadata.schema.children) > 10

    # Basic validation of alltypes_plain structure
    assert len(alltypes_plain_metadata.schema.children) == 11

    # Test row group column names
    if alltypes_plain_metadata.row_groups:
        column_names = alltypes_plain_metadata.row_groups[0].column_names()
        assert isinstance(column_names, list)
        assert all(isinstance(name, str) for name in column_names)
