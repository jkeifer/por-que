"""Shared test fixtures."""

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
