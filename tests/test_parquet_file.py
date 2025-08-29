import pickle

from pathlib import Path

import pytest

from por_que import ParquetFile
from por_que.util.http_file import HttpFile

from .util import parquet_url

METADATA_FIXTURES = Path(__file__).parent / 'fixtures' / 'metadata'


@pytest.fixture
def alltypes_plain_file() -> ParquetFile:
    return ParquetFile(HttpFile(parquet_url('alltypes_plain')))


def test_page_parser_integration(alltypes_plain_file):
    """Test that PageParser gets exercised through the reader stack."""
    rg0 = alltypes_plain_file.row_group(0)
    id_column = rg0.column('id')

    # This is where the magic happens - PageParser reads actual page data
    page_count = 0
    total_data_bytes = 0

    for page_data in id_column.read():
        page_header, raw_data = page_data

        # Verify PageParser successfully parsed the page header
        assert page_header is not None
        assert hasattr(page_header, 'type')
        assert hasattr(page_header, 'compressed_page_size')
        assert hasattr(page_header, 'uncompressed_page_size')

        # Verify PageParser read the correct amount of data
        assert raw_data is not None
        assert len(raw_data) == page_header.compressed_page_size
        assert len(raw_data) > 0

        total_data_bytes += len(raw_data)
        page_count += 1

        # Only read first page for this test
        break

    assert page_count > 0, 'Should have read at least one page'
    assert total_data_bytes > 0, 'Should have read some data bytes'


def test_multiple_columns_page_reading(alltypes_plain_file):
    """Test page reading across multiple columns."""
    rg0 = alltypes_plain_file.row_group(0)

    # Test a few different column types
    test_columns = ['id', 'bool_col', 'string_col']

    for col_name in test_columns:
        column = rg0.column(col_name)
        assert column.value_count() == 8

        # Each column should be readable
        pages_read = 0
        for page_data in column.read():
            page_header, raw_data = page_data
            assert page_header is not None
            assert raw_data is not None
            pages_read += 1
            break  # Just test first page

        assert pages_read > 0, f'Should have read pages from {col_name}'


def test_error_handling(alltypes_plain_file):
    """Test error handling in reader stack."""
    rg0 = alltypes_plain_file.row_group(0)

    # Invalid column name
    with pytest.raises(ValueError, match='not found'):
        rg0.column('nonexistent_column')

    # Invalid row group index
    with pytest.raises(IndexError):
        alltypes_plain_file.row_group(999)


@pytest.mark.parametrize(
    'dataset_name',
    [
        'alltypes_plain',
        'nested_structs.rust',
        'delta_encoding_optional_column',
    ],
)
def test_parquet_file_dict_comparison(dataset_name: str) -> None:
    url = parquet_url(dataset_name)
    fixture = METADATA_FIXTURES / f'{dataset_name}_expected.pkl'

    pf = ParquetFile(HttpFile(url))
    actual_dict = pf.to_dict()

    # we try to load the fixture file to compare
    # if it doesn't exist we write the fixture to file
    # to update, delete the fixture file it and re-run
    try:
        with fixture.open('rb') as f:
            expected_dict = pickle.load(f)  # noqa: S301
        assert actual_dict == expected_dict
    except FileNotFoundError:
        with fixture.open('wb') as f:
            pickle.dump(actual_dict, f)
        pytest.skip(f'Generated fixture {fixture}. Re-run test to compare.')
