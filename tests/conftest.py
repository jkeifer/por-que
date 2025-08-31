import pytest

PARQUET_BASE_URL = (
    'https://raw.githubusercontent.com/apache/parquet-testing/master/data'
)


@pytest.fixture
def parquet_url(parquet_file_name: str) -> str:
    return f'{PARQUET_BASE_URL}/{parquet_file_name}.parquet'
