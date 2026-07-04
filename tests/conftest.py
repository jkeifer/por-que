import pytest

# The commit SHA of apache/parquet-testing our fixtures are pinned to.
#
# We pin this instead of tracking a branch because upstream changing a
# file out from under us silently breaks our fixtures (it happened with
# `fixed_length_byte_array.parquet`). This is the ONLY place this SHA
# should be defined -- everything else derives the download URL from it.
#
# To update: run `uv run scripts/update-fixtures.py`. See that script's
# docstring (and the "Updating test fixtures" section in the README) for
# details.
PARQUET_TESTING_REF = '1a2a75127be06fc0123f03ebd36c966f7beda27d'

PARQUET_BASE_URL = (
    f'https://raw.githubusercontent.com/apache/parquet-testing/'
    f'{PARQUET_TESTING_REF}/data'
)


@pytest.fixture
def parquet_url(parquet_file_name: str) -> str:
    return f'{PARQUET_BASE_URL}/{parquet_file_name}.parquet'
