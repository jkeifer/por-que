import json
import tempfile

from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import pytest

from deepdiff import DeepDiff

from por_que import AsyncHttpFile, ParquetFile

from .shared import FixtureDecoder, FixtureEncoder

FIXTURES = Path(__file__).parent / 'fixtures'
METADATA_FIXTURES = FIXTURES / 'metadata'
DATA_FIXTURES = FIXTURES / 'data'

TEST_FILES = [
    'alltypes_dictionary',
    'alltypes_plain',
    'alltypes_plain.snappy',
    'binary',
    'binary_truncated_min_max',
    'byte_array_decimal',
    'byte_stream_split.zstd',
    'byte_stream_split_extended.gzip',
    # Unknown page type: None
    #'column_chunk_key_value_metadata',
    'concatenated_gzip_members',
    'data_index_bloom_encoding_stats',
    'data_index_bloom_encoding_with_length',
    'datapage_v1-corrupt-checksum',
    'datapage_v1-snappy-compressed-checksum',
    'datapage_v1-uncompressed-checksum',
    'delta_binary_packed',
    'delta_byte_array',
    'delta_encoding_optional_column',
    'delta_encoding_required_column',
    'delta_length_byte_array',
    # Not sure about this one,
    # strange things happen with it
    #'dict-page-offset-zero',
    'fixed_length_decimal',
    'float16_nonzeros_and_nans',
    'float16_zeros_and_nans',
    'geospatial/crs-arbitrary-value',
    'geospatial/crs-default',
    'geospatial/crs-geography',
    'geospatial/crs-projjson',
    'geospatial/crs-srid',
    'geospatial/geospatial',
    'geospatial/geospatial-with-nan',
    'incorrect_map_schema',
    'int32_decimal',
    'int32_with_null_pages',
    'int64_decimal',
    # cannot parse these timestamps with python
    'int96_from_spark',
    'list_columns',
    # pyarrow output is inconsistent with this one
    'map_no_value',
    'nested_lists.snappy',
    'nested_maps.snappy',
    'nested_structs.rust',
    'nonnullable.impala',
    'null_list',
    'nullable.impala',
    'nulls.snappy',
    'old_list_structure',
    'plain-dict-uncompressed-checksum',
    'repeated_no_annotation',
    'repeated_primitive_no_list',
    'rle-dict-snappy-checksum',
    'rle-dict-uncompressed-corrupt-checksum',
    'rle_boolean_encoding',
    'single_nan',
    'sort_columns',
    'page_v2_empty_compressed',
    'datapage_v2_empty_datapage.snappy',
    'unknown-logical-type',
]
SCHEMA_ONLY_FILES = [
    # Can't even parse this data with pyarrow due to parquet-mr bug
    'fixed_length_byte_array',
]
DATA_ONLY_FILES = [
    # the following are massive schemas
    'alltypes_tiny_pages',
    'alltypes_tiny_pages_plain',
    'overflow_i16_page_cnt',
    # too hard to handle NaN because is serialized as None
    'nan_in_stats',
]

# columns to exclude from logical type conversion, per file
EXCLUDED_LOGICAL_COLUMNS = {
    'nested_structs.rust': (
        'ul_observation_date.max',
        'ul_observation_date.min',
    ),
    'int96_from_spark': ('a',),
}


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES + SCHEMA_ONLY_FILES,
)
@pytest.mark.asyncio
async def test_parquet_file(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = METADATA_FIXTURES / f'{parquet_file_name}_expected.json'

    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)

        actual_json = pf.to_json(indent=2)
        actual = json.loads(actual_json)
        del actual['_meta']['por_que_version']

        # we try to load the fixture file to compare
        # if it doesn't exist we write the fixture to file
        # to update, delete the fixture file it and re-run
        try:
            # in this test we compare what we parsed out of the
            # file directly to what we have in our fixture, so
            # we can ensure parsing alone works as expected, per
            # the fixture content
            expected = json.loads(fixture.read_text())
            assert actual == expected
        except FileNotFoundError:
            fixture.write_text(
                json.dumps(actual, indent=2),
            )
            pytest.skip(
                f'Generated fixture {fixture}. Re-run test to compare.',
            )


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES + SCHEMA_ONLY_FILES,
)
@pytest.mark.asyncio
async def test_parquet_file_from_dict(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = METADATA_FIXTURES / f'{parquet_file_name}_expected.json'

    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)

        actual = pf.to_dict()

        # the key difference with this test is that we ensure
        # loading the fixture into a ParquetFile results in the
        # same data as parsing it from a file -- because we
        # validate parsing in test_parquet_file, this gives us
        # a way to ensure from_dict works as we expect
        expected = ParquetFile.from_dict(
            json.loads(fixture.read_text()),
        ).to_dict()
        assert actual == expected


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES + DATA_ONLY_FILES,
)
@pytest.mark.asyncio
async def test_read_data(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        actual = {
            'source': parquet_url,
            'data': await pf.read_all_data(
                hf,
                excluded_logical_columns=EXCLUDED_LOGICAL_COLUMNS.get(
                    parquet_file_name,
                ),
            ),
        }
    _comparison(parquet_file_name, actual)


def _comparison(
    file_name: str,
    actual: dict[str, Any],
) -> None:
    # we try to load the fixture file to compare;
    # if it doesn't exist we write the fixture to file;
    # to update, delete the fixture file it and re-run
    fixture = DATA_FIXTURES / f'{file_name}_expected.json'
    try:
        expected = json.loads(fixture.read_text(), cls=FixtureDecoder)
    except FileNotFoundError:
        fixture.write_text(
            json.dumps(actual, indent=2, cls=FixtureEncoder),
        )
        pytest.skip(
            f'Generated fixture {fixture}. Re-run test to compare.',
        )

    actual_serialized = json.loads(
        json.dumps(actual, cls=FixtureEncoder),
        cls=FixtureDecoder,
    )

    if 'nan' in file_name:
        assert not DeepDiff(
            expected,
            actual_serialized,
            ignore_nan_inequality=True,
        )
    else:
        assert actual_serialized == expected


def _pyarrow_to_fixture_format(table, parquet_url: str) -> dict[str, Any]:
    """Convert PyArrow table to the same format as por-que fixtures."""
    data = {}
    for column_name in table.schema.names:
        column = table[column_name]
        values: list[Any] = []
        for i in range(len(column)):
            if not column[i].is_valid:
                values.append(None)
                continue

            values.append(column[i].as_py())

        data[column_name] = values

    return {
        'source': parquet_url,
        'data': data,
    }


@pytest.mark.skip(reason='only run this to validate data fixtures')
@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES + DATA_ONLY_FILES,
)
def test_pyarrow_comparison(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """Compare PyArrow parsing with por-que parsing using existing fixtures."""
    import pyarrow.parquet as pq

    # Download the parquet file to a temporary location
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        tmp_path = tmp_file.name

    try:
        urlretrieve(parquet_url, tmp_path)  # noqa: S310

        try:
            table = pq.read_table(tmp_path)
            actual = _pyarrow_to_fixture_format(
                table,
                parquet_url,
            )
        except Exception as e:  # noqa: BLE001
            # Some files can't be read by PyArrow due to various limitations
            pytest.skip(f'PyArrow cannot read {parquet_file_name}: {e}')

        _comparison(parquet_file_name, actual)
    finally:
        # Clean up the temporary file
        Path(tmp_path).unlink(missing_ok=True)
