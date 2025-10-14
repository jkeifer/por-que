import base64
import json
import tempfile

from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import pytest

from deepdiff import DeepDiff

from por_que import ParquetFile
from por_que.util.http_file import HttpFile

FIXTURES = Path(__file__).parent / 'fixtures'
METADATA_FIXTURES = FIXTURES / 'metadata'
DATA_FIXTURES = FIXTURES / 'data'
BASE64_ENCODE_PREFIX = '*-*-*-||por-que_base64_encoded||-*-*-*>'
DECIMAL_ENCODE_PREFIX = '*-*-*-||por-que_decimal_encoded||-*-*-*>'

TEST_FILES = [
    'alltypes_plain',
    'alltypes_dictionary',
    'alltypes_plain.snappy',
    'delta_byte_array',
    'delta_length_byte_array',
    'delta_binary_packed',
    'delta_encoding_required_column',
    'delta_encoding_optional_column',
    'nested_structs.rust',
    'data_index_bloom_encoding_stats',
    'data_index_bloom_encoding_with_length',
    'null_list',
    'rle_boolean_encoding',
    'int32_with_null_pages',
    'datapage_v1-uncompressed-checksum',
    'datapage_v1-snappy-compressed-checksum',
    'datapage_v1-corrupt-checksum',
    'rle-dict-snappy-checksum',
    'plain-dict-uncompressed-checksum',
    'rle-dict-uncompressed-corrupt-checksum',
    'large_string_map.brotli',
    'float16_nonzeros_and_nans',
    'float16_zeros_and_nans',
    'concatenated_gzip_members',
    'byte_stream_split.zstd',
    'incorrect_map_schema',
    'list_columns',
    'sort_columns',
    'old_list_structure',
    'repeated_no_annotation',
    'repeated_primitive_no_list',
    # pyarrow output is inconsistent with this one
    'map_no_value',
    'page_v2_empty_compressed',
    'datapage_v2_empty_datapage.snappy',
    'unknown-logical-type',
    'binary',
    'binary_truncated_min_max',
    'byte_array_decimal',
    'byte_stream_split.zstd',
    'byte_stream_split_extended.gzip',
    # Unknown page type: None
    #'column_chunk_key_value_metadata',
    'fixed_length_decimal',
    'geospatial/crs-projjson',
    'geospatial/geospatial',
    'geospatial/geospatial-with-nan',
    'geospatial/crs-default',
    'geospatial/crs-geography',
    'geospatial/crs-srid',
    'geospatial/crs-arbitrary-value',
]
SCHEMA_ONLY_FILES = [
    # Can't even parse this data with pyarrow due to parquet-mr bug
    'fixed_length_byte_array',
    # cannot parse these timestamps with python
    'int96_from_spark',
]
DATA_ONLY_FILES = [
    # the following are massive schemas
    'alltypes_tiny_pages',
    'alltypes_tiny_pages_plain',
    'overflow_i16_page_cnt',
    # too hard to handle NaN because is serialized as None
    'nan_in_stats',
]


def large_string_map_brotli(actual: dict[str, Any]) -> bool:
    href = (
        'https://raw.githubusercontent.com/apache/parquet-testing/'
        'master/data/large_string_map.brotli.parquet'
    )
    expected = {
        'source': href,
        'data': {
            'arr': [
                {'a': 1},
                {'a': 1},
            ],
        },
    }

    try:
        rows = actual['data']['arr']
    except KeyError:
        return False

    row_count = len(rows)
    assert row_count == 2
    for row in rows:
        keys = list(row.keys())
        key_count = len(keys)
        assert key_count == 1
        key = keys[0]
        key_len = len(key)
        assert key_len == 2**30
        assert set(key) == {'a'}
        row['a'] = row[key]
        del row[key]

    assert actual == expected
    return True


DATA_PAGE_FIXTURE_COMPARATOR = {
    'large_string_map.brotli': large_string_map_brotli,
}


class FixtureEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return BASE64_ENCODE_PREFIX + base64.b64encode(o).decode()
        if isinstance(o, Decimal):
            return DECIMAL_ENCODE_PREFIX + str(o)
        if hasattr(o, 'isoformat'):  # datetime, date, time objects
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


class FixtureDecoder(json.JSONDecoder):
    def decode(self, s):  # type: ignore
        # Parse normally first
        obj = super().decode(s)
        # Then post-process
        return self._decode_base64_strings(obj)

    def _decode_base64_strings(self, obj):
        if isinstance(obj, str):
            if obj.startswith(BASE64_ENCODE_PREFIX):
                return base64.b64decode(obj[len(BASE64_ENCODE_PREFIX) :])
            if obj.startswith(DECIMAL_ENCODE_PREFIX):
                return Decimal(obj[len(DECIMAL_ENCODE_PREFIX) :])
        if isinstance(obj, dict):
            return {k: self._decode_base64_strings(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._decode_base64_strings(item) for item in obj]
        return obj


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES + SCHEMA_ONLY_FILES,
)
def test_parquet_file(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = METADATA_FIXTURES / f'{parquet_file_name}_expected.json'

    with HttpFile(parquet_url) as hf:
        pf = ParquetFile.from_reader(hf, parquet_url)

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
def test_parquet_file_from_dict(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = METADATA_FIXTURES / f'{parquet_file_name}_expected.json'

    with HttpFile(parquet_url) as hf:
        pf = ParquetFile.from_reader(hf, parquet_url)

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
def test_read_data(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    with HttpFile(parquet_url) as hf:
        pf = ParquetFile.from_reader(hf, parquet_url)
        # Parse with por-que using consistent error handling
        actual = _parse_with_por_que(pf, hf, parquet_url)

    _comparison(parquet_file_name, actual)


def _parse_with_por_que(
    pf: ParquetFile,
    hf: HttpFile,
    parquet_url: str,
) -> dict[str, Any]:
    """Parse parquet file with por-que, handling conversion errors consistently."""
    flat_data: dict[str, Any] = {}

    for cc in pf.column_chunks:
        try:
            page_data = cc.parse_all_data_pages(hf)
        except (ValueError, OverflowError, OSError):
            # Handle conversion errors using shared logic
            page_data = ['unconvertible_type']

        try:
            flat_data[cc.path_in_schema].extend(page_data)
        except KeyError:
            flat_data[cc.path_in_schema] = page_data

    # Schema-aware reconstruction using ParquetFile's schema information
    data = pf.metadata.metadata.schema_root.renest(flat_data)

    return {
        'source': parquet_url,
        'data': data,
    }


def _comparison(
    file_name: str,
    actual: dict[str, Any],
) -> None:
    # first check if we have a fixture generator
    if comparator := DATA_PAGE_FIXTURE_COMPARATOR.get(file_name):
        assert comparator(actual)
        return

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
