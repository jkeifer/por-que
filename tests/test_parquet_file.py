import base64
import json

from pathlib import Path

import pytest

from por_que import ParquetFile
from por_que.util.http_file import HttpFile

FIXTURES = Path(__file__).parent / 'fixtures'
METADATA_FIXTURES = FIXTURES / 'metadata'
DATA_FIXTURES = FIXTURES / 'data'
ENCODED_PREFIX = '*-*-*-||por-que_base64_encoded||-*-*-*>'

TEST_FILES = [
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
    'fixed_length_byte_array',
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
    'sort_columns',
    'old_list_structure',
    'repeated_primitive_no_list',
    'map_no_value',
    'page_v2_empty_compressed',
    'datapage_v2_empty_datapage.snappy',
    'unknown-logical-type',
    'int96_from_spark',
    'binary_truncated_min_max',
    'geospatial/crs-projjson',
    # Too hard to support this one in test cases
    #'nan_in_stats',
    # The following are massive schemas
    #'alltypes_tiny_pages',
    #'alltypes_tiny_pages_plain',
    #'overflow_i16_page_cnt',
]


class Base64Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return ENCODED_PREFIX + base64.b64encode(o).decode()
        return json.JSONEncoder.default(self, o)


class Base64Decoder(json.JSONDecoder):
    def decode(self, s):  # type: ignore
        # Parse normally first
        obj = super().decode(s)
        # Then post-process
        return self._decode_base64_strings(obj)

    def _decode_base64_strings(self, obj):
        if isinstance(obj, str) and obj.startswith(ENCODED_PREFIX):
            return base64.b64decode(obj[len(ENCODED_PREFIX) :])
        if isinstance(obj, dict):
            return {k: self._decode_base64_strings(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._decode_base64_strings(item) for item in obj]
        return obj


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES,
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
            fixture.write_text(json.dumps(actual, indent=2))
            pytest.skip(
                f'Generated fixture {fixture}. Re-run test to compare.',
            )


@pytest.mark.parametrize(
    'parquet_file_name',
    TEST_FILES,
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
    TEST_FILES,
)
def test_read_data(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = DATA_FIXTURES / f'{parquet_file_name}_expected.json'

    with HttpFile(parquet_url) as hf:
        pf = ParquetFile.from_reader(hf, parquet_url)
        actual = [cc.parse_all_data_pages(hf) for cc in pf.column_chunks]

        # we try to load the fixture file to compare
        # if it doesn't exist we write the fixture to file
        # to update, delete the fixture file it and re-run
        try:
            expected = json.loads(fixture.read_text(), cls=Base64Decoder)
            assert actual == expected
        except FileNotFoundError:
            print(actual)
            fixture.write_text(json.dumps(actual, indent=2, cls=Base64Encoder))
            pytest.skip(
                f'Generated fixture {fixture}. Re-run test to compare.',
            )
