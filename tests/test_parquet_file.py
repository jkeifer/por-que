import json

from pathlib import Path

import pytest

from por_que import ParquetFile
from por_que.util.http_file import HttpFile

METADATA_FIXTURES = Path(__file__).parent / 'fixtures' / 'metadata'


@pytest.mark.parametrize(
    'parquet_file_name',
    [
        'alltypes_plain',
        'nested_structs.rust',
        'delta_encoding_optional_column',
    ],
)
def test_parquet_file_dict_comparison(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    fixture = METADATA_FIXTURES / f'{parquet_file_name}_expected.json'

    with HttpFile(parquet_url) as hf:
        print(hf)
        hf.seek(-4, 2)
        print(hf.read())
        hf.seek(0)
        pf = ParquetFile.from_reader(hf, parquet_url)

        actual_json = pf.to_json(indent=2)
        actual = json.loads(actual_json)
        del actual['_meta']['por_que_version']

        # we try to load the fixture file to compare
        # if it doesn't exist we write the fixture to file
        # to update, delete the fixture file it and re-run
        try:
            expected = json.loads(fixture.read_text())
            assert actual == expected
        except FileNotFoundError:
            fixture.write_text(json.dumps(actual, indent=2))
            pytest.skip(
                f'Generated fixture {fixture}. Re-run test to compare.',
            )
