import json

from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from por_que.file_metadata import SchemaRoot
from por_que.structuring import reconstruct

TESTS_DIR = Path(__file__).parent
FIXTURES = TESTS_DIR / 'fixtures' / 'reconstruction'


async def tuple_stream(
    data: Sequence[tuple[Any, int, int]],
) -> AsyncIterator[tuple[Any, int, int]]:
    """Convert list to async iterator."""
    for tup in data:
        yield tup


TEST_CASES = (
    'simple_struct',
    'nested_struct',
    'map_with_primitive_value',
    'list_of_primitives',
    'list_of_structs',
    'map_with_struct_value',
    'map_keys_only',
    'repeated_struct',
    'multiple_records',
    'empty_list',
    'null_list',
    'empty_map',
    'null_map',
    'list_with_nulls',
    'map_with_null_values',
    'struct_with_map_field',
    'optional_nested_struct',
    'three_level_nested_struct',
    'list_of_lists',
    'list_of_maps',
    'map_with_list_value',
    'map_with_map_value',
    'list_of_maps_with_struct_value',
    # real files
    'map_no_value',
    'nested_lists.snappy',
    'nested_maps.snappy',
    'nested_structs.rust',
    'nonnullable.impala',
    'null_list',
    'nullable.impala',
    'nulls.snappy',
    'old_list_structure',
    'repeated_no_annotation',
    'repeated_primitive_no_list',
    'incorrect_map_schema',
    'list_columns',
    'alltypes_dictionary',
    'alltypes_plain',
)


@pytest.mark.parametrize('test_case', TEST_CASES)
@pytest.mark.asyncio
async def test_reconstruction(test_case) -> None:
    fixture = FIXTURES / f'{test_case}.json'
    fixture_data = json.loads(fixture.read_text())
    schema_root = SchemaRoot(**fixture_data['schema_root'])
    tuple_streams = {
        key: tuple_stream(val) for key, val in fixture_data['data_tuples'].items()
    }
    expected = fixture_data['expected']

    if isinstance(expected, str):
        expected = json.loads((TESTS_DIR / expected).read_text())['data']

    result = await reconstruct(schema_root, tuple_streams)

    print(test_case, json.dumps(result))
    assert result == expected
