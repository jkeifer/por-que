"""PageValue carries physical and logical provenance separately.

``physical`` is the decoder's raw output, always populated on real parses
(``None`` only for nulls). ``logical`` is populated iff logical-type
conversion actually ran for this entry (``None`` for nulls, and when
conversion is off or the column is excluded) -- provenance travels with the
value because the consumer of a stream isn't always the code that chose the
conversion flag. ``.value`` is a convenience property: logical when set,
else physical.
"""

import pytest

from por_que import AsyncHttpFile, ParquetFile
from por_que.parsers.page_content import PageValue


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_page_values_carry_physical(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(cc for cc in pf.column_chunks if cc.data_pages)
        schema_element = chunk.metadata.schema_element

        converted = list(await chunk.parse_data_page(0, hf))
        raw = list(
            await chunk.parse_data_page(0, hf, apply_logical_types=False),
        )

    assert converted, 'fixture page decoded no values'
    for pv, rv in zip(converted, raw, strict=True):
        assert isinstance(pv, PageValue)
        assert pv.definition_level == rv.definition_level
        assert pv.repetition_level == rv.repetition_level

        # With conversion off, logical is never populated; value falls back
        # to physical via the property.
        assert rv.logical is None
        assert rv.value is rv.physical

        # One decode serves both: physical is the pre-conversion value...
        assert pv.physical == rv.physical
        if pv.physical is None:
            assert pv.logical is None
            assert pv.value is None
        else:
            # ...and logical/value are exactly its logical conversion.
            assert pv.logical is not None
            assert pv.value == schema_element.physical_to_logical_type(
                pv.physical,
            )


def test_page_value_named_fields() -> None:
    pv = PageValue('a', 1, 0, b'a')
    assert (pv.logical, pv.definition_level, pv.repetition_level, pv.physical) == (
        'a',
        1,
        0,
        b'a',
    )
    assert pv.value == 'a'

    # value falls back to physical when logical wasn't populated.
    assert PageValue(None, 1, 0, b'a').value == b'a'

    # physical defaults to None so fixtures/tests can build bare triples.
    assert PageValue('a', 1, 0).physical is None
