"""Public dictionary-page decoding on the column chunk.

``parse_dictionary`` mirrors ``parse_data_page`` ergonomics: takes a sync
or async reader, applies logical types by default, and returns ``[]``
when the chunk has no dictionary page.
"""

import io
import urllib.request

import pytest

from por_que import AsyncHttpFile, ParquetFile


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_parse_dictionary_logical_and_physical(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(cc for cc in pf.column_chunks if cc.dictionary_page is not None)
        schema_element = chunk.metadata.schema_element

        physical = await chunk.parse_dictionary(hf, apply_logical_types=False)
        logical = await chunk.parse_dictionary(hf)

    assert physical, 'fixture chunk decoded an empty dictionary'
    assert logical == [schema_element.physical_to_logical_type(v) for v in physical]


@pytest.mark.parametrize('parquet_file_name', ['alltypes_dictionary'])
@pytest.mark.asyncio
async def test_parse_dictionary_accepts_sync_reader(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    with urllib.request.urlopen(parquet_url) as resp:  # noqa: S310
        data = resp.read()

    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(cc for cc in pf.column_chunks if cc.dictionary_page is not None)
        via_async = await chunk.parse_dictionary(hf)

    via_sync = await chunk.parse_dictionary(io.BytesIO(data))
    assert via_sync == via_async


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
@pytest.mark.asyncio
async def test_parse_dictionary_without_dictionary_page(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        # Find a chunk without a dictionary page
        chunk = next(cc for cc in pf.column_chunks if cc.dictionary_page is None)
        assert await chunk.parse_dictionary(hf) == []
