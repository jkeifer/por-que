"""Tests for on-demand bloom filter loading and querying.

Ground truth here is por-que itself: we parse a column chunk's real values
with ``parse_all_data_pages`` and then assert the bloom filter recognizes every
one of them. Because a split-block bloom filter has *no false negatives*, any
real value that reports ``might_contain(...) is False`` would be a bug in the
xxHash, the PLAIN encoding, or the block math.

Two fixtures exercise the two load paths:

* ``data_index_bloom_encoding_stats`` records ``bloom_filter_length = null``,
  so it drives the speculative-span path (parse the header, learn the bitset
  size, then read the bitset).
* ``data_index_bloom_encoding_with_length`` records an explicit length, so it
  drives the exact single-read path.
"""

import pytest

from por_que import AsyncHttpFile, ParquetFile
from por_que.exceptions import ParquetFormatError
from por_que.statistics import BloomFilter

BLOOM_FILES = [
    'data_index_bloom_encoding_stats',  # bloom_filter_length is null
    'data_index_bloom_encoding_with_length',  # bloom_filter_length recorded
]

# Strings that are not among the fixture's values. Both bloom fixtures store the
# same small string column, so a single deterministic probe set works for both.
# These were checked to produce no false positives against either filter.
ABSENT_PROBES = [
    'definitely-not-present',
    'xylophone-zzz',
    'no-such-value-42',
    'absent-probe-string',
]


async def _column_values(chunk, reader) -> list:
    values = []
    async for value, _def_level, _rep_level in chunk.parse_all_data_pages(reader):
        if value is not None:
            values.append(value)
    return values


@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_bloom_filter_has_no_false_negatives(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)

        chunks = [
            cc for cc in pf.column_chunks if cc.metadata.bloom_filter_offset is not None
        ]
        assert chunks, 'fixture should have at least one bloom-filtered column'

        for chunk in chunks:
            bloom = await chunk.load_bloom_filter(hf)
            assert isinstance(bloom, BloomFilter)

            values = await _column_values(chunk, hf)
            assert values, 'expected some real values to probe'

            for value in values:
                assert bloom.might_contain(value) is True, (
                    f'false negative for real value {value!r}'
                )


@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_bloom_filter_rejects_absent_values(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(
            cc for cc in pf.column_chunks if cc.metadata.bloom_filter_offset is not None
        )
        bloom = await chunk.load_bloom_filter(hf)

        present = set(await _column_values(chunk, hf))
        for probe in ABSENT_PROBES:
            assert probe not in present, 'probe accidentally is a real value'
            assert bloom.might_contain(probe) is False, (
                f'false positive for absent probe {probe!r}'
            )


@pytest.mark.parametrize('parquet_file_name', ['data_index_bloom_encoding_stats'])
@pytest.mark.asyncio
async def test_load_bloom_filter_length_absent_uses_speculative_path(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """The stats fixture records no length, so the header size is discovered."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(
            cc for cc in pf.column_chunks if cc.metadata.bloom_filter_offset is not None
        )
        assert chunk.metadata.bloom_filter_length is None

        bloom = await chunk.load_bloom_filter(hf)
        # The bitset really was read, and its length equals the header's claim.
        assert bloom.num_bytes > 0
        assert len(bloom.bitset) == bloom.num_bytes
        assert bloom.byte_length == bloom.header_length + bloom.num_bytes


@pytest.mark.parametrize('parquet_file_name', ['data_index_bloom_encoding_with_length'])
@pytest.mark.asyncio
async def test_load_bloom_filter_uses_recorded_length(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """The with_length fixture records the exact span, read in one shot."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(
            cc for cc in pf.column_chunks if cc.metadata.bloom_filter_offset is not None
        )
        assert chunk.metadata.bloom_filter_length is not None

        bloom = await chunk.load_bloom_filter(hf)
        assert bloom.byte_length == chunk.metadata.bloom_filter_length


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
@pytest.mark.asyncio
async def test_load_bloom_filter_without_filter_raises(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = next(iter(pf.column_chunks))
        assert chunk.metadata.bloom_filter_offset is None

        with pytest.raises(ParquetFormatError, match='no bloom filter'):
            await chunk.load_bloom_filter(hf)


@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_explain_matches_might_contain(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """explain() is the same derivation as might_contain, with intermediates."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = pf.column_chunks[0]
        bloom = await chunk.load_bloom_filter(hf)
        present = await _column_values(chunk, hf)

    for value in [*present, *ABSENT_PROBES]:
        probe = bloom.explain(value)
        assert probe.might_contain == bloom.might_contain(value)


@pytest.mark.parametrize('parquet_file_name', BLOOM_FILES)
@pytest.mark.asyncio
async def test_explain_intermediates_are_consistent(
    parquet_file_name: str,
    parquet_url: str,
) -> None:
    """The intermediates describe one 256-bit block and its 8 checked bits."""
    async with AsyncHttpFile(parquet_url) as hf:
        pf = await ParquetFile.from_reader(hf, parquet_url)
        chunk = pf.column_chunks[0]
        bloom = await chunk.load_bloom_filter(hf)
        present = await _column_values(chunk, hf)

    for value in [present[0], ABSENT_PROBES[0]]:
        probe = bloom.explain(value)

        assert probe.num_blocks == bloom.num_blocks
        assert 0 <= probe.block_index < probe.num_blocks
        assert len(probe.block_bytes) == 32
        # The block is a verbatim window into the (public) bitset.
        start = probe.block_index * 32
        assert probe.block_bytes == bloom.bitset[start : start + 32]

        assert len(probe.bits) == 8
        for i, check in enumerate(probe.bits):
            assert check.word_index == i
            assert 0 <= check.bit_index < 32
            # Each check must agree with the block bytes it claims to describe.
            word = int.from_bytes(
                probe.block_bytes[i * 4 : (i + 1) * 4],
                'little',
            )
            assert check.is_set == bool((word >> check.bit_index) & 1)

        assert probe.might_contain == all(c.is_set for c in probe.bits)
