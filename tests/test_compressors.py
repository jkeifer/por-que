import pytest

from por_que.parsers.page_content.compressors import (
    _lz4_raw_decompress,
    _snappy_decompress,
)

snappy = pytest.importorskip('snappy', reason='python-snappy not installed')


def test_literal_and_varint_preamble():
    # b"\x05" preamble (len 5), literal tag (len-1=4), then "hello".
    assert _snappy_decompress(b'\x05\x10hello') == b'hello'


def test_overlapping_copy():
    # literal "ab", then 1-byte-offset copy (offset 2, len 4) -> "ababab".
    assert _snappy_decompress(b'\x06\x04ab\x01\x02') == b'ababab'


@pytest.mark.parametrize(
    'raw',
    [
        b'',
        b'a',
        b'the quick brown fox ' * 100,  # long literals + 2-byte-offset copies
        bytes(range(256)) * 50,
        b'\x00' * 100_000,  # long RLE-style copies
    ],
)
def test_matches_c_snappy(raw):
    assert _snappy_decompress(snappy.compress(raw)) == raw


lz4_block = pytest.importorskip('lz4.block', reason='python-lz4 not installed')


@pytest.mark.parametrize(
    'raw',
    [
        b'',
        b'a',
        b'the quick brown fox ' * 100,  # long literals + matches, extended lens
        bytes(range(256)) * 50,
        b'\x00' * 100_000,  # long overlapping (RLE-style) copies
    ],
)
def test_matches_c_lz4_raw(raw):
    # store_size=False produces a raw block (no size header), matching LZ4_RAW.
    compressed = lz4_block.compress(raw, store_size=False)
    assert _lz4_raw_decompress(compressed, uncompressed_size=len(raw)) == raw
