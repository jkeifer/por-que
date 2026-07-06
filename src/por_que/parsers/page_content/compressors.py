import io

from typing import Literal, assert_never, cast

from por_que.enums import Compression
from por_que.exceptions import ParquetDataError


def decompress_data(
    compressed_data: bytes,
    codec: Compression,
    expected_size: int | None = None,
) -> bytes:
    """
    Decompress data using the specified compression codec.

    Args:
        compressed_data: The compressed bytes to decompress
        codec: The compression codec to use
        expected_size: Expected uncompressed size (for validation and optimization)

    Returns:
        Decompressed bytes

    Raises:
        ParquetDataError: If compression codec is unsupported
        ValueError: If compression codec is unsupported (for data.py compatibility)
    """
    # Handle uncompressed data
    if codec == Compression.UNCOMPRESSED:
        return compressed_data

    # Handle empty data - decompression libraries fail on empty input
    if len(compressed_data) == 0:
        return compressed_data

    match codec:
        case Compression.SNAPPY:
            return get_snappy().decompress(compressed_data)
        case Compression.GZIP:
            return get_gzip().decompress(compressed_data)
        case Compression.LZO:
            return get_lzo().decompress(compressed_data)
        case Compression.BROTLI:
            return get_brotli().decompress(compressed_data)
        case Compression.ZSTD:
            dctx = get_zstd().ZstdDecompressor()
            # Use streaming decompression for frames without content size
            input_stream = io.BytesIO(compressed_data)
            reader = dctx.stream_reader(input_stream)
            return reader.readall()
        case _:
            raise ValueError(f"Compression codec '{codec}' is not supported")


def get_brotli():
    try:
        import brotli
    except ImportError:
        raise ParquetDataError(
            'Brotli compression requires brotli package',
        ) from None
    return brotli


def get_gzip():
    import gzip

    return gzip


def get_lzo():
    try:
        import lzo
    except ImportError:
        raise ParquetDataError(
            'LZO compression requires python-lzo package',
        ) from None
    return lzo


def get_snappy():
    try:
        import snappy
    except ImportError:
        # Fall back to the pure-python decompressor (slower, but no C library
        # or wasm-unavailable dependency needed, e.g. under pyodide).
        return _PurePythonSnappy
    return snappy


def _snappy_decompress(data: bytes) -> bytes:
    """Decompress a raw snappy block (no framing).

    Pure-python fallback for python-snappy. Format reference:
    https://github.com/google/snappy/blob/main/format_description.txt
    """
    # Preamble: uncompressed length as a varint.
    length = 0
    shift = 0
    pos = 0
    while True:
        b = data[pos]
        pos += 1
        length |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7

    out = bytearray()
    n = len(data)
    while pos < n:
        tag = data[pos]
        pos += 1
        if tag & 0x03 == 0:  # literal
            lit_len = tag >> 2
            if lit_len >= 60:
                nbytes = lit_len - 59
                lit_len = int.from_bytes(data[pos : pos + nbytes], 'little')
                pos += nbytes
            lit_len += 1
            out += data[pos : pos + lit_len]
            pos += lit_len
            continue

        # Copy: offset back into the already-produced output.
        copy_len, offset, pos = _snappy_copy(tag, data, pos)
        start = len(out) - offset
        if offset >= copy_len:
            out += out[start : start + copy_len]
        else:
            # Overlapping copy (RLE-style): must read as we write.
            for i in range(copy_len):
                out.append(out[start + i])

    if len(out) != length:
        raise ParquetDataError(
            f'Snappy decompressed size ({len(out)}) does not match '
            f'declared size ({length})',
        )
    return bytes(out)


def _snappy_copy(tag: int, data: bytes, pos: int) -> tuple[int, int, int]:
    """Decode a snappy copy element. Returns (copy_len, offset, new_pos).

    Caller guarantees this is a copy tag (kind 1-3); literals are handled
    before we get here.
    """
    kind = cast('Literal[1, 2, 3]', tag & 0x03)
    match kind:
        case 1:  # 1-byte offset
            copy_len = ((tag >> 2) & 0x07) + 4
            offset = ((tag >> 5) << 8) | data[pos]
            pos += 1
        case 2:  # 2-byte offset
            copy_len = (tag >> 2) + 1
            offset = int.from_bytes(data[pos : pos + 2], 'little')
            pos += 2
        case 3:  # 4-byte offset
            copy_len = (tag >> 2) + 1
            offset = int.from_bytes(data[pos : pos + 4], 'little')
            pos += 4
        case _:
            assert_never(kind)
    return copy_len, offset, pos


class _PurePythonSnappy:
    """Shim exposing the same ``decompress`` entry point as python-snappy."""

    decompress = staticmethod(_snappy_decompress)


def get_zstd():
    try:
        import zstandard
    except ImportError:
        raise ParquetDataError(
            'Zstandard compression requires zstandard package',
        ) from None
    return zstandard
