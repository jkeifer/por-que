import io

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
        raise ParquetDataError(
            'Snappy compression requires python-snappy package',
        ) from None
    return snappy


def get_zstd():
    try:
        import zstandard
    except ImportError:
        raise ParquetDataError(
            'Zstandard compression requires zstandard package',
        ) from None
    return zstandard
