from por_que.exceptions import ParquetDataError


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
