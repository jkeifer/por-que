"""The missing-optional-codec condition is a typed error, not prose.

Consumers (the ver-por-que worker) distinguish "codec package not
installed" from real data errors. Before ``CodecUnavailableError`` they
sniffed the message text; these tests pin the typed contract instead.
"""

import sys

import pytest

from por_que.exceptions import CodecUnavailableError, ParquetDataError
from por_que.parsers.page_content import compressors

CODEC_GETTERS = [
    ('brotli', compressors.get_brotli, 'BROTLI'),
    ('lzo', compressors.get_lzo, 'LZO'),
    ('zstandard', compressors.get_zstd, 'ZSTD'),
]


@pytest.mark.parametrize(
    ('module_name', 'getter', 'codec_name'),
    CODEC_GETTERS,
    ids=[c[2] for c in CODEC_GETTERS],
)
def test_missing_codec_package_raises_typed_error(
    module_name: str,
    getter,
    codec_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # sys.modules[name] = None makes ``import name`` raise ImportError,
    # simulating the package being absent even when it is installed.
    monkeypatch.setitem(sys.modules, module_name, None)

    with pytest.raises(CodecUnavailableError) as excinfo:
        getter()

    assert excinfo.value.codec == codec_name
    # Existing consumers catch ParquetDataError; the new class must still be one.
    assert isinstance(excinfo.value, ParquetDataError)
