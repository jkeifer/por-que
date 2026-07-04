"""Parsing for the ``BloomFilterHeader`` thrift struct.

A column chunk's bloom filter is laid out as a thrift-compact
``BloomFilterHeader`` struct immediately followed by the raw bitset bytes. The
header records how many bitset bytes follow (``numBytes``) and pins down the
three parameters that make the filter interpretable: which algorithm builds
it, which hash feeds it, and whether the bitset is compressed.

Only one variant of each is defined by the format today (split-block /
xxHash / uncompressed). Each of those choices is encoded as a thrift *union*:
a struct where exactly one field is set and the field ID names the variant.
We read that single field ID and reject anything we don't understand rather
than silently mis-parsing a filter we can't actually query.
"""

from __future__ import annotations

import warnings

from typing import Any

from por_que.exceptions import ParquetFormatError

from .base import BaseParser
from .enums import (
    BloomFilterAlgorithmFieldId,
    BloomFilterCompressionFieldId,
    BloomFilterHashFieldId,
    BloomFilterHeaderFieldId,
)


class BloomFilterHeaderParser(BaseParser):
    """Parses a ``BloomFilterHeader`` struct into a plain dict."""

    def read_header(self) -> dict[str, Any]:
        """Read the header, returning ``{'num_bytes': int}``.

        The algorithm/hash/compression unions are validated for support as a
        side effect; only ``numBytes`` is carried forward because everything
        else is fixed once we know the filter is a plain split-block xxHash
        filter.
        """
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case BloomFilterHeaderFieldId.NUM_BYTES:
                    props['num_bytes'] = value
                case BloomFilterHeaderFieldId.ALGORITHM:
                    self._read_algorithm()
                case BloomFilterHeaderFieldId.HASH:
                    self._read_hash()
                case BloomFilterHeaderFieldId.COMPRESSION:
                    self._read_compression()
                case _:
                    warnings.warn(
                        f'Skipping unknown bloom filter header field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        if 'num_bytes' not in props:
            raise ParquetFormatError(
                'Bloom filter header is missing required numBytes field',
            )

        return props

    def _read_union_variant(self) -> int:
        """Read a single-field thrift union and return its variant field ID."""
        variant_id = -1
        for field_id, field_type, _ in self.parse_struct_fields():
            variant_id = field_id
            # Variant payloads are empty structs, but skip defensively in case
            # a future variant carries data.
            self.maybe_skip_field(field_type)
        return variant_id

    def _read_algorithm(self) -> None:
        variant = self._read_union_variant()
        if variant != BloomFilterAlgorithmFieldId.BLOCK:
            raise ParquetFormatError(
                f'Unsupported bloom filter algorithm (variant {variant}); '
                'only the split-block algorithm (BLOCK) is supported',
            )

    def _read_hash(self) -> None:
        variant = self._read_union_variant()
        if variant != BloomFilterHashFieldId.XXHASH:
            raise ParquetFormatError(
                f'Unsupported bloom filter hash (variant {variant}); '
                'only XXHASH is supported',
            )

    def _read_compression(self) -> None:
        variant = self._read_union_variant()
        if variant != BloomFilterCompressionFieldId.UNCOMPRESSED:
            raise ParquetFormatError(
                f'Unsupported bloom filter compression (variant {variant}); '
                'only UNCOMPRESSED is supported',
            )
