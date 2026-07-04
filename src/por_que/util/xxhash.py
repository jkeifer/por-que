"""Pure-python XXH64, the hash Parquet bloom filters are built on.

This is a from-scratch implementation of the 64-bit variant of xxHash,
following the algorithm described in the reference specification:
https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md

Why reimplement it here? por-que is a teaching library with no room for
native extensions, and bloom-filter point lookups happen a handful of times
at a time, so raw throughput is irrelevant. Spelling the algorithm out in
Python makes the mechanics readable: the four interleaved accumulators, the
32-byte "stripe" processed each round, the tail handling for the trailing
bytes, and the final avalanche that scrambles the result.

Everything is done in unsigned 64-bit arithmetic. Python ints are unbounded,
so every operation that could overflow a real 64-bit register is masked back
down with ``& MASK64``.
"""

from __future__ import annotations

# The five prime constants that drive xxHash's mixing. They are irregular bit
# patterns chosen so that multiplication spreads input bits across the whole
# 64-bit word.
PRIME64_1 = 0x9E3779B185EBCA87
PRIME64_2 = 0xC2B2AE3D27D4EB4F
PRIME64_3 = 0x165667B19E3779F9
PRIME64_4 = 0x85EBCA77C2B2AE63
PRIME64_5 = 0x27D4EB2F165667C5

# 64-bit truncation mask: applied after every add/multiply to emulate the
# wraparound of unsigned 64-bit hardware registers.
MASK64 = 0xFFFFFFFFFFFFFFFF

# Bytes consumed per accumulator round (one 64-bit lane) and per stripe (four
# lanes = 32 bytes). The stripe size is why tests must cross 32-byte inputs.
_LANE = 8
_STRIPE = 32


def _rotl64(value: int, bits: int) -> int:
    """Rotate a 64-bit value left by ``bits`` positions.

    Bits that fall off the top wrap around to the bottom. This is the one
    operation that keeps the hash from being a pure sequence of multiplies
    (which would be linear and easy to invert).
    """
    return ((value << bits) | (value >> (64 - bits))) & MASK64


def _round(acc: int, lane: int) -> int:
    """Fold one 64-bit input lane into an accumulator.

    The lane is scaled by PRIME64_2, added in, rotated, then scaled by
    PRIME64_1. This is the core per-lane mixing step used both while
    consuming full 32-byte stripes and, with ``acc=0``, to pre-mix tail lanes.
    """
    acc = (acc + lane * PRIME64_2) & MASK64
    acc = _rotl64(acc, 31)
    return (acc * PRIME64_1) & MASK64


def _merge_round(acc: int, lane: int) -> int:
    """Merge a finished stripe accumulator into the running hash."""
    lane = _round(0, lane)
    acc ^= lane
    return (acc * PRIME64_1 + PRIME64_4) & MASK64


def _avalanche(acc: int) -> int:
    """Final bit-mixing so every input bit can flip every output bit."""
    acc ^= acc >> 33
    acc = (acc * PRIME64_2) & MASK64
    acc ^= acc >> 29
    acc = (acc * PRIME64_3) & MASK64
    acc ^= acc >> 32
    return acc


def _read_u64_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 8], 'little')


def _read_u32_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 4], 'little')


def xxh64(data: bytes, seed: int = 0) -> int:
    """Compute the 64-bit xxHash of ``data`` with an optional ``seed``.

    Args:
        data: The bytes to hash.
        seed: A 64-bit seed. Parquet bloom filters always use ``0``.

    Returns:
        The hash as an unsigned 64-bit integer.
    """
    seed &= MASK64
    length = len(data)
    index = 0

    if length >= _STRIPE:
        # Four accumulators seeded to distinct starting points let the main
        # loop consume four 64-bit lanes per iteration independently, which is
        # what makes hardware xxHash fast. We keep the structure even though we
        # gain nothing from parallelism in Python: it must match the spec bit
        # for bit.
        acc1 = (seed + PRIME64_1 + PRIME64_2) & MASK64
        acc2 = (seed + PRIME64_2) & MASK64
        acc3 = seed
        acc4 = (seed - PRIME64_1) & MASK64

        limit = length - _STRIPE
        while index <= limit:
            acc1 = _round(acc1, _read_u64_le(data, index))
            acc2 = _round(acc2, _read_u64_le(data, index + _LANE))
            acc3 = _round(acc3, _read_u64_le(data, index + 2 * _LANE))
            acc4 = _round(acc4, _read_u64_le(data, index + 3 * _LANE))
            index += _STRIPE

        acc = (
            _rotl64(acc1, 1)
            + _rotl64(acc2, 7)
            + _rotl64(acc3, 12)
            + _rotl64(acc4, 18)
        ) & MASK64
        acc = _merge_round(acc, acc1)
        acc = _merge_round(acc, acc2)
        acc = _merge_round(acc, acc3)
        acc = _merge_round(acc, acc4)
    else:
        # Short inputs skip the striped loop entirely and start from a single
        # prime-derived seed.
        acc = (seed + PRIME64_5) & MASK64

    acc = (acc + length) & MASK64

    # Tail: consume any remaining full 8-byte lanes, then a 4-byte word, then
    # single bytes, each with its own mixing constants.
    while index + _LANE <= length:
        acc ^= _round(0, _read_u64_le(data, index))
        acc = (_rotl64(acc, 27) * PRIME64_1 + PRIME64_4) & MASK64
        index += _LANE

    if index + 4 <= length:
        acc ^= (_read_u32_le(data, index) * PRIME64_1) & MASK64
        acc = (_rotl64(acc, 23) * PRIME64_2 + PRIME64_3) & MASK64
        index += 4

    while index < length:
        acc ^= (data[index] * PRIME64_5) & MASK64
        acc = (_rotl64(acc, 11) * PRIME64_1) & MASK64
        index += 1

    return _avalanche(acc)
