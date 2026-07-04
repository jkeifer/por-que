"""Unit tests for the pure-python XXH64 implementation.

The expected values come from the official xxHash sanity check
(``cli/xsum_sanity_check.c`` in https://github.com/Cyan4973/xxHash). That test
fills a deterministic buffer with a small PRNG and checks XXH64 of various
prefixes against known-good hashes, using both seed 0 and a nonzero seed
(PRIME32). Several lengths cross the 32-byte stripe boundary, exercising both
the striped main loop and the byte/word tail handling.
"""

from por_que.util.xxhash import MASK64, xxh64

PRIME32 = 2654435761
PRIME64 = 11400714785074694797


def _fill_test_buffer(length: int) -> bytes:
    """Reproduce xxHash's ``XSUM_fillTestBuffer`` PRNG.

    Starts from PRIME32 and repeatedly multiplies by PRIME64 (mod 2**64),
    emitting the top byte of the accumulator for each position.
    """
    byte_gen = PRIME32
    out = bytearray(length)
    for i in range(length):
        out[i] = (byte_gen >> 56) & 0xFF
        byte_gen = (byte_gen * PRIME64) & MASK64
    return bytes(out)


# (length, seed, expected) triples straight from the official sanity check.
XXH64_VECTORS = [
    (0, 0, 0xEF46DB3751D8E999),
    (0, PRIME32, 0xAC75FDA2929B17EF),
    (1, 0, 0xE934A84ADB052768),
    (1, PRIME32, 0x5014607643A9B4C3),
    (4, 0, 0x9136A0DCA57457EE),
    (14, 0, 0x8282DCC4994E35C8),
    (14, PRIME32, 0xC3BD6BF63DEB6DF0),
    (222, 0, 0xB641AE8CB691C174),
    (222, PRIME32, 0x20CB8AB7AE10C14A),
]


def test_xxh64_official_sanity_vectors() -> None:
    buffer = _fill_test_buffer(max(length for length, _, _ in XXH64_VECTORS))
    for length, seed, expected in XXH64_VECTORS:
        assert xxh64(buffer[:length], seed) == expected, (
            f'length={length} seed={seed:#x}'
        )


def test_xxh64_empty_seed_zero() -> None:
    # The most widely cited single vector, called out explicitly.
    assert xxh64(b'') == 0xEF46DB3751D8E999


def test_xxh64_result_is_64_bit() -> None:
    for length in (0, 1, 7, 8, 31, 32, 33, 222):
        assert 0 <= xxh64(_fill_test_buffer(length)) <= MASK64
