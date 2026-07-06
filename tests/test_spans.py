import pytest

from por_que.exceptions import BufferExhaustedError
from por_que.parsers.thrift.parser import ThriftCompactParser
from por_que.util.spans import read_thrift_span


class FakeReader:
    """Minimal AsyncReadableSeekable over in-memory bytes.

    Records the position and size of every read so tests can assert on the
    speculative span growth behavior.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.read_calls: list[tuple[int, int | None]] = []

    async def read(self, size: int | None = None, /) -> bytes:
        self.read_calls.append((self._pos, size))
        start = self._pos
        end = len(self._data) if size is None else min(start + size, len(self._data))
        chunk = self._data[start:end]
        self._pos = end
        return chunk

    def seek(self, offset: int, whence: int = 0, /) -> int:
        self._pos = offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    async def close(self) -> None:
        pass


class TestBufferExhausted:
    def test_read_past_end_raises(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        with pytest.raises(BufferExhaustedError):
            parser.read(4)

    def test_read_at_end_raises(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        parser.read(3)
        with pytest.raises(BufferExhaustedError):
            parser.read(1)

    def test_skip_past_end_raises(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        with pytest.raises(BufferExhaustedError):
            parser.skip(4)

    def test_read_reports_exact_shortfall(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        parser.read(2)
        with pytest.raises(BufferExhaustedError) as excinfo:
            parser.read(5)
        # Index 2 + read of 5 = 7, buffer holds 3: short by exactly 4.
        assert excinfo.value.needed == 4

    def test_skip_reports_exact_shortfall(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        with pytest.raises(BufferExhaustedError) as excinfo:
            parser.skip(10)
        assert excinfo.value.needed == 7

    def test_exhaustion_does_not_advance_position(self) -> None:
        parser = ThriftCompactParser(b'abc', 100)
        parser.read(2)
        with pytest.raises(BufferExhaustedError):
            parser.read(2)
        assert parser.pos == 102

    def test_pos_and_tell_are_absolute(self) -> None:
        parser = ThriftCompactParser(b'abcdef', 1000)
        assert parser.pos == 1000
        assert parser.tell() == 1000
        parser.read(4)
        assert parser.pos == 1004
        assert parser.tell() == 1004


class TestReadThriftSpan:
    @pytest.mark.asyncio
    async def test_parses_within_initial_span(self) -> None:
        reader = FakeReader(b'x' * 100)

        def parse(data: memoryview, offset: int) -> tuple[int, bytes]:
            return offset, bytes(data[:5])

        result = await read_thrift_span(reader, 10, parse, initial_size=16)
        assert result == (10, b'xxxxx')
        # One read, no retries.
        assert reader.read_calls == [(10, 16)]

    @pytest.mark.asyncio
    async def test_retry_fetches_only_the_tail(self) -> None:
        reader = FakeReader(bytes(range(200)))

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(20)

        result = await read_thrift_span(reader, 100, parse, initial_size=16)
        assert result == bytes(range(100, 120))
        # The retry starts exactly where the first span ended; the 16-byte
        # prefix is never re-read.
        first_call, second_call = reader.read_calls
        assert first_call == (100, 16)
        assert second_call[0] == 100 + 16

    @pytest.mark.asyncio
    async def test_exact_shortfall_hint_jumps_in_one_retry(self) -> None:
        # The parser trips on a single 100-byte read, so the exception's
        # ``needed`` (84) beats the doubling floor (16) and the second span
        # is exactly big enough: one retry, not a doubling ladder.
        reader = FakeReader(b'x' * 1000)

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(100)

        result = await read_thrift_span(reader, 0, parse, initial_size=16)
        assert result == b'x' * 100
        assert reader.read_calls == [(0, 16), (16, 84)]

    @pytest.mark.asyncio
    async def test_small_shortfall_still_gets_geometric_floor(self) -> None:
        # The parser is short by a single byte, but the buffer must at least
        # double so a struct that keeps tripping one primitive at a time
        # doesn't degrade into one round trip per field.
        reader = FakeReader(b'x' * 1000)

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(17)

        result = await read_thrift_span(reader, 0, parse, initial_size=16)
        assert result == b'x' * 17
        assert reader.read_calls == [(0, 16), (16, 16)]

    @pytest.mark.asyncio
    async def test_reraises_when_file_ends_short(self) -> None:
        # Only 10 bytes exist past the offset; growing the span cannot help,
        # so the short read must end the retry loop immediately.
        reader = FakeReader(b'x' * 10)

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(100)

        with pytest.raises(BufferExhaustedError):
            await read_thrift_span(reader, 0, parse, initial_size=16)
        assert reader.read_calls == [(0, 16)]

    @pytest.mark.asyncio
    async def test_reraises_when_growth_hits_end_of_file(self) -> None:
        # The initial span fills completely, but the retry's fetch comes back
        # short: the file is exhausted, so the next failure propagates.
        reader = FakeReader(b'x' * 20)

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(100)

        with pytest.raises(BufferExhaustedError):
            await read_thrift_span(reader, 0, parse, initial_size=16)
        assert reader.read_calls == [(0, 16), (16, 84)]

    @pytest.mark.asyncio
    async def test_no_size_cap_on_growth(self) -> None:
        # Larger than the 16 MiB cap the old implementation enforced; growth
        # is now bounded only by end of file.
        target = 33 * 1024 * 1024
        reader = FakeReader(b'x' * (target + 10))

        def parse(data: memoryview, offset: int) -> int:
            if len(data) < target:
                raise BufferExhaustedError(
                    'need more',
                    needed=target - len(data),
                )
            return len(data)

        result = await read_thrift_span(reader, 0, parse)
        assert result >= target
