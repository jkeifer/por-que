import pytest

from por_que.exceptions import BufferExhaustedError
from por_que.parsers.thrift.parser import ThriftCompactParser
from por_que.util.spans import read_thrift_span


class FakeReader:
    """Minimal AsyncReadableSeekable over in-memory bytes.

    Records the size of every read so tests can assert on the speculative
    span growth behavior.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.read_sizes: list[int | None] = []

    async def read(self, size: int | None = None, /) -> bytes:
        self.read_sizes.append(size)
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
        assert reader.read_sizes == [16]

    @pytest.mark.asyncio
    async def test_doubles_span_on_buffer_exhaustion(self) -> None:
        # Structure "needs" 40 bytes; initial span of 16 is too small, so the
        # helper should retry with 32, then succeed at 64.
        reader = FakeReader(b'x' * 1000)

        def parse(data: memoryview, offset: int) -> int:
            parser = ThriftCompactParser(data, offset)
            return len(parser.read(40))

        result = await read_thrift_span(reader, 0, parse, initial_size=16)
        assert result == 40
        assert reader.read_sizes == [16, 32, 64]

    @pytest.mark.asyncio
    async def test_each_attempt_rereads_from_offset(self) -> None:
        reader = FakeReader(bytes(range(200)))

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(20)

        result = await read_thrift_span(reader, 100, parse, initial_size=16)
        assert result == bytes(range(100, 120))

    @pytest.mark.asyncio
    async def test_reraises_at_max_size(self) -> None:
        reader = FakeReader(b'x' * 1000)

        def parse(data: memoryview, offset: int) -> bytes:
            parser = ThriftCompactParser(data, offset)
            return parser.read(100)

        with pytest.raises(BufferExhaustedError):
            await read_thrift_span(reader, 0, parse, initial_size=16, max_size=64)
        # 16, 32, 64: capped at max_size, then the failure propagates.
        assert reader.read_sizes == [16, 32, 64]

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
        assert reader.read_sizes == [16]
