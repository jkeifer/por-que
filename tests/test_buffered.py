from io import SEEK_CUR, SEEK_END, SEEK_SET

import pytest

from por_que.util.buffered import BufferedRangeReader

# A span of 10 bytes that begins at absolute file offset 100 in a 200-byte file.
DATA = bytes(range(10))
OFFSET = 100
FILESIZE = 200


def make_reader() -> BufferedRangeReader:
    return BufferedRangeReader(DATA, OFFSET, FILESIZE)


def test_starts_at_absolute_offset() -> None:
    reader = make_reader()
    assert reader.tell() == OFFSET


def test_read_returns_span_bytes_and_advances_absolute_position() -> None:
    reader = make_reader()
    assert reader.read(4) == DATA[:4]
    assert reader.tell() == OFFSET + 4
    assert reader.read(6) == DATA[4:]
    assert reader.tell() == OFFSET + 10


def test_read_all_remaining_with_none() -> None:
    reader = make_reader()
    reader.seek(OFFSET + 3)
    assert reader.read() == DATA[3:]
    assert reader.tell() == OFFSET + 10


def test_seek_set_is_absolute() -> None:
    reader = make_reader()
    assert reader.seek(OFFSET + 5, SEEK_SET) == OFFSET + 5
    assert reader.read(1) == DATA[5:6]


def test_seek_cur_is_relative() -> None:
    reader = make_reader()
    reader.read(2)
    assert reader.seek(3, SEEK_CUR) == OFFSET + 5
    assert reader.read(1) == DATA[5:6]


def test_seek_end_resolves_against_filesize() -> None:
    reader = make_reader()
    # Seeking to the end of the file lands at filesize, not the span end.
    assert reader.seek(0, SEEK_END) == FILESIZE
    assert reader.seek(-FILESIZE, SEEK_END) == 0


def test_seek_invalid_whence_raises() -> None:
    reader = make_reader()
    with pytest.raises(ValueError, match='Invalid whence'):
        reader.seek(0, 99)


def test_read_past_end_of_span_raises() -> None:
    reader = make_reader()
    reader.seek(OFFSET + 8)
    with pytest.raises(ValueError, match='outside the buffered range'):
        reader.read(5)


def test_read_before_span_raises() -> None:
    reader = make_reader()
    reader.seek(OFFSET - 1)
    with pytest.raises(ValueError, match='outside the buffered range'):
        reader.read(1)


def test_close_is_noop() -> None:
    reader = make_reader()
    reader.close()
