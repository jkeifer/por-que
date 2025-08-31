import os

from collections.abc import Callable

import pytest

from por_que.util.file_read_cache import FileReadCache

type FetchLog = list[tuple[int, int]]
type FetchRange = Callable[[int, int], bytes]


@pytest.fixture
def fetch_log() -> FetchLog:
    return []


@pytest.fixture
def fetch_range(
    fetch_log: FetchLog,
    file_size: int,
) -> FetchRange:
    content = os.urandom(file_size)

    def fake_request(start: int, end: int) -> bytes:
        if start < 0 or end > file_size:
            raise ValueError('Request out of bounds')
        fetch_log.append((start, end))
        return content[start:end]

    return fake_request


@pytest.mark.parametrize('file_size', [2**20])
def test_file_read_cache(
    fetch_log: FetchLog,
    fetch_range: FetchRange,
    file_size: int,
) -> None:
    cache = FileReadCache(file_size, fetch_range)
    cache.read(100, 150)
    cache.read(5000, 6000)
    cache.read(8000, 9000)
    cache.read(20000, 20100)
    cache.read(30000, 32000)
    cache.read(10000, 100000)
    cache.read(file_size - 4, file_size)
    cache.read(file_size - 10000, file_size)
    cache.read(200000, 500000)
    cache.read(0, file_size)
    assert fetch_log == [
        (100, 8292),  # request coerced to min fetch size
        (8292, 16484),
        (20000, 28192),
        (30000, 38192),
        (16484, 20000),  # request spanning holes fills just holes
        (28192, 30000),
        (38192, 100000),
        (1040384, 1048576),  # request coerced to min fetch size backwards
        (1032192, 1040384),  # also expands backwards from a filled block
        (200000, 500000),
        (0, 100),
        (100000, 200000),
        (500000, 1032192),
    ]
