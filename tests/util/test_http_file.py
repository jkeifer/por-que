import os

import pytest

from por_que.util.http_file import HttpFile


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_http_file(parquet_url: str) -> None:
    with HttpFile(
        parquet_url,
        minimum_range_request_bytes=80,
        prefetch_bytes=100,
    ) as hf:
        # Test initial state
        assert hf.tell() == 0

        # Test reading a chunk
        data = hf.read(100)
        assert len(data) == 100
        assert hf.tell() == 100

        # Test reading another chunk
        data2 = hf.read(50)
        assert len(data2) == 50
        assert hf.tell() == 150

        # Test seeking from the beginning of the file (SEEK_SET)
        hf.seek(0, os.SEEK_SET)
        assert hf.tell() == 0
        data3 = hf.read(10)
        assert len(data3) == 10
        assert hf.tell() == 10

        # Test seeking from the current position (SEEK_CUR)
        hf.seek(20, os.SEEK_CUR)
        assert hf.tell() == 30
        data4 = hf.read(20)
        assert len(data4) == 20
        assert hf.tell() == 50

        # Test seeking from the end of the file (SEEK_END)
        hf.seek(-50, os.SEEK_END)
        file_size = hf._ohf._size
        assert hf.tell() == file_size - 50
        data5 = hf.read(50)
        assert len(data5) == 50
        assert hf.tell() == file_size

        # Test reading past the end of the file
        assert hf.read() == b''
        assert hf.tell() == file_size

        # Test seeking past the end of the file
        hf.seek(1000, os.SEEK_END)
        assert hf.tell() == file_size  # Position should be clamped
        assert hf.read() == b''

        # Test seeking before the beginning of the file
        hf.seek(-file_size - 100, os.SEEK_END)
        assert hf.tell() == 0  # Position should be clamped
