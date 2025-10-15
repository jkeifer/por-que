"""
HTTP file-like wrapper for remote Parquet file access.

Teaching Points:
- Enables reading remote Parquet files as if they were local files
- Uses HTTP range requests to fetch only needed data sections
- Implements standard file-like interface (read, seek, tell)
- Caches data to minimize HTTP requests for efficiency
"""

from __future__ import annotations

import urllib.request

from typing import Literal, Self

from por_que.exceptions import ParquetNetworkError, ParquetUrlError

from .file_read_cache import FileReadCache


def _check_url(url: str) -> None:
    """
    Validate that URL is a valid HTTP/HTTPS URL.

    Args:
        url: URL to validate

    Raises:
        ParquetUrlError: If URL doesn't start with http: or https:
    """
    if not url.startswith(('http:', 'https:')):
        raise ParquetUrlError("URL must start with 'http:' or 'https:'")


class _OpenedHttpFile:
    """
    Internal class of HttpFile for managing state while open
    """

    def __init__(
        self,
        http_file: HttpFile,
    ) -> None:
        """
        Open the HTTP URL for the HttpFile.

        Args:
            http_file: An HttpFile instance.

        Teaching Points:
        - Determines file size using HEAD request or range request
        - Tuned for parquet metadata reading: requests last 1MB of file

        Raises:
            ParquetNetworkError: If server doesn't support range requests
        """

        self.http_file = http_file
        self._position = 0
        self._size = self._get_file_size()
        self._minimum_range_request_bytes = min(
            self.http_file._minimum_range_request_bytes,
            self._size,
        )
        self._cache = FileReadCache(
            self._size,
            self._fetch_range,
            minimum_request_size=min(
                self.http_file._minimum_range_request_bytes,
                self._size,
            ),
        )

        prefetch_bytes = min(
            self.http_file._prefetch_bytes,
            self._size,
        )
        prefetch_direction = self.http_file._prefetch_direction
        if prefetch_bytes > 0 and prefetch_direction == 'START':
            self._cache.read(0, prefetch_bytes)
        elif prefetch_bytes > 0 and prefetch_direction == 'END':
            self._cache.read(self._size - prefetch_bytes, self._size)

    def _get_file_size(self) -> int:
        """
        Get total file size using HTTP range request.

        Returns:
            File size in bytes

        Raises:
            ParquetNetworkError: If size cannot be determined
        """
        try:
            request = urllib.request.Request(  # noqa: S310
                self.http_file.url,
                headers={'Range': 'bytes=0-'},
            )
            with urllib.request.urlopen(request) as response:  # noqa: S310
                content_range = response.headers.get('Content-Range')
                if content_range:
                    return int(content_range.split('/')[-1])

                # If no Content-Range header, server doesn't support ranges
                raise ParquetNetworkError(
                    f'Server does not support range requests for {self.http_file.url}',
                )
        except Exception as e:
            raise ParquetNetworkError(
                f'Cannot determine file size for {self.http_file.url}',
            ) from e

    def read(self, size: int | None = None, /) -> bytes:
        if size is None:
            size = self._size - self._position

        if size < 0:
            raise ValueError(f'Cannot read negative number of bytes, got: {size}')

        if size == 0:
            return b''

        start = self._position
        end = min(start + size, self._size)

        data = self._cache.read(start, end)

        self._position = end
        return data

    def _fetch_range(self, start: int, end: int) -> bytes:
        """
        Fetch byte range using HTTP request and add to cache.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Raises:
            ParquetNetworkError: If range request fails
        """
        if start >= end or start < 0 or end > self._size:
            raise ParquetUrlError(
                f'Invalid byte range: {start}-{end} (file size: {self._size})',
            )

        try:
            request = urllib.request.Request(  # noqa: S310
                self.http_file.url,
                headers={'Range': f'bytes={start}-{end - 1}'},
            )
            with urllib.request.urlopen(request) as response:  # noqa: S310
                return response.read()
        except Exception as e:
            raise ParquetNetworkError(
                f'Failed to fetch bytes {start}-{end} from {self.http_file.url}:',
            ) from e

    def seek(self, offset: int, whence: int = 0, /) -> int:
        if whence == 0:  # Absolute position
            new_pos = offset
        elif whence == 1:  # Relative to current position
            new_pos = self._position + offset
        elif whence == 2:  # Relative to end
            new_pos = self._size + offset
        else:
            raise ValueError(f'Invalid whence value: {whence}')

        if new_pos < 0:
            new_pos = 0
        elif new_pos > self._size:
            new_pos = self._size

        self._position = new_pos
        return self._position

    def tell(self) -> int:
        return self._position


class HttpFile:
    """
    File-like wrapper for HTTP URLs with range request support.

    Teaching Points:
    - Provides file-like interface for remote Parquet files
    - Uses HTTP range requests to read specific byte ranges
    - Implements seek/tell for random access to pages
    - Caches data to avoid repeated requests for the same ranges
    - Essential for lazy loading from remote files
    """

    def __init__(
        self,
        url: str,
        minimum_range_request_bytes: int = 8192,
        prefetch_bytes: int = 2**20,
        prefetch_direction: Literal['START', 'END'] = 'END',
    ) -> None:
        """
        Initialize HTTP file wrapper.

        Args:
            url: HTTP/HTTPS URL to the Parquet file

        Keyword Args:
            minimum_range_request_bytes:
                Least number of bytes to request,
                except when filling cache gaps
            prefetch_bytes:
                How many bytes to request when initializing the class.
                Set to 0 or less to disable prefetch. Default 1 MiB.
            prefetch_direction:
                Whether to prefetch from file start or file end.
                Possible values `START` or `END`.
                Parquet has metadata at end, so use `END`.


        Teaching Points:
        - Validates URL format and server capabilities
        - Determines file size using HEAD request or range request
        - Tuned for parquet metadata reading: requests last 1MB of file

        Raises:
            ParquetUrlError: If URL is invalid
            ParquetNetworkError: If server doesn't support range requests
        """

        _check_url(url)
        self.url = url
        self._prefetch_bytes = prefetch_bytes
        self._prefetch_direction = prefetch_direction
        self._minimum_range_request_bytes = minimum_range_request_bytes
        self._opened: _OpenedHttpFile | None = None

    @property
    def _ohf(self) -> _OpenedHttpFile:
        if not self._opened:
            raise ValueError('I/O operation on closed file')
        return self._opened

    def open(self) -> Self:
        self._opened = _OpenedHttpFile(self)
        return self

    def close(self) -> None:
        """
        Close the file (clears cache).

        Teaching Points:
        - Implements standard file close() interface
        - Clears internal cache to free memory
        - No actual connection to close (HTTP is stateless)
        """
        self._opened = None

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        if self._opened:
            return (
                f'HttpFile(url={self.url!r}, opened=True, '
                f'size={self._ohf._size}, pos={self._ohf._position})'
            )
        return f'HttpFile(url={self.url!r}, opened=False)'

    def read(self, size: int | None = None, /) -> bytes:
        """
        Read bytes from current position.

        Args:
            size: Number of bytes to read (-1 for all remaining)

        Returns:
            Bytes read from the file

        Teaching Points:
        - Implements standard file read() interface
        - Uses HTTP range requests to fetch only needed data
        - Updates internal position pointer
        - Caches results to avoid duplicate requests
        """
        return self._ohf.read(size)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change stream position.

        Args:
            offset: Byte offset
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end)

        Returns:
            New absolute position

        Teaching Points:
        - Implements standard file seek() interface
        - Essential for random access to pages at different file offsets
        - No network request needed - just updates position pointer
        - Enables efficient jumping between pages
        """
        return self._ohf.seek(offset, whence)

    def tell(self) -> int:
        """
        Get current stream position.

        Returns:
            Current byte position in file

        Teaching Points:
        - Implements standard file tell() interface
        - No network request needed
        - Used by parsers to track reading progress
        """
        return self._ohf._position

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True
