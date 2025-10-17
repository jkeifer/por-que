from __future__ import annotations

import asyncio

from typing import Literal, Self

import aiohttp

from por_que.exceptions import ParquetNetworkError, ParquetUrlError

from .async_file_read_cache import AsyncFileReadCache


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


class _OpenedAsyncHttpFile:
    """
    Internal class managing shared state for AsyncHttpFile.
    """

    def __init__(
        self,
        http_file: AsyncHttpFile,
        session: aiohttp.ClientSession,
        size: int,
    ) -> None:
        """
        Initialize opened HTTP file with pre-fetched async values.

        Args:
            http_file: The parent AsyncHttpFile instance
            session: Pre-created aiohttp session
            size: File size obtained via async HTTP request
        """
        self.http_file = http_file
        self.session = session
        self.size = size
        self.cache = AsyncFileReadCache(
            self.size,
            self._fetch_range,
            minimum_request_size=min(
                self.http_file._minimum_range_request_bytes,
                self.size,
            ),
        )

    @classmethod
    async def create(cls, http_file: AsyncHttpFile) -> Self:
        """
        Async factory method to create _OpenedAsyncHttpFile.

        Args:
            http_file: The parent AsyncHttpFile instance

        Returns:
            Fully initialized _OpenedAsyncHttpFile instance
        """
        session = aiohttp.ClientSession()
        size = await cls._get_file_size(session, http_file.url)
        return cls(http_file, session, size)

    @staticmethod
    async def _get_file_size(
        session: aiohttp.ClientSession,
        url: str,
    ) -> int:
        """
        Get total file size using async HTTP range request.

        Args:
            session: aiohttp session to use for request
            url: URL to fetch size for

        Returns:
            File size in bytes

        Raises:
            ParquetNetworkError: If size cannot be determined
        """
        try:
            headers = {'Range': 'bytes=0-'}
            async with session.get(url, headers=headers) as response:
                content_range = response.headers.get('Content-Range')
                if content_range:
                    return int(content_range.split('/')[-1])

                # If no Content-Range header, server doesn't support ranges
                raise ParquetNetworkError(
                    f'Server does not support range requests for {url}',
                )
        except Exception as e:
            raise ParquetNetworkError(
                f'Cannot determine file size for {url}',
            ) from e

    def _fetch_range(self, start: int, end: int) -> asyncio.Task[bytes]:
        """
        Create an async task to fetch byte range using HTTP request.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Asyncio task that will fetch the requested bytes
        """
        return asyncio.create_task(self._do_fetch_range(start, end))

    async def _do_fetch_range(self, start: int, end: int) -> bytes:
        """
        Actually fetch byte range using async HTTP request.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            ParquetNetworkError: If range request fails
        """
        if start >= end or start < 0 or end > self.size:
            raise ParquetUrlError(
                f'Invalid byte range: {start}-{end} (file size: {self.size})',
            )

        try:
            headers = {'Range': f'bytes={start}-{end - 1}'}
            async with self.session.get(
                self.http_file.url,
                headers=headers,
            ) as response:
                return await response.read()
        except RuntimeError:
            raise
        except Exception as e:
            raise ParquetNetworkError(
                f'Failed to fetch bytes {start}-{end} from {self.http_file.url}',
            ) from e

    async def read(self, position: int, size: int | None = None, /) -> bytes:
        """
        Read bytes from a specific position without managing cursor state.

        Args:
            position: Starting byte position to read from
            size: Number of bytes to read (None for all remaining)

        Returns:
            Bytes read from the file
        """
        if size is None:
            size = self.size - position

        if size < 0:
            raise ValueError(f'Cannot read negative number of bytes, got: {size}')

        if size == 0:
            return b''

        start = position
        end = min(start + size, self.size)

        return await self.cache.read(start, end)

    async def close(self) -> None:
        """
        Close the file and session.
        """
        await self.session.close()


class AsyncHttpFileCursor:
    """
    Lightweight cursor for reading from AsyncHttpFile with independent position.
    """

    def __init__(self, opened_file: _OpenedAsyncHttpFile) -> None:
        """
        Create a cursor for reading from an opened HTTP file.

        Args:
            opened_file: The shared opened file state
        """
        self.ohf = opened_file
        self.position = 0

    @property
    def size(self) -> int:
        return self.ohf.size

    async def read(self, size: int | None = None, /) -> bytes:
        """
        Read bytes from current position asynchronously.

        Args:
            size: Number of bytes to read (None for all remaining)

        Returns:
            Bytes read from the file
        """
        data = await self.ohf.read(self.position, size)
        self.position += len(data)
        return data

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change stream position (synchronous - no I/O).

        Args:
            offset: Byte offset
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end)

        Returns:
            New absolute position
        """
        if whence == 0:  # Absolute position
            new_pos = offset
        elif whence == 1:  # Relative to current position
            new_pos = self.position + offset
        elif whence == 2:  # Relative to end
            new_pos = self.size + offset
        else:
            raise ValueError(f'Invalid whence value: {whence}')

        if new_pos < 0:
            new_pos = 0
        elif new_pos > self.size:
            new_pos = self.size

        self.position = new_pos
        return self.position

    def tell(self) -> int:
        """
        Get current stream position (synchronous - no I/O).

        Returns:
            Current byte position in file
        """
        return self.position

    def clone(self) -> AsyncHttpFileCursor:
        """
        Create a new sibling cursor with independent position.

        Returns:
            New cursor sharing cache and session but with independent position

        Raises:
            ValueError: If file is not opened
        """
        return AsyncHttpFileCursor(self.ohf)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True


class AsyncHttpFile:
    """
    Async file-like wrapper for HTTP URLs with concurrent read support.
    """

    def __init__(
        self,
        url: str,
        minimum_range_request_bytes: int = 8192,
        prefetch_bytes: int = 2**20,
        prefetch_direction: Literal['START', 'END'] = 'END',
    ) -> None:
        """
        Initialize async HTTP file wrapper.

        Args:
            url: HTTP/HTTPS URL to the Parquet file

        Keyword Args:
            minimum_range_request_bytes:
                Least number of bytes to request,
                except when filling cache gaps
            prefetch_bytes:
                How many bytes to request when opening the file.
                Set to 0 or less to disable prefetch. Default 1 MiB.
            prefetch_direction:
                Whether to prefetch from file start or file end.
                Possible values `START` or `END`.
                Parquet has metadata at end, so use `END`.

        Raises:
            ParquetUrlError: If URL is invalid
        """
        _check_url(url)
        self.url = url
        self._prefetch_bytes = prefetch_bytes
        self._prefetch_direction = prefetch_direction
        self._minimum_range_request_bytes = minimum_range_request_bytes
        self._cursor: AsyncHttpFileCursor | None = None

    @property
    def cursor(self) -> AsyncHttpFileCursor:
        if not self._cursor:
            raise ValueError('I/O operation on closed file')
        return self._cursor

    @property
    def size(self) -> int:
        return self.cursor.size

    async def open(self) -> Self:
        """
        Open the file asynchronously.

        Returns:
            Self for use in context manager
        """
        self._cursor = AsyncHttpFileCursor(await _OpenedAsyncHttpFile.create(self))

        prefetch_bytes = min(self._prefetch_bytes, self.size)
        if prefetch_bytes > 0 and self._prefetch_direction == 'START':
            await self.read(prefetch_bytes)
        elif prefetch_bytes > 0 and self._prefetch_direction == 'END':
            self.cursor.seek(prefetch_bytes, 2)
            await self.read(prefetch_bytes)

        self.cursor.seek(0)

        return self

    def clone(self) -> AsyncHttpFileCursor:
        """
        Create a new cursor for concurrent reads.

        Returns:
            New cursor sharing cache but with independent position

        Raises:
            ValueError: If file is not opened
        """
        return self.cursor.clone()

    async def close(self) -> None:
        """
        Close the file and release resources.
        """
        if self._cursor:
            await self._cursor.ohf.close()
        self._cursor = None

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        return await self.open()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    def __repr__(self) -> str:
        if self._cursor:
            return (
                f'AsyncHttpFile(url={self.url!r}, opened=True, '
                f'size={self.size}, pos={self._cursor.position})'
            )
        return f'AsyncHttpFile(url={self.url!r}, opened=False)'

    async def read(self, size: int | None = None, /) -> bytes:
        """
        Read bytes from current position asynchronously.

        Args:
            size: Number of bytes to read (None for all remaining)

        Returns:
            Bytes read from the file
        """
        return await self.cursor.read(size)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change stream position (synchronous - no I/O).

        Args:
            offset: Byte offset
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end)

        Returns:
            New absolute position
        """
        return self.cursor.seek(offset, whence)

    def tell(self) -> int:
        """
        Get current stream position (synchronous - no I/O).

        Returns:
            Current byte position in file
        """
        return self.cursor.tell()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True
