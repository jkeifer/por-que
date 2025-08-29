"""
HTTP file-like wrapper for remote Parquet file access.

Teaching Points:
- Enables reading remote Parquet files as if they were local files
- Uses HTTP range requests to fetch only needed data sections
- Implements standard file-like interface (read, seek, tell)
- Caches data to minimize HTTP requests for efficiency
"""

import urllib.request

from ..exceptions import ParquetNetworkError, ParquetUrlError


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

    def __init__(self, url: str):
        """
        Initialize HTTP file wrapper.

        Args:
            url: HTTP/HTTPS URL to the Parquet file

        Teaching Points:
        - Validates URL format and server capabilities
        - Determines file size using HEAD request or range request
        - Does not download entire file - only metadata initially

        Raises:
            ParquetUrlError: If URL is invalid
            ParquetNetworkError: If server doesn't support range requests
        """
        _check_url(url)
        self.url = url
        self._position = 0
        self._cache: dict[
            tuple[int, int],
            bytes,
        ] = {}  # Simple cache: {(start, end): bytes}

        # Determine file size
        self._size = self._get_file_size()

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
                self.url,
                headers={'Range': 'bytes=0-0'},
            )
            with urllib.request.urlopen(request) as response:  # noqa: S310
                content_range = response.headers.get('Content-Range')
                if content_range:
                    return int(content_range.split('/')[-1])

                # If no Content-Range header, server doesn't support ranges
                raise ParquetNetworkError(
                    f'Server does not support range requests for {self.url}',
                )
        except Exception as e:
            raise ParquetNetworkError(
                f'Cannot determine file size for {self.url}: {e}',
            ) from e

    def read(self, size: int = -1) -> bytes:
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
        if size == -1:
            size = self._size - self._position

        if size <= 0:
            return b''

        start = self._position
        end = min(start + size, self._size)

        # Check cache first
        cache_key = (start, end)
        if cache_key in self._cache:
            data = self._cache[cache_key]
        else:
            # Fetch data using range request
            data = self._fetch_range(start, end)
            self._cache[cache_key] = data

        # Update position
        self._position = end
        return data

    def _fetch_range(self, start: int, end: int) -> bytes:
        """
        Fetch byte range using HTTP request.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes from the specified range

        Raises:
            ParquetNetworkError: If range request fails
        """
        if start >= end or start < 0 or end > self._size:
            raise ParquetUrlError(
                f'Invalid byte range: {start}-{end} (file size: {self._size})',
            )

        try:
            request = urllib.request.Request(  # noqa: S310
                self.url,
                headers={'Range': f'bytes={start}-{end - 1}'},
            )
            with urllib.request.urlopen(request) as response:  # noqa: S310
                return response.read()
        except Exception as e:
            raise ParquetNetworkError(
                f'Failed to fetch bytes {start}-{end} from {self.url}: {e}',
            ) from e

    def seek(self, offset: int, whence: int = 0) -> int:
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
        """
        Get current stream position.

        Returns:
            Current byte position in file

        Teaching Points:
        - Implements standard file tell() interface
        - No network request needed
        - Used by parsers to track reading progress
        """
        return self._position

    def close(self) -> None:
        """
        Close the file (clears cache).

        Teaching Points:
        - Implements standard file close() interface
        - Clears internal cache to free memory
        - No actual connection to close (HTTP is stateless)
        """
        self._cache.clear()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def readable(self) -> bool:
        """Check if file is readable."""
        return True

    def writable(self) -> bool:
        """Check if file is writable."""
        return False

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return True

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f'HttpFile(url={self.url!r}, size={self._size}, pos={self._position})'
