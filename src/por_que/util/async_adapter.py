from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from por_que.protocols import ReadableSeekable


class AsyncReadableSeekableAdapter:
    """
    Wraps a synchronous ReadableSeekable file with an async interface.

    This adapter allows synchronous file objects (like local files) to be used
    in async contexts with the AsyncReadableSeekable protocol. The async
    methods don't actually perform any awaiting - they return immediately since
    local file I/O completes instantly.

    This enables a unified interface where code can work with both async HTTP
    files and local files without needing separate code paths.
    """

    def __init__(self, file: ReadableSeekable) -> None:
        """
        Initialize adapter with a synchronous file.

        Args:
            file: Synchronous file-like object supporting read/seek/tell
        """
        self._file = file

    async def read(self, size: int | None = None, /) -> bytes:
        """
        Read bytes from the file (async interface, instant completion).

        Args:
            size: Number of bytes to read (None for all remaining)

        Returns:
            Bytes read from the file
        """
        return self._file.read(size)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change stream position (synchronous - no I/O).

        Args:
            offset: Byte offset
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end)

        Returns:
            New absolute position
        """
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        """
        Get current stream position (synchronous - no I/O).

        Returns:
            Current byte position in file
        """
        return self._file.tell()

    def __repr__(self) -> str:
        return f'AsyncReadableSeekableAdapter({self._file!r})'
