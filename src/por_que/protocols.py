from __future__ import annotations

from typing import Protocol, runtime_checkable


class ReadableSeekable(Protocol):
    """Protocol for file-like objects that support reading and seeking.

    This is more permissive than BinaryIO and doesn't require write methods.
    """

    def read(self, size: int | None = None, /) -> bytes:
        """Read up to size bytes."""
        ...

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """Change stream position."""
        ...

    def tell(self) -> int:
        """Return current stream position."""
        ...

    def close(self) -> None:
        """Close the file."""
        ...


@runtime_checkable
class AsyncReadableSeekable(Protocol):
    """Protocol for async file-like objects that support reading and seeking.

    This is the async equivalent of ReadableSeekable, supporting async read
    operations while keeping seek/tell synchronous (no I/O required).
    """

    async def read(self, size: int | None = None, /) -> bytes:
        """Read up to size bytes asynchronously."""
        ...

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """Change stream position (synchronous - no I/O)."""
        ...

    def tell(self) -> int:
        """Return current stream position (synchronous - no I/O)."""
        ...


@runtime_checkable
class AsyncCursableReadableSeekable(AsyncReadableSeekable, Protocol):
    """Protocol for async file-like objects that support creating cursors.

    Cursors share underlying resources (cache, session) but have independent
    positions, enabling concurrent reads from different file locations.
    """

    def clone(self) -> AsyncCursableReadableSeekable:
        """Create a new cursor with independent position."""
        ...
