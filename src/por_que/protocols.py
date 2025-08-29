"""Protocol definitions for por_que."""

from typing import Protocol


class ReadableSeekable(Protocol):
    """Protocol for file-like objects that support reading and seeking.

    This is more permissive than BinaryIO and doesn't require write methods.
    """

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes."""
        ...

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change stream position."""
        ...

    def tell(self) -> int:
        """Return current stream position."""
        ...

    def close(self) -> None:
        """Close the file."""
        ...
