from __future__ import annotations

from typing import Any, Protocol

from por_que.file_metadata import (
    SchemaGroup,
    SchemaLeaf,
    SchemaRoot,
)
from por_que.util.iteration import PeekableAsyncIterator

SchemaElement = SchemaGroup | SchemaLeaf | SchemaRoot


class Assembler[T: SchemaElement](Protocol):
    """
    A state machine that consumes from one or more tuple streams to build
    one logical item from a Parquet file.
    """

    schema_element: T
    streams: dict[str, PeekableAsyncIterator]

    @property
    def leader_stream(self) -> PeekableAsyncIterator:
        """
        The primary stream that drives the assembler's state machine.

        This is used by parent assemblers for validation and coordination.
        Returns None if the assembler has no driving stream (e.g., a struct of
        only complex types).
        """
        ...

    async def next_item(self, parent_max_rl: int) -> tuple[Any, tuple[int, int] | None]:
        """
        Assembles and returns the next complete item based on the parent's context.

        Args:
            parent_max_rl: The repetition level of the container this item
                is being built for. The assembler knows its work is done when
                it encounters a tuple with an RL less than this value.

        Returns:
            A tuple of (value, boundary_info) where:
            - If successful: (assembled_item, None)
            - If not my tuple: (None, (dl, rl)) where dl, rl are the levels of the
              tuple that doesn't belong to this assembler (tuple NOT consumed)

        Raises:
            StopAsyncIteration: When its input stream(s) are exhausted in a way
                that indicates a clean end of all data.
            ParquetDataError: If the data is corrupt or inconsistent
                (e.g., RL/DL mismatch).
        """
        ...
