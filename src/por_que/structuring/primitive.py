from __future__ import annotations

from typing import Any

from ..file_metadata import SchemaLeaf
from ..util.iteration import PeekableAsyncIterator
from .protocols import Assembler


class PrimitiveAssembler(Assembler):
    """
    Assembler for primitive (SchemaLeaf) columns.

    It consumes one tuple from a single stream and returns the value,
    or None if the value is not present according to the definition level.
    """

    def __init__(
        self,
        schema_element: SchemaLeaf,
        streams: dict[str, PeekableAsyncIterator],
    ) -> None:
        self.schema_element = schema_element
        self.streams = streams
        self._stream = next(iter(streams.values()))

    @property
    def leader_stream(self) -> PeekableAsyncIterator:
        return self._stream

    async def next_item(self, parent_max_rl: int) -> tuple[Any, tuple[int, int] | None]:
        """
        Consumes one tuple and returns the value or None.
        Returns: (value, None) since primitives always consume their tuple.
        """
        # For a primitive, we always consume one tuple to produce one item.
        # The parent's state machine is responsible for not calling `next_item`
        # if the tuple's RL indicates the primitive is outside the parent's bounds.
        value, dl, _ = await anext(self.leader_stream)

        if dl >= self.schema_element.definition_level:
            return (value, None)
        # Any DL less than the max for a primitive means it's a null
        # at some level of its ancestry. The assembled value is None.
        return (None, None)
