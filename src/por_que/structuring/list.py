from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from por_que.exceptions import ParquetDataError
from por_que.file_metadata import SchemaGroup, SchemaLeaf
from por_que.util.iteration import PeekableAsyncIterator

from .factory import make_assembler
from .primitive import PrimitiveAssembler
from .protocols import Assembler
from .struct import StructAssembler


class BaseListAssembler[T: SchemaGroup | SchemaLeaf](Assembler, ABC):
    """
    Base Assembler for any repeated field, which is interpreted as a list.
    """

    def __init__(
        self,
        schema_element: T,
        streams: dict[str, PeekableAsyncIterator],
    ) -> None:
        self.schema_element = schema_element
        self.streams = streams

        # The driving stream for the state machine is the first leaf of the element.
        self.element_stream = next(iter(self.streams.values()))

        self.repetition_level, self.definition_level, self.element_assembler = (
            self._configure(
                self.schema_element,
                self.streams,
            )
        )

    @property
    def leader_stream(self) -> PeekableAsyncIterator:
        return self.element_stream

    async def next_item(  # noqa: C901
        self,
        parent_max_rl: int,
    ) -> tuple[list[Any] | None, tuple[int, int] | None]:
        """
        Assembles one complete list.
        The state machine is driven by peeking at the RL/DL of the element stream.

        The first_element flag is necessary because Dremel's RL semantics are ambiguous
        for the first element: RL=0 can mean either "first element of nested structure"
        or "new top-level record". We can only distinguish by tracking whether we've
        started processing elements yet.

        Returns:
            (list, None) if successful, (None, (dl, rl)) if not my tuple
        Raises:
            StopAsyncIteration: When the stream is exhausted
        """
        peeked_tuple = await self.element_stream.peek()
        if peeked_tuple is None:
            raise StopAsyncIteration

        _, dl, rl = peeked_tuple

        # Boundary check: We need to detect when a tuple doesn't belong to us
        # to avoid infinite loops. But we must be careful with the first element.

        # If this tuple is at a definition level below our container's level,
        # it might be a null marker or belong to a parent
        if dl < self.definition_level:
            # Only return boundary if rl clearly indicates a higher-level parent
            # Don't check rl < parent_max_rl alone because rl=0 is valid for first
            # elements
            # Only return boundary if parent is at a much higher repetition context
            if rl < parent_max_rl and parent_max_rl > self.repetition_level:
                return (None, (dl, rl))
            # Otherwise it's a null list marker for us - consume from ALL streams
            for stream in self.streams.values():
                peeked = await stream.peek()
                if peeked and peeked[1] == dl and peeked[2] == rl:
                    await stream.__anext__()
            return (None, None)

        # Empty List Check
        if dl == self.definition_level and rl < self.repetition_level:
            # Check if this empty list marker belongs to us or parent
            if rl < parent_max_rl:
                # Empty list marker for a parent container
                return (None, (dl, rl))
            # Empty list marker for us - consume from ALL streams
            # For struct elements, the empty marker appears in all field streams
            for stream in self.streams.values():
                peeked = await stream.peek()
                if peeked and peeked[1] == dl and peeked[2] == rl:
                    await stream.__anext__()
            return ([], None)

        # Populated List: If we reach here, the list has at least one item.
        result_list: list[Any] = []
        first_element = True

        # Loop while elements belong to this list instance
        while True:
            peeked_tuple = await self.element_stream.peek()
            if peeked_tuple is None:
                break

            _, _, rl = peeked_tuple

            # Stop when RL drops below our repetition level
            # EXCEPT for first element which can have lower RL
            if not first_element and rl < self.repetition_level:
                break

            # Build one element
            item_result, boundary = await self.element_assembler.next_item(
                parent_max_rl=self.repetition_level,
            )

            if boundary is not None:
                # Child says tuple doesn't belong to it
                # Check if the tuple indicates list boundary or null/empty element
                dl, rl = boundary

                # Check if this is actually a null/empty element for this list position
                # This happens when rl is valid for our list but child can't process it
                if rl >= self.repetition_level or (
                    first_element and rl < self.repetition_level
                ):
                    # This is a null or empty element at our level
                    # Consume it from ALL streams
                    # (important for complex elements like structs)
                    for stream in self.streams.values():
                        peeked = await stream.peek()
                        if peeked and peeked[1] == dl and peeked[2] == rl:
                            await stream.__anext__()

                    # Determine if it's null or empty
                    # based on the child's definition level
                    # If child has a definition level, check if element exists
                    child_def_level = getattr(
                        self.element_assembler,
                        'definition_level',
                        None,
                    )
                    if child_def_level is not None and dl == child_def_level:
                        # Element container exists but is empty
                        result_list.append([])
                    else:
                        # Element is null
                        result_list.append(None)
                    first_element = False
                elif rl < self.repetition_level:
                    # This tuple indicates the list is ending
                    # don't consume, just break
                    break
                elif dl < self.definition_level:
                    # The entire list container doesn't exist at this level
                    break
                else:
                    # This tuple is deeper than us, shouldn't happen
                    raise ValueError(
                        f'Unexpected boundary from child: dl={dl}, rl={rl}',
                    )
            else:
                result_list.append(item_result)
                first_element = False

        return (result_list, None)

    @staticmethod
    @abstractmethod
    def _configure(
        schema_element: T,
        streams: dict[str, PeekableAsyncIterator],
    ) -> tuple[int, int, Assembler]:
        _, _ = schema_element, streams
        raise NotImplementedError


class ModernListAssembler(BaseListAssembler):
    """
    Handles modern 3-level lists.
    """

    @staticmethod
    def _configure(
        schema_element: SchemaGroup,
        streams: dict[str, PeekableAsyncIterator],
    ) -> tuple[int, int, Assembler]:
        # For 3-level lists, list_group is the repeated wrapper
        list_group = next(iter(schema_element.children.values()))

        if not isinstance(list_group, SchemaGroup):
            raise ParquetDataError('Modern list must be 3-level')

        # The actual list item schema is its child
        inner_schema = next(iter(list_group.children.values()))

        return (
            list_group.repetition_level,
            schema_element.definition_level,
            make_assembler(inner_schema, streams),
        )


class PrimativeListAssembler(BaseListAssembler):
    """
    Repeated SchemaLeaf.
    """

    @staticmethod
    def _configure(
        schema_element: SchemaLeaf,
        streams: dict[str, PeekableAsyncIterator],
    ) -> tuple[int, int, Assembler]:
        # Repeated primitive. The element is the primitive itself.
        # The list is present at one DL below the element's DL.
        # Create PrimitiveAssembler directly to avoid infinite recursion
        # (factory would see repeated leaf and create ListAssembler again)
        return (
            schema_element.repetition_level,
            schema_element.definition_level - 1,
            PrimitiveAssembler(
                schema_element,
                streams,
            ),
        )


class LegacyListAssembler(BaseListAssembler):
    """
    Legacy two-level list.
    """

    @staticmethod
    def _configure(
        schema_element: SchemaGroup,
        streams: dict[str, PeekableAsyncIterator],
    ) -> tuple[int, int, Assembler]:
        # For legacy lists (repeated groups without LIST annotation), the list
        # exists when the parent exists, not when the repeated group exists.
        # So the list's definition level is one less than the repeated group's
        # level.
        return (
            schema_element.repetition_level,
            schema_element.definition_level - 1,
            StructAssembler(schema_element, streams),
        )
