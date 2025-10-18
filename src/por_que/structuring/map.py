from __future__ import annotations

from typing import Any, cast

from ..exceptions import ParquetDataError
from ..file_metadata import SchemaGroup, SchemaLeaf
from ..util.iteration import PeekableAsyncIterator
from .factory import make_assembler
from .protocols import Assembler


class MapAssembler(Assembler):
    """
    Assembler for MAP fields.

    A map is a list of [key, value] pairs. This assembler coordinates a key
    assembler and a value assembler, driven by the key's tuple stream.
    """

    def __init__(
        self,
        schema_element: SchemaGroup,
        streams: dict[str, PeekableAsyncIterator],
    ) -> None:
        self.schema_element = schema_element
        self.streams = streams

        # Navigate schema: MAP -> key_value(repeated) -> key, value
        self.map_group = cast(
            SchemaGroup,
            next(iter(schema_element.children.values())),
        )
        key_schema = self.map_group.children['key']
        value_schema = self.map_group.children.get('value')

        self.repetition_level = self.map_group.repetition_level
        # Use the MAP's definition level for null/empty detection,
        # not the key_value group's level
        self.definition_level = schema_element.definition_level

        # Create child assemblers
        self.key_assembler = make_assembler(key_schema, streams)
        self.value_assembler = (
            make_assembler(
                value_schema,
                streams,
            )
            if value_schema
            else None
        )

        # The key stream drives the state machine
        self.key_stream = streams[key_schema.full_path]

    @property
    def leader_stream(self) -> PeekableAsyncIterator:
        return self.key_stream

    async def next_item(  # noqa: C901
        self,
        parent_max_rl: int,
    ) -> tuple[list[Any] | None, tuple[int, int] | None]:
        """
        Assembles one complete map (list of [key, value] pairs).

        The first_entry flag is necessary because Dremel's RL semantics are
        ambiguous for the first element: RL=0 can mean either "first element of
        nested structure" or "new top-level record". We can only distinguish by
        tracking whether we've started processing entries yet.

        Returns:
            (map, None) if successful, (None, (dl, rl)) if not my tuple
        Raises:
            StopAsyncIteration: When the stream is exhausted
        """
        peeked_tuple = await self.key_stream.peek()
        if peeked_tuple is None:
            raise StopAsyncIteration

        _, dl, rl = peeked_tuple

        # Check if this tuple clearly belongs to a parent container
        # This requires BOTH conditions:
        # 1. RL indicates parent level (rl < parent_max_rl)
        # 2. DL indicates not even our container exists (dl < definition_level)
        if rl < parent_max_rl and dl < self.definition_level:
            # This tuple definitely doesn't belong to us
            return (None, (dl, rl))

        # Null Map Check: If the peeked DL is below the map's own definition level,
        # the entire map is null.
        if dl < self.definition_level:
            # We reach here only if rl >= parent_max_rl
            # So this null map marker belongs to us - consume from both streams
            await anext(self.key_stream)
            # Also consume from value stream if it exists
            if self.value_assembler and self.value_assembler.leader_stream:
                value_peek = await self.value_assembler.leader_stream.peek()
                if value_peek and value_peek[1] == dl and value_peek[2] == rl:
                    await anext(self.value_assembler.leader_stream)
            return (None, None)

        # Empty Map Check: Container exists but no entries
        # This should only trigger for optional maps where the map is present
        # but has no key-value pairs
        if dl == self.definition_level and rl < self.repetition_level:
            # If we're being called and see our definition level,
            # this empty map belongs to us
            # The parent (e.g., ListAssembler) has already determined
            # this position is ours
            # Exception: For nested maps as values,
            # check if this is a parent boundary
            if rl < parent_max_rl and parent_max_rl > self.repetition_level:
                # This is for a parent container at a higher repetition level
                return (None, (dl, rl))

            # Empty map marker for us - consume from both streams
            await anext(self.key_stream)
            # Also consume from value stream if it exists
            if self.value_assembler and self.value_assembler.leader_stream:
                value_peek = await self.value_assembler.leader_stream.peek()
                if value_peek and value_peek[1] == dl and value_peek[2] == rl:
                    await anext(self.value_assembler.leader_stream)
            return ([], None)

        # Populated Map: Has at least one entry
        result_map: list[Any] = []
        first_entry = True

        # Loop while entries belong to this map instance
        while True:
            peeked_key_tuple = await self.key_stream.peek()
            if peeked_key_tuple is None:
                break

            _, _, key_rl = peeked_key_tuple

            # Stop when RL drops below our repetition level
            # EXCEPT for first entry which can have lower RL
            if not first_entry and key_rl < self.repetition_level:
                break

            # Validate key-value synchronization for primitive values
            if self.value_assembler and isinstance(
                self.value_assembler.schema_element,
                SchemaLeaf,
            ):
                value_leader_stream = self.value_assembler.leader_stream
                if value_leader_stream:
                    val_peek = await value_leader_stream.peek()
                    if val_peek is None:
                        raise ParquetDataError(
                            'Map value stream exhausted while key stream has data',
                        )
                    if val_peek[2] > key_rl:
                        raise ParquetDataError(
                            f'RL mismatch in map: key RL={key_rl}, '
                            f'value RL={val_peek[2]}',
                        )

            # Build key and value
            key_result, key_boundary = await self.key_assembler.next_item(
                self.repetition_level,
            )

            if key_boundary is not None:
                # Key says tuple doesn't belong to it
                # Check if the tuple belongs to us or should be re-raised
                dl, rl = key_boundary
                if dl < self.definition_level or rl < self.repetition_level:
                    # This tuple is at our boundary or above - we're done
                    break
                # This tuple is deeper than us, shouldn't happen
                raise ValueError(
                    f'Unexpected boundary from key assembler: dl={dl}, rl={rl}',
                )

            # We have a key, now get the value
            if self.value_assembler:
                # Normal map with key-value pairs
                value_result, value_boundary = await self.value_assembler.next_item(
                    self.repetition_level,
                )

                if value_boundary is not None:
                    # Value assembler says tuple doesn't belong to it
                    # Since we already have the key, this means the value is null/empty
                    # The boundary tuple is still in the stream - we must consume it
                    # since it represents this key's value (null or empty)
                    dl, rl = value_boundary

                    # For nested maps, we need to consume from all streams
                    # under the value path
                    # Get all streams that belong to the value's schema subtree
                    value_streams = [
                        s
                        for path, s in self.streams.items()
                        if path.startswith(
                            self.value_assembler.schema_element.full_path + '.',
                        )
                    ]

                    # Add the value schema's own stream if it's a leaf
                    if self.value_assembler.schema_element.full_path in self.streams:
                        value_streams.append(
                            self.streams[self.value_assembler.schema_element.full_path],
                        )

                    # Consume the boundary marker from all relevant streams
                    for stream in value_streams:
                        peeked = await stream.peek()
                        if peeked and peeked[1] == dl and peeked[2] == rl:
                            await anext(stream)

                    if dl < self.value_assembler.schema_element.definition_level:
                        # Value is null
                        value = None
                    elif dl == self.value_assembler.schema_element.definition_level:
                        # Value exists but is empty (for maps/lists)
                        value = []
                    else:
                        # This shouldn't happen
                        raise ValueError(
                            'Unexpected boundary from value assembler: '
                            f'dl={dl}, rl={rl}',
                        )
                else:
                    value = value_result

                result_map.append([key_result, value])
            else:
                # Map with no value column - represents a set
                # Just append the key directly
                result_map.append(key_result)

            first_entry = False

        return (result_map, None)
