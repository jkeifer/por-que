from __future__ import annotations

from typing import Any

from ..exceptions import ParquetDataError
from ..file_metadata import SchemaGroup, SchemaRoot
from ..util.iteration import PeekableAsyncIterator
from .factory import get_all_leaf_paths, get_direct_leaf_paths, make_assembler
from .protocols import Assembler


class StructAssembler(Assembler):
    """
    Assembler for STRUCT (SchemaGroup with no converted_type).

    This assembler coordinates multiple child assemblers (one for each field
    in the struct). It uses a leader-follower pattern for its direct leaf
    children to ensure all streams are consumed in lockstep.
    """

    def __init__(
        self,
        schema_element: SchemaGroup | SchemaRoot,
        streams: dict[str, PeekableAsyncIterator],
    ) -> None:
        self.schema_element = schema_element
        self.streams = streams

        # The definition level at which this struct is considered present.
        # For an optional group, this is one level higher than its parent's definition.
        self.definition_level = self.schema_element.definition_level

        self.children = {
            name: make_assembler(child_schema, streams)
            for name, child_schema in schema_element.children.items()
        }
        self._leader_stream: PeekableAsyncIterator | None

        # Identify only direct leaf children to synchronize them. We don't
        # want to synchronize leaves nested inside complex types like
        # lists/maps, as those are managed by their own assemblers.
        leaf_paths = get_direct_leaf_paths(schema_element)
        self.leaf_streams = [streams[path] for path in leaf_paths if path in streams]

        if self.leaf_streams:
            self._leader_stream = self.leaf_streams[0]
            self.follower_streams = self.leaf_streams[1:]
        else:
            # This case (a struct with no primitive fields, e.g., struct-of-lists)
            # We need to pick a leaf from somewhere to drive the state machine
            # Use the first available stream from any child
            all_leaf_paths = get_all_leaf_paths(schema_element)
            if all_leaf_paths and all_leaf_paths[0] in streams:
                self._leader_stream = streams[all_leaf_paths[0]]
            else:
                self._leader_stream = None
            self.follower_streams = []

    @property
    def leader_stream(self) -> PeekableAsyncIterator:
        if not self._leader_stream:
            raise ValueError('StructAssembler has no leader stream')
        return self._leader_stream

    async def next_item(  # noqa: C901
        self,
        parent_max_rl: int,
    ) -> tuple[dict[str, Any] | None, tuple[int, int] | None]:
        """
        Assembles one struct dictionary with proper RL validation.

        Structs don't have the first_element ambiguity that lists/maps have,
        so they can use simple boundary checking.

        Returns:
            (struct_dict, None) if successful, (None, (dl, rl)) if not my tuple
        Raises:
            StopAsyncIteration: When the stream is exhausted
        """
        if not self._leader_stream:
            # Handle struct of only complex types. The first child drives the
            # process.
            # TODO: This is a simplification for now.
            first_child = next(iter(self.children.values()))
            # This path is not fully implemented per the robust plan, as it needs to
            # derive its driving RL/DL from a non-leaf source.
            return await first_child.next_item(parent_max_rl)

        peeked_tuple = await self.leader_stream.peek()
        if peeked_tuple is None:
            raise StopAsyncIteration

        _, dl, rl = peeked_tuple

        # Boundary check for structs
        # When a struct is an element of a list, the first element might have
        # rl < parent_max_rl. We should only signal boundary when we're certain.
        if rl < parent_max_rl and dl < self.definition_level:
            # This tuple definitely doesn't belong to us
            return (None, (dl, rl))

        # Null Struct Check: If the leader's DL is less than the level required for
        # the struct itself to be present, the struct is null.
        if dl < self.definition_level:
            # Consume the null-defining tuple from ALL leaf streams under this struct
            # BUT only if they also have dl < definition_level (indicating null struct)
            all_leaf_paths = get_all_leaf_paths(self.schema_element)
            for path in all_leaf_paths:
                if path in self.streams:
                    stream = self.streams[path]
                    peeked = await stream.peek()
                    if peeked and peeked[1] < self.definition_level:
                        await stream.__anext__()
            return (None, None)

        # Validate follower streams have data available
        # NOTE: We don't validate RLs strictly match because:
        # 1. Optional fields may have different RLs at boundaries
        # 2. Legacy encodings handle repetition differently
        # 3. Fields in repeated contexts may have RL variations
        for follower in self.follower_streams:
            follower_tuple = await follower.peek()
            if follower_tuple is None:
                raise ParquetDataError(
                    'Follower stream exhausted while leader has data',
                )

        # Build the struct
        record: dict[str, Any] = {}
        for name, child_assembler in self.children.items():
            # Pass the same parent_max_rl we received
            # structs don't change repetition context
            child_result, child_boundary = await child_assembler.next_item(
                parent_max_rl=parent_max_rl,
            )

            if child_boundary is not None:
                # Child says tuple doesn't belong to it
                # For structs, if a child can't process a tuple, it usually means
                # the entire struct is ending (not just that field is null)
                boundary_dl, boundary_rl = child_boundary
                if boundary_dl >= self.definition_level:
                    # The tuple is at or above our definition level
                    # This shouldn't happen - child should have handled it
                    raise ParquetDataError(
                        f"Child assembler for field '{name}' "
                        'returned boundary unexpectedly '
                        f'for tuple with dl={boundary_dl}, rl={boundary_rl}. '
                        f'Struct definition_level={self.definition_level}.',
                    )
                # Tuple is below our definition level - return boundary to parent
                return (None, (boundary_dl, boundary_rl))
            # Successfully got the child value
            record[name] = child_result

        return (record, None)
