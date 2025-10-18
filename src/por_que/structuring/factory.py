from __future__ import annotations

from por_que.enums import ConvertedType, Repetition
from por_que.exceptions import ParquetDataError
from por_que.file_metadata import (
    SchemaGroup,
    SchemaLeaf,
    SchemaRoot,
)
from por_que.util.iteration import PeekableAsyncIterator

from .protocols import Assembler, SchemaElement


def get_all_leaf_paths(schema_element: SchemaElement) -> list[str]:
    """
    Recursively find all leaf (SchemaLeaf) full_paths under this schema node.
    """
    paths = []
    if isinstance(schema_element, SchemaLeaf):
        return [schema_element.full_path]

    for child in schema_element.children.values():
        paths.extend(get_all_leaf_paths(child))
    return paths


def get_direct_leaf_paths(schema_element: SchemaElement) -> list[str]:
    """
    Find only direct leaf children, not nested leaves.
    This is used by StructAssembler to synchronize only its immediate primitive fields.
    """
    paths = []
    if not isinstance(schema_element, SchemaLeaf):
        for child in schema_element.children.values():
            if isinstance(child, SchemaLeaf):
                paths.append(child.full_path)
    return paths


def _get_child_streams(
    schema_element: SchemaElement,
    all_streams: dict[str, PeekableAsyncIterator],
) -> dict[str, PeekableAsyncIterator]:
    """
    Get all streams for a schema node's descendant leaves.
    """
    leaf_paths = get_all_leaf_paths(schema_element)
    return {path: all_streams[path] for path in leaf_paths if path in all_streams}


def make_assembler(
    schema_element: SchemaElement,
    streams: dict[str, PeekableAsyncIterator],
) -> Assembler:
    """
    Factory to create the appropriate assembler based on schema type.
    This is the central dispatch point for the recursive-descent parser.
    """
    from .list import (
        LegacyListAssembler,
        ModernListAssembler,
        PrimativeListAssembler,
    )
    from .map import MapAssembler
    from .primitive import PrimitiveAssembler
    from .struct import StructAssembler

    # Get all streams relevant to the current schema node's subtree.
    child_streams = _get_child_streams(
        schema_element,
        streams,
    )

    match schema_element:
        # Case 0: Root of the schema tree
        case SchemaRoot():
            return StructAssembler(schema_element, child_streams)

        # Case 1: Primitive Type (Base Case)
        case SchemaLeaf(repetition=Repetition.REPEATED):
            # A repeated leaf is a simple list of primitives.
            # We wrap a PrimitiveAssembler inside a ListAssembler.
            return PrimativeListAssembler(schema_element, child_streams)

        case SchemaLeaf():
            # A non-repeated leaf, which has only one stream.
            return PrimitiveAssembler(
                schema_element,
                child_streams,
            )

        # Case 2: Modern 3-Level LIST
        case SchemaGroup(converted_type=ConvertedType.LIST):
            return ModernListAssembler(schema_element, child_streams)

        # Case 3: Modern 3-Level MAP
        case SchemaGroup(
            converted_type=ConvertedType.MAP | ConvertedType.MAP_KEY_VALUE,
        ):
            return MapAssembler(schema_element, child_streams)

        # Case 4: Legacy 2-Level LIST (implicit)
        # A group that is marked as repeated but is not a standard LIST/MAP group.
        case SchemaGroup(repetition=Repetition.REPEATED):
            return LegacyListAssembler(schema_element, child_streams)

        # Case 5: Regular STRUCT
        case SchemaGroup():
            return StructAssembler(schema_element, child_streams)

        case _:
            raise ParquetDataError(
                f'Unhandled schema combination for factory: {schema_element}',
            )
