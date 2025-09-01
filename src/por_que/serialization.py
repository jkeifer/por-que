from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import cattrs

from .enums import PageType, SchemaElementType

if TYPE_CHECKING:
    from . import logical
    from .physical import (
        AnyPageLayout,
        DataPageV1Layout,
        DataPageV2Layout,
        PhysicalColumnChunk,
    )


def _create_page_layout_hook(
    converter: cattrs.Converter,
) -> Callable[
    [dict[str, Any], Any],
    AnyPageLayout,
]:
    """Create page layout structure hook."""

    def structure_page_layout(data: dict[str, Any], _) -> AnyPageLayout:
        """Structure page layout unions using page_type discriminator."""
        from .physical import (
            DataPageV1Layout,
            DataPageV2Layout,
            DictionaryPageLayout,
            IndexPageLayout,
        )

        page_type = PageType(data['page_type'])
        match page_type:
            case PageType.DICTIONARY_PAGE:
                return converter.structure(data, DictionaryPageLayout)
            case PageType.DATA_PAGE:
                return converter.structure(data, DataPageV1Layout)
            case PageType.DATA_PAGE_V2:
                return converter.structure(data, DataPageV2Layout)
            case PageType.INDEX_PAGE:
                return converter.structure(data, IndexPageLayout)
            case _:
                raise ValueError(f'Unknown page type: {page_type}')

    return structure_page_layout


def _create_schema_element_hook() -> Callable[
    [dict[str, Any], Any],
    logical.SchemaElement,
]:
    """Create schema element structure hook."""

    def structure_schema_element(data: dict[str, Any], _) -> logical.SchemaElement:
        """Structure schema element unions using element_type discriminator."""
        from . import logical

        element_type = SchemaElementType(data['element_type'])
        # Use separate converter to avoid recursion
        inner_converter = cattrs.Converter()

        match element_type:
            case SchemaElementType.ROOT:
                return inner_converter.structure(data, logical.SchemaRoot)
            case SchemaElementType.GROUP:
                return inner_converter.structure(data, logical.SchemaGroup)
            case SchemaElementType.COLUMN:
                return inner_converter.structure(data, logical.SchemaLeaf)
            case _:
                raise ValueError(f'Unknown element type: {element_type}')

    return structure_schema_element


def _create_column_chunk_unstructure_hook() -> Callable[
    [PhysicalColumnChunk],
    dict[str, Any],
]:
    """Create column chunk unstructure hook."""

    def unstructure_column_chunk(chunk: PhysicalColumnChunk) -> dict[str, Any]:
        """Unstructure PhysicalColumnChunk, excluding metadata field."""
        # Use separate converter to avoid recursion
        inner_converter = cattrs.Converter()
        result = inner_converter.unstructure(chunk)
        result.pop('metadata', None)
        return result

    return unstructure_column_chunk


def create_converter() -> cattrs.Converter:
    """Create a configured cattrs converter for ParquetFile serialization."""
    from . import logical
    from .physical import AnyPageLayout, PhysicalColumnChunk

    converter = cattrs.Converter()

    # Register hooks for union types
    converter.register_structure_hook(
        AnyPageLayout,
        _create_page_layout_hook(converter),
    )
    converter.register_structure_hook(
        logical.SchemaElement,
        _create_schema_element_hook(),
    )
    converter.register_unstructure_hook(
        PhysicalColumnChunk,
        _create_column_chunk_unstructure_hook(),
    )

    # Register hook for statistics union types
    def structure_statistics_value(obj, _) -> str | int | float | bool | None:
        """Handle union types in statistics fields."""
        return obj  # Values are already correct types from JSON

    converter.register_structure_hook(
        str | int | float | bool | None,
        structure_statistics_value,
    )

    return converter


def structure_single_data_page(
    converter: cattrs.Converter,
    page_data: dict,
) -> DataPageV1Layout | DataPageV2Layout:
    """Structure a single data page, returning the appropriate type."""
    from .physical import DataPageV1Layout, DataPageV2Layout

    page_type = PageType(page_data['page_type'])
    if page_type == PageType.DATA_PAGE:
        return converter.structure(page_data, DataPageV1Layout)
    if page_type == PageType.DATA_PAGE_V2:
        return converter.structure(page_data, DataPageV2Layout)
    raise ValueError(f'Expected data page, got {page_type}')
