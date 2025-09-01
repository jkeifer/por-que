from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import cattrs

from .enums import LogicalType, PageType, SchemaElementType

if TYPE_CHECKING:
    from . import logical
    from .pages import AnyDataPage, AnyPage
    from .physical import PhysicalColumnChunk


def _create_page_hook(
    converter: cattrs.Converter,
) -> Callable[
    [dict[str, Any], Any],
    AnyPage,
]:
    """Create page layout structure hook."""

    def structure_page(data: dict[str, Any], _) -> AnyPage:
        """Structure page unions using page_type discriminator."""
        from .pages import (
            DataPageV1,
            DataPageV2,
            DictionaryPage,
            IndexPage,
        )

        page_type = PageType(data['page_type'])
        match page_type:
            case PageType.DICTIONARY_PAGE:
                return converter.structure(data, DictionaryPage)
            case PageType.DATA_PAGE:
                return converter.structure(data, DataPageV1)
            case PageType.DATA_PAGE_V2:
                return converter.structure(data, DataPageV2)
            case PageType.INDEX_PAGE:
                return converter.structure(data, IndexPage)
            case _:
                raise ValueError(f'Unknown page type: {page_type}')

    return structure_page


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


def _create_logical_type_hook() -> Callable[  # noqa: C901
    [dict[str, Any], Any],
    logical.LogicalTypeInfo,
]:
    """Create logical type structure hook."""

    def structure_logical_type(data: dict[str, Any], _) -> logical.LogicalTypeInfo:  # noqa: C901
        """Structure logical type unions using logical_type discriminator."""
        from . import logical

        logical_type = LogicalType(data['logical_type'])
        # Use separate converter to avoid recursion
        inner_converter = cattrs.Converter()

        match logical_type:
            case LogicalType.STRING:
                return inner_converter.structure(data, logical.StringTypeInfo)
            case LogicalType.INTEGER:
                return inner_converter.structure(data, logical.IntTypeInfo)
            case LogicalType.DECIMAL:
                return inner_converter.structure(data, logical.DecimalTypeInfo)
            case LogicalType.TIME:
                return inner_converter.structure(data, logical.TimeTypeInfo)
            case LogicalType.TIMESTAMP:
                return inner_converter.structure(data, logical.TimestampTypeInfo)
            case LogicalType.DATE:
                return inner_converter.structure(data, logical.DateTypeInfo)
            case LogicalType.ENUM:
                return inner_converter.structure(data, logical.EnumTypeInfo)
            case LogicalType.JSON:
                return inner_converter.structure(data, logical.JsonTypeInfo)
            case LogicalType.BSON:
                return inner_converter.structure(data, logical.BsonTypeInfo)
            case LogicalType.UUID:
                return inner_converter.structure(data, logical.UuidTypeInfo)
            case LogicalType.FLOAT16:
                return inner_converter.structure(data, logical.Float16TypeInfo)
            case LogicalType.MAP:
                return inner_converter.structure(data, logical.MapTypeInfo)
            case LogicalType.LIST:
                return inner_converter.structure(data, logical.ListTypeInfo)
            case LogicalType.VARIANT:
                return inner_converter.structure(data, logical.VariantTypeInfo)
            case LogicalType.GEOMETRY:
                return inner_converter.structure(data, logical.GeometryTypeInfo)
            case LogicalType.GEOGRAPHY:
                return inner_converter.structure(data, logical.GeographyTypeInfo)
            case LogicalType.UNKNOWN:
                return inner_converter.structure(data, logical.UnknownTypeInfo)
            case _:
                raise ValueError(f'Unknown logical type: {logical_type}')

    return structure_logical_type


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
    from .pages import AnyPage
    from .physical import PhysicalColumnChunk

    converter = cattrs.Converter()

    # Register hooks for union types
    converter.register_structure_hook(
        AnyPage,
        _create_page_hook(converter),
    )
    converter.register_structure_hook(
        logical.SchemaElement,
        _create_schema_element_hook(),
    )
    converter.register_structure_hook(
        logical.LogicalTypeInfo,
        _create_logical_type_hook(),
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
) -> AnyDataPage:
    """Structure a single data page, returning the appropriate type."""
    from .pages import DataPageV1, DataPageV2

    page_type = PageType(page_data['page_type'])
    if page_type == PageType.DATA_PAGE:
        return converter.structure(page_data, DataPageV1)
    if page_type == PageType.DATA_PAGE_V2:
        return converter.structure(page_data, DataPageV2)
    raise ValueError(f'Expected data page, got {page_type}')
