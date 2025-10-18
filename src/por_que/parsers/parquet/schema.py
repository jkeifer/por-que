"""
Schema parsing components for understanding Parquet's type system.

Teaching Points:
- Parquet schemas are hierarchical trees that mirror nested data structures
- Each schema element describes a field's type, repetition, and metadata
- The schema tree enables columnar storage of complex nested data
- Field relationships are encoded through parent-child structure and repetition levels
"""

import logging
import struct
import warnings

from collections.abc import AsyncIterator
from typing import Any, assert_never, cast

from por_que.enums import (
    ConvertedType,
    ListSemantics,
    LogicalType,
    Repetition,
    TimeUnit,
    Type,
)
from por_que.exceptions import ThriftParsingError
from por_que.file_metadata import (
    BsonTypeInfo,
    DateTypeInfo,
    DecimalTypeInfo,
    EnumTypeInfo,
    Float16TypeInfo,
    GeographyTypeInfo,
    GeometryTypeInfo,
    IntTypeInfo,
    JsonTypeInfo,
    ListTypeInfo,
    LogicalTypeInfoUnion,
    MapTypeInfo,
    SchemaElement,
    SchemaGroup,
    SchemaLeaf,
    SchemaRoot,
    StringTypeInfo,
    TimestampTypeInfo,
    TimeTypeInfo,
    UnknownTypeInfo,
    UuidTypeInfo,
    VariantTypeInfo,
)

from .base import BaseParser
from .enums import SchemaElementFieldId

logger = logging.getLogger(__name__)


class SchemaParser(BaseParser):
    """
    Parses Parquet schema elements and builds the schema tree.

    Teaching Points:
    - Schema elements define the structure and types of data in Parquet files
    - The root element represents the entire record structure
    - Child elements represent nested fields, arrays, and maps
    - Repetition types (REQUIRED, OPTIONAL, REPEATED) control nullability and arrays
    """

    async def read_schema_element(self) -> SchemaRoot | SchemaGroup | SchemaLeaf:  # noqa: C901
        """
        Read a single SchemaElement struct from the Thrift stream.

        Teaching Points:
        - Each schema element encodes field metadata: name, type, repetition
        - Physical types (INT32, BYTE_ARRAY) describe storage format
        - Logical types (UTF8, TIMESTAMP) describe semantic meaning
        - num_children indicates how many child elements follow this one

        Returns:
            SchemaElement with parsed metadata and byte range information
        """
        start_offset = self.parser.pos
        props: dict[str, Any] = {
            'start_offset': start_offset,
        }

        async for _field_id, field_type, value in self.parse_struct_fields():
            match _field_id:
                case SchemaElementFieldId.TYPE:
                    props['type'] = Type(value)
                case SchemaElementFieldId.TYPE_LENGTH:
                    props['type_length'] = value
                case SchemaElementFieldId.REPETITION_TYPE:
                    props['repetition'] = Repetition(value)
                case SchemaElementFieldId.NAME:
                    props['name'] = value.decode('utf-8')
                    props['full_path'] = props['name']
                case SchemaElementFieldId.NUM_CHILDREN:
                    props['num_children'] = value
                case SchemaElementFieldId.CONVERTED_TYPE:
                    props['converted_type'] = ConvertedType(value)
                case SchemaElementFieldId.SCALE:
                    props['scale'] = value
                case SchemaElementFieldId.PRECISION:
                    props['precision'] = value
                case SchemaElementFieldId.FIELD_ID:
                    props['field_id'] = value
                case SchemaElementFieldId.LOGICAL_TYPE:
                    props['logical_type'] = await self._parse_logical_type()
                case _:
                    warnings.warn(
                        f'Skipping unknown schema field ID {_field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        end_offset = self.parser.pos
        props['byte_length'] = end_offset - start_offset

        return SchemaElement.new(**props)

    def read_schema_tree(
        self,
        elements_iter,
        current_path: str = '',
        current_def_level: int = 0,
        current_rep_level: int = 0,
        current_list_context: ListSemantics | None = None,
    ) -> SchemaRoot | SchemaGroup | SchemaLeaf:
        """
        Recursively build nested schema tree from flat list of elements.

        Teaching Points:
        - Parquet stores schema as a depth-first traversal of the tree
        - Each parent element specifies how many children follow it
        - This enables efficient reconstruction of the full tree structure
        - The tree structure mirrors how nested data is stored in columns
        - Definition and repetition levels are calculated during tree building

        Args:
            elements_iter: Iterator over flat list of schema elements
            current_def_level: Current definition level in the tree
            current_rep_level: Current repetition level in the tree

        Returns:
            SchemaRoot with all children attached

        Raises:
            ThriftParsingError: If schema structure is malformed
        """
        try:
            element = next(elements_iter)
        except StopIteration:
            raise ThriftParsingError(
                'Unexpected end of schema elements. This suggests a malformed '
                'schema where a parent element claims more children than exist.',
            ) from None

        match element:
            case SchemaRoot() | SchemaGroup():
                return self._read_schema_group(
                    element,
                    elements_iter,
                    current_path,
                    current_def_level,
                    current_rep_level,
                    current_list_context,
                )
            case SchemaLeaf():
                return self._read_schema_leaf(
                    element,
                    current_path,
                    current_def_level,
                    current_rep_level,
                    current_list_context,
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _read_schema_group(
        self,
        element: SchemaGroup | SchemaRoot,
        elements_iter,
        current_path: str,
        current_def_level: int,
        current_rep_level: int,
        current_list_context: ListSemantics | None,
    ) -> SchemaGroup | SchemaRoot:
        if isinstance(element, SchemaGroup):
            # update current schema path
            current_path += '.' + element.name if current_path else element.name

        logger.debug(
            'Building schema tree for %s with %d children',
            element.name,
            current_path,
            element.num_children,
        )

        # Calculate levels based on this group's repetition
        # Definition level increases for non-REQUIRED fields
        if element.repetition != Repetition.REQUIRED:
            current_def_level += 1
        # Repetition level increases for REPEATED fields
        if element.repetition == Repetition.REPEATED:
            current_rep_level += 1

        # Update list context based on this group's logical type
        logical_type_info = element.get_logical_type()
        if logical_type_info and logical_type_info.logical_type == LogicalType.LIST:
            # This group establishes LIST semantics for descendants
            current_list_context = ListSemantics.MODERN_LIST
        elif current_list_context is None and element.repetition == Repetition.REPEATED:
            # This is a legacy repeated group with no established list context
            current_list_context = ListSemantics.LEGACY_REPEATED

        element = element.model_copy(
            update={
                'full_path': current_path,
                'definition_level': current_def_level,
                'repetition_level': current_rep_level,
            },
        )

        for i in range(element.num_children):
            child = self.read_schema_tree(
                elements_iter,
                current_path,
                current_def_level,
                current_rep_level,
                current_list_context,
            )

            if isinstance(child, SchemaRoot):
                raise ThriftParsingError('Schema can have only one root')

            element.add_element(child)
            logger.debug(
                '  Added child %d/%d: %s',
                i + 1,
                element.num_children,
                child.name,
            )

        return element

    def _read_schema_leaf(
        self,
        element: SchemaLeaf,
        current_path: str,
        current_def_level: int,
        current_rep_level: int,
        current_list_context: ListSemantics | None,
    ) -> SchemaLeaf:
        # Calculate final levels for this leaf
        final_def_level = current_def_level
        final_rep_level = current_rep_level

        # Add this leaf's contribution to levels
        if element.repetition != Repetition.REQUIRED:
            final_def_level += 1
        if element.repetition == Repetition.REPEATED:
            final_rep_level += 1

        # Update the leaf with calculated levels and list semantics
        # Since the model is frozen, we need to use model_copy
        element = element.model_copy(
            update={
                'full_path': (
                    current_path + '.' + element.name if current_path else element.name
                ),
                'definition_level': final_def_level,
                'repetition_level': final_rep_level,
                'list_semantics': current_list_context,
            },
        )

        logger.debug(
            '  Leaf %s: def_level=%d, rep_level=%d, list_semantics=%s',
            element.name,
            final_def_level,
            final_rep_level,
            current_list_context,
        )

        return element

    async def parse_schema_field(
        self,
        list_iter: AsyncIterator[SchemaElement],
    ) -> SchemaRoot:
        """
        Parse the schema field from file metadata.

        Teaching Points:
        - The schema field contains a flat list of all schema elements
        - Elements are ordered in depth-first traversal of the schema tree
        - The first element is always the root (representing the full record)
        - Child elements are nested based on their parent's num_children value

        Returns:
            Root SchemaElement with complete tree structure
        """
        # Read flat list of schema elements
        schema_elements = [await self.read_schema_element() async for _ in list_iter]

        logger.debug('Read %d schema elements, building tree', len(schema_elements))

        schema_root = schema_elements[0]
        if not isinstance(schema_root, SchemaRoot):
            raise ThriftParsingError(
                f'Schema must start with SchemaRoot element, got {schema_root}',
            )

        # Convert flat list to tree structure
        elements_iter = iter(schema_elements)
        return cast(SchemaRoot, self.read_schema_tree(elements_iter))

    async def _parse_logical_type(self) -> LogicalTypeInfoUnion | None:  # noqa: C901
        """
        Parse a LogicalType union from the Thrift stream.

        The LogicalType is a union with different types for different logical types.
        Each union variant has its own field ID and structure.
        """
        logical_type: LogicalTypeInfoUnion | None = None

        async for field_id, field_type, _ in self.parse_struct_fields():
            # types with struct values
            match field_id:
                case LogicalType.INTEGER:
                    logical_type = await self._parse_int_type()
                case LogicalType.DECIMAL:
                    logical_type = await self._parse_decimal_type()
                case LogicalType.TIME:
                    logical_type = await self._parse_time_type()
                case LogicalType.TIMESTAMP:
                    logical_type = await self._parse_timestamp_type()

            if logical_type:
                continue

            # types with empty structs
            # need to skip to the STOP
            await self.maybe_skip_field(field_type)

            match field_id:
                case LogicalType.STRING:
                    logical_type = StringTypeInfo()
                case LogicalType.DATE:
                    logical_type = DateTypeInfo()
                case LogicalType.ENUM:
                    logical_type = EnumTypeInfo()
                case LogicalType.JSON:
                    logical_type = JsonTypeInfo()
                case LogicalType.BSON:
                    logical_type = BsonTypeInfo()
                case LogicalType.UUID:
                    logical_type = UuidTypeInfo()
                case LogicalType.FLOAT16:
                    logical_type = Float16TypeInfo()
                case LogicalType.MAP:
                    logical_type = MapTypeInfo()
                case LogicalType.LIST:
                    logical_type = ListTypeInfo()
                case LogicalType.VARIANT:
                    logical_type = VariantTypeInfo()
                case LogicalType.GEOMETRY:
                    logical_type = GeometryTypeInfo()
                case LogicalType.GEOGRAPHY:
                    logical_type = GeographyTypeInfo()
                case LogicalType.UNKNOWN:
                    logical_type = UnknownTypeInfo()
                case _:
                    warnings.warn(
                        f'Unknown logical type {field_id}',
                        stacklevel=1,
                    )

        return logical_type

    async def _parse_int_type(self) -> IntTypeInfo:
        """Parse an IntType struct."""
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case 1:
                    props['bit_width'] = struct.unpack('<B', value)[0]
                case 2:
                    props['is_signed'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown int type field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return IntTypeInfo(**props)

    async def _parse_decimal_type(self) -> DecimalTypeInfo:
        """Parse a DecimalType struct."""
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case 1:
                    props['scale'] = value
                case 2:
                    props['precision'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown decimal type field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return DecimalTypeInfo(**props)

    async def _parse_time_type(self) -> TimeTypeInfo:
        """Parse a TimeType struct."""
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case 1:
                    props['is_adjusted_to_utc'] = value
                case 2:
                    props['unit'] = TimeUnit(value)
                case _:
                    warnings.warn(
                        f'Skipping unknown time type field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return TimeTypeInfo(**props)

    async def _parse_timestamp_type(self) -> TimestampTypeInfo:
        """Parse a TimestampType struct."""
        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case 1:
                    props['is_adjusted_to_utc'] = value
                case 2:
                    props['unit'] = TimeUnit(value)
                case _:
                    warnings.warn(
                        f'Skipping unknown timestamp type field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        return TimestampTypeInfo(**props)
