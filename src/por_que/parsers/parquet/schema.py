"""
Schema parsing components for understanding Parquet's type system.

Teaching Points:
- Parquet schemas are hierarchical trees that mirror nested data structures
- Each schema element describes a field's type, repetition, and metadata
- The schema tree enables columnar storage of complex nested data
- Field relationships are encoded through parent-child structure and repetition levels
"""

import logging

from por_que.enums import ConvertedType, Repetition, Type
from por_que.exceptions import ThriftParsingError
from por_que.types import SchemaElement

from ..thrift.enums import ThriftFieldType
from ..thrift.parser import ThriftStructParser
from .base import BaseParser
from .constants import DEFAULT_SCHEMA_NAME
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

    def read_schema_element(self) -> SchemaElement:
        """
        Read a single SchemaElement struct from the Thrift stream.

        Teaching Points:
        - Each schema element encodes field metadata: name, type, repetition
        - Physical types (INT32, BYTE_ARRAY) describe storage format
        - Logical types (UTF8, TIMESTAMP) describe semantic meaning
        - num_children indicates how many child elements follow this one

        Returns:
            SchemaElement with parsed metadata
        """
        struct_parser = ThriftStructParser(self.parser)
        element = SchemaElement(name=DEFAULT_SCHEMA_NAME)
        logger.debug('Reading schema element')

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            # `read_value` returns the primitive value, or None if it's a
            # complex type or should be skipped.
            value = struct_parser.read_value(field_type)
            if value is None:
                # This indicates a complex type that the caller must handle,
                # or a type that was skipped.
                continue

            match field_id:
                case SchemaElementFieldId.TYPE:
                    element.type = Type(value)
                case SchemaElementFieldId.TYPE_LENGTH:
                    element.type_length = value
                case SchemaElementFieldId.REPETITION_TYPE:
                    element.repetition = Repetition(value)
                case SchemaElementFieldId.NAME:
                    element.name = value.decode('utf-8')
                case SchemaElementFieldId.NUM_CHILDREN:
                    element.num_children = value
                case SchemaElementFieldId.CONVERTED_TYPE:
                    element.converted_type = ConvertedType(value)
                case _:
                    # This case is not strictly necessary since `read_value`
                    # already skipped unknown fields, but it's good practice.
                    pass

        logger.debug(
            'Read schema element: %s (type=%s, children=%d)',
            element.name,
            element.type,
            element.num_children,
        )
        return element

    def read_schema_tree(self, elements_iter) -> SchemaElement:
        """
        Recursively build nested schema tree from flat list of elements.

        Teaching Points:
        - Parquet stores schema as a depth-first traversal of the tree
        - Each parent element specifies how many children follow it
        - This enables efficient reconstruction of the full tree structure
        - The tree structure mirrors how nested data is stored in columns

        Args:
            elements_iter: Iterator over flat list of schema elements

        Returns:
            Root SchemaElement with all children attached

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

        logger.debug(
            'Building schema tree for %s with %d children',
            element.name,
            element.num_children,
        )

        # If this element has children, recursively read them
        # This implements depth-first tree reconstruction
        for i in range(element.num_children):
            child = self.read_schema_tree(elements_iter)
            element.children[child.name] = child
            logger.debug(
                '  Added child %d/%d: %s',
                i + 1,
                element.num_children,
                child.name,
            )

        return element

    def parse_schema_field(self) -> SchemaElement:
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
        schema_elements = self.read_list(self.read_schema_element)
        logger.debug('Read %d schema elements, building tree', len(schema_elements))

        # Convert flat list to tree structure
        elements_iter = iter(schema_elements)
        return self.read_schema_tree(elements_iter)
