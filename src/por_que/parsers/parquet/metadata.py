"""
Metadata orchestrator that composes all component parsers.

Teaching Points:
- The metadata orchestrator coordinates parsing of the entire FileMetadata structure
- It demonstrates composition over inheritance by using specialized parsers
- Tracing support allows learners to visualize the parsing process
- This design makes the complex metadata parsing more understandable and maintainable
"""

import logging

from por_que.exceptions import ThriftParsingError
from por_que.types import FileMetadata, SchemaElement

from ..thrift.enums import ThriftFieldType
from ..thrift.parser import ThriftCompactParser, ThriftStructParser
from .base import BaseParser
from .constants import DEFAULT_SCHEMA_NAME
from .enums import FileMetadataFieldId, KeyValueFieldId
from .row_group import RowGroupParser
from .schema import SchemaParser

logger = logging.getLogger(__name__)


class MetadataParser(BaseParser):
    """
    Orchestrates parsing of the complete FileMetadata structure.

    Teaching Points:
    - FileMetadata is the root of all Parquet file information
    - It coordinates multiple specialized parsers for different data structures
    - Tracing capability helps learners understand the parsing flow
    - Component-based design makes complex parsing more manageable
    """

    def __init__(self, metadata_bytes: bytes):
        """
        Initialize metadata parser from raw bytes.

        Args:
            metadata_bytes: Raw Thrift-encoded metadata from Parquet footer
        """
        parser = ThriftCompactParser(metadata_bytes)
        super().__init__(parser)
        self.schema: SchemaElement | None = None

    def parse(self) -> FileMetadata:  # noqa: C901
        """
        Parse the complete FileMetadata structure.

        Teaching Points:
        - FileMetadata contains schema, row groups, and file-level information
        - Schema must be parsed first to provide context for statistics
        - Row groups contain the actual data organization information
        - Key-value metadata provides extensibility for custom attributes

        Note:
            Parsing progress can be monitored by enabling debug logging for this module.

        Returns:
            Complete FileMetadata structure with all components parsed
        """
        logger.debug('Starting FileMetadata parsing...')

        struct_parser = ThriftStructParser(self.parser)
        metadata = FileMetadata(
            version=0,
            schema=SchemaElement(name=DEFAULT_SCHEMA_NAME),
            num_rows=0,
            row_groups=[],
        )

        while True:
            field_type, field_id = struct_parser.read_field_header()
            if field_type == ThriftFieldType.STOP:
                break

            logger.debug('Processing field %s of type %s', field_id, field_type)

            # Dispatch to specialized parsers for complex list types
            if field_type == ThriftFieldType.LIST:
                match field_id:
                    case FileMetadataFieldId.SCHEMA:
                        logger.debug('  Parsing schema elements...')
                        metadata.schema = self._parse_schema_field()
                        self.schema = metadata.schema  # Cache for statistics parsing
                    case FileMetadataFieldId.ROW_GROUPS:
                        logger.debug('  Parsing row groups...')
                        metadata.row_groups = self._parse_row_groups_field()
                    case FileMetadataFieldId.KEY_VALUE_METADATA:
                        logger.debug('  Parsing key-value metadata...')
                        metadata.key_value_metadata = (
                            self._parse_key_value_metadata_field()
                        )
                    case _:
                        logger.debug('  Skipping unknown list field %s', field_id)
                        struct_parser.skip_field(field_type)
                continue

            # Handle primitive types
            value = struct_parser.read_value(field_type)
            if value is None:
                continue

            match field_id:
                case FileMetadataFieldId.VERSION:
                    metadata.version = value
                    logger.debug('  File format version: %s', value)
                case FileMetadataFieldId.NUM_ROWS:
                    metadata.num_rows = value
                    logger.debug('  Total rows in file: %s', value)
                case FileMetadataFieldId.CREATED_BY:
                    metadata.created_by = value.decode('utf-8')
                    logger.debug('  Created by: %s', metadata.created_by)

        logger.debug('FileMetadata parsing complete!')

        return metadata

    def _parse_schema_field(self) -> SchemaElement:
        """
        Parse the schema field using SchemaParser.

        Teaching Points:
        - Schema parsing is delegated to a specialized parser
        - The schema provides the logical structure of the data
        - Schema must be parsed before statistics for type context
        """
        logger.debug('    Delegating to SchemaParser...')

        schema_parser = SchemaParser(self.parser)
        return schema_parser.parse_schema_field()

    def _parse_row_groups_field(self) -> list:
        """
        Parse the row_groups field using RowGroupParser.

        Teaching Points:
        - Row group parsing requires schema context for column metadata
        - Each row group is parsed by a specialized parser
        - Row groups contain the actual data organization
        """
        logger.debug('    Delegating to RowGroupParser...')

        if not self.schema:
            raise ValueError('Schema must be parsed before row groups')

        def parse_single_row_group():
            row_group_parser = RowGroupParser(self.parser, self.schema)
            return row_group_parser.read_row_group()

        row_groups = self.read_list(parse_single_row_group)

        logger.debug('    Parsed %s row groups', len(row_groups))

        return row_groups

    def _parse_key_value_metadata_field(self) -> dict[str, str]:
        """
        Parse the key_value_metadata field.

        Teaching Points:
        - Key-value metadata provides extensibility
        - Common uses include encoding information and custom attributes
        - This metadata is optional and application-specific
        """

        def parse_key_value():
            struct_parser = ThriftStructParser(self.parser)
            key = None
            value = None

            while True:
                field_type, field_id = struct_parser.read_field_header()
                if field_type == ThriftFieldType.STOP:
                    break

                field_value = struct_parser.read_value(field_type)
                if field_value is None:
                    continue

                if field_id == KeyValueFieldId.KEY:
                    key = field_value.decode('utf-8')
                elif field_id == KeyValueFieldId.VALUE:
                    value = field_value.decode('utf-8')

            if key is None or value is None:
                raise ThriftParsingError(
                    'Incomplete key/value pair: missing key or value field. '
                    'This may indicate corrupted metadata.',
                )

            return key, value

        kvs = self.read_list(parse_key_value)
        return {k: v for k, v in kvs if k}
