"""
Metadata orchestrator that composes all component parsers.

Teaching Points:
- The metadata orchestrator coordinates parsing of the entire FileMetadata structure
- It demonstrates composition over inheritance by using specialized parsers
- Tracing support allows learners to visualize the parsing process
- This design makes the complex metadata parsing more understandable and maintainable
"""

import logging
import warnings

from collections.abc import AsyncIterator
from typing import Any

from por_que.file_metadata import (
    FileMetadata,
    RowGroups,
    SchemaRoot,
)
from por_que.parsers.thrift.parser import ThriftCompactParser
from por_que.protocols import AsyncReadableSeekable

from .base import BaseParser
from .enums import FileMetadataFieldId
from .keyvalue import KeyValueParser
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

    def __init__(self, reader: AsyncReadableSeekable, start_offset: int):
        """
        Initialize metadata parser to read directly from file.

        Args:
            reader: File-like object positioned at metadata start
            start_offset: Absolute file offset where metadata begins
        """
        parser = ThriftCompactParser(reader, start_offset)
        super().__init__(parser)

    async def parse(self) -> FileMetadata:
        """
        Parse the complete FileMetadata structure using the new generic parser.

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

        props: dict[str, Any] = {}

        async for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case FileMetadataFieldId.VERSION:
                    props['version'] = value
                case FileMetadataFieldId.NUM_ROWS:
                    # we calculate this, so don't need it
                    pass
                case FileMetadataFieldId.CREATED_BY:
                    props['created_by'] = (
                        value.decode('utf-8') if isinstance(value, bytes) else value
                    )
                case FileMetadataFieldId.SCHEMA:
                    props['schema_root'] = await SchemaParser(
                        self.parser,
                    ).parse_schema_field(value)
                case FileMetadataFieldId.ROW_GROUPS:
                    props['row_groups'] = await self._parse_row_groups_field(
                        value,
                        props['schema_root'],
                    )
                case FileMetadataFieldId.KEY_VALUE_METADATA:
                    props['key_value_metadata'] = [
                        await KeyValueParser(self.parser).parse() async for _ in value
                    ]
                case FileMetadataFieldId.COLUMN_ORDERS:
                    # column orders has some complexity
                    # but little meaning at current
                    await self.maybe_skip_field(field_type)
                case (
                    FileMetadataFieldId.ENCRYPTION_ALGORITHM
                    | FileMetadataFieldId.FOOTER_SIGNING_KEY_METADATA
                ):
                    # encryption is not supported
                    await self.maybe_skip_field(field_type)
                case _:
                    warnings.warn(
                        f'Skipping unknown metadata field ID {field_id}',
                        stacklevel=1,
                    )
                    await self.maybe_skip_field(field_type)

        logger.debug('FileMetadata parsing complete!')
        return FileMetadata(**props)

    async def _parse_row_groups_field(
        self,
        list_iter: AsyncIterator,
        schema_root: SchemaRoot | None,
    ) -> RowGroups:
        """
        Parse the row_groups field using RowGroupParser.

        Teaching Points:
        - Row group parsing requires schema context for column metadata
        - Each row group is parsed by a specialized parser
        - Row groups contain the actual data organization
        """
        logger.debug('    Delegating to RowGroupParser...')

        if not schema_root:
            raise ValueError('Schema must be parsed before row groups')

        row_groups: RowGroups = []
        row_group_parser = RowGroupParser(self.parser, schema_root)
        async for _ in list_iter:
            row_groups.append(await row_group_parser.read_row_group())

        logger.debug('    Parsed %s row groups', len(row_groups))

        return row_groups
