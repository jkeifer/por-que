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

from collections.abc import Callable, Iterator, Sequence
from typing import Any

from por_que.enums import ProgressPhase
from por_que.file_metadata import RowGroups
from por_que.parsers.thrift.parser import ThriftCompactParser
from por_que.schema import SchemaRoot

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

    def __init__(self, data: bytes | memoryview, base_offset: int) -> None:
        """
        Initialize metadata parser over the in-memory metadata span.

        Args:
            data: The bytes of the metadata span, already fetched in one read.
            base_offset: Absolute file offset where the metadata span begins.
        """
        parser = ThriftCompactParser(data, base_offset)
        super().__init__(parser)

    def parse(
        self,
        columns: Sequence[str] | None = None,
        *,
        progress: Callable[[ProgressPhase, int, int], None] | None = None,
    ) -> dict[str, Any]:
        """
        Parse the complete FileMetadata structure using the new generic parser.

        Teaching Points:
        - FileMetadata contains schema, row groups, and file-level information
        - Schema must be parsed first to provide context for statistics
        - Row groups contain the actual data organization information
        - Key-value metadata provides extensibility for custom attributes

        Args:
            columns: Optional projection of dotted ``path_in_schema`` strings.
                When given, each row group's ``column_chunks`` retains only the
                selected columns; the schema and key-value metadata are still
                parsed in full.
            progress: Optional callback fired as row groups are parsed,
                called as ``progress(ProgressPhase.METADATA_PARSE, done,
                total)``. It fires once with ``(phase, 0, total)`` before
                the first row group, then once after each row group with
                the number parsed so far.

        Note:
            Parsing progress can be monitored by enabling debug logging for this module.

        Returns:
            Dict with arguments for FileMetadata constructure with all
            components parsed
        """
        logger.debug('Starting FileMetadata parsing...')

        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
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
                    props['schema_root'] = SchemaParser(
                        self.parser,
                    ).parse_schema_field(value)
                case FileMetadataFieldId.ROW_GROUPS:
                    props['row_groups'] = self._parse_row_groups_field(
                        value,
                        props['schema_root'],
                        columns,
                        progress,
                    )
                case FileMetadataFieldId.KEY_VALUE_METADATA:
                    props['key_value_metadata'] = [
                        KeyValueParser(self.parser).parse() for _ in value
                    ]
                case FileMetadataFieldId.COLUMN_ORDERS:
                    # column orders has some complexity
                    # but little meaning at current
                    self.maybe_skip_field(field_type)
                case (
                    FileMetadataFieldId.ENCRYPTION_ALGORITHM
                    | FileMetadataFieldId.FOOTER_SIGNING_KEY_METADATA
                ):
                    # encryption is not supported
                    self.maybe_skip_field(field_type)
                case _:
                    warnings.warn(
                        f'Skipping unknown metadata field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        logger.debug('FileMetadata parsing complete!')
        return props

    def _parse_row_groups_field(
        self,
        list_iter: Iterator,
        schema_root: SchemaRoot | None,
        columns: Sequence[str] | None = None,
        progress: Callable[[ProgressPhase, int, int], None] | None = None,
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
        row_group_parser = RowGroupParser(self.parser, schema_root, columns)
        # Draining the iterator reads only the thrift list header (which
        # carries the element count), so the total is known before any row
        # group is parsed.
        total = sum(1 for _ in list_iter)
        if progress is not None:
            progress(ProgressPhase.METADATA_PARSE, 0, total)
        for done in range(1, total + 1):
            row_groups.append(row_group_parser.read_row_group())
            if progress is not None:
                progress(ProgressPhase.METADATA_PARSE, done, total)

        logger.debug('    Parsed %s row groups', len(row_groups))

        return row_groups
