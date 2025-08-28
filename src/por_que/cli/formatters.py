from por_que.types import FileMetadata

from .exceptions import InvalidValueError


def _header(title: str) -> str:
    return f'{title}\n{"=" * 60}'


def _format_basic_info(stats) -> list[str]:
    return [
        f'Version: {stats.version}',
        f'Created by: {stats.created_by or "unknown"}',
        f'Total rows: {stats.total_rows:,}',
        f'Row groups: {stats.num_row_groups}',
        f'Schema elements: {stats.num_schema_elements}',
    ]


def _format_schema_element(element, index: int) -> str:
    if element.is_group():
        return f'  {index:2}: {element.name} (GROUP, {element.num_children} children)'

    rep = f' {element.repetition.name}' if element.repetition else ''
    type_name = element.type.name if element.type else 'unknown'
    length_info = f' (length: {element.type_length})' if element.type_length else ''
    converted_info = (
        f' [converted: {element.converted_type}]' if element.converted_type else ''
    )

    return f'  {index:2}: {element.name}: {type_name}{length_info}{rep}{converted_info}'


def _format_key_value_metadata_summary(metadata: FileMetadata) -> list[str]:
    """Show just the available metadata keys for summary."""
    if not metadata.key_value_metadata:
        return []

    lines = [f'\nKey-Value Metadata: {len(metadata.key_value_metadata)} keys']
    keys = list(metadata.key_value_metadata.keys())
    if keys:
        lines.append('Available keys:')
        lines.extend(f'  {key}' for key in keys)

    return lines


def format_summary(metadata: FileMetadata) -> str:
    stats = metadata.get_stats()
    lines = [
        _header('Parquet File Summary'),
        *_format_basic_info(stats),
    ]

    # Add compression ratio if we have compression data
    if stats.compression.total_uncompressed > 0:
        lines.append(f'Compression ratio: {stats.compression.ratio:.3f}')

    # Schema structure - all elements, one line each
    if metadata.schema:
        lines.extend(['\nSchema Structure:', '-' * 40])
        for i, element in enumerate(metadata.schema):
            lines.append(_format_schema_element(element, i))

    # Row groups - all groups, one line each
    if metadata.row_groups:
        lines.extend(['\nRow Groups:', '-' * 40])
        for i, rg in enumerate(metadata.row_groups):
            rg_stats = rg.get_stats()
            lines.append(
                f'  {i:2}: {rg_stats.num_rows:,} rows, {rg_stats.num_columns} cols, '
                f'{rg_stats.total_byte_size:,} bytes',
            )

    # Key-value metadata
    lines.extend(_format_key_value_metadata_summary(metadata))

    return '\n'.join(lines)


def format_schema(metadata: FileMetadata) -> str:
    lines = [_header('Schema Structure')]

    for i, element in enumerate(metadata.schema):
        lines.append(_format_schema_element(element, i))

    return '\n'.join(lines)


def format_stats(metadata: FileMetadata) -> str:
    stats = metadata.get_stats()
    lines = [
        _header('File Statistics'),
        *_format_basic_info(stats),
    ]

    if metadata.row_groups:
        lines.extend(
            [
                f'Total columns: {stats.total_columns}',
                f'Rows per group: {stats.min_rows_per_group:,} - '
                f'{stats.max_rows_per_group:,}',
            ],
        )

        if stats.compression.total_uncompressed > 0:
            lines.extend(
                [
                    'Uncompressed size: '
                    f'{stats.compression.total_uncompressed:,} bytes '
                    f'({stats.compression.uncompressed_mb:.1f} MB)',
                    f'Compressed size: {stats.compression.total_compressed:,} bytes '
                    f'({stats.compression.compressed_mb:.1f} MB)',
                    f'Compression ratio: {stats.compression.ratio:.3f} '
                    f'({stats.compression.space_saved_percent:.1f}% space saved)',
                ],
            )

    return '\n'.join(lines)


def format_rowgroups(metadata: FileMetadata, group: int | None = None) -> str:
    if group is not None:
        # Single row group details
        if group < 0 or group >= len(metadata.row_groups):
            raise InvalidValueError(
                f'Row group index {group} does not exist. '
                f'File has {len(metadata.row_groups)} row groups.',
            )

        rg = metadata.row_groups[group]
        rg_stats = rg.get_stats()
        lines = [
            _header(f'Row Group {group}'),
            f'Rows: {rg_stats.num_rows:,}',
            f'Total byte size: {rg_stats.total_byte_size:,}',
            f'Columns: {rg_stats.num_columns}',
        ]

        if rg.columns:
            lines.append('\nColumns:')
            column_names = rg.column_names()
            for i, name in enumerate(column_names):
                col = rg.columns[i]
                if col.meta_data:
                    codec = col.meta_data.codec.name
                    compressed = col.meta_data.total_compressed_size
                    uncompressed = col.meta_data.total_uncompressed_size
                    values = col.meta_data.num_values
                    ratio = compressed / uncompressed if uncompressed > 0 else 0
                    lines.append(
                        f'  {i:2}: {name} ({codec}, {values:,} values, '
                        f'{compressed}B/{uncompressed}B C/UC ({ratio:.2f}x))',
                    )
                else:
                    lines.append(f'  {i:2}: {name} (no metadata)')

        return '\n'.join(lines)

    # All row groups summary
    lines = [_header('Row Groups')]

    for i, rg in enumerate(metadata.row_groups):
        rg_stats = rg.get_stats()
        lines.append(
            f'  {i:2}: {rg_stats.num_rows:,} rows, {rg_stats.num_columns} cols, '
            f'{rg_stats.total_byte_size:,} bytes '
            f'(avg {rg_stats.avg_column_size:,}/col)',
        )

    return '\n'.join(lines)


def format_columns(metadata: FileMetadata) -> str:
    lines = [_header('Column Information')]

    if not metadata.row_groups:
        return '\n'.join([*lines, 'No row groups found.'])

    # Get column info from first row group
    # (all row groups should have same columns)
    rg = metadata.row_groups[0]

    for i, col in enumerate(rg.columns):
        if col.meta_data:
            meta = col.meta_data
            encodings_str = ', '.join([e.name for e in meta.encodings])
            compression_ratio = (
                meta.total_compressed_size / meta.total_uncompressed_size
                if meta.total_uncompressed_size > 0
                else 0
            )

            lines.extend(
                [
                    f'  {i:2}: {meta.path_in_schema}',
                    f'      Type: {meta.type.name}',
                    f'      Codec: {meta.codec.name}',
                    f'      Encodings: {encodings_str}',
                    f'      Values: {meta.num_values:,}',
                    f'      Size: {meta.total_compressed_size:,} '
                    f'bytes (ratio: {compression_ratio:.3f})',
                ],
            )

            if len(metadata.row_groups) > 1:
                lines.append(f'      (from row group 0 of {len(metadata.row_groups)})')
            lines.append('')

    return '\n'.join(lines)


def format_metadata_keys(metadata: FileMetadata) -> str:
    return '\n'.join(metadata.key_value_metadata)
