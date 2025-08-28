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
    ]


def _format_key_value_metadata_summary(metadata: FileMetadata) -> list[str]:
    if not metadata.key_value_metadata:
        return []

    lines = [f'\nKey-Value Metadata: {len(metadata.key_value_metadata)} keys']
    keys = list(metadata.key_value_metadata.keys())
    if keys:
        lines.append('Available keys:')
        lines.extend(f'  {key}' for key in keys)

    return lines


def _format_single_column_in_rowgroup(col, name: str, index: int) -> list[str]:
    if not col.meta_data:
        return [f'  {index:2}: {name} (no metadata)', '']

    meta = col.meta_data
    ratio = (
        meta.total_compressed_size / meta.total_uncompressed_size
        if meta.total_uncompressed_size > 0
        else 0
    )

    lines = [
        f'  {index:2}: {name}',
        f'      Codec: {meta.codec.name}, Values: {meta.num_values:,}',
        f'      Size: {meta.total_compressed_size:,}B compressed '
        f'/ {meta.total_uncompressed_size:,}B uncompressed ({ratio:.2f}x)',
    ]

    # Add statistics if available
    if meta.statistics:
        stats_parts = []
        if meta.statistics.null_count is not None:
            stats_parts.append(f'{meta.statistics.null_count:,} nulls')
        if meta.statistics.distinct_count is not None:
            stats_parts.append(f'{meta.statistics.distinct_count:,} distinct')
        if meta.statistics.min_value is not None:
            stats_parts.append(f'min: {meta.statistics.min_value}')
        if meta.statistics.max_value is not None:
            stats_parts.append(f'max: {meta.statistics.max_value}')

        if stats_parts:
            lines.append(f'      Stats: {", ".join(stats_parts)}')

    lines.append('')
    return lines


def _format_single_rowgroup(rg, group_index: int) -> list[str]:
    rg_stats = rg.get_stats()
    lines = [
        _header(f'Row Group {group_index}'),
        f'Rows: {rg_stats.num_rows:,}',
        f'Total byte size: {rg_stats.total_byte_size:,}',
        f'Columns: {rg_stats.num_columns}',
    ]

    if rg.columns:
        lines.append('\nColumns:')
        column_names = rg.column_names()
        for i, name in enumerate(column_names):
            col = rg.columns[i]
            column_lines = _format_single_column_in_rowgroup(col, name, i)
            lines.extend(column_lines)

    return lines


def _format_all_rowgroups_summary(metadata: FileMetadata) -> list[str]:
    lines = [_header('Row Groups')]

    for i, rg in enumerate(metadata.row_groups):
        rg_stats = rg.get_stats()
        lines.append(
            f'  {i:2}: {rg_stats.num_rows:,} rows, {rg_stats.num_columns} cols, '
            f'{rg_stats.total_byte_size:,} bytes '
            f'(avg {rg_stats.avg_column_size:,}/col)',
        )

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

    # Schema structure - use the recursive representation
    if metadata.schema:
        lines.extend(['\nSchema Structure:', '-' * 40])
        lines.append(str(metadata.schema))

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
    lines.append(str(metadata.schema))
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
        lines = _format_single_rowgroup(rg, group)
        return '\n'.join(lines)

    # All row groups summary
    lines = _format_all_rowgroups_summary(metadata)
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
