"""Human-readable rendering of parquet structures with rich.

These helpers turn library models into rich renderables (trees, tables). They
read only public model attributes, keeping the CLI a clean consumer of the
library.
"""

from __future__ import annotations

from typing import Any

from rich.table import Table
from rich.tree import Tree

from por_que import FileMetadata, ParquetFile


def human_bytes(num: int) -> str:
    size = float(num)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size < 1024 or unit == 'TB':
            if unit == 'B':
                return f'{int(size)} {unit}'
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} TB'


def _logical_suffix(element: Any) -> str:
    logical = element.get_logical_type()
    if logical is None:
        return ''
    return f' [yellow]<{logical.logical_type.name}>[/yellow]'


def _add_schema_children(node: Tree, group: Any) -> None:
    for child in group.children.values():
        grandchildren = getattr(child, 'children', None)
        if grandchildren is not None:
            label = (
                f'[bold]{child.name}[/bold] [dim]GROUP[/dim] '
                f'[dim]({child.repetition.name})[/dim]{_logical_suffix(child)}'
            )
            branch = node.add(label)
            _add_schema_children(branch, child)
        else:
            label = (
                f'[cyan]{child.name}[/cyan]: [green]{child.type.name}[/green] '
                f'[dim]({child.repetition.name})[/dim]{_logical_suffix(child)}'
            )
            node.add(label)


def schema_tree(metadata: FileMetadata) -> Tree:
    root = metadata.schema_root
    tree = Tree(f'[bold]{root.name}[/bold] [dim](root)[/dim]')
    _add_schema_children(tree, root)
    return tree


def meta_summary(source: str, metadata: FileMetadata) -> Table:
    stats = metadata.compression_stats
    codecs = sorted(
        {
            chunk.codec.name
            for rg in metadata.row_groups
            for chunk in rg.column_chunks.values()
        },
    )

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column('field', style='bold cyan')
    table.add_column('value')

    table.add_row('source', source)
    table.add_row('format version', str(metadata.version))
    table.add_row('created by', metadata.created_by or '(unknown)')
    table.add_row('rows', f'{metadata.row_count:,}')
    table.add_row('row groups', str(metadata.row_group_count))
    table.add_row('columns', str(metadata.column_count))
    table.add_row('compression', ', '.join(codecs) or '(none)')
    table.add_row(
        'compressed',
        f'{human_bytes(stats.total_compressed)} '
        f'({stats.space_saved_percent:.1f}% saved vs '
        f'{human_bytes(stats.total_uncompressed)})',
    )

    keys = [kv.key for kv in metadata.key_value_metadata]
    table.add_row(
        'key-value metadata',
        ', '.join(keys) if keys else '(none)',
    )
    return table


def _stat_repr(value: Any) -> str:
    if value is None:
        return '-'
    text = str(value)
    if len(text) > 40:
        return text[:37] + '...'
    return text


def row_groups_table(metadata: FileMetadata, column: str | None) -> Table:
    table = Table()
    table.add_column('#', justify='right')
    table.add_column('rows', justify='right')
    table.add_column('compressed', justify='right')
    table.add_column('uncompressed', justify='right')
    table.add_column('saved', justify='right')
    if column is not None:
        table.add_column('min')
        table.add_column('max')
        table.add_column('nulls', justify='right')

    for index, rg in enumerate(metadata.row_groups):
        stats = rg.compression_stats
        row = [
            str(index),
            f'{rg.row_count:,}',
            human_bytes(stats.total_compressed),
            human_bytes(stats.total_uncompressed),
            f'{stats.space_saved_percent:.1f}%',
        ]
        if column is not None:
            chunk = rg.column_chunks.get(column)
            col_stats = chunk.statistics if chunk is not None else None
            if col_stats is None:
                row += ['-', '-', '-']
            else:
                nulls = col_stats.null_count
                row += [
                    _stat_repr(col_stats.converted_min_value),
                    _stat_repr(col_stats.converted_max_value),
                    '-' if nulls is None else str(nulls),
                ]
        table.add_row(*row)

    return table


def pages_tables(parquet: ParquetFile, column: str) -> list[Table]:
    tables: list[Table] = []
    for chunk in parquet.column_chunks:
        if chunk.path_in_schema != column:
            continue

        table = Table(
            title=f'{column} - row group {chunk.row_group}',
            title_justify='left',
        )
        table.add_column('page', justify='right')
        table.add_column('type')
        table.add_column('values', justify='right')
        table.add_column('encoding')
        table.add_column('compressed', justify='right')
        table.add_column('uncompressed', justify='right')
        table.add_column('offset', justify='right')

        chunk_pages: list[Any] = list(chunk.data_pages) + list(chunk.index_pages)
        if chunk.dictionary_page is not None:
            chunk_pages.append(chunk.dictionary_page)
        rows: list[tuple[int, Any]] = [
            (page.start_offset, page) for page in chunk_pages
        ]
        rows.sort(key=lambda item: item[0])

        for position, (_, page) in enumerate(rows):
            table.add_row(
                str(position),
                page.page_type.name,
                f'{getattr(page, "num_values", 0):,}',
                getattr(getattr(page, 'encoding', None), 'name', '-'),
                human_bytes(page.compressed_page_size),
                human_bytes(page.uncompressed_page_size),
                str(page.start_offset),
            )
        tables.append(table)

    return tables
