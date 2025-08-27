import sys

from dataclasses import dataclass
from pathlib import Path

import click

from click_option_group import (
    RequiredMutuallyExclusiveOptionGroup,
    optgroup,
)

from .exceptions import PorQueError
from .types import FileMetadata


@dataclass
class MetadataContext:
    metadata: FileMetadata


def load_metadata(path: Path | str) -> FileMetadata:
    """Load metadata from file or URL."""
    try:
        if isinstance(path, Path):
            return FileMetadata.from_file(path)
        return FileMetadata.from_url(path)
    except PorQueError as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)


@click.group()
@click.version_option()
def cli():
    """¿Por Qué? - pure-python parquet parsing"""
    pass


@cli.group()
@optgroup.group(
    'Parquet source file',
    cls=RequiredMutuallyExclusiveOptionGroup,
    help='A parquet file local path or remote HTTP(S) url',
)
@optgroup.option(
    '-f',
    '--file',
    'file_path',
    type=click.Path(exists=True, path_type=Path),
    help='Path to Parquet file',
)
@optgroup.option('-u', '--url', help='HTTP(S) URL to Parquet file')
@click.pass_context
def metadata(ctx, file_path: Path | None, url: str | None):
    """Read and inspect Parquet file metadata."""
    path = file_path if file_path else url
    if path is None:
        raise click.UsageError("Didn't get a file or a url")

    # Load metadata and store in context
    metadata_obj = load_metadata(
        path,
    )
    ctx.obj = MetadataContext(metadata=metadata_obj)


@metadata.command()
@click.pass_obj
def summary(ctx: MetadataContext):
    """Show high-level summary of Parquet file."""
    click.echo(ctx.metadata.summary())


@metadata.command()
@click.pass_obj
def schema(ctx: MetadataContext):
    """Show detailed schema structure."""
    click.echo('Schema Structure:')
    click.echo('=' * 60)

    for i, element in enumerate(ctx.metadata.schema):
        if element.is_group():
            click.echo(
                f'  {i:2}: {element.name} (GROUP, {element.num_children} children)',
            )
        else:
            rep = f' {element.repetition.name}' if element.repetition else ''
            type_name = element.type.name if element.type else 'unknown'
            length_info = (
                f' (length: {element.type_length})' if element.type_length else ''
            )
            converted_info = (
                f' [converted: {element.converted_type}]'
                if element.converted_type
                else ''
            )
            click.echo(
                f'  {i:2}: {element.name}: '
                f'{type_name}{length_info}{rep}{converted_info}',
            )


@metadata.command()
@click.pass_obj
def stats(ctx: MetadataContext):
    """Show file statistics and compression info."""
    metadata = ctx.metadata

    click.echo('File Statistics:')
    click.echo('=' * 60)
    click.echo(f'Format version: {metadata.version}')
    click.echo(f'Created by: {metadata.created_by or "unknown"}')
    click.echo(f'Total rows: {metadata.num_rows:,}')
    click.echo(f'Row groups: {len(metadata.row_groups)}')
    click.echo(f'Schema elements: {len(metadata.schema)}')

    if metadata.row_groups:
        total_compressed = 0
        total_uncompressed = 0
        total_columns = 0
        min_rows = float('inf')
        max_rows = 0

        for rg in metadata.row_groups:
            total_columns += len(rg.columns)
            min_rows = min(min_rows, rg.num_rows)
            max_rows = max(max_rows, rg.num_rows)

            for col in rg.columns:
                if col.meta_data:
                    total_compressed += col.meta_data.total_compressed_size
                    total_uncompressed += col.meta_data.total_uncompressed_size

        click.echo(f'Total columns: {total_columns}')
        click.echo(f'Rows per group: {min_rows:,} - {max_rows:,}')

        if total_uncompressed > 0:
            ratio = total_compressed / total_uncompressed
            click.echo(
                f'Uncompressed size: {total_uncompressed:,} bytes '
                f'({total_uncompressed / (1024 * 1024):.1f} MB)',
            )
            click.echo(
                f'Compressed size: {total_compressed:,} '
                f'bytes ({total_compressed / (1024 * 1024):.1f} MB)',
            )
            click.echo(
                f'Compression ratio: {ratio:.3f} '
                f'({(1 - ratio) * 100:.1f}% space saved)',
            )


@metadata.command()
@click.option(
    '--group',
    '-g',
    type=int,
    help='Show specific row group (0-indexed)',
)
@click.pass_obj
def rowgroups(ctx: MetadataContext, group: int | None = None):
    """Show row group information."""
    metadata = ctx.metadata

    if group is not None:
        if group < 0 or group >= len(metadata.row_groups):
            click.echo(
                f'Error: Row group {group} does not exist. '
                f'File has {len(metadata.row_groups)} row groups.',
                err=True,
            )
            sys.exit(1)

        rg = metadata.row_groups[group]
        click.echo(f'Row Group {group}:')
        click.echo('=' * 60)
        click.echo(f'Rows: {rg.num_rows:,}')
        click.echo(f'Total byte size: {rg.total_byte_size:,}')
        click.echo(f'Columns: {len(rg.columns)}')

        if rg.columns:
            click.echo('\nColumns:')
            column_names = rg.column_names()
            for i, name in enumerate(column_names):
                col = rg.columns[i]
                if col.meta_data:
                    codec = col.meta_data.codec.name
                    compressed = col.meta_data.total_compressed_size
                    uncompressed = col.meta_data.total_uncompressed_size
                    values = col.meta_data.num_values
                    ratio = compressed / uncompressed if uncompressed > 0 else 0
                    click.echo(
                        f'  {i:2}: {name} ({codec}, {values:,} values, '
                        f'{compressed}B/{uncompressed}B C/UC ({ratio:.2f}x))',
                    )
                else:
                    click.echo(f'  {i:2}: {name} (no metadata)')
    else:
        click.echo('Row Groups:')
        click.echo('=' * 60)

        for i, rg in enumerate(metadata.row_groups):
            avg_col_size = rg.total_byte_size // len(rg.columns) if rg.columns else 0
            click.echo(
                f'  {i:2}: {rg.num_rows:,} rows, '
                f'{len(rg.columns)} cols, {rg.total_byte_size:,} '
                f'bytes (avg {avg_col_size:,}/col)',
            )


@metadata.command()
@click.pass_obj
def columns(ctx: MetadataContext):
    """Show column-level metadata and encoding information."""
    metadata = ctx.metadata

    click.echo('Column Information:')
    click.echo('=' * 60)

    if not metadata.row_groups:
        click.echo('No row groups found.')
        return

    # Get column info from first row group (representative)
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

            click.echo(f'  {i:2}: {meta.path_in_schema}')
            click.echo(f'      Type: {meta.type.name}')
            click.echo(f'      Codec: {meta.codec.name}')
            click.echo(f'      Encodings: {encodings_str}')
            click.echo(f'      Values: {meta.num_values:,}')
            click.echo(
                f'      Size: {meta.total_compressed_size:,} '
                f'bytes (ratio: {compression_ratio:.3f})',
            )
            if len(metadata.row_groups) > 1:
                click.echo(f'      (from row group 0 of {len(metadata.row_groups)})')
            click.echo()


@metadata.command()
@click.pass_obj
def dump(ctx: MetadataContext):
    """Show complete detailed metadata dump."""
    click.echo(ctx.metadata.detailed_dump())


if __name__ == '__main__':
    cli()
