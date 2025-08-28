import sys

from dataclasses import dataclass
from pathlib import Path

import click

from click_option_group import (
    RequiredMutuallyExclusiveOptionGroup,
    optgroup,
)

from por_que.exceptions import PorQueError
from por_que.types import FileMetadata

from . import formatters
from .exceptions import InvalidValueError


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
    click.echo(formatters.format_summary(ctx.metadata))


@metadata.command()
@click.pass_obj
def schema(ctx: MetadataContext):
    """Show detailed schema structure."""
    click.echo(formatters.format_schema(ctx.metadata))


@metadata.command()
@click.pass_obj
def stats(ctx: MetadataContext):
    """Show file statistics and compression info."""
    click.echo(formatters.format_stats(ctx.metadata))


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
    click.echo(formatters.format_rowgroups(ctx.metadata, group))


@metadata.command()
@click.pass_obj
def columns(ctx: MetadataContext):
    """Show column-level metadata and encoding information."""
    click.echo(formatters.format_columns(ctx.metadata))


@metadata.command()
@click.argument('key', required=False)
@click.pass_obj
def kv(ctx: MetadataContext, key: str | None):
    """Show key-value metadata keys, or value for specific key."""
    if key is None:
        # Show all available keys
        click.echo(formatters.format_metadata_keys(ctx.metadata))
        return

    # Show value for specific key
    if key not in ctx.metadata.key_value_metadata:
        raise InvalidValueError(
            f"Metadata key '{key}' not found. ",
        )

    click.echo(ctx.metadata.key_value_metadata[key])


if __name__ == '__main__':
    cli()
