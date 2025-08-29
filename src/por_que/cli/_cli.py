import sys

from dataclasses import dataclass
from pathlib import Path

import click

from por_que._version import get_version
from por_que.exceptions import ParquetNetworkError, ParquetUrlError, PorQueError
from por_que.parquet_file import ParquetFile
from por_que.protocols import ReadableSeekable
from por_que.types import FileMetadata
from por_que.util.http_file import HttpFile

from . import formatters
from .exceptions import InvalidValueError


class ParquetFileType(click.ParamType):
    """Click type that converts file path or URL to appropriate file-like object."""

    name = 'parquet_file'

    def convert(self, value, param, ctx):
        # Check if it's a URL
        if value.startswith(('http://', 'https://')):
            try:
                return HttpFile(value)
            except ParquetUrlError as e:
                self.fail(f'Invalid URL: {e}', param, ctx)
            except ParquetNetworkError as e:
                self.fail(f'Network error: {e}', param, ctx)
        else:
            # Treat as file path
            try:
                return Path(value).open('rb')
            except FileNotFoundError:
                self.fail(f'File not found: {value}', param, ctx)
            except PermissionError:
                self.fail(f'Permission denied: {value}', param, ctx)


@dataclass
class MetadataContext:
    metadata: FileMetadata


@click.group()
def cli():
    """¿Por Qué? - pure-python parquet parsing"""
    pass


@cli.command()
def version():
    """Show version information."""
    click.echo(get_version())


@cli.group()
@click.argument('file', type=ParquetFileType())
@click.pass_context
def metadata(ctx, file: ReadableSeekable):
    """Read and inspect Parquet file metadata."""
    try:
        parquet_file = ParquetFile(file)
        ctx.obj = MetadataContext(metadata=parquet_file.metadata)
    except PorQueError as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)


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
def keyvalue(ctx: MetadataContext, key: str | None):
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
