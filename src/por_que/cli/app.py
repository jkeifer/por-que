"""por-que command-line interface (optional ``cli`` extra).

A pragmatic, non-interactive tool to inspect a parquet file's schema,
metadata, row groups, and page structure, or dump its full JSON serialization.
Works on local paths and ``http(s)`` URLs alike.
"""

from __future__ import annotations

import asyncio
import sys

from collections.abc import Callable, Coroutine
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import typer

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from por_que.enums import ProgressPhase

from . import loaders, render

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help='Inspect parquet files (schema, metadata, row groups, pages).',
)

console = Console()
err_console = Console(stderr=True)

SourceArg = Annotated[
    str,
    typer.Argument(
        metavar='PATH_OR_URL',
        help='Local file path or http(s) URL of a parquet file.',
    ),
]


@dataclass(frozen=True)
class Verbosity:
    verbose: bool = False
    quiet: bool = False


@app.callback()
def _main(
    ctx: typer.Context,
    verbose: Annotated[
        bool,
        typer.Option('--verbose', '-v', help='Show diagnostic output.'),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option('--quiet', '-q', help='Suppress non-essential output.'),
    ] = False,
) -> None:
    if verbose and quiet:
        raise typer.BadParameter('--verbose and --quiet are mutually exclusive')
    ctx.obj = Verbosity(verbose=verbose, quiet=quiet)


def _verbosity(ctx: typer.Context) -> Verbosity:
    obj = ctx.obj
    return obj if isinstance(obj, Verbosity) else Verbosity()


ProgressCb = Callable[[ProgressPhase, int, int], None]

_PHASE_LABELS = {
    ProgressPhase.METADATA_READ: 'reading metadata',
    ProgressPhase.METADATA_PARSE: 'parsing metadata',
    ProgressPhase.COLUMN_CHUNKS: 'reading columns',
}


@contextmanager
def _progress(ctx: typer.Context):
    """Yield a progress callback backed by a rich bar, or None when suppressed.

    The bar draws on stderr so JSON on stdout stays clean, and is skipped for
    ``--quiet`` or non-interactive stderr (pipes, redirects).
    """
    if _verbosity(ctx).quiet or not err_console.is_terminal:
        yield None
        return

    bar = Progress(
        TextColumn('[dim]{task.description}[/dim]'),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=err_console,
        transient=True,
    )
    tasks: dict[ProgressPhase, TaskID] = {}

    def report(phase: ProgressPhase, done: int, total: int) -> None:
        task_id = tasks.get(phase)
        if task_id is None:
            task_id = bar.add_task(_PHASE_LABELS.get(phase, str(phase)), total=total)
            tasks[phase] = task_id
        bar.update(task_id, completed=done, total=total)

    with bar:
        yield report


def _run[T](
    ctx: typer.Context,
    source: str,
    make_coro: Callable[[ProgressCb | None], Coroutine[Any, Any, T]],
) -> T:
    """Drive an async load to completion, mapping errors to clean messages."""
    if _verbosity(ctx).verbose:
        err_console.print(f'[dim]reading {source}[/dim]')
    try:
        with _progress(ctx) as report:
            return asyncio.run(make_coro(report))
    except (OSError, ValueError) as exc:
        err_console.print(f'[red]error:[/red] {exc}')
        raise typer.Exit(1) from exc


def _hint(ctx: typer.Context, text: str) -> None:
    if not _verbosity(ctx).quiet:
        console.print(f'[dim]{text}[/dim]')


@app.command()
def schema(ctx: typer.Context, source: SourceArg) -> None:
    """Show the schema tree (types, repetition, logical types)."""
    metadata = _run(ctx, source, lambda cb: loaders.load_metadata(source, progress=cb))
    console.print(render.schema_tree(metadata))


@app.command()
def meta(
    ctx: typer.Context,
    source: SourceArg,
    key: Annotated[
        str | None,
        typer.Option(
            '--key',
            '-k',
            help='Print the raw value of a single key-value metadata entry.',
        ),
    ] = None,
) -> None:
    """Show a file-level summary and key-value metadata."""
    metadata = _run(ctx, source, lambda cb: loaders.load_metadata(source, progress=cb))

    if key is not None:
        for entry in metadata.key_value_metadata:
            if entry.key == key:
                sys.stdout.write(entry.value + '\n')
                return
        available = ', '.join(kv.key for kv in metadata.key_value_metadata) or '(none)'
        err_console.print(
            f'[red]error:[/red] key {key!r} not found. available: {available}',
        )
        raise typer.Exit(1)

    console.print(render.meta_summary(source, metadata))


@app.command(name='row-groups')
def row_groups(
    ctx: typer.Context,
    source: SourceArg,
    column: Annotated[
        str | None,
        typer.Option(
            '--column',
            '-c',
            help='Add converted min/max/null stats for this column path.',
        ),
    ] = None,
) -> None:
    """Show a per-row-group table of sizes and (optionally) column stats."""
    metadata = _run(ctx, source, lambda cb: loaders.load_metadata(source, progress=cb))

    if column is not None:
        available = (
            list(metadata.row_groups[0].column_chunks) if metadata.row_groups else []
        )
        if column not in available:
            err_console.print(
                f'[red]error:[/red] column {column!r} not found. '
                f'available: {", ".join(available) or "(none)"}',
            )
            raise typer.Exit(1)

    console.print(render.row_groups_table(metadata, column))


@app.command()
def pages(
    ctx: typer.Context,
    source: SourceArg,
    column: Annotated[
        str,
        typer.Option('--column', '-c', help='Column path to inspect.'),
    ],
    row_group: Annotated[
        int | None,
        typer.Option('--row-group', '-r', help='Limit to a single row group.'),
    ] = None,
) -> None:
    """Show page-level structure for a column via selective loading."""
    row_groups_arg = None if row_group is None else [row_group]
    parquet = _run(
        ctx,
        source,
        lambda cb: loaders.load_file(
            source,
            columns=[column],
            row_groups=row_groups_arg,
            progress=cb,
        ),
    )

    tables = render.pages_tables(parquet, column)
    if not tables:
        available = (
            list(parquet.metadata.row_groups[0].column_chunks)
            if parquet.metadata.row_groups
            else []
        )
        err_console.print(
            f'[red]error:[/red] no pages for column {column!r}'
            + (f' in row group {row_group}' if row_group is not None else '')
            + f'. available columns: {", ".join(available) or "(none)"}',
        )
        raise typer.Exit(1)

    for table in tables:
        console.print(table)
    _hint(
        ctx,
        'reading one column touches only its pages -- this is why columnar '
        'formats excel at analytics.',
    )


@app.command()
def dump(
    ctx: typer.Context,
    source: SourceArg,
    metadata_only: Annotated[
        bool,
        typer.Option('--metadata-only', help='Dump only file metadata, not pages.'),
    ] = False,
) -> None:
    """Dump the JSON serialization to stdout."""
    if metadata_only:
        export = _run(
            ctx,
            source,
            lambda cb: loaders.load_metadata_export(source, progress=cb),
        )
        sys.stdout.write(export.to_json(indent=2) + '\n')
        return

    parquet = _run(ctx, source, lambda cb: loaders.load_file(source, progress=cb))
    sys.stdout.write(parquet.to_json(indent=2) + '\n')
