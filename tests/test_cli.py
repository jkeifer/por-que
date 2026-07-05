"""Tests for the optional por-que CLI.

The CLI is exercised with typer's ``CliRunner`` against a real parquet file
downloaded to a local path (the same way the rest of the suite obtains test
data via its ``*_url`` fixtures). URL plumbing shares one code path with local
paths in ``loaders``, so covering local paths here is sufficient.
"""

import builtins
import json

from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType
from urllib.request import urlretrieve

import pytest

from typer.testing import CliRunner

from por_que.cli.app import app
from por_que.cli.entry import main

PARQUET_BASE_URL = (
    'https://raw.githubusercontent.com/apache/parquet-testing/master/data'
)

runner = CliRunner()


@pytest.fixture(scope='session')
def local_parquet(tmp_path_factory: pytest.TempPathFactory) -> str:
    """A real parquet file on the local filesystem.

    ``binary.parquet`` is small and carries a single column (``foo``), one row
    group, and key-value metadata, so it exercises every command.
    """
    dest = tmp_path_factory.mktemp('cli') / 'binary.parquet'
    urlretrieve(f'{PARQUET_BASE_URL}/binary.parquet', dest)  # noqa: S310
    return str(dest)


def test_schema(local_parquet: str) -> None:
    result = runner.invoke(app, ['schema', local_parquet])
    assert result.exit_code == 0
    assert 'foo' in result.output
    assert 'BYTE_ARRAY' in result.output


def test_meta(local_parquet: str) -> None:
    result = runner.invoke(app, ['meta', local_parquet])
    assert result.exit_code == 0
    assert 'created by' in result.output
    assert 'writer.model.name' in result.output


def test_meta_key(local_parquet: str) -> None:
    result = runner.invoke(app, ['meta', local_parquet, '--key', 'writer.model.name'])
    assert result.exit_code == 0
    assert result.output.strip() == 'protobuf'


def test_meta_key_missing(local_parquet: str) -> None:
    result = runner.invoke(app, ['meta', local_parquet, '--key', 'nope'])
    assert result.exit_code == 1
    assert 'not found' in result.output


def test_row_groups(local_parquet: str) -> None:
    result = runner.invoke(app, ['row-groups', local_parquet])
    assert result.exit_code == 0
    assert 'rows' in result.output


def test_row_groups_with_column(local_parquet: str) -> None:
    result = runner.invoke(app, ['row-groups', local_parquet, '--column', 'foo'])
    assert result.exit_code == 0
    assert 'min' in result.output
    assert 'max' in result.output


def test_row_groups_bad_column(local_parquet: str) -> None:
    result = runner.invoke(app, ['row-groups', local_parquet, '--column', 'nope'])
    assert result.exit_code == 1
    assert 'not found' in result.output


def test_pages(local_parquet: str) -> None:
    result = runner.invoke(app, ['pages', local_parquet, '--column', 'foo'])
    assert result.exit_code == 0
    assert 'DATA_PAGE' in result.output


def test_pages_row_group(local_parquet: str) -> None:
    result = runner.invoke(
        app,
        ['pages', local_parquet, '--column', 'foo', '--row-group', '0'],
    )
    assert result.exit_code == 0
    assert 'DATA_PAGE' in result.output


def test_pages_bad_column(local_parquet: str) -> None:
    result = runner.invoke(app, ['pages', local_parquet, '--column', 'nope'])
    assert result.exit_code == 1
    assert 'available columns' in result.output


def test_dump(local_parquet: str) -> None:
    result = runner.invoke(app, ['dump', local_parquet])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload['metadata']['version'] == 1


def test_dump_metadata_only(local_parquet: str) -> None:
    result = runner.invoke(app, ['dump', local_parquet, '--metadata-only'])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload['version'] == 1
    assert 'column_chunks' not in payload


def test_url_source_uses_http_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """A source starting with http(s) routes through AsyncHttpFile, not open()."""
    from por_que.cli import loaders

    assert loaders.is_url('https://example.com/data.parquet')
    assert loaders.is_url('http://example.com/data.parquet')
    assert not loaders.is_url('/local/data.parquet')

    def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError('local open() must not be used for URLs')

    monkeypatch.setattr(Path, 'open', boom)

    seen: dict[str, str] = {}

    class FakeHttpFile:
        def __init__(self, url: str) -> None:
            seen['url'] = url

        async def __aenter__(self) -> 'FakeHttpFile':
            raise RuntimeError('stop before real network I/O')

        async def __aexit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(loaders, 'AsyncHttpFile', FakeHttpFile)

    result = runner.invoke(app, ['schema', 'https://example.com/data.parquet'])
    assert seen['url'] == 'https://example.com/data.parquet'
    assert result.exit_code == 1


def test_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / 'does-not-exist.parquet'
    result = runner.invoke(app, ['schema', str(missing)])
    assert result.exit_code == 1
    assert 'error:' in result.output


def test_missing_extra_message(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without the extra installed, the entry point prints one friendly line."""
    real_import = builtins.__import__

    def fake_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> ModuleType:
        if name == 'por_que.cli.app':
            raise ImportError('simulated missing cli extra')
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, '__import__', fake_import)

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 1
    assert "pip install 'por-que[cli]'" in capsys.readouterr().err
