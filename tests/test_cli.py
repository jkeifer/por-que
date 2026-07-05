"""Tests for the optional por-que CLI.

The CLI is exercised with typer's ``CliRunner`` against a real parquet file
downloaded to a local path (the same way the rest of the suite obtains test
data via its ``*_url`` fixtures). URL plumbing shares one code path with local
paths in ``loaders``, so covering local paths here is sufficient.
"""

import builtins
import http.client
import json
import threading
import time

from collections.abc import Mapping, Sequence
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.request import urlretrieve

import pytest

from typer.testing import CliRunner

import por_que

from por_que.cli import webapp
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

    # A self-identifying MetadataExport envelope, not a bare FileMetadata.
    assert payload['_meta']['model'] == 'metadata'
    assert payload['_meta']['format_version'] == 1
    assert payload['source'] == local_parquet
    assert payload['filesize'] > 0
    assert payload['metadata']['version'] == 1
    assert 'column_chunks' not in payload

    # ...and it round-trips back into the model it came from.
    export = por_que.MetadataExport.model_validate(payload)
    assert export.source == local_parquet
    assert export.filesize == payload['filesize']


def test_dump_discriminator_distinct(local_parquet: str) -> None:
    """The full dump and metadata export carry distinct `_meta.model` values."""
    full = json.loads(runner.invoke(app, ['dump', local_parquet]).output)
    meta = json.loads(
        runner.invoke(app, ['dump', local_parquet, '--metadata-only']).output,
    )
    assert full['_meta']['model'] == 'file'
    assert meta['_meta']['model'] == 'metadata'
    assert full['_meta']['model'] != meta['_meta']['model']


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


# -- `serve`: asset resolution --------------------------------------------


def test_resolve_webapp_dir_override(tmp_path: Path) -> None:
    (tmp_path / 'index.html').write_text('<html></html>')
    assert webapp.resolve_webapp_dir(tmp_path) == tmp_path


def test_resolve_webapp_dir_override_missing_index(tmp_path: Path) -> None:
    with pytest.raises(webapp.WebappNotFoundError, match=r'index\.html'):
        webapp.resolve_webapp_dir(tmp_path)


def test_resolve_webapp_dir_nothing_found(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """With no override, no packaged assets, and no repo checkout in reach."""
    monkeypatch.setattr(por_que, '__file__', str(tmp_path / 'pkg' / '__init__.py'))
    with pytest.raises(webapp.WebappNotFoundError, match='npm run build'):
        webapp.resolve_webapp_dir(None)


def test_serve_missing_webapp_dir(tmp_path: Path) -> None:
    """The CLI surfaces asset-resolution failures as a clean error."""
    empty = tmp_path / 'empty'
    empty.mkdir()
    result = runner.invoke(
        app,
        ['serve', 'unused.parquet', '--webapp-dir', str(empty)],
    )
    assert result.exit_code == 1
    assert 'index.html' in result.output


# -- `serve`: request handler -----------------------------------------------


def test_webapp_handler(tmp_path: Path) -> None:
    (tmp_path / 'index.html').write_text('<html>hi</html>')
    (tmp_path / 'app.js').write_text('console.log(1)')
    payload = b'{"hello": "world"}'

    handler_cls = webapp.make_handler(tmp_path, payload, verbose=False)
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        port = httpd.server_address[1]
        conn = http.client.HTTPConnection('127.0.0.1', port)

        conn.request('GET', '/')
        resp = conn.getresponse()
        assert resp.status == 200
        assert b'hi' in resp.read()

        conn.request('GET', '/data.json')
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.getheader('Content-Type') == 'application/json'
        assert resp.getheader('Cache-Control') == 'no-store'
        assert resp.read() == payload

        conn.request('GET', '/nope')
        resp = conn.getresponse()
        assert resp.status == 404

        conn.close()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def test_serve_json_dump_end_to_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`serve` on a pre-made ``.json`` dump skips parsing and serves it as-is."""
    webapp_dir = tmp_path / 'webapp'
    webapp_dir.mkdir()
    (webapp_dir / 'index.html').write_text('<html></html>')

    dump_path = tmp_path / 'dump.json'
    dump_path.write_text('{"hello": "world"}')

    # The real httpd instance never escapes the `serve` command, so capture it
    # via its constructor -- lets the test call `shutdown()` from here once
    # the background thread's `serve_forever()` is confirmed running.
    servers: list[ThreadingHTTPServer] = []
    orig_init = ThreadingHTTPServer.__init__

    def capturing_init(self: ThreadingHTTPServer, *a: Any, **kw: Any) -> None:
        orig_init(self, *a, **kw)
        servers.append(self)

    monkeypatch.setattr(ThreadingHTTPServer, '__init__', capturing_init)

    thread = threading.Thread(
        target=lambda: runner.invoke(
            app,
            [
                'serve',
                str(dump_path),
                '--webapp-dir',
                str(webapp_dir),
                '--no-browser',
            ],
        ),
        daemon=True,
    )
    thread.start()
    try:
        for _ in range(200):
            if servers:
                break
            time.sleep(0.01)
        assert servers, 'server was never constructed'
        httpd = servers[0]
        port = httpd.server_address[1]

        resp = None
        for _ in range(200):
            try:
                conn = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                conn.request('GET', '/data.json')
                resp = conn.getresponse()
                break
            except OSError:
                time.sleep(0.01)
        assert resp is not None, 'server never came up'
        assert resp.status == 200
        assert resp.getheader('Content-Type') == 'application/json'
        assert resp.read() == b'{"hello": "world"}'
        conn.close()
    finally:
        httpd.shutdown()
        thread.join(timeout=5)
        assert not thread.is_alive()
