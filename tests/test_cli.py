import shlex

from collections.abc import Callable

import pytest

from click.testing import CliRunner, Result

from por_que.cli import cli

type Invoke = Callable[..., Result]


@pytest.fixture(scope='session')
def invoke() -> Invoke:
    runner = CliRunner()

    def _invoke(cmd: str, **kwargs) -> Result:
        kwargs['catch_exceptions'] = kwargs.get('catch_exceptions', False)
        return runner.invoke(cli, shlex.split(cmd), **kwargs)

    return _invoke


def test_cli_help(invoke: Invoke) -> None:
    result = invoke('--help')
    assert result.exit_code == 0
    assert 'Por Qué' in result.output


def test_version_command(invoke: Invoke) -> None:
    from por_que._version import get_version

    result = invoke('version')
    assert result.exit_code == 0
    assert result.output.strip() == get_version()


def test_metadata_help(invoke: Invoke) -> None:
    result = invoke('inspect --help')
    assert result.exit_code == 0
    assert 'Inspect Parquet file structure and metadata' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_summary_command(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url}')
    assert result.exit_code == 0
    assert 'Parquet File Summary' in result.output
    assert 'Version: 1' in result.output
    assert 'Schema Structure:' in result.output
    assert 'Row Groups: 1' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_schema_command(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} schema')
    assert result.exit_code == 0
    assert 'Schema Structure' in result.output
    assert 'Group(schema)' in result.output
    assert 'Column(id: INT32 OPTIONAL)' in result.output
    assert 'Column(bool_col: BOOLEAN OPTIONAL)' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_stats_command(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} stats')
    assert result.exit_code == 0
    assert 'File Statistics' in result.output
    assert 'Version: 1' in result.output
    assert 'Total rows: 8' in result.output
    assert 'Compression ratio:' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_rowgroups_command(invoke: Invoke, parquet_url: str) -> None:
    # rowgroups info is now part of default summary
    result = invoke(f'inspect {parquet_url}')
    assert result.exit_code == 0
    assert 'Row Groups: 1' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_rowgroups_specific_group(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} rowgroup 0')
    assert result.exit_code == 0
    # This will be implemented later - just check it doesn't crash for now


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_rowgroups_invalid_group(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} rowgroup 999')
    # Error handling will be implemented later - just check it doesn't succeed
    assert result.exit_code != 0


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_columns_command(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} columns')
    assert result.exit_code == 0
    assert 'Column Information' in result.output
    assert '0: id' in result.output
    assert 'Type: INT32' in result.output
    assert 'Codec: UNCOMPRESSED' in result.output
    assert 'Values: 8' in result.output


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_keyvalue_command_list_keys(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} metadata')
    assert result.exit_code == 0


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_keyvalue_command_nonexistent_key(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} metadata nonexistent')
    assert result.exit_code == 2
    assert 'not found' in result.output


def test_invalid_url_error(invoke: Invoke) -> None:
    result = invoke('inspect https://invalid-url-that-does-not-exist.com/file.parquet')
    assert result.exit_code == 1
    assert 'Error:' in result.output


@pytest.mark.parametrize('parquet_file_name', ['nested_structs.rust'])
def test_cli_with_nested_structs(invoke: Invoke, nested_structs_url: str) -> None:
    # Test CLI commands with nested structs file
    result = invoke(f'inspect {nested_structs_url}')
    print(f'Exit code: {result.exit_code}')
    print(f'Output: {result.output!r}')
    assert result.exit_code == 0
    assert 'Parquet File Summary' in result.output
    assert 'Group(schema)' in result.output

    result = invoke(f'inspect {nested_structs_url} schema')
    assert result.exit_code == 0
    assert 'Schema Structure' in result.output
    assert 'Group(' in result.output

    # Stats, row groups, and column info are now all in the default inspect output
    summary_result = invoke(f'inspect {nested_structs_url}')
    assert summary_result.exit_code == 0
    assert 'Version:' in summary_result.output  # File stats content
    assert 'Total rows:' in summary_result.output  # File stats content
    assert 'Row Groups:' in summary_result.output  # Row groups info
    assert 'Columns:' in summary_result.output  # Column info


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_cli_error_handling(invoke: Invoke, parquet_url: str) -> None:
    # Test invalid row group index via CLI
    result = invoke(f'inspect {parquet_url} rowgroup 999')
    assert result.exit_code == 2
    assert 'does not exist' in result.output


@pytest.mark.parametrize('parquet_file_name', ['nested_structs.rust'])
def test_schema_display_shows_logical_types(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} schema')
    assert result.exit_code == 0
    assert '[TIMESTAMP_MICROS]' in result.output
    assert '[INT_64]' in result.output
    assert '[UINT_64]' in result.output


@pytest.mark.parametrize('parquet_file_name', ['nested_structs.rust'])
def test_column_statistics_display(invoke: Invoke, parquet_url: str) -> None:
    result = invoke(f'inspect {parquet_url} rowgroup 0')
    assert result.exit_code == 0
    # Should show statistics if they exist
    assert 'Row Group 0' in result.output
    assert 'Columns:' in result.output
