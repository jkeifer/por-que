import pytest

from click.testing import CliRunner

from por_que.cli import cli

from .util import parquet_url


@pytest.fixture
def alltypes_plain_url() -> str:
    return parquet_url('alltypes_plain')


@pytest.fixture
def nested_structs_url() -> str:
    return parquet_url('nested_structs.rust')


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_help(runner):
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Por Qué' in result.output


def test_version_command(runner):
    from por_que._version import get_version

    result = runner.invoke(cli, ['version'])
    assert result.exit_code == 0
    assert result.output.strip() == get_version()


def test_metadata_help(runner):
    result = runner.invoke(cli, ['metadata', '--help'])
    assert result.exit_code == 0
    assert 'Read and inspect Parquet file metadata' in result.output


def test_summary_command(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'summary'])
    assert result.exit_code == 0
    assert 'Parquet File Summary' in result.output
    assert 'Version: 1' in result.output
    assert 'Schema Structure:' in result.output
    assert 'Row Groups: 1' in result.output


def test_schema_command(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'schema'])
    assert result.exit_code == 0
    assert 'Schema Structure' in result.output
    assert 'Group(schema)' in result.output
    assert 'Column(id: INT32 OPTIONAL)' in result.output
    assert 'Column(bool_col: BOOLEAN OPTIONAL)' in result.output


def test_stats_command(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'stats'])
    assert result.exit_code == 0
    assert 'File Statistics' in result.output
    assert 'Version: 1' in result.output
    assert 'Total rows: 8' in result.output
    assert 'Compression ratio:' in result.output


def test_rowgroups_command(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'rowgroups'])
    assert result.exit_code == 0
    assert 'Row Groups' in result.output
    assert '0: 8 rows, 11 cols' in result.output


def test_rowgroups_specific_group(runner, alltypes_plain_url):
    result = runner.invoke(
        cli,
        ['metadata', alltypes_plain_url, 'rowgroups', '--group', '0'],
    )
    assert result.exit_code == 0
    assert 'Row Group 0' in result.output
    assert 'Rows: 8' in result.output
    assert 'Columns: 11' in result.output


def test_rowgroups_invalid_group(runner, alltypes_plain_url):
    result = runner.invoke(
        cli,
        ['metadata', alltypes_plain_url, 'rowgroups', '--group', '999'],
    )
    assert result.exit_code == 2
    assert 'does not exist' in result.output


def test_columns_command(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'columns'])
    assert result.exit_code == 0
    assert 'Column Information' in result.output
    assert '0: id' in result.output
    assert 'Type: INT32' in result.output
    assert 'Codec: UNCOMPRESSED' in result.output
    assert 'Values: 8' in result.output


def test_keyvalue_command_list_keys(runner, alltypes_plain_url):
    result = runner.invoke(cli, ['metadata', alltypes_plain_url, 'keyvalue'])
    assert result.exit_code == 0


def test_keyvalue_command_nonexistent_key(runner, alltypes_plain_url):
    result = runner.invoke(
        cli,
        ['metadata', alltypes_plain_url, 'keyvalue', 'nonexistent'],
    )
    assert result.exit_code == 2
    assert 'not found' in result.output


def test_invalid_url_error(runner):
    result = runner.invoke(
        cli,
        [
            'metadata',
            'https://invalid-url-that-does-not-exist.com/file.parquet',
            'summary',
        ],
    )
    assert result.exit_code == 1
    assert 'Error:' in result.output


def test_cli_with_nested_structs(runner, nested_structs_url):
    # Test CLI commands with nested structs file
    result = runner.invoke(cli, ['metadata', nested_structs_url, 'summary'])
    print(f'Exit code: {result.exit_code}')
    print(f'Output: {result.output!r}')
    assert result.exit_code == 0
    assert 'Parquet File Summary' in result.output
    assert 'Group(schema)' in result.output

    result = runner.invoke(cli, ['metadata', nested_structs_url, 'schema'])
    assert result.exit_code == 0
    assert 'Schema Structure' in result.output
    assert 'Group(' in result.output

    result = runner.invoke(cli, ['metadata', nested_structs_url, 'stats'])
    assert result.exit_code == 0
    assert 'File Statistics' in result.output

    result = runner.invoke(cli, ['metadata', nested_structs_url, 'rowgroups'])
    assert result.exit_code == 0
    assert 'Row Groups' in result.output

    result = runner.invoke(cli, ['metadata', nested_structs_url, 'columns'])
    assert result.exit_code == 0
    assert 'Column Information' in result.output


def test_cli_formatters_work_without_errors(runner, alltypes_plain_url):
    # Test all CLI formatter commands work without errors
    commands = ['summary', 'schema', 'stats', 'rowgroups', 'columns', 'keyvalue']

    for command in commands:
        result = runner.invoke(cli, ['metadata', alltypes_plain_url, command])
        assert result.exit_code == 0, (
            f'Command {command} failed with output: {result.output}'
        )


def test_cli_error_handling(runner, alltypes_plain_url):
    # Test invalid row group index via CLI
    result = runner.invoke(
        cli,
        ['metadata', alltypes_plain_url, 'rowgroups', '--group', '999'],
    )
    assert result.exit_code == 2
    assert 'does not exist' in result.output


def test_schema_display_shows_logical_types(runner, nested_structs_url):
    result = runner.invoke(cli, ['metadata', nested_structs_url, 'schema'])
    assert result.exit_code == 0
    assert '[TIMESTAMP_MICROS]' in result.output
    assert '[INT_64]' in result.output
    assert '[UINT_64]' in result.output


def test_column_statistics_display(runner, nested_structs_url):
    result = runner.invoke(
        cli,
        ['metadata', nested_structs_url, 'rowgroups', '--group', '0'],
    )
    assert result.exit_code == 0
    # Should show statistics if they exist
    assert 'Row Group 0' in result.output
    assert 'Columns:' in result.output
