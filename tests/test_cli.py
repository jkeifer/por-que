"""Tests for CLI commands."""

import pytest

from click.testing import CliRunner

from por_que.cli import cli, formatters
from por_que.cli.exceptions import InvalidValueError


@pytest.fixture
def runner():
    """CLI test runner."""
    return CliRunner()


@pytest.fixture
def test_file_url():
    """URL to a test Parquet file."""
    return 'https://github.com/apache/parquet-testing/raw/master/data/alltypes_plain.parquet'


def test_cli_help(runner):
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Por Qué' in result.output


def test_metadata_help(runner):
    result = runner.invoke(cli, ['metadata', '--help'])
    assert result.exit_code == 0
    assert 'Read and inspect Parquet file metadata' in result.output


def test_summary_command(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'summary'])
    assert result.exit_code == 0
    assert 'Parquet File Summary' in result.output
    assert 'Version: 1' in result.output
    assert 'Schema Structure:' in result.output
    assert 'Row Groups: 1' in result.output


def test_schema_command(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'schema'])
    assert result.exit_code == 0
    assert 'Schema Structure' in result.output
    assert 'Group(schema)' in result.output
    assert 'Column(id: INT32 OPTIONAL)' in result.output
    assert 'Column(bool_col: BOOLEAN OPTIONAL)' in result.output


def test_stats_command(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'stats'])
    assert result.exit_code == 0
    assert 'File Statistics' in result.output
    assert 'Version: 1' in result.output
    assert 'Total rows: 8' in result.output
    assert 'Compression ratio:' in result.output


def test_rowgroups_command(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'rowgroups'])
    assert result.exit_code == 0
    assert 'Row Groups' in result.output
    assert '0: 8 rows, 11 cols' in result.output


def test_rowgroups_specific_group(runner, test_file_url):
    result = runner.invoke(
        cli,
        ['metadata', '--url', test_file_url, 'rowgroups', '--group', '0'],
    )
    assert result.exit_code == 0
    assert 'Row Group 0' in result.output
    assert 'Rows: 8' in result.output
    assert 'Columns: 11' in result.output


def test_rowgroups_invalid_group(runner, test_file_url):
    result = runner.invoke(
        cli,
        ['metadata', '--url', test_file_url, 'rowgroups', '--group', '999'],
    )
    assert result.exit_code == 2
    assert 'does not exist' in result.output


def test_columns_command(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'columns'])
    assert result.exit_code == 0
    assert 'Column Information' in result.output
    assert '0: id' in result.output
    assert 'Type: INT32' in result.output
    assert 'Codec: UNCOMPRESSED' in result.output
    assert 'Values: 8' in result.output


def test_keyvalue_command_list_keys(runner, test_file_url):
    result = runner.invoke(cli, ['metadata', '--url', test_file_url, 'keyvalue'])
    assert result.exit_code == 0


def test_keyvalue_command_nonexistent_key(runner, test_file_url):
    result = runner.invoke(
        cli,
        ['metadata', '--url', test_file_url, 'keyvalue', 'nonexistent'],
    )
    assert result.exit_code == 2
    assert 'not found' in result.output


def test_missing_file_url_error(runner):
    result = runner.invoke(cli, ['metadata', 'summary'])
    assert result.exit_code == 2
    assert 'Missing one of the required' in result.output


def test_invalid_url_error(runner):
    result = runner.invoke(
        cli,
        [
            'metadata',
            '--url',
            'https://invalid-url-that-does-not-exist.com/file.parquet',
            'summary',
        ],
    )
    assert result.exit_code == 1
    assert 'Error:' in result.output


def test_formatters_with_nested_structs(nested_structs_metadata):
    # Test all formatter functions
    summary = formatters.format_summary(nested_structs_metadata)
    assert 'Parquet File Summary' in summary
    assert 'Group(schema)' in summary

    schema = formatters.format_schema(nested_structs_metadata)
    assert 'Schema Structure' in schema
    assert 'Group(' in schema

    stats = formatters.format_stats(nested_structs_metadata)
    assert 'File Statistics' in stats

    rowgroups = formatters.format_rowgroups(nested_structs_metadata)
    assert 'Row Groups' in rowgroups

    if nested_structs_metadata.row_groups:
        single_rg = formatters.format_rowgroups(nested_structs_metadata, 0)
        assert 'Row Group 0' in single_rg

    columns = formatters.format_columns(nested_structs_metadata)
    assert 'Column Information' in columns

    keys = formatters.format_metadata_keys(nested_structs_metadata)
    assert isinstance(keys, str)  # Should return string, may be empty


def test_formatters_with_alltypes(alltypes_plain_metadata):
    # Test all formatter functions work without errors
    summary = formatters.format_summary(alltypes_plain_metadata)
    assert 'Parquet File Summary' in summary

    schema = formatters.format_schema(alltypes_plain_metadata)
    assert 'Schema Structure' in schema

    stats = formatters.format_stats(alltypes_plain_metadata)
    assert 'File Statistics' in stats

    rowgroups = formatters.format_rowgroups(alltypes_plain_metadata)
    assert 'Row Groups' in rowgroups

    columns = formatters.format_columns(alltypes_plain_metadata)
    assert 'Column Information' in columns


def test_formatter_error_handling(alltypes_plain_metadata):
    # Test invalid row group index
    with pytest.raises(InvalidValueError):
        formatters.format_rowgroups(alltypes_plain_metadata, 999)


def test_schema_display_shows_logical_types(nested_structs_metadata):
    schema_str = formatters.format_schema(nested_structs_metadata)
    assert '[TIMESTAMP_MICROS]' in schema_str
    assert '[INT_64]' in schema_str
    assert '[UINT_64]' in schema_str


def test_column_statistics_display(nested_structs_metadata):
    if nested_structs_metadata.row_groups:
        single_rg = formatters.format_rowgroups(nested_structs_metadata, 0)
        # Should show statistics if they exist
        assert 'Row Group 0' in single_rg
        assert 'Columns:' in single_rg
