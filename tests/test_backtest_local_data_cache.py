import pytest

from tools import backtest_local_data as backtest


def test_coverage_cache_cli_was_removed():
    with pytest.raises(SystemExit):
        backtest.parse_args(["--coverage-cache-enabled"])

    with pytest.raises(SystemExit):
        backtest.parse_args(["--coverage-cache-path", "coverage.json"])


def test_filename_hint_cli_was_removed():
    with pytest.raises(SystemExit):
        backtest.parse_args(["--filename-date-hint-enabled"])

    with pytest.raises(SystemExit):
        backtest.parse_args(["--filename-date-hint-padding-days", "2"])


def test_removed_cache_helpers_are_not_exported():
    assert not hasattr(backtest, "load_coverage_cache")
    assert not hasattr(backtest, "save_coverage_cache")
    assert not hasattr(backtest, "CoverageCacheStats")
