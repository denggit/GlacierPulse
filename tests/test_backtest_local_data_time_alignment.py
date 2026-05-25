from datetime import datetime

import pytest

from tools import backtest_local_data as backtest


def _ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def test_start_time_defaults_to_utc():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--start-time",
            "2026-05-21 01:00",
            "--end-time",
            "2026-05-21 02:00",
        ]
    )

    window = backtest.parse_time_window(args)

    assert window.start_ts == pytest.approx(_ts("2026-05-21T01:00:00Z"))
    assert window.end_ts == pytest.approx(_ts("2026-05-21T02:00:00Z"))


def test_start_time_with_offset_is_converted_to_utc():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--start-time",
            "2026-05-21T09:00:00+08:00",
            "--end-time",
            "2026-05-21T10:00:00+08:00",
        ]
    )

    window = backtest.parse_time_window(args)

    assert window.start_ts == pytest.approx(_ts("2026-05-21T01:00:00Z"))
    assert window.end_ts == pytest.approx(_ts("2026-05-21T02:00:00Z"))


def test_start_date_defaults_to_utc_day():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--start-date",
            "2026-05-21",
            "--end-date",
            "2026-05-21",
        ]
    )

    window = backtest.parse_time_window(args)

    assert window.start_ts == pytest.approx(_ts("2026-05-21T00:00:00Z"))


def test_end_date_is_inclusive_date_exclusive_next_day():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--start-date",
            "2026-05-21",
            "--end-date",
            "2026-05-21",
        ]
    )

    window = backtest.parse_time_window(args)

    assert window.end_ts == pytest.approx(_ts("2026-05-22T00:00:00Z"))


def test_local_timezone_cli_was_removed():
    with pytest.raises(SystemExit):
        backtest.parse_args(["--timezone", "Asia/Shanghai"])


def test_coverage_cli_was_removed():
    with pytest.raises(SystemExit):
        backtest.parse_args(["--allow-partial-coverage"])
