from datetime import datetime
from pathlib import Path

import pytest

from tools import backtest_local_data as backtest


SYMBOL = "ETH-USDT-SWAP"


def _ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _requested() -> backtest.RequestedWindow:
    return backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-05-20T00:00:00Z"), _ts("2026-05-20T20:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="2026-05-20T08:00:00+08:00",
        requested_end_local="2026-05-21T04:00:00+08:00",
        requested_start_utc="2026-05-20T00:00:00Z",
        requested_end_utc="2026-05-20T20:00:00Z",
    )


def _selection(*pairs: tuple[str, str]) -> backtest.CoverageSelection:
    return backtest.CoverageSelection(
        selected=[
            backtest.FileCoverage(
                path=Path(__file__),
                first_ts=_ts(start),
                last_ts=_ts(end),
                row_count=2,
            )
            for start, end in pairs
        ],
        all_coverage=[],
    )


def test_boundary_tolerance_does_not_fail_on_subsecond_edges():
    requested = backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-05-20T16:00:00Z"), _ts("2026-05-23T16:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="2026-05-21T00:00:00+08:00",
        requested_end_local="2026-05-24T00:00:00+08:00",
        requested_start_utc="2026-05-20T16:00:00Z",
        requested_end_utc="2026-05-23T16:00:00Z",
    )
    trades = _selection(("2026-05-20T16:00:00.082Z", "2026-05-23T15:59:59.731Z"))
    books = _selection(("2026-05-20T16:00:00Z", "2026-05-23T16:00:00Z"))

    _, alignment = backtest.resolve_effective_time_filter(
        requested,
        trades,
        books,
        require_full_coverage=True,
        coverage_tolerance_sec=60.0,
    )

    assert alignment["full_coverage"] is True


def test_internal_gap_90_seconds_with_60_second_tolerance_fails():
    requested = _requested()
    trades = _selection(
        ("2026-05-20T00:00:00Z", "2026-05-20T10:00:00Z"),
        ("2026-05-20T10:01:30Z", "2026-05-20T20:00:00Z"),
    )
    books = _selection(("2026-05-20T00:00:00Z", "2026-05-20T20:00:00Z"))

    with pytest.raises(SystemExit) as exc:
        backtest.resolve_effective_time_filter(
            requested,
            trades,
            books,
            require_full_coverage=True,
            coverage_tolerance_sec=60.0,
        )

    message = str(exc.value)
    assert "trades missing 2026-05-20T10:00:00Z to 2026-05-20T10:01:30Z" in message
    assert "coverage boundary tolerance: 60.0 sec" in message


def test_allow_partial_trims_head_but_not_middle_gap():
    requested = _requested()
    trades_head_missing = _selection(("2026-05-20T08:00:00Z", "2026-05-20T20:00:00Z"))
    books_full = _selection(("2026-05-20T00:00:00Z", "2026-05-20T20:00:00Z"))

    effective, alignment = backtest.resolve_effective_time_filter(
        requested,
        trades_head_missing,
        books_full,
        require_full_coverage=False,
        coverage_tolerance_sec=60.0,
    )
    assert effective.start_ts == _ts("2026-05-20T08:00:00Z")
    assert alignment["partial_coverage_used"] is True
    assert any("trimmed head" in warning for warning in alignment["warnings"])

    trades_middle_gap = _selection(
        ("2026-05-20T00:00:00Z", "2026-05-20T10:00:00Z"),
        ("2026-05-20T10:10:00Z", "2026-05-20T20:00:00Z"),
    )
    with pytest.raises(SystemExit) as exc:
        backtest.resolve_effective_time_filter(
            requested,
            trades_middle_gap,
            books_full,
            require_full_coverage=False,
            coverage_tolerance_sec=60.0,
        )
    assert "missing" in str(exc.value)


def test_scan_file_coverage_reads_ts_without_parsing_invalid_book_json(tmp_path):
    path = tmp_path / "books.csv"
    path.write_text(
        "\n".join(
            [
                "instId,ts,bids,asks",
                'ETH-USDT-SWAP,1776864000000,"not-json","still-not-json"',
                'ETH-USDT-SWAP,1776864060000,"bad","bad"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    coverage = backtest.scan_file_coverage(path, "book", SYMBOL, 0.1, backtest.BookCleaningOptions())

    assert coverage is not None
    assert coverage.first_ts == pytest.approx(1776864000.0)
    assert coverage.last_ts == pytest.approx(1776864060.0)
    assert coverage.row_count == 2


def test_book_replay_prefilters_ts_before_normalize_book(tmp_path, monkeypatch):
    path = tmp_path / "books.csv"
    path.write_text(
        "\n".join(
            [
                "instId,ts,bids,asks",
                'ETH-USDT-SWAP,1776864000000,"[[2500,10]]","[[2501,10]]"',
                'ETH-USDT-SWAP,1776865000000,"not-json-after-end","bad"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    calls = {"count": 0}
    original = backtest.normalize_book

    def wrapped(row, symbol, multiplier, options):
        calls["count"] += 1
        return original(row, symbol, multiplier, options)

    monkeypatch.setattr(backtest, "normalize_book", wrapped)
    rows = list(
        backtest.iter_normalized_book_rows_from_file(
            path,
            SYMBOL,
            0.1,
            backtest.Stats(),
            backtest.BookCleaningOptions(),
            backtest.TimeFilter(start_ts=1776863000.0, end_ts=1776864500.0),
            assume_sorted=True,
        )
    )

    assert len(rows) == 1
    assert calls["count"] == 1


def test_filename_date_hint_filters_large_directory_to_padded_utc_dates(tmp_path):
    candidates = [tmp_path / f"trades-2026-04-{day:02d}.zip" for day in range(1, 31)]
    requested = backtest.TimeFilter(_ts("2026-04-20T00:00:00Z"), _ts("2026-04-20T01:00:00Z"))

    filtered = backtest.filter_candidates_by_filename_date_hint(
        candidates,
        requested,
        timezone_name="Asia/Shanghai",
        padding_days=2,
    )
    names = {path.name for path in filtered}

    assert "trades-2026-04-01.zip" not in names
    assert names == {f"trades-2026-04-{day:02d}.zip" for day in range(18, 23)}


def test_filename_date_hint_for_local_time_window_keeps_requested_utc_neighborhood(tmp_path):
    candidates = [tmp_path / f"trades-2026-05-{day:02d}.zip" for day in range(1, 32)]
    requested = backtest.TimeFilter(_ts("2026-05-20T16:00:00Z"), _ts("2026-05-20T17:00:00Z"))

    filtered = backtest.filter_candidates_by_filename_date_hint(
        candidates,
        requested,
        timezone_name="Asia/Shanghai",
        padding_days=2,
    )
    names = {path.name for path in filtered}

    assert names == {f"trades-2026-05-{day:02d}.zip" for day in range(18, 23)}
    assert "trades-2026-05-01.zip" not in names


def test_filename_date_hint_keeps_files_without_parseable_date(tmp_path):
    candidates = [tmp_path / "trades-latest.zip", tmp_path / "trades-2026-04-01.zip"]
    requested = backtest.TimeFilter(_ts("2026-04-20T00:00:00Z"), _ts("2026-04-20T01:00:00Z"))

    filtered = backtest.filter_candidates_by_filename_date_hint(
        candidates,
        requested,
        timezone_name="Asia/Shanghai",
        padding_days=2,
    )

    assert tmp_path / "trades-latest.zip" in filtered
    assert tmp_path / "trades-2026-04-01.zip" not in filtered


def test_filename_date_hint_fallbacks_to_all_candidates_when_empty(tmp_path, capsys):
    candidates = [tmp_path / "trades-2026-04-01.zip", tmp_path / "trades-2026-04-02.zip"]
    requested = backtest.TimeFilter(_ts("2026-05-20T00:00:00Z"), _ts("2026-05-20T01:00:00Z"))

    filtered = backtest.filter_candidates_by_filename_date_hint(
        candidates,
        requested,
        timezone_name="Asia/Shanghai",
        padding_days=0,
    )
    captured = capsys.readouterr()

    assert filtered == candidates
    assert "LOCAL-FILENAME-HINT-WARNING" in captured.err


def test_select_files_full_directory_scan_skips_hint_filter(tmp_path, monkeypatch):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    for day in range(1, 31):
        (trades_dir / f"trades-2026-04-{day:02d}.csv").write_text("", encoding="utf-8")
    seen = []

    def fake_get_file_coverage(path, **kwargs):
        seen.append(path.name)
        return backtest.FileCoverage(path=path, first_ts=_ts("2026-04-20T00:00:00Z"), last_ts=_ts("2026-04-20T01:00:00Z"))

    monkeypatch.setattr(backtest, "get_file_coverage", fake_get_file_coverage)
    summary = {}
    backtest.select_files_for_replay(
        trades_dir,
        "trade",
        SYMBOL,
        0.1,
        backtest.BookCleaningOptions(),
        backtest.TimeFilter(_ts("2026-04-20T00:00:00Z"), _ts("2026-04-20T01:00:00Z")),
        auto_discover=True,
        coverage_tolerance_sec=60.0,
        full_directory_coverage_scan=True,
        filename_hint_summary=summary,
    )

    assert len(seen) == 30
    assert summary["trades_candidates_before"] == 30
    assert summary["trades_candidates_after"] == 30


def test_filename_hint_only_limits_scan_candidates_final_selection_uses_row_ts(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    inside = trades_dir / "trades-2026-04-20.csv"
    outside = trades_dir / "trades-2026-04-21.csv"
    far = trades_dir / "trades-2026-04-01.csv"
    inside.write_text("instId,px,sz,side,ts\nETH-USDT-SWAP,2500,1,buy,1776645000000\n", encoding="utf-8")
    outside.write_text("instId,px,sz,side,ts\nETH-USDT-SWAP,2500,1,buy,1776902400000\n", encoding="utf-8")
    far.write_text("instId,px,sz,side,ts\nETH-USDT-SWAP,2500,1,buy,1775001600000\n", encoding="utf-8")
    requested = backtest.TimeFilter(_ts("2026-04-20T00:00:00Z"), _ts("2026-04-20T01:00:00Z"))
    summary = {}

    selection = backtest.select_files_for_replay(
        trades_dir,
        "trade",
        SYMBOL,
        0.1,
        backtest.BookCleaningOptions(),
        requested,
        auto_discover=True,
        coverage_tolerance_sec=0.0,
        filename_date_hint_enabled=True,
        filename_date_hint_padding_days=1,
        full_directory_coverage_scan=False,
        filename_hint_summary=summary,
    )

    assert summary["trades_candidates_before"] == 3
    assert summary["trades_candidates_after"] == 2
    assert selection.paths == [inside]
