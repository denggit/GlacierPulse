import inspect
import json
from datetime import datetime
from pathlib import Path

import pytest

from src.context.market_context import MarketContext
from tools import backtest_local_data as backtest


SYMBOL = "ETH-USDT-SWAP"


def _ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _trade(ts: str, size: float = 2.0) -> dict:
    return {"instId": SYMBOL, "ts": _ts(ts), "px": 3000.0, "sz": size, "side": "buy"}


def _book(ts: str, bid_size: float = 10.0, ask_size: float = 12.0) -> dict:
    return {
        "instId": SYMBOL,
        "ts": _ts(ts),
        "bids": [[2999.0, bid_size]],
        "asks": [[3001.0, ask_size]],
        "action": "snapshot",
    }


def _fake_dataset(tmp_path: Path, include_books_0422: bool = True) -> tuple[Path, Path]:
    trades_dir = tmp_path / "trades"
    books_dir = tmp_path / "books"
    trades_dir.mkdir()
    books_dir.mkdir()
    _write_jsonl(
        trades_dir / "trades-2026-04-23.jsonl",
        [
            _trade("2026-04-22T16:00:00Z", size=2.0),
            _trade("2026-04-23T00:00:00Z", size=3.0),
            _trade("2026-04-23T16:00:00Z", size=4.0),
        ],
    )
    if include_books_0422:
        _write_jsonl(
            books_dir / "books-2026-04-22.jsonl",
            [
                _book("2026-04-22T00:00:00Z", bid_size=10.0, ask_size=12.0),
                _book("2026-04-22T16:00:00Z", bid_size=11.0, ask_size=13.0),
                _book("2026-04-23T00:00:00Z", bid_size=14.0, ask_size=16.0),
            ],
        )
    _write_jsonl(
        books_dir / "books-2026-04-23.jsonl",
        [
            _book("2026-04-23T00:00:00Z", bid_size=14.0, ask_size=16.0),
            _book("2026-04-24T00:00:00Z", bid_size=18.0, ask_size=20.0),
        ],
    )
    return trades_dir, books_dir


def _parsed_window(trades_dir: Path, books_dir: Path):
    args = backtest.parse_args(
        [
            "--trades-dir",
            str(trades_dir),
            "--books-dir",
            str(books_dir),
            "--start-date",
            "2026-04-23",
            "--end-date",
            "2026-04-23",
            "--timezone",
            "Asia/Shanghai",
        ]
    )
    return backtest.parse_requested_window(args)


def test_local_date_window_converts_to_utc_without_shifting_row_timestamps(tmp_path):
    trades_dir, books_dir = _fake_dataset(tmp_path)
    requested = _parsed_window(trades_dir, books_dir)
    multiplier = backtest.CONTRACT_MULTIPLIERS[SYMBOL]
    options = backtest.BookCleaningOptions(bucket_ms=0.0)

    trades = backtest.select_files_for_replay(
        trades_dir, "trade", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )
    books = backtest.select_files_for_replay(
        books_dir, "book", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )
    effective, alignment = backtest.resolve_effective_time_filter(requested, trades, books, require_full_coverage=True)

    assert requested.requested_start_utc == "2026-04-22T16:00:00Z"
    assert requested.requested_end_utc == "2026-04-23T16:00:00Z"
    assert alignment["partial_coverage_used"] is False
    assert alignment["full_coverage"] is True
    assert [Path(path).name for path in alignment["books_selected_files"]] == [
        "books-2026-04-22.jsonl",
        "books-2026-04-23.jsonl",
    ]

    events = list(
        backtest.build_events(
            trades.paths,
            books.paths,
            SYMBOL,
            multiplier,
            backtest.Stats(),
            sort_in_memory=False,
            book_cleaning=options,
            time_filter=effective,
        )
    )
    assert events[0].ts == _ts("2026-04-22T16:00:00Z")
    assert all(event.ts < _ts("2026-04-23T16:00:00Z") for event in events)
    first_trade = next(event for event in events if event.kind == "trade")
    assert first_trade.payload["ts"] == _ts("2026-04-22T16:00:00Z")
    assert first_trade.payload["size"] == pytest.approx(0.2)
    assert multiplier == pytest.approx(0.1)


def test_missing_books_file_fails_full_coverage_and_partial_uses_intersection(tmp_path):
    trades_dir, books_dir = _fake_dataset(tmp_path, include_books_0422=False)
    requested = _parsed_window(trades_dir, books_dir)
    multiplier = backtest.CONTRACT_MULTIPLIERS[SYMBOL]
    options = backtest.BookCleaningOptions(bucket_ms=0.0)
    trades = backtest.select_files_for_replay(
        trades_dir, "trade", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )
    books = backtest.select_files_for_replay(
        books_dir, "book", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )

    with pytest.raises(SystemExit) as exc:
        backtest.resolve_effective_time_filter(requested, trades, books, require_full_coverage=True)
    message = str(exc.value)
    assert "requested local window" in message
    assert "requested UTC window" in message
    assert "trades coverage" in message
    assert "books coverage" in message
    assert "books missing 2026-04-22T16:00:00Z to 2026-04-23T00:00:00Z" in message

    effective, alignment = backtest.resolve_effective_time_filter(requested, trades, books, require_full_coverage=False)
    assert effective.start_ts == _ts("2026-04-23T00:00:00Z")
    assert effective.end_ts == _ts("2026-04-23T16:00:00Z")
    assert alignment["partial_coverage_used"] is True
    assert alignment["warnings"]


def test_book_warmup_builds_context_before_start_without_engine_update(tmp_path):
    trades_dir, books_dir = _fake_dataset(tmp_path)
    requested = _parsed_window(trades_dir, books_dir)
    multiplier = backtest.CONTRACT_MULTIPLIERS[SYMBOL]
    options = backtest.BookCleaningOptions(bucket_ms=0.0)
    books = backtest.select_files_for_replay(
        books_dir, "book", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )

    class DummyRuntime:
        def __init__(self):
            self.ctx = MarketContext()
            self.engine_calls = 0

        def on_book_update(self, book_data, stats):
            self.engine_calls += 1

    runtime = DummyRuntime()
    warmup = backtest.warmup_books_state(runtime, books.paths, SYMBOL, multiplier, options, requested.time_filter.start_ts)

    assert warmup["book_warmup_rows"] == 1
    assert warmup["book_bootstrap_bid_levels"] == 1
    assert warmup["book_bootstrap_ask_levels"] == 1
    assert runtime.ctx.bids[2999.0] == pytest.approx(1.0)
    assert runtime.ctx.asks[3001.0] == pytest.approx(1.2)
    assert runtime.engine_calls == 0


def test_replay_merges_multiple_files_by_real_timestamp_not_filename(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    _write_jsonl(trades_dir / "z-later.jsonl", [_trade("2026-04-23T00:00:00Z")])
    _write_jsonl(trades_dir / "a-earlier.jsonl", [_trade("2026-04-22T16:00:00Z")])
    requested = backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-04-22T16:00:00Z"), _ts("2026-04-23T16:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="",
        requested_end_local="",
        requested_start_utc="",
        requested_end_utc="",
    )
    multiplier = backtest.CONTRACT_MULTIPLIERS[SYMBOL]
    options = backtest.BookCleaningOptions(bucket_ms=0.0)
    trades = backtest.select_files_for_replay(
        trades_dir, "trade", SYMBOL, multiplier, options, requested.time_filter, auto_discover=True
    )

    events = list(
        backtest.build_events(
            trades.paths,
            [],
            SYMBOL,
            multiplier,
            backtest.Stats(),
            sort_in_memory=False,
            book_cleaning=options,
            time_filter=requested.time_filter,
        )
    )
    assert [event.ts for event in events] == [_ts("2026-04-22T16:00:00Z"), _ts("2026-04-23T00:00:00Z")]


def test_local_replay_runtime_does_not_instantiate_trader_or_pnl_paths():
    source = inspect.getsource(backtest.LocalA1ResearchRuntime)
    assert "IcebergTrader(" not in source
    assert "process_signal(" not in source
    assert "pnl" not in source.lower()


def test_multi_day_local_date_window_uses_asia_shanghai_by_default():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--start-date",
            "2026-05-21",
            "--end-date",
            "2026-05-23",
            "--timezone",
            "Asia/Shanghai",
        ]
    )
    requested = backtest.parse_requested_window(args)

    assert requested.requested_start_local == "2026-05-21T00:00:00+08:00"
    assert requested.requested_end_local == "2026-05-24T00:00:00+08:00"
    assert requested.requested_start_utc == "2026-05-20T16:00:00Z"
    assert requested.requested_end_utc == "2026-05-23T16:00:00Z"


def test_utc_date_mode_preserves_old_utc_day_semantics():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.csv",
            "--books-file",
            "books.csv",
            "--date-mode",
            "utc",
            "--start-date",
            "2026-05-21",
            "--end-date",
            "2026-05-23",
        ]
    )
    requested = backtest.parse_requested_window(args)

    assert requested.requested_start_utc == "2026-05-21T00:00:00Z"
    assert requested.requested_end_utc == "2026-05-24T00:00:00Z"


def test_coverage_boundary_tolerance_accepts_subsecond_edge_gaps(tmp_path):
    requested = backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-05-20T16:00:00Z"), _ts("2026-05-23T16:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="2026-05-21T00:00:00+08:00",
        requested_end_local="2026-05-24T00:00:00+08:00",
        requested_start_utc="2026-05-20T16:00:00Z",
        requested_end_utc="2026-05-23T16:00:00Z",
    )
    trades = backtest.CoverageSelection(
        selected=[
            backtest.FileCoverage(
                tmp_path / "trades.csv",
                _ts("2026-05-20T16:00:00.082Z"),
                _ts("2026-05-23T15:59:59.731Z"),
                row_count=2,
            )
        ],
        all_coverage=[],
    )
    books = backtest.CoverageSelection(
        selected=[backtest.FileCoverage(tmp_path / "books.csv", _ts("2026-05-20T16:00:00Z"), _ts("2026-05-23T16:00:00Z"))],
        all_coverage=[],
    )

    _, alignment = backtest.resolve_effective_time_filter(
        requested,
        trades,
        books,
        require_full_coverage=True,
        coverage_tolerance_sec=60.0,
    )
    assert alignment["full_coverage"] is True
    assert alignment["coverage_boundary_tolerance_sec"] == pytest.approx(60.0)


def test_coverage_tolerance_does_not_hide_ten_minute_gap(tmp_path):
    requested = backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-05-20T16:00:00Z"), _ts("2026-05-23T16:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="2026-05-21T00:00:00+08:00",
        requested_end_local="2026-05-24T00:00:00+08:00",
        requested_start_utc="2026-05-20T16:00:00Z",
        requested_end_utc="2026-05-23T16:00:00Z",
    )
    trades = backtest.CoverageSelection(
        selected=[
            backtest.FileCoverage(tmp_path / "trades-a.csv", _ts("2026-05-20T16:00:00Z"), _ts("2026-05-21T00:00:00Z")),
            backtest.FileCoverage(tmp_path / "trades-b.csv", _ts("2026-05-21T00:10:00Z"), _ts("2026-05-23T16:00:00Z")),
        ],
        all_coverage=[],
    )
    books = backtest.CoverageSelection(
        selected=[backtest.FileCoverage(tmp_path / "books.csv", _ts("2026-05-20T16:00:00Z"), _ts("2026-05-23T16:00:00Z"))],
        all_coverage=[],
    )

    with pytest.raises(SystemExit) as exc:
        backtest.resolve_effective_time_filter(
            requested,
            trades,
            books,
            require_full_coverage=True,
            coverage_tolerance_sec=60.0,
        )
    message = str(exc.value)
    assert "trades missing" in message
    assert "coverage boundary tolerance: 60.0 sec" in message


def test_partial_coverage_moves_effective_start_and_records_warning(tmp_path):
    requested = backtest.RequestedWindow(
        time_filter=backtest.TimeFilter(_ts("2026-05-20T16:00:00Z"), _ts("2026-05-21T16:00:00Z")),
        timezone_name="Asia/Shanghai",
        date_mode="local",
        requested_start_local="2026-05-21T00:00:00+08:00",
        requested_end_local="2026-05-22T00:00:00+08:00",
        requested_start_utc="2026-05-20T16:00:00Z",
        requested_end_utc="2026-05-21T16:00:00Z",
    )
    trades = backtest.CoverageSelection(
        selected=[backtest.FileCoverage(tmp_path / "trades.csv", _ts("2026-05-20T16:00:00Z"), _ts("2026-05-21T16:00:00Z"))],
        all_coverage=[],
    )
    books = backtest.CoverageSelection(
        selected=[backtest.FileCoverage(tmp_path / "books.csv", _ts("2026-05-21T00:00:00Z"), _ts("2026-05-21T16:00:00Z"))],
        all_coverage=[],
    )

    with pytest.raises(SystemExit):
        backtest.resolve_effective_time_filter(
            requested,
            trades,
            books,
            require_full_coverage=True,
            coverage_tolerance_sec=60.0,
        )

    effective, alignment = backtest.resolve_effective_time_filter(
        requested,
        trades,
        books,
        require_full_coverage=False,
        coverage_tolerance_sec=60.0,
    )
    assert effective.start_ts == _ts("2026-05-21T00:00:00Z")
    assert alignment["full_coverage"] is False
    assert alignment["partial_coverage_used"] is True
    assert alignment["warnings"]
