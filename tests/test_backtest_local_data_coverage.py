import inspect
import json
from datetime import datetime
from pathlib import Path

import pytest

from tools import backtest_local_data as backtest


SYMBOL = "ETH-USDT-SWAP"


def _ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _trade(ts: float, price: float = 2500.0, size: float = 10.0) -> dict:
    return {"instId": SYMBOL, "px": price, "sz": size, "side": "buy", "ts": ts}


def _book(ts: float, bid: float = 2499.0, ask: float = 2501.0) -> dict:
    return {
        "instId": SYMBOL,
        "ts": ts,
        "bids": [[bid, 10.0]],
        "asks": [[ask, 10.0]],
        "action": "snapshot",
    }


def _touch_days(directory: Path, prefix: str, days: list[str], suffix: str = ".csv") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for day in days:
        (directory / f"{prefix}-{day}{suffix}").write_text("", encoding="utf-8")


def test_select_files_same_utc_date(tmp_path):
    trades_dir = tmp_path / "trades"
    books_dir = tmp_path / "books"
    days = ["2026-05-20", "2026-05-21", "2026-05-22"]
    _touch_days(trades_dir, "trades", days)
    _touch_days(books_dir, "books", days)
    time_filter = backtest.TimeFilter(_ts("2026-05-21T01:00:00Z"), _ts("2026-05-21T02:00:00Z"))
    date_set = backtest.utc_date_set_for_filter(time_filter)

    trades = backtest.select_files_by_utc_dates(trades_dir, date_set, "trades")
    books = backtest.select_files_by_utc_dates(books_dir, date_set, "books")

    assert [path.name for path in trades] == ["trades-2026-05-21.csv"]
    assert [path.name for path in books] == ["books-2026-05-21.csv"]


def test_select_files_cross_utc_date(tmp_path):
    trades_dir = tmp_path / "trades"
    books_dir = tmp_path / "books"
    days = ["2026-05-21", "2026-05-22"]
    _touch_days(trades_dir, "trades", days)
    _touch_days(books_dir, "books", days)
    time_filter = backtest.TimeFilter(_ts("2026-05-21T23:30:00Z"), _ts("2026-05-22T00:30:00Z"))
    date_set = backtest.utc_date_set_for_filter(time_filter)

    trades = backtest.select_files_by_utc_dates(trades_dir, date_set, "trades")
    books = backtest.select_files_by_utc_dates(books_dir, date_set, "books")

    assert [path.name for path in trades] == ["trades-2026-05-21.csv", "trades-2026-05-22.csv"]
    assert [path.name for path in books] == ["books-2026-05-21.csv", "books-2026-05-22.csv"]


def test_does_not_scan_unrelated_dates(tmp_path, monkeypatch):
    trades_dir = tmp_path / "trades"
    _touch_days(trades_dir, "trades", [f"2026-04-{day:02d}" for day in range(1, 31)])
    _touch_days(trades_dir, "trades", [f"2026-05-{day:02d}" for day in range(1, 31)])
    time_filter = backtest.TimeFilter(_ts("2026-05-21T01:00:00Z"), _ts("2026-05-21T02:00:00Z"))

    def fail_if_scanned(*args, **kwargs):
        raise AssertionError("select_files_by_utc_dates must not read file contents")

    monkeypatch.setattr(backtest, "iter_rows", fail_if_scanned)
    selected = backtest.select_files_by_utc_dates(trades_dir, backtest.utc_date_set_for_filter(time_filter), "trades")

    assert [path.name for path in selected] == ["trades-2026-05-21.csv"]


def test_missing_utc_date_raises(tmp_path):
    trades_dir = tmp_path / "trades"
    _touch_days(trades_dir, "trades", ["2026-05-21"])
    date_set = {"2026-05-21", "2026-05-22"}

    with pytest.raises(SystemExit) as exc:
        backtest.select_files_by_utc_dates(trades_dir, date_set, "trades")

    assert "Missing trades files for UTC date(s): 2026-05-22" in str(exc.value)


def test_row_timestamp_not_shifted(tmp_path):
    ts = _ts("2026-05-21T01:00:00Z")
    trade_file = tmp_path / "trades-2026-05-21.jsonl"
    book_file = tmp_path / "books-2026-05-21.jsonl"
    _write_jsonl(trade_file, [_trade(ts)])
    _write_jsonl(book_file, [_book(ts)])
    stats = backtest.Stats()
    time_filter = backtest.TimeFilter(ts - 1, ts + 1)

    events = list(
        backtest.build_events(
            [trade_file],
            [book_file],
            SYMBOL,
            0.1,
            stats,
            sort_in_memory=False,
            book_cleaning=backtest.BookCleaningOptions(bucket_ms=0.0),
            time_filter=time_filter,
        )
    )

    assert events
    assert {event.ts for event in events} == {ts}
    assert all(event.payload["ts"] == ts for event in events)


def test_trades_books_merge_by_row_ts(tmp_path):
    trade_file = tmp_path / "z-trades-2026-05-21.jsonl"
    book_file = tmp_path / "a-books-2026-05-21.jsonl"
    t1 = _ts("2026-05-21T01:00:01Z")
    t2 = _ts("2026-05-21T01:00:00Z")
    _write_jsonl(trade_file, [_trade(t1)])
    _write_jsonl(book_file, [_book(t2)])

    events = list(
        backtest.build_events(
            [trade_file],
            [book_file],
            SYMBOL,
            0.1,
            backtest.Stats(),
            sort_in_memory=False,
            book_cleaning=backtest.BookCleaningOptions(bucket_ms=0.0),
            time_filter=backtest.TimeFilter(t2 - 1, t1 + 1),
        )
    )

    assert [event.kind for event in events] == ["book", "trade"]
    assert [event.ts for event in events] == [t2, t1]


def test_fast_ts_prefilter_before_normalize_book(tmp_path, monkeypatch):
    path = tmp_path / "books-2026-05-21.csv"
    path.write_text(
        "\n".join(
            [
                "instId,ts,bids,asks",
                'ETH-USDT-SWAP,1779325200000,"[[2500,10]]","[[2501,10]]"',
                'ETH-USDT-SWAP,1779328800000,"not-json-after-end","bad"',
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
            backtest.TimeFilter(start_ts=1779325199.0, end_ts=1779325300.0),
            assume_sorted=True,
        )
    )

    assert len(rows) == 1
    assert calls["count"] == 1


def test_research_only_no_trader_no_pnl():
    module_source = inspect.getsource(backtest)
    runtime_source = inspect.getsource(backtest.LocalA1ResearchRuntime)

    assert "IcebergTrader(" not in module_source
    assert "process_signal(" not in runtime_source
    assert "opens_or_closes_positions" in module_source
    assert "simulates_pnl" in module_source
