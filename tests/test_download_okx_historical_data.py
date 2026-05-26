import argparse
import importlib
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

downloader = importlib.import_module("tools.download_okx_historical_data")


def _base_args(**overrides):
    values = {
        "kind": "books",
        "symbol": "ETH-USDT-SWAP",
        "start_date": "2026-05-01",
        "end_date": "2026-05-01",
        "url_template": None,
        "url": [],
        "manifest": None,
        "out_root": Path("/tmp/okx/raw"),
        "books_module": "",
        "books_depth": 400,
        "inst_type": "AUTO",
        "inst_family": "",
        "date_aggr": "daily",
        "export_timeout": 1,
        "export_retries": 1,
        "export_backoff_sec": 0,
        "export_sleep_sec": 0,
        "allow_missing_book_days": False,
        "chunk_days": 1,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_trades_template_date_range_includes_start_and_end():
    args = _base_args(
        kind="trades",
        start_date="2026-05-01",
        end_date="2026-05-03",
        url_template="https://example.test/{yyyymmdd}/{symbol}-trades-{date}.zip",
    )

    tasks = list(downloader.build_tasks(args))

    assert [task.date_tag for task in tasks] == ["2026-05-01", "2026-05-02", "2026-05-03"]


def test_books_export_uses_date_boundaries_and_filters_adjacent_links(monkeypatch):
    calls = []

    def fake_request_okx_export_links(**kwargs):
        calls.append(kwargs)
        return {
            "details": [
                {
                    "groupDetails": [
                        {
                            "url": "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily/20260502/ETH-USDT-SWAP-L2orderbook-400lv-2026-05-02.tar.gz",
                            "fileName": "ETH-USDT-SWAP-L2orderbook-400lv-2026-05-02.tar.gz",
                        },
                        {
                            "url": "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily/20260501/ETH-USDT-SWAP-L2orderbook-400lv-2026-05-01.tar.gz",
                            "fileName": "ETH-USDT-SWAP-L2orderbook-400lv-2026-05-01.tar.gz",
                        },
                    ]
                }
            ]
        }

    monkeypatch.setattr(downloader, "request_okx_export_links", fake_request_okx_export_links)

    tasks = list(downloader.build_tasks(_base_args()))

    assert calls[0]["begin_ms"] == str(downloader.date_start_ms(date(2026, 5, 1)))
    assert calls[0]["end_ms"] == str(downloader.date_start_ms(date(2026, 5, 1)))
    assert len(tasks) == 1
    assert tasks[0].date_tag == "2026-05-01_2026-05-01"
    assert tasks[0].output_path.name == "ETH-USDT-SWAP-L2orderbook-400lv-2026-05-01.tar.gz"
