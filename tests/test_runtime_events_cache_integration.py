import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.research.runtime_three_a.runtime_event_builder import (
    RuntimeEventAccumulator,
    RuntimeEventCacheManager,
    RuntimeEventDailyCacheWriter,
)
from tools import backtest_local_data as backtest


def _write_replay_inputs(tmp_path: Path, days=("2026-04-01",)) -> tuple[Path, Path, Path]:
    trades_dir = tmp_path / "trades"
    books = tmp_path / "books.jsonl"
    kline = tmp_path / "kline.csv"
    trades_dir.mkdir(exist_ok=True)
    base_ts = {
        "2026-04-01": 1775001600.1,
        "2026-04-02": 1775088000.1,
    }
    for day in days:
        ts = base_ts[day]
        (trades_dir / f"ETH-USDT-SWAP-trades-{day}.jsonl").write_text(
            "\n".join(
                [
                    json.dumps({"ts": ts, "price": 100.0, "size": 10.0, "side": "buy", "instId": "ETH-USDT-SWAP"}),
                    json.dumps({"ts": ts + 1.0, "price": 101.0, "size": 5.0, "side": "sell", "instId": "ETH-USDT-SWAP"}),
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    books.write_text("", encoding="utf-8")
    kline.write_text("ts,open,high,low,close,volume\n", encoding="utf-8")
    return trades_dir, books, kline


def _patch_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    class Runtime:
        def __init__(self, symbol, research_events_path, **_kwargs):
            self.research_events_file = Path(research_events_path).open("w", encoding="utf-8")
            self.ctx = type("Ctx", (), {"bids": {}, "asks": {}})()

        def close(self):
            self.research_events_file.close()

        def on_trade_tick(self, trade_data, stats):
            return None

        def on_book_update(self, book_data, stats):
            return None

    monkeypatch.setattr(backtest, "LocalA1ResearchRuntime", Runtime)


def _run_backtest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, days=("2026-04-01",), generate_reports=False):
    _patch_runtime(monkeypatch)
    trades_dir, books, kline = _write_replay_inputs(tmp_path, days=days)
    out = tmp_path / "out"
    argv = [
        "--symbol",
        "ETH-USDT-SWAP",
        "--contract-multiplier",
        "0.01",
        "--trades-dir",
        str(trades_dir),
        "--books-file",
        str(books),
        "--out-dir",
        str(out),
        "--runtime-events-cache-root",
        str(tmp_path / "cache"),
    ]
    if generate_reports:
        argv.extend(["--generate-reports", "--kline", str(kline)])
    code = backtest.main(argv)
    return code, out, tmp_path / "cache"


def test_runtime_events_cache_path_is_parameterized(tmp_path):
    a = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01).cache_dir()
    b = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 2, 3, 0.01).cache_dir()
    c = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 5, 0.01).cache_dir()
    d = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 1.0).cache_dir()
    assert len({a, b, c, d}) == 4
    assert a.name == "bucket_1s_rolling_3s_cm_0p01"


def test_backtest_local_data_generates_missing_day_cache(tmp_path, monkeypatch):
    code, out, cache_root = _run_backtest(tmp_path, monkeypatch)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["cache_miss_days"] == ["2026-04-01"]
    assert ref["generated_days"] == ["2026-04-01"]
    assert (Path(ref["cache_dir"]) / "2026-04-01.jsonl").exists()
    assert not list(out.rglob("*.jsonl.tmp"))


def test_backtest_local_data_reuses_existing_day_cache(tmp_path, monkeypatch):
    _run_backtest(tmp_path, monkeypatch)
    shard = next((tmp_path / "cache").rglob("2026-04-01.jsonl"))
    mtime = shard.stat().st_mtime_ns
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert shard.stat().st_mtime_ns == mtime
    assert ref["cache_hit_days"] == ["2026-04-01"]
    assert ref["generated_days"] == []


def test_backtest_local_data_partial_cache_hit(tmp_path, monkeypatch):
    _run_backtest(tmp_path, monkeypatch, days=("2026-04-01",))
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch, days=("2026-04-01", "2026-04-02"))
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["cache_hit_days"] == ["2026-04-01"]
    assert ref["cache_miss_days"] == ["2026-04-02"]
    assert ref["generated_days"] == ["2026-04-02"]


def test_reports_do_not_copy_runtime_events(tmp_path, monkeypatch):
    import tools.generate_research_reports as reports

    monkeypatch.setattr(reports, "main", lambda _argv: 0)
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch, generate_reports=True)
    assert code == 0
    assert (out / "runtime_events_ref.json").exists()
    assert not any(path.name.startswith("runtime_events") and path.suffix == ".jsonl" for path in out.rglob("*"))


def test_analyze_zone_truth_auto_receives_runtime_cache(tmp_path, monkeypatch):
    import tools.generate_research_reports as reports

    captured = {}
    monkeypatch.setattr(reports, "main", lambda argv: captured.setdefault("argv", argv) and 0)
    code, _out, _cache_root = _run_backtest(tmp_path, monkeypatch, generate_reports=True)
    argv = captured["argv"]
    assert code == 0
    assert "--runtime-events" in argv
    assert "data/derived/runtime_events" not in argv[argv.index("--runtime-events") + 1]


def test_live_mode_does_not_require_runtime_events_file():
    accumulator = RuntimeEventAccumulator("ETH-USDT-SWAP", 1, 3, 0.01)
    assert accumulator.update_trade(1775001600.1, 100.0, "buy", size=10.0) == []
    events = accumulator.update_trade(1775001601.1, 101.0, "sell", size=5.0)
    assert events[0]["active_buy_notional_3s"] == 10.0
    assert events[0]["active_sell_notional_3s"] == 0.0
    assert set(events[0]) >= {"cvd_delta_3s", "price_velocity_u_per_sec", "condition_available_ts"}


def test_runtime_events_cache_atomic_day_write(tmp_path):
    path = tmp_path / "2026-04-01.jsonl"
    writer = RuntimeEventDailyCacheWriter(path)
    writer.write({"ts": 1775001600.1})
    writer.cleanup()
    assert not path.exists()
    assert not path.with_name("2026-04-01.jsonl.tmp").exists()


def test_runtime_events_cache_lock(tmp_path):
    path = tmp_path / "2026-04-01.jsonl"
    writer = RuntimeEventDailyCacheWriter(path)
    try:
        with pytest.raises(RuntimeError):
            RuntimeEventDailyCacheWriter(path)
    finally:
        writer.cleanup()


def test_existing_builder_cli_still_works(tmp_path):
    trades = tmp_path / "trades.jsonl"
    out = tmp_path / "runtime_events.jsonl"
    trades.write_text(
        '{"ts":1775001600100,"px":"100","sz":"10","side":"buy","instId":"ETH-USDT-SWAP"}\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            sys.executable,
            "tools/build_runtime_events_from_okx_trades.py",
            "--symbol",
            "ETH-USDT-SWAP",
            "--trades-dir",
            str(trades),
            "--out",
            str(out),
            "--contract-multiplier",
            "0.01",
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert out.exists()
