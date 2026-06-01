import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.research.runtime_three_a.contract_specs import resolve_contract_multiplier
from src.research.runtime_three_a import runtime_event_builder as runtime_builder
from src.research.runtime_three_a.runtime_event_builder import (
    RuntimeEventAccumulator,
    RuntimeEventCacheManager,
    RuntimeEventDailyCacheWriter,
    RuntimeEventDayStats,
    runtime_events_param_key,
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


def _utc_ts(day: str, hour: int, minute: int, second: float) -> float:
    whole_second = int(second)
    microsecond = int(round((float(second) - whole_second) * 1_000_000))
    return datetime.fromisoformat(day).replace(
        hour=hour,
        minute=minute,
        second=whole_second,
        microsecond=microsecond,
        tzinfo=timezone.utc,
    ).timestamp()


def _write_replay_inputs_from_rows(tmp_path: Path, rows_by_day: dict[str, list[dict]]) -> tuple[Path, Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    trades_dir = tmp_path / "trades"
    books = tmp_path / "books.jsonl"
    kline = tmp_path / "kline.csv"
    trades_dir.mkdir(exist_ok=True)
    for day, rows in rows_by_day.items():
        (trades_dir / f"ETH-USDT-SWAP-trades-{day}.jsonl").write_text(
            "\n".join(json.dumps(row) for row in rows) + "\n",
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


def _run_backtest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    days=("2026-04-01",),
    generate_reports=False,
    contract_multiplier: float | None = 0.01,
    extra_args: list[str] | None = None,
):
    _patch_runtime(monkeypatch)
    trades_dir, books, kline = _write_replay_inputs(tmp_path, days=days)
    out = tmp_path / "out"
    argv = [
        "--symbol",
        "ETH-USDT-SWAP",
        "--trades-dir",
        str(trades_dir),
        "--books-file",
        str(books),
        "--out-dir",
        str(out),
        "--runtime-events-cache-root",
        str(tmp_path / "cache"),
    ]
    if contract_multiplier is not None:
        argv.extend(["--contract-multiplier", str(contract_multiplier)])
    if generate_reports:
        argv.extend(["--generate-reports", "--kline", str(kline)])
    if extra_args:
        argv.extend(extra_args)
    code = backtest.main(argv)
    return code, out, tmp_path / "cache"


def _run_backtest_with_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows_by_day: dict[str, list[dict]],
    *,
    cache_root: Path | None = None,
    out_name: str = "out",
):
    _patch_runtime(monkeypatch)
    trades_dir, books, _kline = _write_replay_inputs_from_rows(tmp_path, rows_by_day)
    out = tmp_path / out_name
    cache = cache_root or tmp_path / "cache"
    code = backtest.main(
        [
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
            str(cache),
        ]
    )
    return code, out, cache


def _runtime_cache_shard(cache_root: Path, day: str) -> Path:
    return next(cache_root.rglob(f"{day}.jsonl"))


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _build_runtime_events_for_rows(tmp_path: Path, rows_by_day: dict[str, list[dict]]) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    trades = tmp_path / "raw_trades.jsonl"
    rows = [row for day in sorted(rows_by_day) for row in rows_by_day[day]]
    trades.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    out_dir = tmp_path / "runtime_events"
    runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_dir=out_dir,
        shard_by="day",
        contract_multiplier=0.01,
    )
    return out_dir


def _write_dated_replay_dirs(tmp_path: Path, days=("2026-04-01", "2026-04-02")) -> tuple[Path, Path, Path]:
    trades_dir = tmp_path / "trades"
    books_dir = tmp_path / "books"
    kline = tmp_path / "kline.csv"
    trades_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)
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
        (books_dir / f"ETH-USDT-SWAP-books-{day}.jsonl").write_text("", encoding="utf-8")
    kline.write_text("ts,open,high,low,close,volume\n", encoding="utf-8")
    return trades_dir, books_dir, kline


def _day_boundary_rows() -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    day1_buy = {
        "ts": _utc_ts("2026-04-01", 23, 59, 59.5),
        "price": 100.0,
        "size": 10.0,
        "side": "buy",
        "instId": "ETH-USDT-SWAP",
    }
    day2_sell = {
        "ts": _utc_ts("2026-04-02", 0, 0, 0.1),
        "price": 101.0,
        "size": 5.0,
        "side": "sell",
        "instId": "ETH-USDT-SWAP",
    }
    return (
        {"2026-04-01": [day1_buy], "2026-04-02": [day2_sell]},
        {"2026-04-02": [day2_sell]},
    )


def test_runtime_events_cache_path_is_parameterized(tmp_path):
    a = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01).cache_dir()
    b = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 2, 3, 0.01).cache_dir()
    c = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 5, 0.01).cache_dir()
    d = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 1.0).cache_dir()
    e = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01, schema_version="v.next").cache_dir()
    f = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01, notional_mode="raw_notional").cache_dir()
    assert len({a, b, c, d, e, f}) == 6
    assert a.name.startswith("bucket_1s_rolling_3s_cm_0p01_")
    assert "_nm_price_x_size_x_contract_multiplier_" in a.name
    assert "_sv_v7p3_runtime_events_1" in a.name


def test_runtime_events_param_key_includes_schema_and_notional():
    base = runtime_events_param_key(bucket_sec=1, rolling_sec=3, contract_multiplier=0.01)
    schema = runtime_events_param_key(bucket_sec=1, rolling_sec=3, contract_multiplier=0.01, schema_version="v7.3.runtime_events.2")
    notional = runtime_events_param_key(bucket_sec=1, rolling_sec=3, contract_multiplier=0.01, notional_mode="raw_notional")
    assert base != schema
    assert base != notional
    assert base.startswith("bucket_1s_rolling_3s_cm_0p01_")
    assert "_nm_price_x_size_x_contract_multiplier_" in base
    assert "_sv_v7p3_runtime_events_1" in base


def test_runtime_events_imports_available():
    assert RuntimeEventDayStats(day="2026-04-01").day == "2026-04-01"
    assert runtime_events_param_key(bucket_sec=1, rolling_sec=3, contract_multiplier=0.01)


def test_eth_swap_contract_multiplier_defaults_to_0p1():
    resolved = resolve_contract_multiplier("ETH-USDT-SWAP", None)
    assert resolved.multiplier == 0.1
    assert resolved.source == "okx_known_default"


def test_cli_explicit_multiplier_overrides_eth_default():
    resolved = resolve_contract_multiplier("ETH-USDT-SWAP", 0.01)
    assert resolved.multiplier == 0.01
    assert resolved.source == "cli_explicit"


def test_unknown_swap_requires_multiplier():
    with pytest.raises(ValueError, match="contract multiplier is required for unknown SWAP symbol: SOL-USDT-SWAP"):
        resolve_contract_multiplier("SOL-USDT-SWAP", None)


def test_non_swap_defaults_to_1():
    resolved = resolve_contract_multiplier("ETH-USDT", None)
    assert resolved.multiplier == 1.0
    assert resolved.source == "non_swap_default"


def test_backtest_local_data_generates_missing_day_cache(tmp_path, monkeypatch):
    code, out, cache_root = _run_backtest(tmp_path, monkeypatch)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["cache_miss_days"] == ["2026-04-01"]
    assert ref["generated_days"] == ["2026-04-01"]
    assert (Path(ref["cache_dir"]) / "2026-04-01.jsonl").exists()
    assert not list(out.rglob("*.jsonl.tmp"))


def test_backtest_local_data_eth_runs_without_contract_multiplier(tmp_path, monkeypatch):
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch, contract_multiplier=None)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["contract_multiplier"] == 0.1
    assert ref["contract_multiplier_source"] == "okx_known_default"


def test_runtime_events_ref_records_multiplier_source(tmp_path, monkeypatch):
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch, contract_multiplier=None)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["contract_multiplier"] == 0.1
    assert ref["contract_multiplier_source"] == "okx_known_default"


def test_cache_key_uses_default_eth_multiplier(tmp_path, monkeypatch):
    code, out, _cache_root = _run_backtest(tmp_path, monkeypatch, contract_multiplier=None)
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert "cm_0p1" in Path(ref["cache_dir"]).name


def test_cli_explicit_multiplier_uses_different_cache_key(tmp_path, monkeypatch):
    code, default_out, _cache_root = _run_backtest(tmp_path / "default", monkeypatch, contract_multiplier=None)
    explicit_code, explicit_out, _explicit_cache_root = _run_backtest(tmp_path / "explicit", monkeypatch, contract_multiplier=0.01)
    default_ref = json.loads((default_out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    explicit_ref = json.loads((explicit_out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert explicit_code == 0
    assert "cm_0p1" in Path(default_ref["cache_dir"]).name
    assert "cm_0p01" in Path(explicit_ref["cache_dir"]).name
    assert default_ref["cache_dir"] != explicit_ref["cache_dir"]


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


def test_start_end_date_is_normal_runtime_cache_entry(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    trades_dir, books_dir, _kline = _write_dated_replay_dirs(tmp_path)
    out = tmp_path / "out"
    code = backtest.main(
        [
            "--symbol",
            "ETH-USDT-SWAP",
            "--trades-dir",
            str(trades_dir),
            "--books-dir",
            str(books_dir),
            "--out-dir",
            str(out),
            "--runtime-events-cache-root",
            str(tmp_path / "cache"),
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-01",
        ]
    )
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["selected_days"] == ["2026-04-01"]
    assert ref["generated_days"] == ["2026-04-01"]
    assert [Path(path).name for path in summary["selected_files"]["trades"]] == ["ETH-USDT-SWAP-trades-2026-04-01.jsonl"]
    assert [Path(path).name for path in summary["selected_files"]["books"]] == ["ETH-USDT-SWAP-books-2026-04-01.jsonl"]


def test_second_run_same_start_end_date_reuses_cache(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    trades_dir, books_dir, _kline = _write_dated_replay_dirs(tmp_path)
    cache_root = tmp_path / "cache"

    def run(out: Path) -> None:
        assert backtest.main(
            [
                "--symbol",
                "ETH-USDT-SWAP",
                "--trades-dir",
                str(trades_dir),
                "--books-dir",
                str(books_dir),
                "--out-dir",
                str(out),
                "--runtime-events-cache-root",
                str(cache_root),
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-04-01",
            ]
        ) == 0

    run(tmp_path / "out_first")
    run(tmp_path / "out_second")
    ref = json.loads((tmp_path / "out_second" / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert ref["selected_days"] == ["2026-04-01"]
    assert ref["cache_hit_days"] == ["2026-04-01"]
    assert ref["generated_days"] == []


def test_runtime_events_cache_days_help_marked_advanced(capsys):
    with pytest.raises(SystemExit):
        backtest.parse_args(["--help"])
    output = capsys.readouterr().out
    assert "Advanced/debug override for runtime_events cache days" in output
    assert "Normal research should use --start-date/--end-date" in output


def test_runtime_events_cache_days_override_still_works(tmp_path, monkeypatch):
    _patch_runtime(monkeypatch)
    trades_dir, books_dir, _kline = _write_dated_replay_dirs(tmp_path)
    out = tmp_path / "out"
    code = backtest.main(
        [
            "--symbol",
            "ETH-USDT-SWAP",
            "--trades-dir",
            str(trades_dir),
            "--books-dir",
            str(books_dir),
            "--out-dir",
            str(out),
            "--runtime-events-cache-root",
            str(tmp_path / "cache"),
            "--runtime-events-cache-days",
            "2026-04-02",
        ]
    )
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["selected_days"] == ["2026-04-02"]
    assert ref["generated_days"] == ["2026-04-02"]
    assert not (Path(ref["cache_dir"]) / "2026-04-01.jsonl").exists()


def test_backtest_runtime_cache_day_shard_is_independent_of_previous_day(tmp_path, monkeypatch):
    two_day_rows, day2_rows = _day_boundary_rows()
    code, _out, cache_root = _run_backtest_with_rows(tmp_path / "two_day", monkeypatch, two_day_rows)
    assert code == 0
    day2_from_two_day = _runtime_cache_shard(cache_root, "2026-04-02").read_text(encoding="utf-8")

    code, _out, cache_root = _run_backtest_with_rows(tmp_path / "day2_only", monkeypatch, day2_rows)
    assert code == 0
    day2_alone = _runtime_cache_shard(cache_root, "2026-04-02").read_text(encoding="utf-8")

    assert day2_from_two_day == day2_alone


def test_backtest_cache_hit_day_does_not_feed_miss_day_accumulator(tmp_path, monkeypatch):
    two_day_rows, _day2_rows = _day_boundary_rows()
    day1_rows = {"2026-04-01": two_day_rows["2026-04-01"]}
    cache_root = tmp_path / "cache"

    code, _out, _cache_root = _run_backtest_with_rows(
        tmp_path / "day1_seed",
        monkeypatch,
        day1_rows,
        cache_root=cache_root,
        out_name="out_seed",
    )
    assert code == 0

    code, out, _cache_root = _run_backtest_with_rows(
        tmp_path / "two_day",
        monkeypatch,
        two_day_rows,
        cache_root=cache_root,
        out_name="out_two_day",
    )
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    day2_events = _read_jsonl(_runtime_cache_shard(cache_root, "2026-04-02"))
    assert code == 0
    assert ref["cache_hit_days"] == ["2026-04-01"]
    assert ref["generated_days"] == ["2026-04-02"]
    assert day2_events[0]["active_buy_notional_3s"] == 0.0
    assert day2_events[0]["active_sell_notional_3s"] == 5.05
    assert day2_events[0]["cvd_delta_3s"] == -5.05


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


def test_builder_cli_eth_runs_without_contract_multiplier(tmp_path):
    trades = tmp_path / "trades.jsonl"
    out = tmp_path / "runtime_events.jsonl"
    summary = tmp_path / "runtime_events.summary.json"
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
            "--summary-out",
            str(summary),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert result.returncode == 0, result.stderr
    assert out.exists()
    assert payload["contract_multiplier"] == 0.1
    assert payload["contract_multiplier_source"] == "okx_known_default"


def test_standalone_builder_resets_accumulator_at_day_boundary(tmp_path):
    two_day_rows, _day2_rows = _day_boundary_rows()
    out_dir = _build_runtime_events_for_rows(tmp_path, two_day_rows)
    day2_events = _read_jsonl(out_dir / "2026-04-02.jsonl")
    assert day2_events[0]["active_buy_notional_3s"] == 0.0
    assert day2_events[0]["active_sell_notional_3s"] == 5.05
    assert day2_events[0]["cvd_delta_3s"] == -5.05


def test_standalone_day2_same_when_built_alone_or_with_day1(tmp_path):
    two_day_rows, day2_rows = _day_boundary_rows()
    two_day_out = _build_runtime_events_for_rows(tmp_path / "two_day", two_day_rows)
    day2_out = _build_runtime_events_for_rows(tmp_path / "day2_only", day2_rows)
    assert (two_day_out / "2026-04-02.jsonl").read_text(encoding="utf-8") == (
        day2_out / "2026-04-02.jsonl"
    ).read_text(encoding="utf-8")


def test_standalone_builder_cache_manifest_is_reusable_by_backtest(tmp_path, monkeypatch):
    trades = tmp_path / "standalone_trades.jsonl"
    trades.write_text(
        "\n".join(
            [
                '{"ts":1775001600100,"px":"100","sz":"10","side":"buy","instId":"ETH-USDT-SWAP"}',
                '{"ts":1775001601100,"px":"101","sz":"5","side":"sell","instId":"ETH-USDT-SWAP"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cache_root = tmp_path / "cache"
    result = subprocess.run(
        [
            sys.executable,
            "tools/build_runtime_events_from_okx_trades.py",
            "--symbol",
            "ETH-USDT-SWAP",
            "--trades-dir",
            str(trades),
            "--out-dir",
            str(cache_root),
            "--contract-multiplier",
            "0.01",
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    manager = RuntimeEventCacheManager(cache_root, "ETH-USDT-SWAP", 1, 3, 0.01)
    assert manager.has_valid_day("2026-04-01") is True

    _patch_runtime(monkeypatch)
    trades_dir, books, _kline = _write_replay_inputs(tmp_path, days=("2026-04-01",))
    out = tmp_path / "out"
    code = backtest.main(
        [
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
            str(cache_root),
        ]
    )
    ref = json.loads((out / "runtime_events_ref.json").read_text(encoding="utf-8"))
    assert code == 0
    assert ref["cache_hit_days"] == ["2026-04-01"]
    assert ref["generated_days"] == []


def test_runtime_events_manifest_lock(tmp_path):
    manager = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01)
    manager.cache_dir().mkdir(parents=True)
    fd = manager._acquire_manifest_lock()
    try:
        with pytest.raises(RuntimeError, match="runtime_events manifest is locked"):
            manager.finalize_day("2026-04-01", RuntimeEventDayStats(day="2026-04-01", path="2026-04-01.jsonl", first_ts=1.0, last_ts=2.0, row_count=1))
    finally:
        manager._release_manifest_lock(fd)
    assert not manager.manifest_lock_path.exists()


def test_runtime_events_manifest_lock_released_and_preserves_multiple_days(tmp_path):
    manager = RuntimeEventCacheManager(tmp_path, "ETH-USDT-SWAP", 1, 3, 0.01)
    manager.cache_dir().mkdir(parents=True)
    (manager.cache_dir() / "2026-04-01.jsonl").write_text('{"ts":1}\n', encoding="utf-8")
    (manager.cache_dir() / "2026-04-02.jsonl").write_text('{"ts":2}\n', encoding="utf-8")
    manager.finalize_day("2026-04-01", RuntimeEventDayStats(day="2026-04-01", path="2026-04-01.jsonl", first_ts=1.0, last_ts=1.0, row_count=1))
    assert not manager.manifest_lock_path.exists()
    manager.finalize_day("2026-04-02", RuntimeEventDayStats(day="2026-04-02", path="2026-04-02.jsonl", first_ts=2.0, last_ts=2.0, row_count=1))
    manifest = manager.load_manifest()
    assert [shard["day"] for shard in manifest["shards"]] == ["2026-04-01", "2026-04-02"]
    assert manager.has_valid_day("2026-04-01") is True
    assert manager.has_valid_day("2026-04-02") is True
    assert not manager.manifest_lock_path.exists()
