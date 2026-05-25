import inspect
import json
import os
from pathlib import Path

import pytest

from tools import backtest_local_data as backtest


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    trades = tmp_path / "trades.jsonl"
    books = tmp_path / "books.jsonl"
    kline = tmp_path / "kline.csv"
    trades.write_text("", encoding="utf-8")
    books.write_text("", encoding="utf-8")
    kline.write_text("ts,open,high,low,close,volume\n", encoding="utf-8")
    return trades, books, kline


def _patch_empty_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    class Runtime:
        def __init__(self, symbol, research_events_path):
            self.symbol = symbol
            self.research_events_path = research_events_path
            self.research_events_file = research_events_path.open("w", encoding="utf-8")

        def close(self):
            self.research_events_file.close()

        def on_trade_tick(self, trade_data, stats):
            raise AssertionError("empty replay should not process trades")

        def on_book_update(self, book_data, stats):
            raise AssertionError("empty replay should not process books")

    monkeypatch.setattr(backtest, "LocalA1ResearchRuntime", Runtime)
    monkeypatch.setattr(
        backtest,
        "build_events",
        lambda trades_files, books_files, symbol, multiplier, stats, sort_in_memory, book_cleaning, time_filter: iter(()),
    )


def _runtime_args():
    return backtest.parse_args(
        [
            "--trades-file",
            "trades.jsonl",
            "--books-file",
            "books.jsonl",
        ]
    )


def test_local_replay_disables_heartbeat_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("V62_INTEGRATION_HEARTBEAT_ENABLED", raising=False)
    monkeypatch.delenv("V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", raising=False)

    backtest.configure_runtime_environment(
        args=_runtime_args(),
        out_dir=tmp_path / "out",
        research_dir=tmp_path / "out" / "research",
    )

    assert os.environ["V62_INTEGRATION_HEARTBEAT_ENABLED"] == "false"
    assert os.environ["V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED"] == "false"


def test_local_replay_does_not_override_explicit_heartbeat_env(tmp_path, monkeypatch):
    monkeypatch.setenv("V62_INTEGRATION_HEARTBEAT_ENABLED", "true")
    monkeypatch.setenv("V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", "true")

    backtest.configure_runtime_environment(
        args=_runtime_args(),
        out_dir=tmp_path / "out",
        research_dir=tmp_path / "out" / "research",
    )

    assert os.environ["V62_INTEGRATION_HEARTBEAT_ENABLED"] == "true"
    assert os.environ["V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED"] == "true"


def test_jsonl_recorders_still_enabled(tmp_path, monkeypatch):
    monkeypatch.delenv("PHASE1_CANDIDATE_RECORDER_WRITE_JSONL", raising=False)
    monkeypatch.delenv("A1_REACTION_EVENT_RECORDER_WRITE_JSONL", raising=False)
    monkeypatch.delenv("V62_INTEGRATION_HEARTBEAT_ENABLED", raising=False)
    monkeypatch.delenv("V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", raising=False)

    backtest.configure_runtime_environment(
        args=_runtime_args(),
        out_dir=tmp_path / "out",
        research_dir=tmp_path / "out" / "research",
    )

    assert os.environ["PHASE1_CANDIDATE_RECORDER_WRITE_JSONL"] == "true"
    assert os.environ["A1_REACTION_EVENT_RECORDER_WRITE_JSONL"] == "true"


def test_generate_reports_requires_kline(tmp_path):
    trades, books, _kline = _write_inputs(tmp_path)

    with pytest.raises(SystemExit) as exc:
        backtest.parse_args(
            [
                "--trades-file",
                str(trades),
                "--books-file",
                str(books),
                "--generate-reports",
            ]
        )

    assert "--generate-reports requires --kline" in str(exc.value)


def test_generate_reports_invokes_unified_generator(tmp_path, monkeypatch):
    trades, books, kline = _write_inputs(tmp_path)
    out_dir = tmp_path / "out"
    captured = {}
    _patch_empty_replay(monkeypatch)

    import tools.generate_research_reports as reports

    def fake_generate(argv):
        captured["argv"] = argv
        return 0

    monkeypatch.setattr(reports, "main", fake_generate)

    result = backtest.main(
        [
            "--trades-file",
            str(trades),
            "--books-file",
            str(books),
            "--out-dir",
            str(out_dir),
            "--generate-reports",
            "--kline",
            str(kline),
        ]
    )

    assert result == 0
    argv = captured["argv"]
    assert "--phase1-candidates" in argv
    assert "--a1-reactions" in argv
    assert "--kline" in argv
    assert "--snapshot" in argv
    assert "--zip" in argv
    assert argv[argv.index("--phase1-candidates") + 1] == str(out_dir / "research" / "phase1_candidates.jsonl")
    assert argv[argv.index("--a1-reactions") + 1] == str(out_dir / "research" / "a1_reaction_events.jsonl")
    assert argv[argv.index("--kline") + 1] == str(kline)


def test_generate_reports_touches_missing_research_jsonl(tmp_path, monkeypatch):
    trades, books, kline = _write_inputs(tmp_path)
    out_dir = tmp_path / "out"
    _patch_empty_replay(monkeypatch)

    import tools.generate_research_reports as reports

    monkeypatch.setattr(reports, "main", lambda argv: 0)

    result = backtest.main(
        [
            "--trades-file",
            str(trades),
            "--books-file",
            str(books),
            "--out-dir",
            str(out_dir),
            "--generate-reports",
            "--kline",
            str(kline),
        ]
    )

    a1_reactions = out_dir / "research" / "a1_reaction_events.jsonl"
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert result == 0
    assert a1_reactions.exists()
    assert a1_reactions.read_text(encoding="utf-8") == ""
    assert str(a1_reactions) in summary["report_generation"]["created_empty_research_inputs"]


def test_generate_reports_failure_returns_nonzero_and_updates_summary(tmp_path, monkeypatch):
    trades, books, kline = _write_inputs(tmp_path)
    out_dir = tmp_path / "out"
    _patch_empty_replay(monkeypatch)

    import tools.generate_research_reports as reports

    monkeypatch.setattr(reports, "main", lambda argv: 1)

    result = backtest.main(
        [
            "--trades-file",
            str(trades),
            "--books-file",
            str(books),
            "--out-dir",
            str(out_dir),
            "--generate-reports",
            "--kline",
            str(kline),
        ]
    )

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert result == 1
    assert summary["report_generation"]["status"] == "failed"


def test_report_run_name_default_under_backtests_run_dir():
    args = backtest.parse_args(
        [
            "--trades-file",
            "trades.jsonl",
            "--books-file",
            "books.jsonl",
            "--run-name",
            "abc",
        ]
    )

    assert backtest.resolve_report_run_name(args) == "backtests/abc/research_reports"


def test_report_generation_does_not_change_replay_runtime():
    module_source = inspect.getsource(backtest)
    runtime_source = inspect.getsource(backtest.LocalA1ResearchRuntime)

    assert "IcebergTrader(" not in module_source
    assert "main.py" in module_source
    assert "process_signal(" not in runtime_source
    assert '"mode": "research_only"' in module_source
    assert '"simulates_pnl": False' in module_source
