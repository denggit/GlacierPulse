#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import subprocess
import tarfile
from pathlib import Path

import tools.generate_research_reports as generator
from tools import analyze_zone_truth
from tools import build_runtime_events_from_okx_trades as runtime_builder
from src.research.runtime_three_a import runtime_event_source as runtime_event_source_mod
from src.research.runtime_three_a.runtime_event_source import RuntimeEventSource
from src.research.zone_truth import analyzer as analyzer_mod
from src.research.zone_truth.analyzer import (
    V73_RT_BY_STRATEGY_FIELDS,
    V73_RT_BY_VP_SETUP_FIELDS,
    V7_SIMULATED_TRADE_FIELDS,
    ZoneTruthAnalyzer,
)

BASE_TS = 1_779_373_200


def _write_inputs(root: Path) -> dict[str, Path]:
    phase1 = root / "phase1.jsonl"
    rows = [
        {
            "record_type": "candidate_finalized",
            "event_key": "iceberg-1",
            "zone_id": "z1",
            "direction": "BUY",
            "result": "ICEBERG",
            "settle_ts": BASE_TS,
            "trigger_ts": BASE_TS,
            "settle_price": 100,
            "trigger_price": 100,
            "zone_lower": 99,
            "zone_upper": 101,
            "min_trade_price": 99,
            "max_trade_price": 100,
        },
        {
            "record_type": "candidate_finalized",
            "event_key": "non-iceberg-1",
            "zone_id": "z2",
            "direction": "BUY",
            "result": "REJECTED",
            "settle_ts": BASE_TS + 120,
            "trigger_ts": BASE_TS + 120,
            "settle_price": 100,
            "trigger_price": 100,
            "zone_lower": 99,
            "zone_upper": 101,
        },
    ]
    phase1.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    reactions = root / "a1.jsonl"
    reactions.write_text(
        "".join(
            json.dumps(
                {
                    "zone_id": zone_id,
                    "direction": "BUY",
                    "reaction_event_ts": BASE_TS + offset,
                    "frozen_ts": BASE_TS + offset - 60,
                    "frozen_low": 99,
                    "frozen_high": 101,
                    "reaction_type": "CLEAN_HOLD",
                    "a3_future_breakout_seen_flag": True,
                    "a3_future_breakout_entry_ts": BASE_TS + offset,
                    "a3_future_breakout_entry_price": 100,
                }
            )
            + "\n"
            for zone_id, offset in (("z1", 60), ("z2", 180))
        ),
        encoding="utf-8",
    )
    kline = root / "kline.csv"
    lines = ["timestamp,open,high,low,close,volume"]
    for i in range(20):
        ts = BASE_TS + i * 60
        lines.append(f"{ts},100,102,99.5,101,10")
    kline.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"phase1": phase1, "reactions": reactions, "kline": kline}


def _trade(zone_id: str):
    return {
        **{field: "" for field in V7_SIMULATED_TRADE_FIELDS},
        "zone_id": zone_id,
        "direction": "BUY",
        "a1_primary_evidence_type": "ICEBERG",
        "a3_quality_future_type_v2": "STRONG_ORDERFLOW_AGGRESSION",
        "entry_model": "BREAKOUT",
        "stop_model": "V1_ZONE_WIDTH",
        "target_r": 1.0,
        "entry_ts": BASE_TS,
        "entry_price": 100,
        "risk_u": 1,
        "realized_r_1h_sim": 1.0,
        "realized_outcome_1h_sim": "TARGET_1R_FIRST",
        "target_first_flag_sim": True,
        "fee_share_r": 0.1,
        "mfe_r_1h_sim": 1.0,
        "mae_r_1h_sim": -0.1,
        "complete_flag_sim": True,
    }


def test_zone_truth_streams_simulated_trades_without_list(monkeypatch, tmp_path):
    paths = _write_inputs(tmp_path)
    calls = []

    def fake_iter(rows, bars, **kwargs):
        for row in rows:
            calls.append(row["zone_id"])
            yield _trade(row["zone_id"])

    monkeypatch.setattr(analyzer_mod, "iter_3a_proxy_trades", fake_iter)
    out = tmp_path / "out"
    summary = ZoneTruthAnalyzer().analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
    with (out / "zone_truth_3a_simulated_trades.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["zone_id"] for row in rows] == ["z1"]
    assert calls == ["z1"]
    assert summary["simulator_input_scope"] == "ICEBERG_ONLY"
    assert summary["simulator_input_rows"] == 1
    assert summary["memory_profile"]["peak_rss_mb"] >= 0


def test_zone_truth_simulator_disabled_writes_header_only(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "disabled"
    summary = ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
    assert summary["simulator_enabled"] is False
    assert (out / "zone_truth_3a_simulated_trades.csv").read_text(encoding="utf-8").count("\n") == 1
    assert (out / "zone_truth_by_3a_combo_matrix.csv").read_text(encoding="utf-8").count("\n") == 1


def test_zone_truth_all_scope_passes_all_rows_to_simulator(monkeypatch, tmp_path):
    paths = _write_inputs(tmp_path)
    seen = []

    def fake_iter(rows, bars, **kwargs):
        for row in rows:
            seen.append(row["zone_id"])
            yield _trade(row["zone_id"])

    monkeypatch.setattr(analyzer_mod, "iter_3a_proxy_trades", fake_iter)
    summary = ZoneTruthAnalyzer(simulator_input_scope="all").analyze_files(paths["phase1"], paths["reactions"], paths["kline"], tmp_path / "all")
    assert seen == ["z1", "z2"]
    assert summary["simulator_input_rows"] == 2
    assert summary["total_rows"] == 2
    assert summary["iceberg_rows"] == 1


def test_generate_reports_subprocess_streams_and_passes_simulator_args(monkeypatch):
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(generator.subprocess, "run", fake_run)
    result = generator._run_subprocess(["python", "x.py"])
    assert result.returncode == 0
    assert captured["kwargs"] == {"cwd": str(generator.ROOT), "text": True, "check": False}
    assert generator._run_report("zone_truth", ["python", "x.py"], lambda command: subprocess.CompletedProcess(command, 0)) == "success"


def test_generate_reports_passes_zone_truth_simulator_args(tmp_path, monkeypatch):
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    paths = _write_inputs(tmp_path)
    commands = []

    def runner(command):
        commands.append(command)
        out = Path(command[command.index("--out") + 1])
        out.mkdir(parents=True, exist_ok=True)
        script = Path(command[1]).name
        if script == "analyze_a1_edge.py":
            (out / "a1_go_no_go_report.md").write_text("# a1\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    rc = generator.main(
        [
            "--run-name", "run",
            "--phase1-candidates", str(paths["phase1"]),
            "--a1-reactions", str(paths["reactions"]),
            "--kline", str(paths["kline"]),
            "--enable-3a-simulator", "false",
            "--simulator-input-scope", "all",
            "--simulator-include-unavailable", "true",
            "--simulator-max-trades", "1",
        ],
        runner=runner,
    )
    assert rc == 0
    zone_cmd = next(command for command in commands if Path(command[1]).name == "analyze_zone_truth.py")
    assert zone_cmd[-8:] == [
        "--enable-3a-simulator", "false",
        "--simulator-input-scope", "all",
        "--simulator-include-unavailable", "true",
        "--simulator-max-trades", "1",
    ]


def test_runtime_event_source_streams_directory_without_full_list(tmp_path):
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    (events_dir / "b.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": BASE_TS + 20, "symbol": "BTC-USDT", "last_price": 102}),
                json.dumps({"ts": BASE_TS + 30, "symbol": "ETH-USDT", "last_price": 103}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (events_dir / "a.csv").write_text(
        "ts,symbol,last_price\n"
        f"{BASE_TS + 10},BTC-USDT,101\n"
        f"{BASE_TS + 40},BTC-USDT,104\n",
        encoding="utf-8",
    )
    source = RuntimeEventSource(events_dir)
    rows = list(source.get_window(BASE_TS + 5, BASE_TS + 25, "BTC-USDT"))
    assert [float(row["ts"]) for row in rows] == [BASE_TS + 10, BASE_TS + 20]
    assert source.memory_profile()["runtime_event_source_mode"] == "directory_stream"
    assert source.memory_profile()["runtime_ticks_materialized_count"] == 0
    assert source.memory_profile()["runtime_window_reads"] == 1


def test_rt_report_headers_stable_when_empty(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "headers"
    ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
    with (out / "zone_truth_3a_rt_by_strategy.csv").open(encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == V73_RT_BY_STRATEGY_FIELDS
    with (out / "zone_truth_3a_rt_by_vp_setup.csv").open(encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == V73_RT_BY_VP_SETUP_FIELDS


def test_build_runtime_events_from_raw_trades_jsonl(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        "\n".join(
            [
                json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}),
                json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}),
                json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1004, "px": 12, "sz": 1, "side": "buy"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=out,
        bucket_sec=1,
        rolling_sec=3,
        contract_multiplier=1.0,
    )
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["total_trades_read"] == 3
    assert summary["runtime_events_written"] == 3
    assert rows[1]["active_buy_notional_3s"] == 20
    assert rows[1]["active_sell_notional_3s"] == 33
    assert rows[1]["cvd_delta_3s"] == -13
    assert rows[2]["active_buy_notional_3s"] == 12
    assert rows[2]["active_sell_notional_3s"] == 33
    assert rows[2]["cvd_delta_3s"] == -21
    assert summary["bucket_sec"] == 1
    assert summary["rolling_sec"] == 3
    assert summary["merge_sort_files"] is False


def test_non_monotonic_failure_removes_partial_single_file(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    (trades_dir / "a.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1002, "px": 10, "sz": 10, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    (trades_dir / "b.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 1, "side": "sell"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_path=out,
            rolling_sec=3,
            contract_multiplier=1.0,
        )
        assert False, "non-monotonic trades must fail by default"
    except SystemExit as exc:
        assert str(exc) == runtime_builder.NON_MONOTONIC_ERROR
    assert not out.exists()
    assert not out.with_name(f"{out.name}.tmp").exists()


def test_non_monotonic_failure_removes_partial_sharded_dir(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    (trades_dir / "a.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_976_002, "px": 10, "sz": 10, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    (trades_dir / "b.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_976_000, "px": 10, "sz": 1, "side": "sell"}) + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "runtime_events"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_dir=out_dir,
            shard_by="day",
            rolling_sec=3,
            contract_multiplier=1.0,
        )
        assert False, "non-monotonic trades must fail by default"
    except SystemExit as exc:
        assert str(exc) == runtime_builder.NON_MONOTONIC_ERROR
    assert not out_dir.exists()
    assert not runtime_builder._building_dir_path(out_dir).exists()


def test_build_success_atomic_single_file(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=out,
        contract_multiplier=1.0,
    )
    assert summary["runtime_events_written"] == 1
    assert out.exists()
    assert not out.with_name(f"{out.name}.tmp").exists()


def test_build_success_atomic_sharded_dir(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_976_000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "runtime_events"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_dir=out_dir,
        shard_by="day",
        contract_multiplier=1.0,
    )
    assert summary["runtime_events_written"] == 1
    assert out_dir.exists()
    assert (out_dir / "runtime_events_manifest.json").exists()
    assert not runtime_builder._building_dir_path(out_dir).exists()


def test_json_file_disabled_by_default(tmp_path):
    trades = tmp_path / "trades.json"
    trades.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, ".json raw trades must be disabled by default"
    except SystemExit as exc:
        assert str(exc) == runtime_builder.JSON_FILE_DISABLED_ERROR
    assert not out.exists()
    assert not out.with_name(f"{out.name}.tmp").exists()


def test_small_json_file_allowed_with_flag(tmp_path):
    trades = tmp_path / "trades.json"
    trades.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=out,
        contract_multiplier=1.0,
        allow_json_file=True,
    )
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["allow_json_file"] is True
    assert len(rows) == 1
    assert rows[0]["ts"] == 1000


def test_large_json_file_rejected_even_with_flag(tmp_path):
    trades = tmp_path / "trades.json"
    trades.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades,
            out_path=out,
            contract_multiplier=1.0,
            allow_json_file=True,
            max_json_file_bytes=8,
        )
        assert False, "oversized .json must fail even with --allow-json-file"
    except SystemExit as exc:
        assert "json.load is not memory-safe for large files" in str(exc)
    assert not out.exists()
    assert not out.with_name(f"{out.name}.tmp").exists()


def test_tar_inner_json_skipped_by_default(tmp_path):
    """Tar inner .json files are now silently skipped (not raised), counted via read_stats."""
    read_stats = runtime_builder.RuntimeReadStats()
    inner = tmp_path / "trades.json"
    inner.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    archive_path = tmp_path / "trades.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(inner, arcname="nested/trades.json")
    rows = list(runtime_builder.iter_trade_rows(archive_path, read_stats=read_stats))
    assert rows == []
    assert read_stats.skipped_json_file_count == 1


def test_data_jsonl_still_streams_line_by_line(tmp_path, monkeypatch):
    data = tmp_path / "trades.data"
    data.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]) + "\n",
        encoding="utf-8",
    )
    jsonl = tmp_path / "trades.jsonl"
    jsonl.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}) + "\n",
        encoding="utf-8",
    )

    def fail_json_load(handle):
        raise AssertionError(".data/.jsonl must not call json.load")

    monkeypatch.setattr(runtime_builder.json, "load", fail_json_load)
    assert [row["ts"] for row in runtime_builder.iter_trade_rows(data)] == [1000]
    assert [row["ts"] for row in runtime_builder.iter_trade_rows(jsonl)] == [1001]


def test_data_json_array_lines_do_not_call_json_load(tmp_path, monkeypatch):
    trades = tmp_path / "trades.data"
    trades.write_text(
        json.dumps(
            [
                {"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"},
                {"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"},
            ]
        )
        + "\n"
        + json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1002, "px": 12, "sz": 1, "side": "buy"}])
        + "\n",
        encoding="utf-8",
    )

    def fail_json_load(handle):
        raise AssertionError(".data must not call json.load")

    monkeypatch.setattr(runtime_builder.json, "load", fail_json_load)
    rows = list(runtime_builder.iter_trade_rows(trades))
    assert len(rows) == 3
    assert rows[0]["ts"] == 1000
    assert rows[2]["ts"] == 1002


def test_jsonl_line_too_large_fails(tmp_path):
    trades = tmp_path / "trades.data"
    trades.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]) + "\n",
        encoding="utf-8",
    )
    try:
        list(runtime_builder.iter_trade_rows(trades, max_json_line_bytes=16))
        assert False, "oversized .data line must fail"
    except ValueError as exc:
        assert "JSONL/.data line exceeds max-json-line-bytes" in str(exc)
    rows = list(runtime_builder.iter_trade_rows(trades, max_json_line_bytes=1024))
    assert len(rows) == 1


def test_build_runtime_events_from_okx_trades_tar_gz_data(tmp_path):
    trades_dir = tmp_path / "okx_raw"
    trades_dir.mkdir()
    inner = tmp_path / "ETH-USDT-SWAP-trades.data"
    inner.write_text(
        "instId,ts,px,sz,side\n"
        "ETH-USDT-SWAP,1000,10,2,buy\n"
        "ETH-USDT-SWAP,1001,11,3,sell\n",
        encoding="utf-8",
    )
    with tarfile.open(trades_dir / "ETH-USDT-SWAP-trades-2026-05-01.tar.gz", "w:gz") as archive:
        archive.add(inner, arcname="nested/ETH-USDT-SWAP-trades.data")
    out = tmp_path / "runtime_events_tar.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        bucket_sec=1,
        rolling_sec=3,
        contract_multiplier=1.0,
    )
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["total_trades_read"] == 2
    assert rows[-1]["active_buy_notional_3s"] == 20
    assert rows[-1]["active_sell_notional_3s"] == 33


def test_build_runtime_events_streaming_memory_safe(tmp_path, monkeypatch):
    out = tmp_path / "runtime_events.jsonl"
    yielded = []

    def fake_discover(path, *, allow_json_file=False, include_metadata_json=False):
        return [path], 0

    def fake_iter(paths, *, symbol, contract_multiplier=1.0, merge_sort_files=False, max_json_line_bytes=runtime_builder.MAX_JSON_LINE_BYTES, allow_json_file=False, max_json_file_bytes=runtime_builder.MAX_JSON_FILE_BYTES, read_stats=None):
        for idx in range(5):
            yielded.append(idx)
            yield runtime_builder.NormalizedTrade(
                ts=1000.0 + idx,
                symbol=symbol,
                price=10.0 + idx,
                side="BUY" if idx % 2 == 0 else "SELL",
                notional=100.0,
            )

    monkeypatch.setattr(runtime_builder, "discover_trade_files", fake_discover)
    monkeypatch.setattr(runtime_builder, "iter_normalized_trades", fake_iter)
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=tmp_path / "unused",
        out_path=out,
        bucket_sec=1,
        rolling_sec=3,
        contract_multiplier=1.0,
    )
    assert yielded == [0, 1, 2, 3, 4]
    assert summary["runtime_events_written"] == 5
    assert out.read_text(encoding="utf-8").count("\n") == 5


def test_build_runtime_events_sharded_by_day(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        "\n".join(
            [
                json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_975_999, "px": 10, "sz": 2, "side": "buy"}),
                json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_976_000, "px": 11, "sz": 3, "side": "sell"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "runtime_events"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_dir=out_dir,
        shard_by="day",
        bucket_sec=1,
        rolling_sec=3,
        contract_multiplier=1.0,
    )
    assert summary["output_mode"] == "sharded_by_day"
    assert summary["shard_count"] == 2
    shard_names = sorted(Path(path).name for path in summary["shard_files"])
    assert shard_names == ["2026-05-16.jsonl", "2026-05-17.jsonl"]
    assert (out_dir / "2026-05-16.jsonl").exists()
    assert (out_dir / "2026-05-17.jsonl").exists()
    manifest = json.loads((out_dir / "runtime_events_manifest.json").read_text(encoding="utf-8"))
    assert manifest["symbol"] == "ETH-USDT-SWAP"
    assert manifest["bucket_sec"] == 1
    assert manifest["rolling_sec"] == 3
    assert manifest["output_mode"] == "sharded_by_day"
    assert [shard["path"] for shard in manifest["shards"]] == ["2026-05-16.jsonl", "2026-05-17.jsonl"]
    assert [shard["row_count"] for shard in manifest["shards"]] == [1, 1]
    assert summary["manifest_path"] == str(out_dir / "runtime_events_manifest.json")
    assert Path(summary["manifest_path"]).exists()


def test_builder_summary_manifest_path(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1_778_976_000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "runtime_events"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_dir=out_dir,
        shard_by="day",
        contract_multiplier=1.0,
    )
    assert summary["manifest_path"] == str(out_dir / "runtime_events_manifest.json")
    assert Path(summary["manifest_path"]).exists()


def test_runtime_event_source_uses_manifest_without_scanning_bounds(tmp_path, monkeypatch):
    root = tmp_path / "runtime_events"
    root.mkdir()
    shard = root / "2026-05-17.jsonl"
    shard.write_text(
        json.dumps({"ts": BASE_TS, "symbol": "BTC-USDT", "last_price": 100}) + "\n",
        encoding="utf-8",
    )
    (root / "runtime_events_manifest.json").write_text(
        json.dumps(
            {
                "symbol": "BTC-USDT",
                "output_mode": "sharded_by_day",
                "shards": [{"path": shard.name, "first_ts": BASE_TS, "last_ts": BASE_TS, "row_count": 1}],
            }
        ),
        encoding="utf-8",
    )

    def fail_time_bounds(path):
        raise AssertionError("manifest-backed source must not scan shard bounds")

    monkeypatch.setattr(runtime_event_source_mod, "_file_time_bounds", fail_time_bounds)
    source = RuntimeEventSource(root)
    assert source.memory_profile()["runtime_event_source_manifest_used"] is True
    assert len(source.files) == 1
    assert source.files[0].row_count == 1
    assert list(source.get_window(BASE_TS - 1, BASE_TS + 1, "BTC-USDT"))[0]["last_price"] == 100


def test_runtime_event_source_discovers_nested_shards(tmp_path):
    root = tmp_path / "runtime_events"
    nested = root / "2026" / "05"
    hidden = root / ".hidden"
    nested.mkdir(parents=True)
    hidden.mkdir(parents=True)
    (nested / "2026-05-17.jsonl").write_text(
        json.dumps({"ts": BASE_TS, "symbol": "BTC-USDT", "last_price": 100}) + "\n",
        encoding="utf-8",
    )
    (hidden / "2026-05-18.jsonl").write_text(
        json.dumps({"ts": BASE_TS + 1, "symbol": "BTC-USDT", "last_price": 101}) + "\n",
        encoding="utf-8",
    )
    (nested / ".ignored.jsonl").write_text(
        json.dumps({"ts": BASE_TS + 2, "symbol": "BTC-USDT", "last_price": 102}) + "\n",
        encoding="utf-8",
    )
    source = RuntimeEventSource(root)
    assert source.memory_profile()["runtime_event_source_manifest_used"] is False
    assert [file.path.name for file in source.files] == ["2026-05-17.jsonl"]
    rows = list(source.get_window(BASE_TS - 1, BASE_TS + 10, "BTC-USDT"))
    assert len(rows) == 1


def test_single_large_runtime_file_warning_or_not_recommended(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=tmp_path / "runtime_events.jsonl",
        contract_multiplier=1.0,
    )
    assert summary["output_mode"] == "single_file"
    assert summary["manifest_path"] == ""
    assert "not recommended" in summary["output_warning"]


def test_swap_requires_explicit_contract_multiplier(tmp_path):
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades,
            out_path=out,
        )
        assert False, "SWAP builder must require explicit contract multiplier"
    except SystemExit as exc:
        assert "contract-multiplier" in str(exc)
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=out,
        contract_multiplier=0.1,
    )
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["contract_multiplier"] == 0.1
    assert rows[0]["contract_multiplier"] == 0.1
    assert rows[0]["active_buy_notional_3s"] == 2.0


def test_sequential_file_reading_does_not_open_all_files(tmp_path, monkeypatch):
    paths = [tmp_path / f"{idx}.jsonl" for idx in range(3)]
    calls = []

    def fake_iter(path, *, symbol, contract_multiplier=1.0, max_json_line_bytes=runtime_builder.MAX_JSON_LINE_BYTES, allow_json_file=False, max_json_file_bytes=runtime_builder.MAX_JSON_FILE_BYTES, read_stats=None):
        calls.append(path.name)
        yield runtime_builder.NormalizedTrade(
            ts=1000.0 + len(calls),
            symbol=symbol,
            price=10.0,
            side="BUY",
            notional=100.0,
            contract_multiplier=contract_multiplier,
        )

    monkeypatch.setattr(runtime_builder, "iter_normalized_trades_from_file", fake_iter)
    iterator = runtime_builder.iter_normalized_trades(
        paths,
        symbol="ETH-USDT-SWAP",
        contract_multiplier=1.0,
    )
    first = next(iterator)
    assert first.ts == 1001.0
    assert calls == ["0.jsonl"]
    second = next(iterator)
    assert second.ts == 1002.0
    assert calls == ["0.jsonl", "1.jsonl"]


def test_non_monotonic_trades_fail_by_default(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    (trades_dir / "a.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1002, "px": 10, "sz": 10, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    (trades_dir / "b.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 1, "side": "sell"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_path=out,
            rolling_sec=3,
            contract_multiplier=1.0,
        )
        assert False, "non-monotonic trades must fail by default"
    except SystemExit as exc:
        assert str(exc) == runtime_builder.NON_MONOTONIC_ERROR


def test_non_monotonic_trades_allow_flag_warns(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    (trades_dir / "a.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1002, "px": 10, "sz": 10, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    (trades_dir / "b.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 1, "side": "sell"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        rolling_sec=3,
        contract_multiplier=1.0,
        allow_non_monotonic_output=True,
    )
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert summary["allow_non_monotonic_output"] is True
    assert summary["non_monotonic_trade_count"] == 1
    assert "non-monotonic trades detected" in summary["output_warning"]
    assert "runtime_events may be non-monotonic and unsafe for windowed backtest" in summary["output_warning"]
    assert rows[-1]["ts"] == 1000
    assert rows[-1]["active_buy_notional_3s"] == 0.0
    assert rows[-1]["active_sell_notional_3s"] == 10.0


def test_merge_sort_files_outputs_monotonic(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    (trades_dir / "a.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1002, "px": 10, "sz": 10, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    (trades_dir / "b.jsonl").write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 1, "side": "sell"}) + "\n",
        encoding="utf-8",
    )
    sorted_out = tmp_path / "runtime_events_sorted.jsonl"
    sorted_summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=sorted_out,
        rolling_sec=3,
        contract_multiplier=1.0,
        merge_sort_files=True,
    )
    sorted_rows = [json.loads(line) for line in sorted_out.read_text(encoding="utf-8").splitlines()]
    assert sorted_summary["merge_sort_files"] is True
    assert sorted_summary["non_monotonic_trade_count"] == 0
    assert [row["ts"] for row in sorted_rows] == [1000, 1002]


def test_analyze_zone_truth_warns_raw_tar_without_runtime_events(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    trades_dir = tmp_path / "raw_trades"
    trades_dir.mkdir()
    inner = tmp_path / "ETH-USDT-SWAP-trades.data"
    inner.write_text('{"instId":"ETH-USDT-SWAP","ts":1000,"px":10,"sz":1,"side":"buy"}\n', encoding="utf-8")
    with tarfile.open(trades_dir / "ETH-USDT-SWAP-trades-2026-05-01.tar.gz", "w:gz") as archive:
        archive.add(inner, arcname="ETH-USDT-SWAP-trades.data")
    rc = analyze_zone_truth.main(
        [
            "--phase1-candidates", str(paths["phase1"]),
            "--a1-reactions", str(paths["reactions"]),
            "--kline", str(paths["kline"]),
            "--out", str(tmp_path / "out_raw_tar"),
            "--trades-dir", str(trades_dir),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "RuntimeEventSource supports jsonl/csv runtime_events" in captured.err


def test_runtime_input_wrong_format_errors(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    trades_dir = tmp_path / "wrong_runtime_input"
    trades_dir.mkdir()
    (trades_dir / "events.txt").write_text("not runtime events\n", encoding="utf-8")
    rc = analyze_zone_truth.main(
        [
            "--phase1-candidates", str(paths["phase1"]),
            "--a1-reactions", str(paths["reactions"]),
            "--kline", str(paths["kline"]),
            "--out", str(tmp_path / "out_wrong_format"),
            "--trades-dir", str(trades_dir),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "No runtime event jsonl/csv files found" in captured.err


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P1: .json.gz must be disabled regardless of --allow-json-file
# ═══════════════════════════════════════════════════════════════════════


def test_json_gz_disabled_by_default(tmp_path):
    """Compressed .json.gz files must raise SystemExit with GZ_JSON_DISABLED_ERROR."""
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    gz_path = tmp_path / "trades.json.gz"
    import gzip

    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write(
            json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}])
        )
    try:
        list(runtime_builder.iter_trade_rows(gz_path))
        assert False, ".json.gz must be disabled"
    except SystemExit as exc:
        assert "Compressed .json.gz is disabled" in str(exc)


def test_json_gz_disabled_even_with_allow_json_file_flag(tmp_path):
    """.json.gz must remain disabled even when --allow-json-file is passed."""
    gz_path = tmp_path / "trades.json.gz"
    import gzip

    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write(
            json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}])
        )
    try:
        list(runtime_builder.iter_trade_rows(gz_path, allow_json_file=True))
        assert False, ".json.gz must be disabled even with --allow-json-file"
    except SystemExit as exc:
        assert "Compressed .json.gz is disabled" in str(exc)


def test_jsonl_gz_streams_normally(tmp_path):
    """.jsonl.gz files still stream line-by-line normally."""
    gz_path = tmp_path / "trades.jsonl.gz"
    import gzip

    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write(
            json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n"
        )
        gz.write(
            json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}) + "\n"
        )
    rows = list(runtime_builder.iter_trade_rows(gz_path))
    assert len(rows) == 2
    assert rows[0]["ts"] == 1000
    assert rows[1]["ts"] == 1001


def test_data_gz_streams_normally(tmp_path):
    """.data.gz files still stream line-by-line normally."""
    gz_path = tmp_path / "trades.data.gz"
    import gzip

    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write(
            json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]) + "\n"
        )
        gz.write(
            json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}]) + "\n"
        )
    rows = list(runtime_builder.iter_trade_rows(gz_path))
    assert len(rows) == 2
    assert rows[0]["ts"] == 1000
    assert rows[1]["ts"] == 1001


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P1: directory scan skips unrelated .json instead of failing
# ═══════════════════════════════════════════════════════════════════════


def test_directory_with_unrelated_json_succeeds(tmp_path):
    """Directory containing valid .jsonl + unrelated summary.json should build successfully."""
    trades_dir = tmp_path / "trades_dir"
    trades_dir.mkdir()

    valid_jsonl = trades_dir / "trades.jsonl"
    valid_jsonl.write_text(
        "\n".join(
            json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000 + i, "px": 10 + i, "sz": 2, "side": "buy"})
            for i in range(3)
        )
        + "\n",
        encoding="utf-8",
    )

    summary = trades_dir / "summary.json"
    summary.write_text(
        json.dumps({"total": 3, "source": "some_other_tool"}),
        encoding="utf-8",
    )

    out = tmp_path / "runtime_events.jsonl"
    summary_out = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        contract_multiplier=1.0,
    )
    assert summary_out["total_trades_read"] == 3
    assert summary_out["runtime_events_written"] == 3
    assert summary_out["skipped_json_file_count"] == 1
    assert "raw .json files were skipped" in summary_out["output_warning"]
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 3


def test_directory_with_allow_json_file_processes_json(tmp_path):
    """With --allow-json-file, directory .json files ARE processed (subject to size limit)."""
    trades_dir = tmp_path / "trades_dir"
    trades_dir.mkdir()

    trades_json = trades_dir / "trades.json"
    trades_json.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )

    out = tmp_path / "runtime_events.jsonl"
    summary_out = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        contract_multiplier=1.0,
        allow_json_file=True,
    )
    assert summary_out["skipped_json_file_count"] == 0
    assert summary_out["total_trades_read"] == 1
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1


def test_direct_json_file_path_still_fails(tmp_path):
    """Directly passing a .json file as trades_path still raises (unchanged behavior)."""
    trades = tmp_path / "trades.json"
    trades.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "direct .json path must still fail"
    except SystemExit as exc:
        assert str(exc) == runtime_builder.JSON_FILE_DISABLED_ERROR


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P1-1: tar/zip archive internal .json skipped (not raised)
# ═══════════════════════════════════════════════════════════════════════


def test_tar_gz_with_trades_data_and_metadata_json_succeeds(tmp_path):
    """tar.gz containing trades.data + metadata.json: build succeeds, metadata skipped."""
    trades_dir = tmp_path / "trades_dir"
    trades_dir.mkdir()
    inner_data = trades_dir / "trades.data"
    inner_data.write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]) + "\n"
        + json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}]) + "\n",
        encoding="utf-8",
    )
    inner_meta = trades_dir / "metadata.json"
    inner_meta.write_text(
        json.dumps({"source": "okx_export", "date": "2024-01-01"}),
        encoding="utf-8",
    )
    archive_path = tmp_path / "trades.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(inner_data, arcname="trades.data")
        archive.add(inner_meta, arcname="metadata.json")
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=archive_path,
        out_path=out,
        contract_multiplier=1.0,
    )
    assert summary["total_trades_read"] == 2
    assert summary["runtime_events_written"] == 2
    assert summary["skipped_json_file_count"] == 1
    assert "raw .json files were skipped" in summary["output_warning"]


def test_zip_with_trades_data_and_metadata_json_succeeds(tmp_path):
    """Zip containing trades.jsonl + summary.json: build succeeds, summary skipped."""
    from zipfile import ZipFile

    inner_data = tmp_path / "trades.jsonl"
    inner_data.write_text(
        json.dumps({"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    inner_summary = tmp_path / "summary.json"
    inner_summary.write_text(
        json.dumps({"count": 100}),
        encoding="utf-8",
    )
    archive_path = tmp_path / "trades.zip"
    with ZipFile(archive_path, "w") as zf:
        zf.write(inner_data, arcname="trades.jsonl")
        zf.write(inner_summary, arcname="summary.json")
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=archive_path,
        out_path=out,
        contract_multiplier=1.0,
    )
    assert summary["total_trades_read"] == 1
    assert summary["runtime_events_written"] == 1
    assert summary["skipped_json_file_count"] == 1
    assert "raw .json files were skipped" in summary["output_warning"]


def test_tar_gz_only_metadata_json_fails(tmp_path):
    """tar.gz with only metadata.json (no trade data) → fails."""
    inner_meta = tmp_path / "metadata.json"
    inner_meta.write_text(
        json.dumps({"source": "okx_export"}),
        encoding="utf-8",
    )
    archive_path = tmp_path / "only_meta.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(inner_meta, arcname="metadata.json")
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=archive_path,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "archive with only metadata .json must fail"
    except SystemExit as exc:
        assert "No valid trades were converted" in str(exc)


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P1-2: no valid trades / empty → fail by default
# ═══════════════════════════════════════════════════════════════════════


def test_empty_directory_fails_by_default(tmp_path):
    """Empty directory: fails, no output file left."""
    trades_dir = tmp_path / "empty_dir"
    trades_dir.mkdir()
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "empty directory must fail"
    except SystemExit as exc:
        assert runtime_builder.NO_TRADE_FILES_ERROR in str(exc)
    assert not out.exists()
    assert not runtime_builder._building_dir_path(out).exists()


def test_directory_only_summary_json_fails(tmp_path):
    """Directory with only summary.json: fails."""
    trades_dir = tmp_path / "only_meta_dir"
    trades_dir.mkdir()
    (trades_dir / "summary.json").write_text(
        json.dumps({"total": 3}),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "directory with only summary.json must fail"
    except SystemExit as exc:
        assert runtime_builder.NO_TRADE_FILES_ERROR in str(exc)


def test_wrong_symbol_no_matches_fails(tmp_path):
    """jsonl with correct format BUT wrong symbol → fails."""
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "BTC-USDT-SWAP", "ts": 1000, "px": 50000, "sz": 1, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "wrong symbol must yield no trades → fail"
    except SystemExit as exc:
        assert runtime_builder.NO_VALID_TRADES_ERROR in str(exc)
    assert not out.exists()


def test_allow_empty_permits_empty_output(tmp_path):
    """--allow-empty permits empty output with warning."""
    trades_dir = tmp_path / "empty_dir"
    trades_dir.mkdir()
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        contract_multiplier=1.0,
        allow_empty=True,
    )
    assert summary["allow_empty"] is True
    assert "no supported trade files found" in summary.get("empty_output_warning", "")
    assert summary["total_trades_read"] == 0
    assert summary["runtime_events_written"] == 0
    assert out.exists()


def test_allow_empty_with_zero_valid_trades(tmp_path):
    """--allow-empty with wrong symbol: 0 trades read, 0 written, output exists with warning."""
    trades = tmp_path / "trades.jsonl"
    trades.write_text(
        json.dumps({"instId": "BTC-USDT-SWAP", "ts": 1000, "px": 50000, "sz": 1, "side": "buy"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades,
        out_path=out,
        contract_multiplier=1.0,
        allow_empty=True,
    )
    assert summary["allow_empty"] is True
    assert "no valid trades were converted" in summary.get("empty_output_warning", "")
    assert summary["total_trades_read"] == 0
    assert summary["runtime_events_written"] == 0
    assert out.exists()


def test_empty_failure_cleans_up_tmp_output(tmp_path):
    """Failure must not leave tmp/building half-products."""
    trades_dir = tmp_path / "empty_dir"
    trades_dir.mkdir()
    out = tmp_path / "runtime_events.jsonl"
    building = runtime_builder._building_dir_path(out)
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_path=out,
            contract_multiplier=1.0,
        )
        assert False, "should fail"
    except SystemExit:
        pass
    assert not out.exists()
    assert not building.exists()


def test_empty_failure_cleans_up_sharded_tmp_output(tmp_path):
    """Sharded failure must not leave tmp/building half-products."""
    trades_dir = tmp_path / "empty_dir"
    trades_dir.mkdir()
    out_dir = tmp_path / "runtime_events"
    building = runtime_builder._building_dir_path(out_dir)
    try:
        runtime_builder.build_runtime_events(
            symbol="ETH-USDT-SWAP",
            trades_path=trades_dir,
            out_dir=out_dir,
            shard_by="day",
            contract_multiplier=1.0,
        )
        assert False, "should fail"
    except SystemExit:
        pass
    assert not out_dir.exists()
    assert not building.exists()


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P2: skip metadata-like .json even with --allow-json-file
# ═══════════════════════════════════════════════════════════════════════


def test_metadata_json_skipped_with_allow_json_file(tmp_path):
    """With --allow-json-file, metadata.json / summary.json are still skipped."""
    trades_dir = tmp_path / "trades_dir"
    trades_dir.mkdir()
    (trades_dir / "trades.json").write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    (trades_dir / "summary.json").write_text(
        json.dumps({"total": 1}),
        encoding="utf-8",
    )
    (trades_dir / "metadata.json").write_text(
        json.dumps({"version": "1.0"}),
        encoding="utf-8",
    )
    (trades_dir / "manifest.json").write_text(
        json.dumps({}),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        contract_multiplier=1.0,
        allow_json_file=True,
    )
    assert summary["total_trades_read"] == 1
    assert summary["skipped_json_file_count"] == 3  # summary, metadata, manifest
    assert "raw .json files were skipped" in summary["output_warning"]


def test_include_metadata_json_allows_metadata_files(tmp_path):
    """--include-metadata-json + --allow-json-file: metadata .json are processed."""
    trades_dir = tmp_path / "trades_dir"
    trades_dir.mkdir()
    # summary.json sorts before trades.json → give it the earlier timestamp
    (trades_dir / "summary.json").write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1000, "px": 10, "sz": 2, "side": "buy"}]),
        encoding="utf-8",
    )
    (trades_dir / "trades.json").write_text(
        json.dumps([{"instId": "ETH-USDT-SWAP", "ts": 1001, "px": 11, "sz": 3, "side": "sell"}]),
        encoding="utf-8",
    )
    out = tmp_path / "runtime_events.jsonl"
    summary = runtime_builder.build_runtime_events(
        symbol="ETH-USDT-SWAP",
        trades_path=trades_dir,
        out_path=out,
        contract_multiplier=1.0,
        allow_json_file=True,
        include_metadata_json=True,
    )
    assert summary["total_trades_read"] == 2
    assert summary["skipped_json_file_count"] == 0
