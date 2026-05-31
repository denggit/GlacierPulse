#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import subprocess
from pathlib import Path

import tools.generate_research_reports as generator
from src.research.zone_truth import analyzer as analyzer_mod
from src.research.zone_truth.analyzer import V7_SIMULATED_TRADE_FIELDS, ZoneTruthAnalyzer

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
