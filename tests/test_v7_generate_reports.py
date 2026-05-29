#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import subprocess
import zipfile
import csv
from pathlib import Path

import tools.generate_research_reports as generator
from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_373_200


def _write_inputs(root: Path) -> dict[str, Path]:
    phase1 = root / "phase1.jsonl"
    phase1.write_text(
        json.dumps(
            {
                "record_type": "candidate_finalized",
                "event_key": "pie-1",
                "zone_id": "z1",
                "direction": "BUY",
                "result": "ICEBERG",
                "settle_ts": BASE_TS,
                "trigger_ts": BASE_TS,
                "settle_price": 100,
                "trigger_price": 100,
                "zone_lower": 99,
                "zone_upper": 101,
                "active_notional": 1_000_000,
                "hidden_volume": 2_000_000,
                "absorption_rate": 0.8,
                "min_trade_price": 99,
                "max_trade_price": 100,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    reactions = root / "a1.jsonl"
    reactions.write_text(
        json.dumps(
            {
                "zone_id": "z1",
                "direction": "BUY",
                "reaction_event_ts": BASE_TS + 60,
                "frozen_ts": BASE_TS,
                "frozen_low": 99,
                "frozen_high": 101,
                "reaction_type": "CLEAN_HOLD",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    kline = root / "kline.csv"
    rows = ["timestamp,open,high,low,close,volume"]
    for i in range(90):
        ts = BASE_TS + i * 60
        rows.append(f"{ts},100,{102 if i >= 2 else 100.5},99.5,101,10")
    kline.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return {"phase1": phase1, "reactions": reactions, "kline": kline}


def test_generate_reports_writes_v7_csvs_and_summary(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "zone_truth"
    summary = ZoneTruthAnalyzer().analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
    assert (out / "zone_truth_events.csv").exists()
    assert (out / "zone_truth_3a_simulated_trades.csv").exists()
    assert (out / "zone_truth_by_3a_combo_matrix.csv").exists()
    assert (out / "zone_truth_by_shadow_evidence.csv").exists()
    assert (out / "zone_truth_by_boll_context.csv").exists()
    assert (out / "summary.json").exists()
    assert summary["v7_enabled"] is True
    summary_md = (out / "zone_truth_summary.md").read_text(encoding="utf-8")
    assert "# V7.2.1 ICEBERG 3A Context Research" in summary_md
    assert "V7.0.0 Zone Truth 3A Full Research Loop Shadow" not in summary_md
    with (out / "zone_truth_3a_simulated_trades.csv").open(encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
        assert "stop_basis_reason" in header
        assert "entry_bar_ts" in header
        assert "entry_price_source" in header


def test_generate_research_reports_zip_includes_v7_files(tmp_path, monkeypatch):
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    paths = _write_inputs(tmp_path)

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name
        out = Path(command[command.index("--out") + 1])
        out.mkdir(parents=True, exist_ok=True)
        if script == "analyze_phase1_candidates.py":
            (out / "phase1_truth_summary.md").write_text("# phase1\n", encoding="utf-8")
        elif script == "analyze_a1_edge.py":
            (out / "a1_go_no_go_report.md").write_text("# a1\n", encoding="utf-8")
        elif script == "analyze_zone_truth.py":
            ZoneTruthAnalyzer().analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = generator.main(
        [
            "--run-name",
            "v7_unit",
            "--phase1-candidates",
            str(paths["phase1"]),
            "--a1-reactions",
            str(paths["reactions"]),
            "--kline",
            str(paths["kline"]),
            "--zip",
        ],
        runner=runner,
    )
    assert rc == 0
    run_dir = tmp_path / "reports" / "v7_unit"
    assert (run_dir / "phase1_truth" / "phase1_truth_summary.md").exists()
    assert (run_dir / "a1_edge" / "a1_edge_summary.md").exists()
    assert (run_dir / "zone_truth" / "zone_truth_3a_simulated_trades.csv").exists()
    with zipfile.ZipFile(tmp_path / "reports" / "v7_unit.zip") as zf:
        assert "v7_unit/zone_truth/zone_truth_3a_simulated_trades.csv" in zf.namelist()


def test_v721_context_combo_prunes_book_proxy_and_writes_new_summaries(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "zone_truth_v721"
    ZoneTruthAnalyzer().analyze_files(paths["phase1"], paths["reactions"], paths["kline"], out)
    header = (out / "zone_truth_by_context_combo.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "book_blocking_liquidity_proxy_strength" not in header
    for name in [
        "zone_truth_shadow_evidence_events.csv",
        "zone_truth_by_vp_node_context.csv",
        "zone_truth_by_value_edge_reclaim_context.csv",
        "zone_truth_by_sweep_failed_auction_context.csv",
        "zone_truth_by_aggression_quality_context.csv",
        "zone_truth_by_session_context.csv",
        "zone_truth_by_ob_quality_context.csv",
        "zone_truth_by_poc_risk_context.csv",
    ]:
        assert (out / name).exists()
    with (out / "zone_truth_events.csv").open(encoding="utf-8", newline="") as handle:
        events_reader = csv.DictReader(handle)
        rows = list(events_reader)
        events_header = events_reader.fieldnames or []
    with (out / "zone_truth_shadow_evidence_events.csv").open(encoding="utf-8", newline="") as handle:
        shadow_header = next(csv.reader(handle))
    assert len(rows) == 1
    for field in ["visible_wall_absorption_flag", "cluster_absorption_flag", "ladder_absorption_flag"]:
        assert field not in events_header
        assert field in shadow_header


def test_v721_no_boll_or_martingale_strategy_files_exist():
    files = [str(path).lower() for path in Path(".").rglob("*.py") if ".git" not in path.parts]
    assert not any("martingale" in name for name in files)
    assert not any("boll_strategy" in name or "bollinger_strategy" in name for name in files)
