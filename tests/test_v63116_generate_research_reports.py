#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import tools.generate_research_reports as generator


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
                "settle_price": 100,
                "zone_lower": 99,
                "zone_upper": 101,
                "truth_score": {"truth_score_total": 80},
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
                "reaction_event_ts": BASE_TS + 3600,
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
    kline.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "\n".join(f"{BASE_TS + i * 60},{100+i*0.01},{101+i*0.01},{99+i*0.01},{100+i*0.01},{10+i%5}" for i in range(420))
        + "\n",
        encoding="utf-8",
    )
    nohup = root / "nohup.out"
    nohup.write_text("running\n", encoding="utf-8")
    return {"phase1": phase1, "reactions": reactions, "kline": kline, "nohup": nohup}


def _fake_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
    script = Path(command[1]).name
    out_dir = Path(command[command.index("--out") + 1])
    out_dir.mkdir(parents=True, exist_ok=True)
    if script == "analyze_phase1_candidates.py":
        (out_dir / "phase1_truth_summary.md").write_text("# phase1\n", encoding="utf-8")
        (out_dir / "phase1_candidate_events.csv").write_text("event_key\n", encoding="utf-8")
    elif script == "analyze_a1_edge.py":
        (out_dir / "a1_go_no_go_report.md").write_text("# A1 Edge Validation Report\n", encoding="utf-8")
        (out_dir / "a1_edge_events.csv").write_text("event_key\n", encoding="utf-8")
    elif script == "analyze_zone_truth.py":
        (out_dir / "zone_truth_summary.md").write_text("# zone\n", encoding="utf-8")
        (out_dir / "zone_truth_events.csv").write_text("market_context_anchor_ts\n", encoding="utf-8")
    return subprocess.CompletedProcess(command, 0, stdout=f"{script} ok\n", stderr="")


def test_generate_research_reports_creates_snapshot_manifest_index_and_zip(tmp_path, monkeypatch):
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    paths = _write_inputs(tmp_path)
    rc = generator.main(
        [
            "--run-name",
            "unit_run",
            "--phase1-candidates",
            str(paths["phase1"]),
            "--a1-reactions",
            str(paths["reactions"]),
            "--kline",
            str(paths["kline"]),
            "--nohup",
            str(paths["nohup"]),
            "--timezone",
            "Asia/Shanghai",
            "--snapshot",
            "--zip",
        ],
        runner=_fake_runner,
    )
    assert rc == 0
    run_dir = tmp_path / "reports" / "unit_run"
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "research_report_index.md").exists()
    assert (run_dir / "phase1_truth").is_dir()
    assert (run_dir / "a1_edge").is_dir()
    assert (run_dir / "zone_truth").is_dir()
    assert (run_dir / "input_snapshot" / "phase1_candidates.jsonl").exists()
    assert (run_dir / "input_snapshot" / "a1_reaction_events.jsonl").exists()
    assert (run_dir / "input_snapshot" / "kline.csv").exists()
    assert (run_dir / "input_snapshot" / "nohup.out").exists()
    assert (run_dir / "a1_edge" / "a1_edge_summary.md").exists()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["reports"]["phase1_truth"]["status"] == "success"
    assert manifest["reports"]["a1_edge"]["status"] == "success"
    assert manifest["reports"]["zone_truth"]["status"] == "success"
    assert manifest["file_inputs"]["phase1_candidates"]["sha256"]
    zip_path = tmp_path / "reports" / "unit_run.zip"
    assert zip_path.exists()
    with zipfile.ZipFile(zip_path) as zf:
        assert "unit_run/manifest.json" in zf.namelist()


def test_generate_research_reports_continues_after_analyzer_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    paths = _write_inputs(tmp_path)
    seen = []

    def failing_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name
        seen.append(script)
        out_dir = Path(command[command.index("--out") + 1])
        out_dir.mkdir(parents=True, exist_ok=True)
        if script == "analyze_phase1_candidates.py":
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="boom")
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    rc = generator.main(
        [
            "--run-name",
            "failure_run",
            "--phase1-candidates",
            str(paths["phase1"]),
            "--a1-reactions",
            str(paths["reactions"]),
            "--kline",
            str(paths["kline"]),
        ],
        runner=failing_runner,
    )
    assert rc == 1
    assert seen == ["analyze_phase1_candidates.py", "analyze_a1_edge.py", "analyze_zone_truth.py"]
    manifest = json.loads((tmp_path / "reports" / "failure_run" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["reports"]["phase1_truth"]["status"] == "failed"
    assert manifest["reports"]["a1_edge"]["status"] == "success"
    assert manifest["reports"]["zone_truth"]["status"] == "success"


def test_generate_research_reports_light_integration_with_real_zone_truth(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "zone"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_zone_truth.py",
            "--phase1-candidates",
            str(paths["phase1"]),
            "--a1-reactions",
            str(paths["reactions"]),
            "--kline",
            str(paths["kline"]),
            "--out",
            str(out),
            "--timezone",
            "Asia/Shanghai",
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    text = (out / "zone_truth_events.csv").read_text(encoding="utf-8")
    assert "market_context_anchor_ts" in text
    assert "pre_1h_return_pct" in text
