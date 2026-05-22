import json
import subprocess
import sys


OUTPUTS = {
    "a1_edge_events.csv",
    "a1_edge_events.jsonl",
    "a1_edge_dataset_summary.json",
    "a1_edge_summary.json",
    "a1_forward_metrics.csv",
    "a1_random_baseline.csv",
    "a1_vs_random_summary.csv",
    "a1_vs_random_summary.md",
    "a1_hypothesis_results.csv",
    "a1_hypothesis_summary.csv",
    "a1_best_hypothesis_by_group.csv",
    "a1_go_no_go_report.md",
    "a1_run_metadata.json",
}

BASE_TS = 1_779_373_200


def _write_klines(path):
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "\n".join(f"{BASE_TS + i * 60},{100+i},{102+i},{99+i},{101+i},1" for i in range(40))
        + "\n",
        encoding="utf-8",
    )


def test_cli_generates_all_outputs_and_no_main_import(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": BASE_TS + 300, "last_price": 105, "frozen_low": 104, "frozen_high": 106}) + "\n", encoding="utf-8")
    klines = tmp_path / "k.csv"
    _write_klines(klines)
    out = tmp_path / "out"
    cmd = [sys.executable, "tools/analyze_a1_edge.py", "--events", str(events), "--klines", str(klines), "--out", str(out), "--seed", "42"]
    result = subprocess.run(cmd, cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]), text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    assert OUTPUTS.issubset({p.name for p in out.iterdir()})
    assert "import main" not in (__import__("pathlib").Path("tools/analyze_a1_edge.py").read_text(encoding="utf-8"))
    assert "offline-only" in subprocess.run(
        [sys.executable, "tools/analyze_a1_edge.py", "--help"],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    ).stdout.lower()


def test_cli_missing_input_and_empty_events(tmp_path):
    klines = tmp_path / "k.csv"
    _write_klines(klines)
    missing = subprocess.run(
        [sys.executable, "tools/analyze_a1_edge.py", "--events", str(tmp_path / "missing.jsonl"), "--klines", str(klines)],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert missing.returncode == 2
    assert "events path does not exist" in missing.stderr
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", encoding="utf-8")
    out = tmp_path / "empty_out"
    ok = subprocess.run(
        [sys.executable, "tools/analyze_a1_edge.py", "--events", str(empty), "--klines", str(klines), "--out", str(out)],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert ok.returncode == 0
    assert "decision=INSUFFICIENT_SAMPLE" in ok.stdout
