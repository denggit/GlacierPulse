#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]


ReportRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate the unified offline research report set.")
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--phase1-candidates", required=True)
    parser.add_argument("--a1-reactions", required=True)
    parser.add_argument("--kline")
    parser.add_argument("--nohup")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--enable-context-labels", default="true")
    parser.add_argument("--vp-bin-size-u", type=float, default=1.0)
    parser.add_argument("--vp-value-area-ratio", type=float, default=0.70)
    parser.add_argument("--min-sample", type=int, default=30)
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--zip", action="store_true", dest="zip_enabled")
    parser.add_argument("--enable-3a-simulator", choices=["true", "false"], default=None)
    parser.add_argument("--simulator-input-scope", choices=["iceberg_only", "all"], default=None)
    parser.add_argument("--simulator-include-unavailable", choices=["true", "false"], default=None)
    parser.add_argument("--simulator-max-trades", type=int, default=None)
    parser.add_argument("--enable-3a-rt-backtest", choices=["true", "false"], default=None)
    parser.add_argument("--a2-rt-max-age-sec", type=float, default=None)
    parser.add_argument("--a2-rt-expiry-sweep-secs", default=None)
    parser.add_argument("--a2-rt-min-quiet-sec", type=float, default=None)
    parser.add_argument("--a2-rt-min-tick-count", type=int, default=None)
    parser.add_argument("--a3-rt-target-model", default=None)
    parser.add_argument("--a3-rt-stop-model", default=None)
    parser.add_argument("--a3-rt-next-tick-entry", choices=["true", "false"], default=None)
    parser.add_argument("--enable-no-future-audit", choices=["true", "false"], default=None)
    parser.add_argument("--trades-jsonl", default=None)
    parser.add_argument("--trades-dir", default=None)
    parser.add_argument("--runtime-events", default=None)
    return parser


def main(argv: list[str] | None = None, runner: ReportRunner | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        ZoneInfo(args.timezone)
    except Exception:
        print(f"Error: invalid --timezone: {args.timezone}", file=sys.stderr)
        return 2

    inputs = {
        "phase1_candidates": Path(args.phase1_candidates),
        "a1_reactions": Path(args.a1_reactions),
    }
    if args.kline:
        inputs["kline"] = Path(args.kline)
    if args.nohup:
        inputs["nohup"] = Path(args.nohup)
    if args.trades_jsonl:
        inputs["trades_jsonl"] = Path(args.trades_jsonl)
    if args.runtime_events:
        inputs["runtime_events"] = Path(args.runtime_events)
    if args.trades_dir:
        inputs["trades_dir"] = Path(args.trades_dir)

    for label, path in inputs.items():
        if not path.exists():
            print(f"Error: {label} path does not exist: {path}", file=sys.stderr)
            return 2

    run_dir = ROOT / "reports" / args.run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(tz=ZoneInfo(args.timezone)).isoformat()
    active_paths = dict(inputs)
    file_inputs: dict[str, dict[str, Any]] = {}

    if args.snapshot:
        snapshot_dir = run_dir / "input_snapshot"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_names = {
            "phase1_candidates": "phase1_candidates.jsonl",
            "a1_reactions": "a1_reaction_events.jsonl",
            "nohup": "nohup.out",
            "trades_jsonl": "runtime_trades.jsonl",
            "runtime_events": "runtime_events.jsonl",
        }
        if "kline" in inputs:
            snapshot_names["kline"] = "kline.csv"
        for label, original in inputs.items():
            if original.is_dir():
                active_paths[label] = original
                file_inputs[label] = _file_manifest(original, None)
                continue
            snapshot_path = snapshot_dir / snapshot_names[label]
            shutil.copy2(original, snapshot_path)
            active_paths[label] = snapshot_path
            file_inputs[label] = _file_manifest(original, snapshot_path)
    else:
        for label, original in inputs.items():
            file_inputs[label] = _file_manifest(original, None)

    reports = {
        "phase1_truth": {"path": str(run_dir / "phase1_truth"), "status": "skipped"},
        "a1_edge": {"path": str(run_dir / "a1_edge"), "status": "skipped"},
        "zone_truth": {"path": str(run_dir / "zone_truth"), "status": "skipped"},
    }
    for report in reports.values():
        Path(report["path"]).mkdir(parents=True, exist_ok=True)
    runner = runner or _run_subprocess

    commands = {
        "phase1_truth": [
            sys.executable,
            str(ROOT / "tools" / "analyze_phase1_candidates.py"),
            "--events",
            str(active_paths["phase1_candidates"]),
            "--out",
            str(run_dir / "phase1_truth"),
            "--min-sample",
            str(args.min_sample),
        ],
        "a1_edge": [
            sys.executable,
            str(ROOT / "tools" / "analyze_a1_edge.py"),
            "--events",
            str(active_paths["a1_reactions"]),
            "--klines",
            str(active_paths.get("kline", "")),
            "--out",
            str(run_dir / "a1_edge"),
            "--kline-timezone",
            args.timezone,
            "--min-group-sample-size",
            str(args.min_sample),
        ],
        "zone_truth": [
            sys.executable,
            str(ROOT / "tools" / "analyze_zone_truth.py"),
            "--phase1-candidates",
            str(active_paths["phase1_candidates"]),
            "--a1-reactions",
            str(active_paths["a1_reactions"]),
            "--out",
            str(run_dir / "zone_truth"),
            "--timezone",
            args.timezone,
            "--enable-context-labels",
            args.enable_context_labels,
            "--vp-bin-size-u",
            str(args.vp_bin_size_u),
            "--vp-value-area-ratio",
            str(args.vp_value_area_ratio),
        ],
    }
    if args.enable_3a_simulator is not None:
        commands["zone_truth"].extend(["--enable-3a-simulator", args.enable_3a_simulator])
    if args.simulator_input_scope is not None:
        commands["zone_truth"].extend(["--simulator-input-scope", args.simulator_input_scope])
    if args.simulator_include_unavailable is not None:
        commands["zone_truth"].extend(["--simulator-include-unavailable", args.simulator_include_unavailable])
    if args.simulator_max_trades is not None:
        commands["zone_truth"].extend(["--simulator-max-trades", str(args.simulator_max_trades)])
    if args.enable_3a_rt_backtest is not None:
        commands["zone_truth"].extend(["--enable-3a-rt-backtest", args.enable_3a_rt_backtest])
    if args.a2_rt_max_age_sec is not None:
        commands["zone_truth"].extend(["--a2-rt-max-age-sec", str(args.a2_rt_max_age_sec)])
    if args.a2_rt_expiry_sweep_secs is not None:
        commands["zone_truth"].extend(["--a2-rt-expiry-sweep-secs", args.a2_rt_expiry_sweep_secs])
    if args.a2_rt_min_quiet_sec is not None:
        commands["zone_truth"].extend(["--a2-rt-min-quiet-sec", str(args.a2_rt_min_quiet_sec)])
    if args.a2_rt_min_tick_count is not None:
        commands["zone_truth"].extend(["--a2-rt-min-tick-count", str(args.a2_rt_min_tick_count)])
    if args.a3_rt_target_model is not None:
        commands["zone_truth"].extend(["--a3-rt-target-model", args.a3_rt_target_model])
    if args.a3_rt_stop_model is not None:
        commands["zone_truth"].extend(["--a3-rt-stop-model", args.a3_rt_stop_model])
    if args.a3_rt_next_tick_entry is not None:
        commands["zone_truth"].extend(["--a3-rt-next-tick-entry", args.a3_rt_next_tick_entry])
    if args.enable_no_future_audit is not None:
        commands["zone_truth"].extend(["--enable-no-future-audit", args.enable_no_future_audit])
    if args.trades_jsonl is not None:
        commands["zone_truth"].extend(["--trades-jsonl", str(active_paths["trades_jsonl"])])
    if args.trades_dir is not None:
        commands["zone_truth"].extend(["--trades-dir", str(active_paths["trades_dir"])])
    if args.runtime_events is not None:
        commands["zone_truth"].extend(["--runtime-events", str(active_paths["runtime_events"])])
    if "kline" in active_paths:
        commands["zone_truth"][6:6] = ["--kline", str(active_paths["kline"])]
    else:
        reports["a1_edge"]["status"] = "skipped_kline_unavailable"
        commands.pop("a1_edge")

    has_failure = False
    for report_name, command in commands.items():
        status = _run_report(report_name, command, runner)
        reports[report_name]["status"] = status
        has_failure = has_failure or status == "failed"
        if report_name == "a1_edge" and status == "success":
            _ensure_a1_summary_alias(run_dir / "a1_edge")

    _write_index(
        run_dir / "research_report_index.md",
        run_name=args.run_name,
        created_at=created_at,
        active_paths=active_paths,
        nohup_provided=bool(args.nohup),
    )

    manifest = {
        "run_name": args.run_name,
        "created_at": created_at,
        "timezone": args.timezone,
        "repo_commit": _git_commit(),
        "python_version": sys.version,
        "phase1_candidates_path": str(active_paths["phase1_candidates"]),
        "a1_reactions_path": str(active_paths["a1_reactions"]),
        "kline_path": str(active_paths["kline"]) if "kline" in active_paths else "",
        "trades_jsonl_path": str(active_paths["trades_jsonl"]) if "trades_jsonl" in active_paths else "",
        "trades_dir_path": str(active_paths["trades_dir"]) if "trades_dir" in active_paths else "",
        "runtime_events_path": str(active_paths["runtime_events"]) if "runtime_events" in active_paths else "",
        "context_labels_status": "ENABLED" if _parse_bool(args.enable_context_labels) and "kline" in active_paths else "KLINE_UNAVAILABLE",
        "nohup_path": str(active_paths["nohup"]) if "nohup" in active_paths else "",
        "snapshot_enabled": bool(args.snapshot),
        "reports": reports,
        "file_inputs": file_inputs,
    }
    _write_json(run_dir / "manifest.json", manifest)

    if args.zip_enabled:
        shutil.make_archive(str(ROOT / "reports" / args.run_name), "zip", root_dir=ROOT / "reports", base_dir=args.run_name)

    return 1 if has_failure else 0


def _run_subprocess(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=str(ROOT), text=True, check=False)


def _run_report(report_name: str, command: list[str], runner: ReportRunner) -> str:
    print(f"[REPORT] running {report_name}")
    try:
        result = runner(command)
    except Exception as exc:
        print(f"[REPORT] {report_name} failed: {exc}", file=sys.stderr)
        return "failed"
    stdout = getattr(result, "stdout", None)
    stderr = getattr(result, "stderr", None)
    if stdout:
        print(stdout, end="" if stdout.endswith("\n") else "\n")
    if stderr:
        print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")
    if result.returncode == 0:
        return "success"
    print(f"[REPORT] {report_name} failed with exit_code={result.returncode}", file=sys.stderr)
    return "failed"


def _ensure_a1_summary_alias(out_dir: Path) -> None:
    alias = out_dir / "a1_edge_summary.md"
    if alias.exists():
        return
    source = out_dir / "a1_go_no_go_report.md"
    if source.exists():
        shutil.copy2(source, alias)


def _file_manifest(original_path: Path, snapshot_path: Path | None) -> dict[str, Any]:
    active = snapshot_path or original_path
    stat = active.stat()
    if active.is_dir():
        return {
            "original_path": str(original_path),
            "snapshot_path": str(snapshot_path) if snapshot_path else "",
            "size_bytes": 0,
            "file_size_bytes": 0,
            "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "sha256": "DIRECTORY",
        }
    return {
        "original_path": str(original_path),
        "snapshot_path": str(snapshot_path) if snapshot_path else "",
        "size_bytes": stat.st_size,
        "file_size_bytes": stat.st_size,
        "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "sha256": _sha256(active),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "UNKNOWN"


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def _write_index(
    path: Path,
    run_name: str,
    created_at: str,
    active_paths: dict[str, Path],
    nohup_provided: bool,
) -> None:
    nohup_text = str(active_paths["nohup"]) if nohup_provided and "nohup" in active_paths else "N/A"
    lines = [
        "# Research Report Index",
        "",
        f"Run: {run_name}",
        "",
        f"Generated at: {created_at}",
        "",
        "Inputs:",
        f"- phase1_candidates: {active_paths['phase1_candidates']}",
        f"- a1_reactions: {active_paths['a1_reactions']}",
        f"- kline: {active_paths['kline'] if 'kline' in active_paths else 'KLINE_UNAVAILABLE'}",
        f"- nohup: {nohup_text}",
        "",
        "Reports:",
        "- Phase1 Truth: phase1_truth/phase1_truth_summary.md",
        "- A1 Edge: a1_edge/a1_edge_summary.md",
        "- Zone Truth: zone_truth/zone_truth_summary.md",
        "",
        "Files to send for review:",
        f"- reports/{run_name}.zip",
        f"- or the whole reports/{run_name}/ directory",
        "",
        "Notes:",
        "- A2_PRE_POOL is zone-level.",
        "- Truth Score and MFE/MAE are offline research fields.",
        "- Kline should cover last zone time + 4h for complete 4h forward metrics.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
