#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import research_evaluator as cfg
from src.research.a1_edge.io_utils import parse_windows, read_csv, read_jsonl
from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="V7.2.1 ICEBERG 3A context research and zone truth analysis.")
    parser.add_argument("--phase1-candidates", required=True, help="Phase1 candidate JSONL with candidate_settled/candidate_finalized rows")
    parser.add_argument("--a1-reactions", required=True, help="A1 reaction JSONL with zone_id/frozen zone/reaction fields")
    parser.add_argument("--kline", help="1m kline CSV used for forward metrics and runtime-compatible context labels")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--price-tolerance-usdt", type=float, default=1.5)
    parser.add_argument("--time-tolerance-sec", type=float, default=300.0)
    parser.add_argument("--windows-sec", default="900,3600,14400")
    parser.add_argument("--enable-context-labels", default="true")
    parser.add_argument("--vp-bin-size-u", type=float, default=1.0)
    parser.add_argument("--vp-value-area-ratio", type=float, default=0.70)
    parser.add_argument("--enable-3a-simulator", default=str(getattr(cfg, "V7_3A_SIMULATOR_ENABLED", True)).lower())
    parser.add_argument("--simulator-input-scope", default=str(getattr(cfg, "V7_3A_SIMULATOR_INPUT_SCOPE", "ICEBERG_ONLY")).lower(), choices=["iceberg_only", "all"])
    parser.add_argument("--simulator-include-unavailable", default=str(getattr(cfg, "V7_3A_SIMULATOR_INCLUDE_UNAVAILABLE", False)).lower())
    parser.add_argument("--simulator-max-trades", type=int, default=int(getattr(cfg, "V7_3A_SIMULATOR_MAX_TRADES", 0)))
    parser.add_argument("--enable-3a-rt-backtest", default=str(getattr(cfg, "V7_3A_RT_ENABLED", True)).lower())
    parser.add_argument("--a2-rt-max-age-sec", type=float, default=float(getattr(cfg, "A2_RT_MAX_AGE_SEC", 900.0)))
    parser.add_argument("--a2-rt-expiry-sweep-secs", default=",".join(str(x) for x in getattr(cfg, "A2_RT_EXPIRY_SWEEP_SECS", [180, 300, 600, 900, 1200, 1800])))
    parser.add_argument("--a2-rt-min-quiet-sec", type=float, default=float(getattr(cfg, "A2_RT_MIN_QUIET_SEC", 3.0)))
    parser.add_argument("--a2-rt-min-tick-count", type=int, default=int(getattr(cfg, "A2_RT_MIN_TICK_COUNT", 20)))
    parser.add_argument("--a3-rt-target-model", default=str(getattr(cfg, "V7_3A_RT_TARGET_MODEL", "TARGET_FIXED_2R")))
    parser.add_argument("--a3-rt-stop-model", default=str(getattr(cfg, "V7_3A_RT_STOP_MODEL", "STOP_STRUCTURAL_ZONE_V2")))
    parser.add_argument("--a3-rt-next-tick-entry", default=str(getattr(cfg, "V7_3A_RT_NEXT_TICK_ENTRY", False)).lower())
    parser.add_argument("--enable-no-future-audit", default=str(getattr(cfg, "V7_3A_RT_ENABLE_NO_FUTURE_AUDIT", True)).lower())
    parser.add_argument("--trades-jsonl", help="Runtime trade tick JSONL used by V7.3 no-future 3A backtest")
    parser.add_argument("--trades-dir", help="Directory containing runtime trade tick JSONL/CSV files")
    parser.add_argument("--runtime-events", help="Runtime tick bucket JSONL/CSV used by V7.3 no-future 3A backtest")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        ZoneInfo(args.timezone)
    except Exception:
        print(f"Error: invalid --timezone: {args.timezone}", file=sys.stderr)
        return 2
    phase1_path = Path(args.phase1_candidates)
    reactions_path = Path(args.a1_reactions)
    kline_path = Path(args.kline) if args.kline else None
    for label, path in (
        ("phase1 candidates", phase1_path),
        ("a1 reactions", reactions_path),
    ):
        if not path.exists():
            print(f"Error: {label} path does not exist: {path}", file=sys.stderr)
            return 2
    if kline_path is not None and not kline_path.exists():
        print(f"Error: kline path does not exist: {kline_path}", file=sys.stderr)
        return 2
    for label, raw_path in (("trades-jsonl", args.trades_jsonl), ("trades-dir", args.trades_dir), ("runtime-events", args.runtime_events)):
        if raw_path and not Path(raw_path).exists():
            print(f"Error: {label} path does not exist: {raw_path}", file=sys.stderr)
            return 2
    windows = parse_windows(args.windows_sec)
    runtime_events = _load_runtime_events(args.trades_jsonl, args.trades_dir, args.runtime_events)
    analyzer = ZoneTruthAnalyzer(
        price_tolerance_usdt=args.price_tolerance_usdt,
        time_tolerance_sec=args.time_tolerance_sec,
        windows_sec=windows,
        timezone=args.timezone,
        enable_context_labels=_parse_bool(args.enable_context_labels),
        vp_bin_size_u=args.vp_bin_size_u,
        vp_value_area_ratio=args.vp_value_area_ratio,
        enable_3a_simulator=_parse_bool(args.enable_3a_simulator),
        simulator_input_scope=args.simulator_input_scope,
        simulator_include_unavailable=_parse_bool(args.simulator_include_unavailable),
        simulator_max_trades=args.simulator_max_trades,
        enable_3a_rt_backtest=_parse_bool(args.enable_3a_rt_backtest),
        a2_rt_max_age_sec=args.a2_rt_max_age_sec,
        a2_rt_expiry_sweep_secs=_parse_int_list(args.a2_rt_expiry_sweep_secs),
        a2_rt_min_quiet_sec=args.a2_rt_min_quiet_sec,
        a2_rt_min_tick_count=args.a2_rt_min_tick_count,
        a3_rt_target_model=args.a3_rt_target_model,
        a3_rt_stop_model=args.a3_rt_stop_model,
        a3_rt_next_tick_entry=_parse_bool(args.a3_rt_next_tick_entry),
        enable_no_future_audit=_parse_bool(args.enable_no_future_audit),
    )
    summary = analyzer.analyze_files(phase1_path, reactions_path, kline_path, args.out, runtime_events=runtime_events)
    print(
        "[ZONE-TRUTH] "
        f"total_zones={summary.get('total_zones')} "
        f"a2_pre_pool_zone_count={summary.get('a2_pre_pool_zone_count')} "
        f"synthetic_zones={summary.get('synthetic_zones')} "
        f"context_labels_status={summary.get('context_labels_status')} "
        f"simulator_enabled={summary.get('simulator_enabled')} "
        f"simulator_input_scope={summary.get('simulator_input_scope')} "
        f"simulator_written_trade_count={summary.get('simulator_written_trade_count')} "
        f"rt_3a_enabled={summary.get('runtime_3a_report_summary', {}).get('runtime_3a_strategy_version', '')} "
        f"out={args.out}"
    )
    return 0


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _parse_int_list(value: object) -> list[int]:
    result: list[int] = []
    for part in str(value or "").split(","):
        try:
            result.append(int(part.strip()))
        except ValueError:
            continue
    return result or [180, 300, 600, 900, 1200, 1800]


def _load_runtime_events(trades_jsonl: str | None, trades_dir: str | None, runtime_events: str | None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path_text in (trades_jsonl, runtime_events):
        if path_text:
            rows.extend(_read_runtime_event_file(Path(path_text)))
    if trades_dir:
        root = Path(trades_dir)
        for path in sorted([*root.glob("*.jsonl"), *root.glob("*.csv")]):
            rows.extend(_read_runtime_event_file(path))
    return rows


def _read_runtime_event_file(path: Path) -> list[dict[str, object]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return [dict(row) for row in read_csv(path)]
    return [dict(row) for row in read_jsonl(path)]


if __name__ == "__main__":
    raise SystemExit(main())
