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
from src.research.a1_edge.io_utils import parse_windows
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
    windows = parse_windows(args.windows_sec)
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
    )
    summary = analyzer.analyze_files(phase1_path, reactions_path, kline_path, args.out)
    print(
        "[ZONE-TRUTH] "
        f"total_zones={summary.get('total_zones')} "
        f"a2_pre_pool_zone_count={summary.get('a2_pre_pool_zone_count')} "
        f"synthetic_zones={summary.get('synthetic_zones')} "
        f"context_labels_status={summary.get('context_labels_status')} "
        f"simulator_enabled={summary.get('simulator_enabled')} "
        f"simulator_input_scope={summary.get('simulator_input_scope')} "
        f"simulator_written_trade_count={summary.get('simulator_written_trade_count')} "
        f"out={args.out}"
    )
    return 0


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    raise SystemExit(main())
