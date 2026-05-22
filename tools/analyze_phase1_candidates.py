#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.phase1_truth.analyzer import Phase1TruthAnalyzer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze V6.3.11 Phase1 truth candidate JSONL.")
    parser.add_argument("--events", default="logs/research/phase1_candidates.jsonl")
    parser.add_argument("--out", default="reports/phase1_truth/default")
    parser.add_argument("--min-sample", type=int, default=30)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    events = Path(args.events)
    if not events.exists():
        print(f"Error: events path does not exist: {events}", file=sys.stderr)
        return 2
    analyzer = Phase1TruthAnalyzer(min_sample=args.min_sample)
    summary = analyzer.analyze_file(events, args.out)
    print(f"[PHASE1-TRUTH-ANALYZER] finalized={summary.get('finalized_total')} out={args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
