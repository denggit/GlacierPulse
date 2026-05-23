#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json

from src.research.phase1_truth.analyzer import GRID_ABS, GRID_DEPTH
from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_000_000.0


def test_zone_truth_analyzer_outputs_required_files(tmp_path):
    phase1 = [
        {
            "record_type": "candidate_finalized",
            "event_key": "pie-1",
            "zone_id": "iz-1",
            "symbol": "ETH-USDT-SWAP",
            "direction": "BUY",
            "result": "ICEBERG",
            "settle_ts": BASE_TS,
            "settle_price": 100,
            "zone_lower": 99,
            "zone_upper": 101,
            "active_notional": 1_000_000,
            "hidden_volume": 1_500_000,
            "absorption_rate": 0.8,
            "truth_score": {"truth_score_total": 80, "truth_label": "HIGH_CONFIDENCE_ICEBERG"},
        }
    ]
    reactions = [
        {
            "zone_id": "iz-1",
            "direction": "BUY",
            "frozen_ts": BASE_TS,
            "reaction_event_ts": BASE_TS + 30,
            "frozen_low": 99,
            "frozen_high": 101,
            "reaction_type": "CLEAN_HOLD",
            "a1_reaction_type": "CLEAN_HOLD",
        }
    ]
    klines = [
        {"timestamp": BASE_TS + 60 + i * 60, "open": 100, "high": 101 + i, "low": 99, "close": 100 + i, "volume": 1}
        for i in range(20)
    ]
    out = tmp_path / "out"
    summary = ZoneTruthAnalyzer().export(phase1, reactions, klines, out)
    assert summary["total_zones"] == 1
    for name in (
        "zone_truth_events.csv",
        "zone_truth_summary.md",
        "zone_truth_by_reaction.csv",
        "zone_truth_match_quality.csv",
        "zone_truth_by_direction.csv",
        "zone_truth_by_session.csv",
        "zone_truth_by_truth_bucket.csv",
        "zone_truth_by_a2_pre_pool.csv",
        "zone_truth_top_cases.csv",
    ):
        assert (out / name).exists()
    with (out / "zone_truth_events.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["zone_id"] == "iz-1"
    assert rows[0]["pie_count"] == "1"
    assert rows[0]["a2_pre_pool_reason"] == "HAS_ICEBERG_PIE"


def test_expanded_parameter_grid_contains_new_values():
    assert 20_000_000 in GRID_DEPTH
    assert 2.0 in GRID_ABS
