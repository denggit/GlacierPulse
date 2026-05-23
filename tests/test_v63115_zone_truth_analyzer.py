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
        "zone_truth_by_final_reaction.csv",
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
    assert rows[0]["pie_event_keys"] == "pie-1"
    assert rows[0]["reaction_count"] == "1"
    assert rows[0]["reaction_types"] == "CLEAN_HOLD"
    assert rows[0]["final_reaction_type"] == "CLEAN_HOLD"
    assert rows[0]["forward_anchor_ts"] == str(BASE_TS + 30)
    assert rows[0]["forward_anchor_source"] == "reaction_event_ts"
    assert rows[0]["forward_entry_price"] == "100.0"
    assert rows[0]["forward_entry_price_source"] == "zone_mid"
    assert rows[0]["a2_pre_pool_reason"] == "HAS_ICEBERG_PIE"


def test_expanded_parameter_grid_contains_new_values():
    assert 20_000_000 in GRID_DEPTH
    assert 2.0 in GRID_ABS


def test_zone_truth_schema_version_updated(tmp_path):
    phase1 = [
        {
            "record_type": "candidate_finalized",
            "event_key": "pie-1",
            "zone_id": "iz-1",
            "direction": "BUY",
            "result": "ICEBERG",
            "settle_ts": BASE_TS,
            "settle_price": 100,
            "zone_lower": 99,
            "zone_upper": 101,
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
        }
    ]
    klines = [{"timestamp": BASE_TS + 60, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}]
    out = tmp_path / "out"
    ZoneTruthAnalyzer().export(phase1, reactions, klines, out)
    with (out / "zone_truth_events.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["schema_version"] == "v6.3.11.5.1.zone_truth.1"


def test_group_stats_complete_only_forward_averages():
    rows = [
        {
            "truth_score_avg": 80,
            "truth_score_max": 80,
            "truth_ge65_count": 1,
            "truth_ge80_count": 1,
            "mfe_1h_u": 10,
            "mae_1h_u": -2,
            "is_complete_1h": True,
        },
        {
            "truth_score_avg": 80,
            "truth_score_max": 80,
            "truth_ge65_count": 1,
            "truth_ge80_count": 1,
            "mfe_1h_u": 100,
            "mae_1h_u": -20,
            "is_complete_1h": False,
        },
    ]
    stats = ZoneTruthAnalyzer()._group_stats("all", rows)
    assert stats["mfe_1h_avg"] == 55
    assert stats["mfe_1h_complete_avg"] == 10
    assert stats["mae_1h_avg"] == -11
    assert stats["mae_1h_complete_avg"] == -2
