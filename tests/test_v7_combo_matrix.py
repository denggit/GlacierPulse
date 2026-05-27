#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.combo_matrix import build_combo_matrix, combo_key, group_stats, bad_combos, top_combos


BASE = {
    "a1_primary_evidence_type": "HIDDEN_RELOAD_ICEBERG",
    "a1_evidence_types": "HIDDEN_RELOAD_ICEBERG",
    "a1_strength_tier": "NORMAL_A1",
    "a1_best_horizon": "UNKNOWN",
    "a2_accumulation_path_v2": "A2_CLEAN_HOLD",
    "a3_aggression_type_v2": "PRICE_BREAKOUT_PERSISTENT",
    "entry_model": "BREAKOUT",
    "stop_model": "V1_ZONE_WIDTH",
    "target_r": 1.0,
    "market_context_bucket": "UNKNOWN",
    "direction": "BUY",
}


def test_combo_key_generation():
    assert combo_key(BASE)[0] == "HIDDEN_RELOAD_ICEBERG"


def test_group_stats_count_avg_median_positive_rate():
    stats = group_stats([{**BASE, "realized_r_1h": 1.0}, {**BASE, "realized_r_1h": -0.5}])
    assert stats["count"] == 2
    assert stats["avg_realized_r"] == 0.25
    assert stats["median_realized_r"] == 0.25
    assert stats["fee_positive_rate"] == 0.5


def test_profit_factor_proxy():
    stats = group_stats([{**BASE, "realized_r_1h": 1.0}, {**BASE, "realized_r_1h": -0.5}])
    assert stats["profit_factor_proxy"] == 2.0


def test_top_combo_min_sample_filter(monkeypatch):
    import config.research_evaluator as cfg

    monkeypatch.setattr(cfg, "V7_3A_MIN_SAMPLE", 2)
    matrix = build_combo_matrix([{**BASE, "realized_r_1h": 1.0}, {**BASE, "realized_r_1h": 0.5}])
    assert len(top_combos(matrix)) == 1


def test_bad_combo_output(monkeypatch):
    import config.research_evaluator as cfg

    monkeypatch.setattr(cfg, "V7_3A_MIN_SAMPLE", 2)
    matrix = build_combo_matrix([{**BASE, "realized_r_1h": -1.0}, {**BASE, "realized_r_1h": -0.5}])
    assert len(bad_combos(matrix)) == 1
