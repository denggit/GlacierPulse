#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.combo_matrix import COMBO_KEY_FIELDS, build_combo_matrix, combo_key, group_stats, bad_combos, top_combos, is_valid_simulated_trade
from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE = {
    "a1_primary_evidence_type": "ICEBERG",
    "a1_evidence_types": "ICEBERG",
    "a1_strength_tier": "NORMAL_A1",
    "a1_best_horizon": "UNKNOWN",
    "a2_accumulation_path_v2": "A2_CLEAN_HOLD",
    "a3_quality_future_type_v2": "STRONG_ORDERFLOW_AGGRESSION",
    "entry_model": "BREAKOUT",
    "stop_model": "V1_ZONE_WIDTH",
    "target_r": 1.0,
    "entry_ts": 1000,
    "entry_price": 100,
    "risk_u": 1,
    "realized_outcome_1h_sim": "CLOSE_EXIT",
    "market_context_bucket": "UNKNOWN",
    "direction": "BUY",
}


def test_combo_key_generation():
    assert combo_key(BASE)[0] == "ICEBERG"


def test_group_stats_count_avg_median_positive_rate():
    stats = group_stats([{**BASE, "realized_r_1h_sim": 1.0}, {**BASE, "realized_r_1h_sim": -0.5}])
    assert stats["count"] == 2
    assert stats["avg_realized_r_sim"] == 0.25
    assert stats["median_realized_r_sim"] == 0.25
    assert stats["fee_positive_rate"] == 0.5


def test_profit_factor_proxy():
    stats = group_stats([{**BASE, "realized_r_1h_sim": 1.0}, {**BASE, "realized_r_1h_sim": -0.5}])
    assert stats["profit_factor_proxy"] == 2.0


def test_combo_metrics_are_sim_fields():
    matrix = build_combo_matrix([{**BASE, "realized_r_1h_sim": 1.0}])
    assert "avg_realized_r_sim" in matrix[0]
    assert "median_realized_r_sim" in matrix[0]
    assert "avg_mfe_r_sim" in matrix[0]
    assert "avg_mae_r_sim" in matrix[0]
    assert "avg_realized_r" not in matrix[0]


def test_top_combo_min_sample_filter(monkeypatch):
    import config.research_evaluator as cfg

    monkeypatch.setattr(cfg, "V7_3A_MIN_SAMPLE", 2)
    matrix = build_combo_matrix([{**BASE, "realized_r_1h_sim": 1.0}, {**BASE, "realized_r_1h_sim": 0.5}])
    assert len(top_combos(matrix)) == 1


def test_bad_combo_output(monkeypatch):
    import config.research_evaluator as cfg

    monkeypatch.setattr(cfg, "V7_3A_MIN_SAMPLE", 2)
    matrix = build_combo_matrix([{**BASE, "realized_r_1h_sim": -1.0}, {**BASE, "realized_r_1h_sim": -0.5}])
    assert len(bad_combos(matrix)) == 1


def test_combo_matrix_excludes_invalid_simulated_trades_but_keeps_source_rows_available():
    trades = [
        {**BASE, "entry_ts": 1000, "entry_price": 100, "risk_u": 1, "realized_r_1h_sim": 1.0, "realized_outcome_1h_sim": "TARGET_1R_FIRST"},
        {**BASE, "entry_ts": 0, "entry_price": 0, "risk_u": 0, "realized_r_1h_sim": 0.0, "realized_outcome_1h_sim": "NO_ENTRY"},
        {**BASE, "entry_ts": 1000, "entry_price": 100, "risk_u": 0, "realized_r_1h_sim": 0.0, "realized_outcome_1h_sim": "INVALID_STOP"},
    ]
    matrix = build_combo_matrix(trades)
    assert len(trades) == 3
    assert len(matrix) == 1
    assert matrix[0]["count"] == 1


def test_zone_truth_simulated_trade_groups_are_mainline_only_by_default():
    trades = [
        {**BASE, "entry_model": "BREAKOUT", "realized_r_1h_sim": 1.0},
        {**BASE, "entry_model": "PULLBACK", "a1_primary_evidence_type": "SWEEP", "realized_r_1h_sim": 1.0},
        {**BASE, "entry_model": "FAILED", "entry_ts": 0, "entry_price": 0, "risk_u": 0, "realized_outcome_1h_sim": "NO_ENTRY"},
    ]
    rows = ZoneTruthAnalyzer().group_simulated_trades(trades, "entry_model")
    assert rows == [{"entry_model": "BREAKOUT", **group_stats([trades[0]])}]


def test_v721_main_combo_prunes_shadow_evidence_from_key():
    assert "a1_evidence_types" not in COMBO_KEY_FIELDS
    rows = build_combo_matrix([
        {**BASE, "a1_evidence_types": "ICEBERG", "realized_r_1h_sim": 1.0},
        {**BASE, "a1_evidence_types": "ICEBERG|VISIBLE_WALL", "realized_r_1h_sim": 2.0},
    ])
    assert len(rows) == 1
    assert rows[0]["count"] == 2


def test_v721_mainline_trade_filter_requirements_remain_strict():
    assert is_valid_simulated_trade(BASE) is True
    assert is_valid_simulated_trade({**BASE, "a1_primary_evidence_type": "VISIBLE_WALL"}) is False
    assert is_valid_simulated_trade({**BASE, "a3_quality_future_type_v2": "PRICE_BREAKOUT_PERSISTENT"}) is False
    assert is_valid_simulated_trade({**BASE, "a3_quality_future_type_v2": "PRICE_BREAKOUT_WEAK"}) is False
    assert is_valid_simulated_trade({**BASE, "target_r": 0.5}) is False
    assert is_valid_simulated_trade({**BASE, "entry_price": 0}) is False
    assert is_valid_simulated_trade({**BASE, "risk_u": 0}) is False
