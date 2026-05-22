#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.phase1_truth.scorer import IcebergTruthScorer


def _candidate(direction="BUY", **overrides):
    base = {
        "direction": direction,
        "active_notional": 1_200_000,
        "active_side_ratio": 0.85,
        "trade_count": 35,
        "hidden_volume": 2_200_000,
        "absorption_rate": 0.9,
        "book_reduction": 100_000,
        "price_displacement": 0.1,
        "trigger_price": 100,
        "settle_price": 100,
        "zone_lower": 99,
        "zone_upper": 100.5,
        "local_zone_width": 1.5,
        "post_features": {
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_30s_min_price": 99.3,
            "post_30s_max_price": 100.4,
            "post_min_price": 99.3,
            "post_max_price": 100.4,
            "post_last_price": 100.2,
            "post_5s_cvd_delta": -400_000 if direction == "BUY" else 400_000,
            "post_30s_cvd_delta": -700_000 if direction == "BUY" else 700_000,
            "depth_recovery_ratio_1s": 0.35,
            "depth_recovery_ratio_5s": 0.60,
            "replenish_count": 3,
            "reload_interval_ms": 1500,
            "has_sweep": True,
            "reclaim_time_sec": 8,
            "time_outside_zone_30s": 2,
            "accepted_beyond_zone": False,
        },
    }
    if direction == "SELL":
        base.update({"zone_lower": 100, "zone_upper": 101.5, "settle_price": 101, "trigger_price": 101})
    base.update(overrides)
    return base


def test_strong_buy_iceberg_scores_high():
    result = IcebergTruthScorer().score(_candidate("BUY"))
    assert result["truth_score_total"] >= 80
    assert result["truth_label"] == "HIGH_CONFIDENCE_ICEBERG"


def test_buy_candidate_that_accepts_lower_price_scores_low():
    candidate = _candidate(
        "BUY",
        active_notional=250_000,
        hidden_volume=100_000,
        absorption_rate=0.2,
        price_displacement=-5.0,
        post_features={
            "post_5s_min_price": 94,
            "post_30s_min_price": 93,
            "post_min_price": 93,
            "post_last_price": 94,
            "post_5s_cvd_delta": -500_000,
            "post_30s_cvd_delta": -900_000,
            "time_outside_zone_30s": 30,
            "accepted_beyond_zone": True,
            "has_sweep": True,
            "reclaim_time_sec": -1,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["truth_score_total"] < 50


def test_sell_direction_is_symmetric():
    result = IcebergTruthScorer().score(_candidate("SELL"))
    assert result["truth_score_total"] >= 80


def test_high_hidden_volume_with_large_price_impact_is_not_extreme():
    candidate = _candidate("BUY", price_displacement=-4.0)
    result = IcebergTruthScorer().score(candidate)
    assert result["truth_score_total"] < 80


def test_low_active_notional_has_low_attack_strength():
    result = IcebergTruthScorer().score(_candidate("BUY", active_notional=100_000))
    assert result["score_components"]["attack_strength_score"] < 8
