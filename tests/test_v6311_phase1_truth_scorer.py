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
            "post_trade_count": 42,
            "post_total_notional": 1_100_000,
            "observation_age_sec": 120,
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
            "local_depth_last": 900_000,
            "local_depth_max": 1_100_000,
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


def test_no_post_data_caps_score_and_labels_insufficient():
    candidate = _candidate("BUY", post_features={})
    result = IcebergTruthScorer().score(candidate)
    assert result["truth_label"] == "INSUFFICIENT_POST_DATA"
    assert result["truth_score_total"] <= 49
    assert "insufficient_post_trade_data" in result["score_warnings"]


def test_no_post_data_does_not_get_non_acceptance_points():
    result = IcebergTruthScorer().score(_candidate("BUY", post_features={}))
    assert result["score_components"]["non_acceptance_score"] == 0


def test_no_cvd_data_does_not_get_cvd_divergence_points():
    candidate = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 0,
            "post_total_notional": 0,
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_30s_min_price": 99.3,
            "post_30s_max_price": 100.4,
            "post_min_price": 99.3,
            "post_max_price": 100.4,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["cvd_divergence_score"] == 0


def test_no_book_data_does_not_get_reload_replenish_points():
    candidate = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 10,
            "post_total_notional": 250_000,
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_30s_min_price": 99.3,
            "post_30s_max_price": 100.4,
            "post_min_price": 99.3,
            "post_max_price": 100.4,
            "local_depth_last": 0,
            "local_depth_max": 0,
            "depth_recovery_ratio_1s": 0,
            "depth_recovery_ratio_5s": 0,
            "depth_recovery_ratio_30s": 0,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["reload_replenish_score"] == 0


def test_valid_post_data_can_score_high():
    result = IcebergTruthScorer().score(_candidate("BUY"))
    assert result["truth_score_total"] >= 80
    assert result["truth_label"] == "HIGH_CONFIDENCE_ICEBERG"


def test_negative_hidden_volume_caps_score_to_not_iceberg():
    result = IcebergTruthScorer().score(
        _candidate("BUY", hidden_volume=-2_000_000, absorption_rate=0.9)
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "NOT_ICEBERG"
    assert "negative_hidden_volume_cap" in result["score_warnings"]


def test_negative_absorption_rate_caps_score_to_not_iceberg():
    result = IcebergTruthScorer().score(
        _candidate("BUY", hidden_volume=2_000_000, absorption_rate=-1.0)
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "NOT_ICEBERG"
    assert "negative_absorption_rate_cap" in result["score_warnings"]


def test_spoofing_withdrawal_behavior_caps_score_even_with_good_post_reaction():
    result = IcebergTruthScorer().score(
        _candidate(
            "BUY",
            behavior="SPOOFING_WITHDRAWAL",
            hidden_volume=-2_000_000,
            absorption_rate=-1.5,
        )
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "NOT_ICEBERG"
    assert "spoofing_withdrawal_cap" in result["score_warnings"]


def test_spoofing_result_caps_score_even_with_good_post_reaction():
    result = IcebergTruthScorer().score(
        _candidate("BUY", result="SPOOFING", behavior="", hidden_volume=2_000_000)
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "NOT_ICEBERG"
    assert "spoofing_result_cap" in result["score_warnings"]


def test_excessive_book_reduction_caps_total_score():
    result = IcebergTruthScorer().score(
        _candidate(
            "BUY",
            active_notional=1_000_000,
            book_reduction=1_500_000,
            hidden_volume=100_000,
        )
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "NOT_ICEBERG"
    assert "excessive_book_reduction_cap" in result["score_warnings"]


def test_insufficient_post_data_label_has_priority_over_spoofing_cap():
    result = IcebergTruthScorer().score(
        _candidate(
            "BUY",
            result="SPOOFING",
            behavior="SPOOFING_WITHDRAWAL",
            hidden_volume=-2_000_000,
            post_features={},
        )
    )
    assert result["truth_score_total"] <= 49
    assert result["truth_label"] == "INSUFFICIENT_POST_DATA"
    assert "insufficient_post_trade_data" in result["score_warnings"]
    assert "negative_hidden_volume_cap" in result["score_warnings"]
    assert "spoofing_result_cap" in result["score_warnings"]
    assert "spoofing_withdrawal_cap" in result["score_warnings"]


def test_valid_positive_hidden_iceberg_not_capped():
    result = IcebergTruthScorer().score(
        _candidate(
            "BUY",
            result="ICEBERG",
            behavior="ICEBERG_ABSORPTION",
            hidden_volume=2_200_000,
            absorption_rate=0.9,
        )
    )
    assert result["truth_score_total"] >= 80
    assert result["truth_label"] == "HIGH_CONFIDENCE_ICEBERG"
    cap_warnings = {
        "negative_hidden_volume_cap",
        "negative_absorption_rate_cap",
        "spoofing_withdrawal_cap",
        "spoofing_result_cap",
        "excessive_book_reduction_cap",
    }
    assert cap_warnings.isdisjoint(result["score_warnings"])


def test_no_sweep_does_not_get_sweep_reclaim_score():
    candidate = _candidate(
        "BUY",
        post_features={
            **_candidate("BUY")["post_features"],
            "has_sweep": False,
            "seen_sweep": False,
            "time_outside_zone": 0,
            "time_outside_zone_30s": 0,
            "reclaim_time_sec": -1,
            "observed_through_30s": True,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["sweep_reclaim_score"] == 0


def test_non_acceptance_requires_post_price_data():
    candidate = _candidate(
        "BUY",
        post_features={
            "observed_through_120s": True,
            "accepted_beyond_zone": False,
            "post_trade_count": 0,
            "post_total_notional": 0,
            "post_last_price": 0,
            "post_min_price": 0,
            "post_max_price": 0,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["non_acceptance_score"] == 0


def test_non_acceptance_can_score_with_valid_120s_price_data():
    candidate = _candidate(
        "BUY",
        post_features={
            "observed_through_120s": True,
            "post_trade_count": 10,
            "post_total_notional": 500_000,
            "post_last_price": 100.2,
            "post_min_price": 99.3,
            "post_max_price": 100.4,
            "accepted_beyond_zone": False,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["non_acceptance_score"] > 0


def test_cvd_30s_score_requires_30s_observation():
    candidate = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 10,
            "post_total_notional": 500_000,
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_min_price": 99.4,
            "post_max_price": 100.3,
            "post_last_price": 100.1,
            "post_5s_cvd_delta": -300_000,
            "post_30s_cvd_delta": -600_000,
            "observed_through_30s": False,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["cvd_divergence_score"] <= 4


def test_cvd_30s_score_requires_observed_through_30s_even_if_post_30s_prices_exist():
    candidate = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 10,
            "post_total_notional": 500_000,
            "observation_age_sec": 5,
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_30s_min_price": 99.3,
            "post_30s_max_price": 100.4,
            "post_min_price": 99.3,
            "post_max_price": 100.4,
            "post_last_price": 100.1,
            "post_5s_cvd_delta": -300_000,
            "post_30s_cvd_delta": -600_000,
            "observed_through_30s": False,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["cvd_divergence_score"] <= 4


def test_cvd_30s_score_allowed_after_observed_through_30s():
    candidate = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 20,
            "post_total_notional": 900_000,
            "observation_age_sec": 31,
            "post_30s_min_price": 99.4,
            "post_30s_max_price": 100.3,
            "post_min_price": 99.4,
            "post_max_price": 100.3,
            "post_last_price": 100.1,
            "post_30s_cvd_delta": -700_000,
            "observed_through_30s": True,
        },
    )
    result = IcebergTruthScorer().score(candidate)
    assert result["score_components"]["cvd_divergence_score"] >= 6


def test_cvd_extreme_price_not_extreme_requires_30s_coverage():
    missing_coverage = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 20,
            "post_total_notional": 900_000,
            "post_5s_min_price": 99.4,
            "post_5s_max_price": 100.3,
            "post_30s_min_price": 99.4,
            "post_30s_max_price": 100.3,
            "post_min_price": 99.4,
            "post_max_price": 100.3,
            "post_last_price": 100.1,
            "post_5s_cvd_delta": -300_000,
            "post_30s_cvd_delta": -700_000,
            "observed_through_30s": False,
            "cvd_extreme_price_not_extreme": True,
        },
    )
    result = IcebergTruthScorer().score(missing_coverage)
    assert result["score_components"]["cvd_divergence_score"] < 10

    valid_coverage = _candidate(
        "BUY",
        post_features={
            "post_trade_count": 20,
            "post_total_notional": 900_000,
            "post_30s_min_price": 99.4,
            "post_30s_max_price": 100.3,
            "post_min_price": 99.4,
            "post_max_price": 100.3,
            "post_last_price": 100.1,
            "post_30s_cvd_delta": -700_000,
            "observed_through_30s": True,
            "cvd_extreme_price_not_extreme": True,
        },
    )
    result = IcebergTruthScorer().score(valid_coverage)
    assert result["score_components"]["cvd_divergence_score"] == 10


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
            "post_max_price": 100,
            "post_last_price": 94,
            "post_trade_count": 20,
            "post_total_notional": 900_000,
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
