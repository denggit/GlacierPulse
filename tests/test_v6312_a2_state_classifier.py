from src.research.zone_truth.a2_state import ZoneA2StateClassifier


def _classify(row):
    return ZoneA2StateClassifier().classify_row(row)


def test_a2_pre_pool_rule_is_only_iceberg_pie_count():
    eligible = _classify({"a2_pre_pool_eligible": True, "iceberg_pie_count": 1, "strong_a1_raw_flag": False})
    non_eligible = _classify(
        {
            "a2_pre_pool_eligible": False,
            "iceberg_pie_count": 0,
            "max_active_notional": 2_000_000,
            "max_hidden_volume": 3_000_000,
            "max_absorption_rate": 0.8,
        }
    )

    assert eligible["a2_state"] == "A2_PRE_POOL"
    assert non_eligible["a2_state"] == "NOT_A2_PRE_POOL"
    assert non_eligible["strong_a1_raw_flag"] is False
    assert non_eligible["strong_a1_tier"] == "NON_A2"


def test_clean_hold_classification():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "has_failed_reclaim": False,
        }
    )
    assert row["a2_state"] == "A2_CLEAN_HOLD"
    assert row["a2_clean_hold_flag"] is True


def test_failed_reclaim_classification():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "reaction_type": "A1_REACTION_FAILED_RECLAIM",
        }
    )
    assert row["a2_state"] == "A2_FAILED_RECLAIM"
    assert row["a2_failed_reclaim_flag"] is True


def test_book_depth_classification():
    classifier = ZoneA2StateClassifier()
    assert classifier.classify_book_depth({"direction": "BUY", "bid_depth_near_zone": 1}) == "BOOK_DEPTH_VALID"
    assert classifier.classify_book_depth({"direction": "SELL", "ask_depth_near_zone": 1}) == "BOOK_DEPTH_VALID"
    assert classifier.classify_book_depth({"direction": "BUY"}) == "BOOK_DEPTH_UNKNOWN"
    assert (
        classifier.classify_book_depth({"direction": "BUY", "relevant_book_depth_available": False})
        == "BOOK_DEPTH_MISSING"
    )


def test_context_alignment():
    classifier = ZoneA2StateClassifier()
    assert classifier.classify_context_alignment({"direction": "BUY", "trend_alignment": "ALIGNED_UP"}) == "ALIGNED"
    assert classifier.classify_context_alignment({"direction": "SELL", "trend_alignment": "ALIGNED_DOWN"}) == "ALIGNED"
    assert classifier.classify_context_alignment({"direction": "BUY", "trend_alignment": "ALIGNED_DOWN"}) == "COUNTER_TREND"
    assert classifier.classify_context_alignment({"direction": "SELL", "trend_alignment": "ALIGNED_UP"}) == "COUNTER_TREND"
    assert classifier.classify_context_alignment({"direction": "BUY", "trend_alignment": "UNKNOWN"}) == "MIXED_OR_UNKNOWN"


def test_strong_a1_thresholds():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "max_active_notional": 1_500_000,
            "max_hidden_volume": 2_000_000,
            "max_absorption_rate": 0.7,
        }
    )
    assert row["strong_a1_raw_flag"] is True
    assert row["strong_a1_tier"] == "STRONG_A1_RAW"
    assert row["strong_a1_reason"] == "active>=1500000|hidden>=2000000|absorption>=0.7"


def test_strong_a1_only_applies_to_a2_pre_pool():
    raw_strong = {
        "max_active_notional": 1_500_000,
        "max_hidden_volume": 2_000_000,
        "max_absorption_rate": 0.7,
    }

    non_a2 = _classify({"a2_pre_pool_eligible": False, **raw_strong})
    strong_a2 = _classify({"a2_pre_pool_eligible": True, **raw_strong})
    normal_a2 = _classify(
        {
            "a2_pre_pool_eligible": True,
            "max_active_notional": 1_499_999,
            "max_hidden_volume": 2_000_000,
            "max_absorption_rate": 0.7,
        }
    )

    assert non_a2["strong_a1_raw_flag"] is False
    assert non_a2["strong_a1_tier"] == "NON_A2"
    assert strong_a2["strong_a1_raw_flag"] is True
    assert strong_a2["strong_a1_tier"] == "STRONG_A1_RAW"
    assert normal_a2["strong_a1_raw_flag"] is False
    assert normal_a2["strong_a1_tier"] == "NORMAL_A1"


def test_validated_candidate_flag_is_research_only():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "has_failed_reclaim": False,
            "relevant_book_depth_available": True,
            "direction": "BUY",
            "trend_alignment": "ALIGNED_UP",
        }
    )
    assert row["a2_validated_candidate_flag"] is True
    assert row["a2_validation_score"] > 0


def test_clean_hold_with_book_depth_unknown_is_not_validated_candidate():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "has_failed_reclaim": False,
            "direction": "BUY",
            "trend_alignment": "ALIGNED_UP",
        }
    )
    assert row["a2_book_depth_state"] == "BOOK_DEPTH_UNKNOWN"
    assert row["a2_validated_candidate_flag"] is False


def test_a2_observe_priority_high_and_watch():
    high = _classify(
        {
            "a2_pre_pool_eligible": True,
            "a2_validated_candidate_flag": True,
            "a2_context_alignment": "ALIGNED",
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
        }
    )
    watch = _classify(
        {
            "a2_pre_pool_eligible": True,
            "a2_validated_candidate_flag": True,
            "a2_context_alignment": "MIXED_OR_UNKNOWN",
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
        }
    )

    assert high["a2_observe_priority"] == "A2_HIGH"
    assert watch["a2_observe_priority"] == "A2_WATCH"


def test_a2_observe_priority_block_reason_precedence_and_low():
    failed = _classify({"a2_pre_pool_eligible": True, "has_failed_reclaim": True})
    missing = _classify({"a2_pre_pool_eligible": True, "a2_book_depth_state": "BOOK_DEPTH_MISSING"})
    counter = _classify(
        {
            "a2_pre_pool_eligible": True,
            "a2_context_alignment": "COUNTER_TREND",
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
        }
    )
    low = _classify({"a2_pre_pool_eligible": True, "a2_book_depth_state": "BOOK_DEPTH_UNKNOWN"})

    assert failed["a2_observe_priority"] == "A2_BLOCKED"
    assert failed["a2_block_reason"] == "FAILED_RECLAIM"
    assert missing["a2_observe_priority"] == "A2_BLOCKED"
    assert missing["a2_block_reason"] == "BOOK_DEPTH_MISSING"
    assert counter["a2_observe_priority"] == "A2_BLOCKED"
    assert counter["a2_block_reason"] == "COUNTER_TREND"
    assert low["a2_observe_priority"] == "A2_LOW"


def test_a2_risk_tiers():
    low = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "ALIGNED",
        }
    )
    unknown_book = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_UNKNOWN",
            "a2_context_alignment": "ALIGNED",
        }
    )
    mixed = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "MIXED_OR_UNKNOWN",
        }
    )
    failed = _classify({"a2_pre_pool_eligible": True, "has_failed_reclaim": True})
    missing = _classify({"a2_pre_pool_eligible": True, "a2_book_depth_state": "BOOK_DEPTH_MISSING"})
    counter = _classify(
        {
            "a2_pre_pool_eligible": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "COUNTER_TREND",
        }
    )

    assert low["a2_risk_tier"] == "LOW_RISK"
    assert unknown_book["a2_risk_tier"] == "MEDIUM_RISK"
    assert mixed["a2_risk_tier"] == "MEDIUM_RISK"
    assert failed["a2_risk_tier"] == "HIGH_RISK"
    assert missing["a2_risk_tier"] == "HIGH_RISK"
    assert counter["a2_risk_tier"] == "HIGH_RISK"


def test_a2_lifecycle_metrics():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "frozen_ts": 100,
            "reaction_event_ts": 160,
            "has_clean_hold": True,
        }
    )

    assert row["a2_reaction_latency_sec"] == 60
    assert row["a2_time_to_clean_hold_sec"] == 60


def test_a2_sweep_reclaim_quality():
    clean_no_sweep = _classify({"a2_pre_pool_eligible": True, "has_clean_hold": True})
    sweep_no_reclaim = _classify({"a2_pre_pool_eligible": True, "reaction_type": "SWEEP"})
    sweep_reclaim_retest = _classify(
        {
            "a2_pre_pool_eligible": True,
            "reaction_type": "SWEEP_RECLAIM_RETEST",
        }
    )
    failed = _classify({"a2_pre_pool_eligible": True, "reaction_type": "A1_REACTION_FAILED_RECLAIM"})

    assert clean_no_sweep["a2_sweep_reclaim_quality"] == "CLEAN_HOLD_NO_SWEEP"
    assert sweep_no_reclaim["a2_sweep_reclaim_quality"] == "SWEEP_NO_RECLAIM"
    assert sweep_reclaim_retest["a2_sweep_reclaim_quality"] == "SWEEP_RECLAIM_RETEST"
    assert failed["a2_sweep_reclaim_quality"] == "FAILED_RECLAIM"


def test_a2_ready_for_a3_watch():
    ready_high = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "ALIGNED",
        }
    )
    ready_watch = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "MIXED_OR_UNKNOWN",
        }
    )
    failed = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "has_failed_reclaim": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "ALIGNED",
        }
    )
    missing = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_MISSING",
            "a2_context_alignment": "ALIGNED",
        }
    )
    counter = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "COUNTER_TREND",
        }
    )

    assert ready_high["a2_ready_for_a3_watch_flag"] is True
    assert ready_high["a3_watch_priority"] == "MEDIUM"
    assert ready_watch["a2_ready_for_a3_watch_flag"] is True
    assert ready_watch["a3_watch_priority"] == "LOW"
    assert failed["a2_ready_for_a3_watch_flag"] is False
    assert missing["a2_ready_for_a3_watch_flag"] is False
    assert counter["a2_ready_for_a3_watch_flag"] is False


def test_no_future_gate_fields_do_not_change_with_forward_metrics():
    base = {
        "a2_pre_pool_eligible": True,
        "has_clean_hold": True,
        "a2_book_depth_state": "BOOK_DEPTH_VALID",
        "a2_context_alignment": "ALIGNED",
        "zone_lower": 99,
        "zone_upper": 101,
        "zone_width": 2,
    }
    quiet = _classify({**base, "mfe_15m_u": 1, "mae_15m_u": -1})
    volatile = _classify({**base, "mfe_15m_u": 20, "mae_15m_u": -10})

    assert quiet["a2_observe_priority"] == volatile["a2_observe_priority"]
    assert quiet["a2_risk_tier"] == volatile["a2_risk_tier"]
    assert quiet["a2_ready_for_a3_watch_flag"] == volatile["a2_ready_for_a3_watch_flag"]
    assert quiet["a2_compression_state"] != volatile["a2_compression_state"]
