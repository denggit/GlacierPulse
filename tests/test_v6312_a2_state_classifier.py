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
    assert non_eligible["strong_a1_raw_flag"] is True
    assert non_eligible["strong_a1_tier"] == "STRONG_A1_RAW"


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
