#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from zoneinfo import ZoneInfo

from src.research.zone_truth.aggregator import ZoneTruthAggregator
from src.research.zone_truth.models import local_session


BASE_TS = 1_779_000_000.0


def _pie(key="pie-1", result="ICEBERG", score=80, zone_id="", price=100.0, ts=BASE_TS, direction="BUY", warnings=None):
    return {
        "record_type": "candidate_finalized",
        "event_key": key,
        "zone_id": zone_id,
        "symbol": "ETH-USDT-SWAP",
        "direction": direction,
        "result": result,
        "quality": "HIGH",
        "behavior": "ICEBERG_ABSORPTION",
        "settle_ts": ts,
        "settle_price": price,
        "zone_lower": price - 1.0,
        "zone_upper": price + 1.0,
        "active_notional": 1_000_000,
        "hidden_volume": 1_500_000,
        "absorption_rate": 0.8,
        "truth_score": {
            "truth_score_total": score,
            "truth_label": "HIGH_CONFIDENCE_ICEBERG" if score >= 65 else "NOT_ICEBERG",
            "score_warnings": warnings or [],
        },
    }


def _reaction(zone_id="iz-1", direction="BUY", ts=BASE_TS + 20, low=99.0, high=101.0, reaction="CLEAN_HOLD"):
    return {
        "zone_id": zone_id,
        "direction": direction,
        "frozen_ts": ts,
        "reaction_event_ts": ts + 10,
        "frozen_low": low,
        "frozen_high": high,
        "a1_reaction_type": reaction,
        "reaction_type": reaction,
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "ACTIVE",
    }


def test_exact_zone_id_match_prioritizes_over_fuzzy():
    rows = ZoneTruthAggregator().aggregate(
        [_pie("pie-exact", zone_id="iz-exact", price=100.0)],
        [
            _reaction("iz-fuzzy", low=99.0, high=101.0, ts=BASE_TS),
            _reaction("iz-exact", low=120.0, high=121.0, ts=BASE_TS + 200),
        ],
    )
    exact = next(row for row in rows if row["zone_id"] == "iz-exact")
    assert exact["zone_match_method"] == "exact"
    assert exact["pie_count"] == 1
    assert next(row for row in rows if row["zone_id"] == "iz-fuzzy")["pie_count"] == 0


def test_fuzzy_match_when_direction_price_and_time_are_close():
    rows = ZoneTruthAggregator().aggregate(
        [_pie("pie-fuzzy", price=100.2, ts=BASE_TS + 30, zone_id="")],
        [_reaction("iz-fuzzy", low=99.0, high=101.0, ts=BASE_TS)],
    )
    row = next(row for row in rows if row["zone_id"] == "iz-fuzzy")
    assert row["zone_match_method"] == "fuzzy"
    assert row["match_score"] >= 50
    assert row["pie_count"] == 1


def test_low_fuzzy_score_creates_synthetic_for_iceberg():
    rows = ZoneTruthAggregator(time_tolerance_sec=300).aggregate(
        [_pie("pie-synthetic", price=150.0, ts=BASE_TS + 5000, zone_id="")],
        [_reaction("iz-far", low=99.0, high=101.0, ts=BASE_TS)],
    )
    synthetic = next(row for row in rows if row["zone_id"] == "synthetic-pie-synthetic")
    assert synthetic["zone_source"] == "synthetic_from_pie"
    assert synthetic["a2_pre_pool_eligible"] is True


def test_zone_aggregates_multiple_pies_and_best_pie():
    rows = ZoneTruthAggregator().aggregate(
        [
            _pie("pie-a", result="ICEBERG", score=60, zone_id="iz-1"),
            _pie("pie-b", result="ICEBERG", score=85, zone_id="iz-1"),
            _pie("pie-c", result="IGNORE", score=40, zone_id="iz-1"),
        ],
        [_reaction("iz-1")],
    )
    row = next(row for row in rows if row["zone_id"] == "iz-1")
    assert row["pie_count"] == 3
    assert row["iceberg_pie_count"] == 2
    assert row["truth_score_max"] == 85
    assert row["truth_score_avg"] == (60 + 85 + 40) / 3
    assert row["truth_ge65_count"] == 1
    assert row["best_pie_event_key"] == "pie-b"


def test_hard_cap_warnings_aggregate():
    rows = ZoneTruthAggregator().aggregate(
        [
            _pie("pie-cap-1", zone_id="iz-1", warnings=["negative_hidden_volume_cap"]),
            _pie("pie-cap-2", zone_id="iz-1", warnings=["spoofing_withdrawal_cap"]),
        ],
        [_reaction("iz-1")],
    )
    row = next(row for row in rows if row["zone_id"] == "iz-1")
    assert row["negative_hidden_cap_count"] == 1
    assert row["spoofing_withdrawal_cap_count"] == 1
    assert row["has_any_hard_cap"] is True


def test_a2_pre_pool_eligible_only_depends_on_iceberg_pie_count():
    high_ignore = _pie("pie-ignore", result="IGNORE", score=95, zone_id="iz-ignore")
    low_iceberg = _pie("pie-ice", result="ICEBERG", score=20, zone_id="iz-ice")
    rows = ZoneTruthAggregator().aggregate(
        [high_ignore, low_iceberg],
        [_reaction("iz-ignore"), _reaction("iz-ice")],
    )
    ignore_row = next(row for row in rows if row["zone_id"] == "iz-ignore")
    ice_row = next(row for row in rows if row["zone_id"] == "iz-ice")
    assert ignore_row["a2_pre_pool_eligible"] is False
    assert ice_row["a2_pre_pool_eligible"] is True


def test_zone_outputs_full_pie_membership():
    rows = ZoneTruthAggregator().aggregate(
        [
            _pie("pie-a", result="ICEBERG", zone_id="iz-1"),
            _pie("pie-b", result="ICEBERG", zone_id="iz-1"),
            _pie("pie-c", result="IGNORE", zone_id="iz-1"),
            _pie("pie-d", result="SPOOFING", zone_id="iz-1"),
            _pie("pie-e", result="CANCEL", zone_id="iz-1"),
        ],
        [_reaction("iz-1")],
    )
    row = next(row for row in rows if row["zone_id"] == "iz-1")
    assert row["pie_event_keys"] == "pie-a|pie-b|pie-c|pie-d|pie-e"
    assert row["iceberg_pie_event_keys"] == "pie-a|pie-b"
    assert row["ignore_pie_event_keys"] == "pie-c"
    assert row["spoofing_pie_event_keys"] == "pie-d"
    assert row["cancel_pie_event_keys"] == "pie-e"


def test_zone_truth_session_tags_match_runtime_profile():
    def ts(hour, minute=0):
        return datetime(2026, 5, 24, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()

    assert local_session(ts(21, 30), "Asia/Shanghai")["session_tag"] == "US_OPEN"
    assert local_session(ts(6), "Asia/Shanghai")["session_tag"] == "ASIA_OFF"
    assert local_session(ts(10), "Asia/Shanghai")["session_tag"] == "ASIA_DAY"
    assert local_session(ts(18), "Asia/Shanghai")["session_tag"] == "EUROPE_PRE_US"
    assert local_session(ts(2), "Asia/Shanghai")["session_tag"] == "US_LATE"


def test_multiple_reactions_preserve_final_and_flags():
    rows = ZoneTruthAggregator().aggregate(
        [_pie("pie-a", zone_id="iz-1")],
        [
            {
                **_reaction("iz-1", reaction="CLEAN_HOLD"),
                "frozen_ts": 90,
                "reaction_event_ts": 100,
            },
            {
                **_reaction("iz-1", reaction="FAILED_RECLAIM"),
                "frozen_ts": 190,
                "reaction_event_ts": 200,
            },
        ],
    )
    row = next(row for row in rows if row["zone_id"] == "iz-1")
    assert row["reaction_count"] == 2
    assert "CLEAN_HOLD" in row["reaction_types"]
    assert "FAILED_RECLAIM" in row["reaction_types"]
    assert row["has_clean_hold"] is True
    assert row["has_failed_reclaim"] is True
    assert row["primary_reaction_type"] == "CLEAN_HOLD"
    assert row["final_reaction_type"] == "FAILED_RECLAIM"
    assert row["final_reaction_ts"] == 200
