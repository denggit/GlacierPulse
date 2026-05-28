#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy

from src.research.zone_truth.zone_boundary_v2 import (
    build_book_bucket_profile,
    compute_zone_boundary_v2,
    update_pending_event_profile,
    update_pending_event_profile_from_bucket_profile,
)


def test_build_book_bucket_profile_aggregates_notional_by_bucket():
    profile = build_book_bucket_profile({100.0: 2.0, 100.2: 3.0, 101.0: 1.0}, bucket_size=0.5)
    assert profile[100.0] == 500.6
    assert profile[101.0] == 101.0


def test_cached_update_matches_full_update():
    event = {
        "book_profile_start": {100.0: 200.0, 100.5: 100.0},
        "book_profile_min": {100.0: 200.0, 100.5: 100.0},
        "book_profile_end": {100.0: 200.0, 100.5: 100.0},
        "_zone_v2_profile_keys": [100.0, 100.5],
    }
    full_event = deepcopy(event)
    cached_event = deepcopy(event)
    updated_book = {100.0: 1.0, 100.5: 0.5}
    update_pending_event_profile(full_event, updated_book)
    update_pending_event_profile_from_bucket_profile(cached_event, build_book_bucket_profile(updated_book), changed_buckets=None)
    assert cached_event["book_profile_min"] == full_event["book_profile_min"]
    assert cached_event["book_profile_end"] == full_event["book_profile_end"]


def test_cached_update_skips_when_changed_buckets_do_not_overlap():
    event = {
        "book_profile_start": {100.0: 200.0},
        "book_profile_min": {100.0: 200.0},
        "book_profile_end": {100.0: 200.0},
        "_zone_v2_profile_keys": [100.0],
    }
    before = deepcopy(event)
    update_pending_event_profile_from_bucket_profile(event, {100.0: 50.0}, changed_buckets={105.0})
    assert event == before


def test_cached_update_only_changes_intersecting_keys_and_matches_full_update():
    event = {
        "book_profile_start": {100.0: 200.0, 100.5: 100.0},
        "book_profile_min": {100.0: 200.0, 100.5: 100.0},
        "book_profile_end": {100.0: 200.0, 100.5: 100.0},
        "_zone_v2_profile_keys": [100.0, 100.5],
    }
    full_event = deepcopy(event)
    cached_event = deepcopy(event)
    profile = {100.0: 50.0, 100.5: 100.0}
    update_pending_event_profile_from_bucket_profile(full_event, profile, changed_buckets=None)
    update_pending_event_profile_from_bucket_profile(cached_event, profile, changed_buckets={100.0})
    assert cached_event["book_profile_min"] == full_event["book_profile_min"]
    assert cached_event["book_profile_end"] == full_event["book_profile_end"]


def test_buy_boundary_core_and_stop():
    out = compute_zone_boundary_v2(
        {
            "direction": "BUY",
            "zone_lower": 99,
            "zone_upper": 101,
            "trade_sweep_low": 98.5,
            "book_profile_start": {99.0: 1_000_000},
            "book_profile_min": {99.0: 100_000},
            "book_profile_end": {99.0: 900_000},
            "trade_notional_by_bucket": {99.0: 50_000},
        },
        "BUY",
        current_price=101,
    )
    assert out["absorption_core_lower"] == 99.0
    assert out["absorption_core_upper"] == 99.5
    assert out["zone_v2_structural_stop_price"] == 98.0


def test_sell_boundary_core_and_stop():
    out = compute_zone_boundary_v2(
        {
            "direction": "SELL",
            "zone_lower": 99,
            "zone_upper": 101,
            "trade_sweep_high": 101.5,
            "book_profile_start": {101.0: 1_000_000},
            "book_profile_min": {101.0: 100_000},
            "book_profile_end": {101.0: 900_000},
            "trade_notional_by_bucket": {101.0: 50_000},
        },
        "SELL",
        current_price=99,
    )
    assert out["absorption_core_lower"] == 101.0
    assert out["zone_v2_structural_stop_price"] == 102.0


def test_fallback_and_coverage_insufficient():
    out = compute_zone_boundary_v2({"direction": "BUY", "zone_lower": 99, "zone_upper": 101}, "BUY")
    assert out["zone_v2_boundary_reason"] == "FALLBACK_BOOK_COVERAGE_INSUFFICIENT"
    assert out["zone_v2_book_coverage_sufficient_flag"] is False


def test_profile_maps_not_written_by_default():
    out = compute_zone_boundary_v2({"direction": "BUY", "zone_lower": 99, "zone_upper": 101}, "BUY")
    assert "book_profile_start" not in out
