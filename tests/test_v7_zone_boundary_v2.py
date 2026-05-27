#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.zone_boundary_v2 import compute_zone_boundary_v2


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
