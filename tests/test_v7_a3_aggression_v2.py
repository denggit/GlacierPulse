#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.a3_aggression_v2 import classify_a3_aggression_v2


def _typ(row):
    return classify_a3_aggression_v2(row)["a3_quality_future_type_v2"]


def test_price_breakout_persistent():
    assert _typ({"a3_preview_breakout_raw_flag": True, "a3_preview_persistence_3m_flag": True, "a3_preview_no_quick_return_3m_flag": True}) == "PRICE_BREAKOUT_PERSISTENT"


def test_price_breakout_weak():
    assert _typ({"a3_preview_breakout_raw_flag": True}) == "PRICE_BREAKOUT_WEAK"


def test_orderflow_aggression():
    assert _typ({"direction": "BUY", "active_buy_notional_3s": 500_000, "active_sell_notional_3s": 100_000, "a3_preview_volume_boost": 2.5}) == "STRONG_ORDERFLOW_AGGRESSION"


def test_reclaim_aggression():
    assert _typ({"direction": "SELL", "active_sell_notional_3s": 500_000, "active_buy_notional_3s": 100_000, "has_reclaimed_boundary": True}) == "RECLAIM_AGGRESSION"


def test_no_aggression():
    assert _typ({}) == "NO_AGGRESSION"
