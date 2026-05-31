#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.context.iceberg_context_labels import _directional_vp_rt_labels


def _cache():
    return {
        "poc": 100.0,
        "val": 95.0,
        "vah": 105.0,
        "total_volume": 1000.0,
        "_hvn_bins": [94.0, 100.0, 106.0],
        "_lvn_bins": [93.0, 97.0, 103.0, 107.0],
    }


def test_vp_above_below_hvn_lvn_are_directional():
    out = _directional_vp_rt_labels(_cache(), 96.0, 1.0, "vp24h", "BUY")
    assert out["vp24h_hvn_above_rt"] == 100.0
    assert out["vp24h_hvn_below_rt"] == 94.0
    assert out["vp24h_lvn_above_rt"] == 97.0
    assert out["vp24h_lvn_below_rt"] == 93.0


def test_buy_vp_setups():
    assert _directional_vp_rt_labels(_cache(), 93.0, 1.0, "vp24h", "BUY")["vp24h_a1_vp_setup_rt"] == "BUY_BELOW_VAL_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 95.5, 1.0, "vp24h", "BUY")["vp24h_a1_vp_setup_rt"] == "BUY_NEAR_VAL_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 97.0, 0.5, "vp24h", "BUY")["vp24h_a1_vp_setup_rt"] == "BUY_LVN_BELOW_HVN_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 98.0, 0.5, "vp24h", "BUY")["vp24h_a1_vp_setup_rt"] == "BUY_INSIDE_VALUE_BELOW_POC_ABSORB"


def test_sell_vp_setups():
    assert _directional_vp_rt_labels(_cache(), 107.0, 1.0, "vp24h", "SELL")["vp24h_a1_vp_setup_rt"] == "SELL_ABOVE_VAH_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 104.5, 1.0, "vp24h", "SELL")["vp24h_a1_vp_setup_rt"] == "SELL_NEAR_VAH_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 103.0, 0.5, "vp24h", "SELL")["vp24h_a1_vp_setup_rt"] == "SELL_LVN_ABOVE_HVN_ABSORB"
    assert _directional_vp_rt_labels(_cache(), 102.0, 0.5, "vp24h", "SELL")["vp24h_a1_vp_setup_rt"] == "SELL_INSIDE_VALUE_ABOVE_POC_ABSORB"

