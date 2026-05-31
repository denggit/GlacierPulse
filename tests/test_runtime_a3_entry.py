#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.runtime_three_a.a3_runtime_entry import A3RuntimeConfig, evaluate_a3_runtime_entry


def _a2():
    return {
        "a2_rt_state": "A2_READY_FOR_A3",
        "a2_rt_ready_for_a3_flag": True,
        "a2_rt_box_low": 99.0,
        "a2_rt_box_high": 101.0,
        "a2_rt_quiet_buy_avg": 50_000.0,
        "a2_rt_quiet_sell_avg": 50_000.0,
    }


def test_runtime_a3_triggers_immediately_without_persistence():
    out = evaluate_a3_runtime_entry(
        _a2(),
        {
            "ts": 1005,
            "last_price": 101.6,
            "direction": "BUY",
            "active_buy_notional_3s": 250_000,
            "active_sell_notional_3s": 50_000,
            "cvd_delta_3s": 10,
            "price_velocity_u_per_sec": 0.3,
        },
    )
    assert out["a3_entry_rt_flag"] is True
    assert out["a3_entry_rt_ts"] == 1005


def test_runtime_a3_requires_orderflow_burst_cvd_and_velocity():
    base = {
        "ts": 1005,
        "last_price": 101.6,
        "direction": "BUY",
        "active_buy_notional_3s": 250_000,
        "active_sell_notional_3s": 50_000,
        "cvd_delta_3s": 10,
        "price_velocity_u_per_sec": 0.3,
    }
    assert not evaluate_a3_runtime_entry(_a2(), {**base, "active_buy_notional_3s": 60_000})["a3_entry_rt_flag"]
    assert not evaluate_a3_runtime_entry(_a2(), {**base, "cvd_delta_3s": -1})["a3_entry_rt_flag"]
    assert not evaluate_a3_runtime_entry(_a2(), {**base, "price_velocity_u_per_sec": 0.01})["a3_entry_rt_flag"]


def test_runtime_a3_sell_mirrors_buy():
    out = evaluate_a3_runtime_entry(
        _a2(),
        {
            "ts": 1005,
            "last_price": 98.4,
            "direction": "SELL",
            "active_buy_notional_3s": 50_000,
            "active_sell_notional_3s": 250_000,
            "cvd_delta_3s": -10,
            "price_velocity_u_per_sec": -0.3,
        },
        config=A3RuntimeConfig(breakout_buffer_u=0.5),
    )
    assert out["a3_entry_rt_flag"] is True
    assert out["a3_entry_rt_breakout_box_side"] == "BOX_LOW"


def test_runtime_a3_inherits_vp_setup_but_does_not_filter_current_price_edge():
    out = evaluate_a3_runtime_entry(
        _a2(),
        {
            "ts": 1005,
            "last_price": 101.6,
            "direction": "BUY",
            "active_buy_notional_3s": 250_000,
            "active_sell_notional_3s": 50_000,
            "cvd_delta_3s": 10,
            "price_velocity_u_per_sec": 0.3,
        },
        inherited_a1_vp_setup="BUY_NEAR_VAL_ABSORB",
    )
    assert out["a3_entry_rt_flag"] is True
    assert out["a3_entry_rt_inherited_a1_vp_setup"] == "BUY_NEAR_VAL_ABSORB"

