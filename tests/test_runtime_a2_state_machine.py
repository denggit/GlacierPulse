#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.runtime_three_a.a2_runtime_state import A2RuntimeConfig, A2RuntimeStateMachine


def _zone(direction="BUY"):
    return {
        "zone_id": "z1",
        "direction": direction,
        "reaction_event_ts": 1000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
        "defended_low": 99.0,
        "defended_high": 101.0,
        "max_active_notional": 1_000_000.0,
    }


def test_runtime_a2_can_light_ready_before_confirmed_quiet_period():
    machine = A2RuntimeStateMachine(_zone(), config=A2RuntimeConfig(min_quiet_sec=30, min_tick_count=30, min_light_sec=2, min_light_tick_count=2))
    out = {}
    for i, price in enumerate([99.8, 100.0], start=1):
        out = machine.update({"ts": 1000 + i * 2, "last_price": price, "active_sell_notional_3s": 500_000, "cvd_delta_3s": -10_000})
    assert out["a2_rt_state"] == "A2_LIGHT_READY"
    assert out["a2_rt_light_ready_for_a3_flag"] is True
    assert out["a2_rt_ready_for_a3_flag"] is True
    assert out["a2_rt_confirmed_ready_for_a3_flag"] is False
    assert out["a2_rt_quality"] == "LIGHT"


def test_runtime_a2_can_ready_after_short_quiet_period():
    machine = A2RuntimeStateMachine(_zone(), config=A2RuntimeConfig(min_quiet_sec=3, min_tick_count=3))
    out = {}
    for i, price in enumerate([99.8, 100.0, 100.1], start=1):
        out = machine.update({"ts": 1000 + i * 2, "last_price": price, "active_sell_notional_3s": 100_000, "cvd_delta_3s": -1})
    assert out["a2_rt_state"] == "A2_READY_FOR_A3"
    assert out["a2_rt_ready_for_a3_flag"] is True
    assert out["a2_rt_confirmed_ready_for_a3_flag"] is True
    assert out["a2_rt_quality"] == "CONFIRMED"


def test_runtime_a2_expires_by_configured_age():
    machine = A2RuntimeStateMachine(_zone(), expiry_sec=3, config=A2RuntimeConfig(min_tick_count=1, max_age_sec=3))
    out = machine.update({"ts": 1005, "last_price": 100, "active_sell_notional_3s": 1, "cvd_delta_3s": 0})
    assert out["a2_rt_state"] == "A2_EXPIRED"
    assert out["a2_rt_expired_flag"] is True


def test_runtime_a2_expiry_sweep_values_are_supported():
    values = [180, 300, 600, 900, 1200, 1800]
    assert [A2RuntimeStateMachine(_zone(), expiry_sec=v).snapshot()["a2_rt_expiry_sec"] for v in values] == values


def test_runtime_a2_buy_invalidates_on_defended_low_break():
    machine = A2RuntimeStateMachine(_zone(), config=A2RuntimeConfig(invalidation_buffer_u=0.5))
    out = machine.update({"ts": 1001, "last_price": 98.4})
    assert out["a2_rt_state"] == "A2_INVALIDATED"
    assert out["a2_rt_invalidated_flag"] is True


def test_runtime_a2_sell_invalidates_on_defended_high_break():
    machine = A2RuntimeStateMachine(_zone("SELL"), config=A2RuntimeConfig(invalidation_buffer_u=0.5))
    out = machine.update({"ts": 1001, "last_price": 101.6})
    assert out["a2_rt_state"] == "A2_INVALIDATED"


def test_runtime_a2_invalidated_does_not_block_new_a1_machine():
    old = A2RuntimeStateMachine(_zone(), config=A2RuntimeConfig(invalidation_buffer_u=0.5))
    assert old.update({"ts": 1001, "last_price": 98.4})["a2_rt_invalidated_flag"] is True
    new = A2RuntimeStateMachine({**_zone(), "zone_id": "z2", "reaction_event_ts": 1010, "defended_low": 98})
    assert new.snapshot()["a2_rt_state"] == "A1_DETECTED"
