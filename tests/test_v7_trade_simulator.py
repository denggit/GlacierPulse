#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.trade_simulator import resolve_stop, simulate_3a_proxy_trades, simulate_single_trade


def test_buy_1r_target_first_fee_deducted():
    out = simulate_single_trade({"zone_id": "z1", "direction": "BUY"}, [{"timestamp": 1000, "high": 101.2, "low": 99.5, "close": 101, "open": 100}], entry_model="BREAKOUT", stop_model="V1_ZONE_WIDTH", target_r=1.0, entry_ts=1000, entry_price=100, stop_price=99, risk_u=1)
    assert out["target_first_flag"] is True
    assert out["realized_r_1h"] == 0.9


def test_buy_stop_first_fee_deducted():
    out = simulate_single_trade({"zone_id": "z1", "direction": "BUY"}, [{"timestamp": 1000, "high": 100.2, "low": 98.9, "close": 99, "open": 100}], entry_model="BREAKOUT", stop_model="V1_ZONE_WIDTH", target_r=1.0, entry_ts=1000, entry_price=100, stop_price=99, risk_u=1)
    assert out["stop_first_flag"] is True
    assert out["realized_r_1h"] == -1.1


def test_sell_15r_target_first_fee_deducted():
    out = simulate_single_trade({"zone_id": "z1", "direction": "SELL"}, [{"timestamp": 1000, "high": 100.5, "low": 98.4, "close": 99, "open": 100}], entry_model="BREAKOUT", stop_model="V1_ZONE_WIDTH", target_r=1.5, entry_ts=1000, entry_price=100, stop_price=101, risk_u=1)
    assert out["target_first_flag"] is True
    assert out["realized_r_1h"] == 1.4


def test_ambiguous_both_hit_conservative_stop():
    out = simulate_single_trade({"zone_id": "z1", "direction": "BUY"}, [{"timestamp": 1000, "high": 101.2, "low": 98.9, "close": 100, "open": 100}], entry_model="BREAKOUT", stop_model="V1_ZONE_WIDTH", target_r=1.0, entry_ts=1000, entry_price=100, stop_price=99, risk_u=1)
    assert out["ambiguous_flag"] is True
    assert out["realized_outcome_1h"] == "AMBIGUOUS_BOTH_HIT"
    assert out["realized_r_1h"] == -1.1


def test_close_exit():
    out = simulate_single_trade({"zone_id": "z1", "direction": "BUY"}, [{"timestamp": 1000, "high": 100.5, "low": 99.5, "close": 100.25, "open": 100}], entry_model="BREAKOUT", stop_model="V1_ZONE_WIDTH", target_r=1.0, entry_ts=1000, entry_price=100, stop_price=99, risk_u=1)
    assert out["realized_outcome_1h"] == "CLOSE_EXIT"
    assert out["realized_r_1h"] == 0.15


def test_invalid_stop():
    stop = resolve_stop({"direction": "BUY"}, {"entry_price": 100}, "STRUCTURAL_PROXY")
    assert stop["available"] is False


def test_structural_proxy_uses_first_iceberg_not_later_sweep():
    stop = resolve_stop({"direction": "BUY", "first_iceberg_pie_min_trade_price": 99, "trade_sweep_low": 95}, {"entry_price": 100}, "STRUCTURAL_PROXY")
    assert stop["stop_price"] == 98.5


def test_target_r_minimum_excludes_half_r():
    rows = [{"zone_id": "z1", "direction": "BUY", "a3_preview_breakout_raw_flag": True, "a3_preview_entry_ts": 1000, "a3_preview_entry_price": 100, "zone_lower": 99, "zone_upper": 100}]
    trades = simulate_3a_proxy_trades(rows, [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}], entry_models=["BREAKOUT"], stop_models=["V1_ZONE_WIDTH"], target_r_list=[0.5, 0.75, 1.0])
    assert {t["target_r"] for t in trades if t["target_r"]} == {1.0}
