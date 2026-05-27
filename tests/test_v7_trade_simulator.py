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


def test_bar_close_entry_skips_entry_bar_first_hit():
    bars = [
        {"timestamp": 1000, "high": 110, "low": 90, "close": 100, "open": 100},
        {"timestamp": 1060, "high": 100.8, "low": 99.2, "close": 100.5, "open": 100},
    ]
    out = simulate_single_trade(
        {"zone_id": "z1", "direction": "BUY"},
        bars,
        entry_model="RECLAIM_CLOSE",
        stop_model="V1_ZONE_WIDTH",
        target_r=1.0,
        entry_ts=1000,
        entry_bar_ts=1000,
        entry_price_source="BAR_CLOSE",
        entry_price=100,
        stop_price=99,
        risk_u=1,
    )
    assert out["entry_bar_ts"] == 1000
    assert out["entry_price_source"] == "BAR_CLOSE"
    assert out["target_first_flag"] is False
    assert out["stop_first_flag"] is False
    assert out["ambiguous_flag"] is False
    assert out["realized_outcome_1h"] == "CLOSE_EXIT"
    assert out["realized_r_1h"] == 0.4


def test_invalid_stop():
    stop = resolve_stop({"direction": "BUY"}, {"entry_price": 100}, "STRUCTURAL_PROXY")
    assert stop["available"] is False


def test_structural_proxy_uses_first_iceberg_not_later_sweep():
    stop = resolve_stop({"direction": "BUY", "first_iceberg_pie_min_trade_price": 99, "trade_sweep_low": 95}, {"entry_price": 100}, "STRUCTURAL_PROXY")
    assert stop["stop_price"] == 98.5


def test_zone_boundary_v2_does_not_use_aggregate_zone_stop_without_event_basis():
    row = {
        "direction": "BUY",
        "zone_v2_structural_stop_price": 90,
        "first_iceberg_pie_min_trade_price": 99,
    }
    stop = resolve_stop(row, {"entry_price": 100}, "ZONE_BOUNDARY_V2")
    assert stop["stop_price"] == 98.5
    assert stop["stop_basis_reason"] == "ZONE_BOUNDARY_V2_FALLBACK_STRUCTURAL_PROXY_NO_FUTURE_BASIS"


def test_zone_boundary_v2_uses_event_level_no_future_stop_when_available():
    row = {
        "direction": "BUY",
        "zone_v2_structural_stop_price": 90,
        "first_event_zone_v2_structural_stop_price": 97,
        "first_iceberg_pie_min_trade_price": 99,
    }
    stop = resolve_stop(row, {"entry_price": 100}, "ZONE_BOUNDARY_V2")
    assert stop["stop_price"] == 97
    assert stop["stop_basis_reason"] == "ZONE_BOUNDARY_V2_EVENT_LEVEL"


def test_simulated_trade_outputs_stop_basis_reason():
    rows = [
        {
            "zone_id": "z1",
            "direction": "BUY",
            "a3_preview_breakout_raw_flag": True,
            "a3_preview_entry_ts": 1000,
            "a3_preview_entry_price": 100,
            "zone_v2_structural_stop_price": 90,
            "first_iceberg_pie_min_trade_price": 99,
        }
    ]
    trades = simulate_3a_proxy_trades(rows, [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}], entry_models=["BREAKOUT"], stop_models=["ZONE_BOUNDARY_V2"], target_r_list=[1.0])
    assert trades[0]["stop_basis_reason"] == "ZONE_BOUNDARY_V2_FALLBACK_STRUCTURAL_PROXY_NO_FUTURE_BASIS"


def test_target_r_minimum_excludes_half_r():
    rows = [{"zone_id": "z1", "direction": "BUY", "a3_preview_breakout_raw_flag": True, "a3_preview_entry_ts": 1000, "a3_preview_entry_price": 100, "zone_lower": 99, "zone_upper": 100}]
    trades = simulate_3a_proxy_trades(rows, [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}], entry_models=["BREAKOUT"], stop_models=["V1_ZONE_WIDTH"], target_r_list=[0.5, 0.75, 1.0])
    assert {t["target_r"] for t in trades if t["target_r"]} == {1.0}
