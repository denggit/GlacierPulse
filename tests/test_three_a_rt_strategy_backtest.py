#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.runtime_three_a.target_models import build_target_candidates
from src.research.runtime_three_a.three_a_strategy_backtest import build_runtime_strategy_reports


def _row():
    return {
        "zone_id": "z1",
        "direction": "BUY",
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "defended_low": 99.0,
        "a2_rt_state": "A2_READY_FOR_A3",
        "a2_rt_ready_for_a3_flag": True,
        "a2_rt_box_low": 100.0,
        "a2_rt_box_high": 101.0,
        "a2_rt_start_ts": 1000.0,
        "a2_rt_last_update_ts": 1005.0,
        "a2_rt_expiry_sec": 900,
        "a3_entry_rt_flag": True,
        "a3_entry_rt_ts": 1006.0,
        "a3_entry_rt_price": 102.0,
        "a3_entry_rt_direction": "BUY",
        "a3_entry_rt_reason": "a2_ready|box_breakout|orderflow_burst|cvd_aligned|price_velocity",
        "vp24h_a1_vp_setup_rt": "BUY_NEAR_VAL_ABSORB",
        "vp24h_a1_target_poc_price_rt": 106.0,
        "vp24h_a1_target_hvn_price_rt": 108.0,
        "vp24h_a1_target_value_edge_price_rt": 110.0,
        "vp24h_a1_target_lvn_price_rt": 107.0,
        "a3_quality_future_type_v2": "STRONG_ORDERFLOW_AGGRESSION",
        "a3_quality_future_score_v2": 0.95,
        "a3_future_realized_r_proxy_1h": 1.2,
        "a3_future_net_mfe_1h_r": 2.1,
        "a3_future_net_mae_1h_r": -0.4,
    }


def test_target_candidates_are_directional():
    buy = build_target_candidates(_row(), 102, "BUY", 2)
    assert buy["target_poc_price_rt"] == 106
    assert buy["target_hybrid_min_2r_available_rt"] is True
    sell = build_target_candidates({**_row(), "vp24h_a1_target_poc_price_rt": 98}, 100, "SELL", 1)
    assert sell["target_poc_price_rt"] == 98


def test_runtime_strategy_reports_emit_zone_truth_family_rows():
    reports = build_runtime_strategy_reports([_row()], expiry_secs=[180, 300, 600, 900, 1200, 1800])
    assert len(reports["signals"]) == 1
    assert len(reports["trades"]) == 1
    assert {row["expiry_sec"] for row in reports["by_expiry"]} == {"180", "300", "600", "900", "1200", "1800"}
    trade = reports["trades"][0]
    assert trade["uses_future_field_flag"] is False
    assert trade["realized_r_sim"] == 1.2

