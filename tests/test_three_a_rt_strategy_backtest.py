#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.runtime_three_a.runtime_engine import (
    RuntimeThreeABacktestEngine,
    default_runtime_engine_config,
    simulate_runtime_trade_exit,
)
from src.research.runtime_three_a.target_models import build_target_candidates
from src.research.runtime_three_a.three_a_strategy_backtest import build_runtime_strategy_reports


def _zone():
    return {
        "zone_id": "z1",
        "symbol": "BTC-USDT",
        "direction": "BUY",
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_mid": 100.0,
        "zone_width": 2.0,
        "reaction_event_ts": 1000.0,
        "defended_low": 99.0,
        "max_active_notional": 1_000_000.0,
        "iceberg_pie_count": 1,
        "vp24h_a1_vp_setup_rt": "BUY_NEAR_VAL_ABSORB",
        "vp24h_a1_target_poc_price_rt": 106.0,
        "vp24h_a1_target_hvn_price_rt": 108.0,
        "vp24h_a1_target_value_edge_price_rt": 110.0,
        "vp24h_a1_target_lvn_price_rt": 107.0,
        "a3_quality_future_type_v2": "STRONG_ORDERFLOW_AGGRESSION",
        "a3_quality_future_score_v2": 0.95,
    }


def _quiet_ticks(start=1000.0, count=3):
    return [
        {
            "symbol": "BTC-USDT",
            "ts": start + idx + 1,
            "last_price": 100.0 + idx * 0.05,
            "active_buy_notional_3s": 10_000,
            "active_sell_notional_3s": 10_000,
            "cvd_delta_3s": 0.0,
            "price_velocity_u_per_sec": 0.01,
        }
        for idx in range(count)
    ]


def _burst_tick(ts=1004.0):
    return {
        "symbol": "BTC-USDT",
        "ts": ts,
        "last_price": 102.0,
        "active_buy_notional_3s": 250_000,
        "active_sell_notional_3s": 20_000,
        "cvd_delta_3s": 50_000,
        "price_velocity_u_per_sec": 0.5,
    }


def _bars():
    return [
        {"timestamp": 1004.0, "open": 102.0, "high": 110.0, "low": 101.8, "close": 109.5},
        {"timestamp": 1064.0, "open": 109.5, "high": 110.0, "low": 108.0, "close": 109.0},
    ]


def test_target_candidates_are_directional():
    buy = build_target_candidates(_zone(), 102, "BUY", 2)
    assert buy["target_poc_price_rt"] == 106
    assert buy["target_hybrid_min_2r_available_rt"] is True
    sell = build_target_candidates({**_zone(), "vp24h_a1_target_poc_price_rt": 98}, 100, "SELL", 1)
    assert sell["target_poc_price_rt"] == 98


def test_runtime_engine_generates_a2_a3_from_ticks():
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=[*_quiet_ticks(), _burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["summary"]["runtime_3a_status"] == "OK"
    assert len(reports["signals"]) == 1
    assert len(reports["trades"]) == 1
    trade = reports["trades"][0]
    assert trade["entry_ts"] == 1004.0
    assert trade["exit_reason"] == "TARGET_FIRST"
    assert trade["realized_r_sim"] > 1.9
    assert "a3_future_realized_r_proxy_1h" not in trade["entry_condition_fields"]


def test_runtime_engine_without_trades_is_skipped_not_fake_empty():
    reports = build_runtime_strategy_reports([_zone()], _bars(), trade_events=[], expiry_secs=[900])
    assert reports["summary"]["runtime_3a_status"] == "SKIPPED_NO_TRADE_EVENTS"
    assert reports["summary"]["trade_count"] == 0


def test_runtime_trade_exit_uses_entry_stop_target_not_future_proxy():
    result = simulate_runtime_trade_exit(
        entry_ts=1004,
        entry_price=102,
        stop_price=99,
        target_price=108,
        direction="BUY",
        future_bars=_bars(),
        fee_share_r=0.1,
        risk_u=3,
    )
    assert result["exit_reason"] == "TARGET_FIRST"
    assert result["realized_r_sim"] == 1.9


def test_expiry_sweep_reexecutes_state_machine():
    ticks = [*_quiet_ticks(count=3), _burst_tick(ts=1185.0)]
    reports = build_runtime_strategy_reports(
        [_zone()],
        [{"timestamp": 1185.0, "open": 102, "high": 110, "low": 101, "close": 109}],
        trade_events=ticks,
        expiry_secs=[180, 900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    by_expiry = {int(row["expiry_sec"]): row for row in reports["by_expiry"]}
    assert by_expiry[180]["expired_count"] == 1
    assert by_expiry[180]["a3_triggered_count"] == 0
    assert by_expiry[900]["a3_triggered_count"] == 1
    assert by_expiry[900]["trade_count"] == 1


def test_runtime_engine_config_uses_cli_min_quiet_and_tick_count_values():
    cfg = default_runtime_engine_config(a2_rt_min_quiet_sec=7, a2_rt_min_tick_count=9)
    engine = RuntimeThreeABacktestEngine(cfg)
    assert engine.config.a2.min_quiet_sec == 7
    assert engine.config.a2.min_tick_count == 9
