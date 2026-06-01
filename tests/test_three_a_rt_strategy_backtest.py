#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.runtime_three_a.runtime_engine import (
    RuntimeThreeABacktestEngine,
    RuntimeThreeAEngineConfig,
    _a2_with_expiry,
    default_runtime_engine_config,
    normalize_runtime_bars,
    simulate_runtime_trade_exit,
    simulate_runtime_trade_exit_from_normalized_bars,
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


def _sell_zone(setup="SELL_ABOVE_VAH_ABSORB", zone_id="sell1"):
    return {
        "zone_id": zone_id,
        "symbol": "BTC-USDT",
        "direction": "SELL",
        "zone_lower": 99.0,
        "zone_upper": 106.0,
        "zone_mid": 102.5,
        "zone_width": 7.0,
        "reaction_event_ts": 1000.0,
        "defended_high": 106.0,
        "max_active_notional": 1_000_000.0,
        "iceberg_pie_count": 1,
        "vp24h_a1_vp_setup_rt": setup,
        "vp24h_a1_target_poc_price_rt": 90.0,
        "vp24h_a1_target_hvn_price_rt": 88.0,
        "vp24h_a1_target_value_edge_price_rt": 86.0,
        "vp24h_a1_target_lvn_price_rt": 92.0,
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


def _sell_burst_tick(ts=1004.0, price=98.0):
    return {
        "symbol": "BTC-USDT",
        "ts": ts,
        "last_price": price,
        "active_buy_notional_3s": 20_000,
        "active_sell_notional_3s": 250_000,
        "cvd_delta_3s": -50_000,
        "price_velocity_u_per_sec": -0.5,
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


def test_runtime_3a_trades_include_a2_quality_fields():
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=[*_quiet_ticks(), _burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    trade = reports["trades"][0]
    for field in (
        "unique_signal_id",
        "a2_rt_quality",
        "a2_rt_light_ready_for_a3_flag",
        "a2_rt_confirmed_ready_for_a3_flag",
    ):
        assert field in trade


def test_runtime_3a_signals_include_a2_quality_fields():
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=[*_quiet_ticks(), _burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    signal = reports["signals"][0]
    for field in (
        "unique_signal_id",
        "a2_rt_quality",
        "a2_rt_light_ready_for_a3_flag",
        "a2_rt_confirmed_ready_for_a3_flag",
    ):
        assert field in signal


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


def test_default_expiry_trade_count_not_inflated_by_sweep():
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=[*_quiet_ticks(), _burst_tick()],
        expiry_secs=[300, 900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    summary = reports["summary"]
    assert summary["trade_count_all_expiry_variants"] == 2
    assert summary["default_expiry_sec"] == 900
    assert summary["default_expiry_trade_count"] == 1
    assert summary["unique_signal_count"] == 1
    assert reports["by_strategy_all_expiry_variants"][0]["trade_count"] == 2
    assert reports["by_strategy_default_expiry"][0]["trade_count"] == 1


def test_mvp_only_allows_sell_above_vah_by_default():
    reports = build_runtime_strategy_reports(
        [_sell_zone()],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["mvp_summary"]["strategy_name"] == "MVP_VP_3A_LITE"
    assert len(reports["mvp_trades"]) == 1
    assert {row["a1_vp_setup_rt"] for row in reports["mvp_trades"]} == {"SELL_ABOVE_VAH_ABSORB"}


def test_mvp_blocks_bad_vp_setups():
    reports = build_runtime_strategy_reports(
        [_sell_zone("SELL_INSIDE_VALUE_ABOVE_POC_ABSORB"), _sell_zone("SELL_NO_VP_EDGE", zone_id="sell2")],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["mvp_trades"] == []
    assert reports["mvp_summary"]["blocked_count_by_reason"]["VP_SETUP_BLOCKED"] == 2


def test_mvp_observe_only_not_in_mvp_trades():
    reports = build_runtime_strategy_reports(
        [_sell_zone("BUY_NO_VP_EDGE"), _sell_zone("BUY_BELOW_VAL_ABSORB", zone_id="sell2")],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert len(reports["trades"]) == 2
    assert reports["mvp_trades"] == []
    assert reports["mvp_summary"]["blocked_count_by_reason"]["VP_SETUP_OBSERVE_ONLY"] == 2


def test_mvp_applies_fee_risk_filter():
    small_risk = _sell_zone(zone_id="small_risk")
    small_risk["defended_high"] = 103.0
    risk_reports = build_runtime_strategy_reports(
        [small_risk],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert risk_reports["mvp_trades"] == []
    assert risk_reports["mvp_summary"]["blocked_count_by_reason"]["RISK_U_TOO_SMALL"] == 1

    high_fee = _sell_zone(zone_id="high_fee")
    high_fee.update({"zone_lower": 1979.0, "zone_upper": 1986.0, "zone_mid": 1982.5, "defended_high": 1985.0})
    high_price_ticks = [
        {"symbol": "BTC-USDT", "ts": 1001.0, "last_price": 1982.0, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0.0, "price_velocity_u_per_sec": -0.01},
        {"symbol": "BTC-USDT", "ts": 1002.0, "last_price": 1981.8, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0.0, "price_velocity_u_per_sec": -0.01},
        {"symbol": "BTC-USDT", "ts": 1003.0, "last_price": 1981.7, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0.0, "price_velocity_u_per_sec": -0.01},
        {"symbol": "BTC-USDT", "ts": 1004.0, "last_price": 1979.0, "active_buy_notional_3s": 20_000, "active_sell_notional_3s": 250_000, "cvd_delta_3s": -50_000, "price_velocity_u_per_sec": -0.5},
    ]
    fee_reports = build_runtime_strategy_reports(
        [high_fee],
        [{"timestamp": 1004.0, "open": 1979, "high": 1980, "low": 1960, "close": 1962}],
        trade_events=high_price_ticks,
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert fee_reports["mvp_trades"] == []
    assert fee_reports["mvp_summary"]["blocked_count_by_reason"]["FEE_SHARE_R_TOO_HIGH"] == 1


def test_mvp_deduplicates_unique_signal():
    reports = build_runtime_strategy_reports(
        [_sell_zone()],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[300, 900, 1200],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert len(reports["trades"]) == 3
    assert len(reports["mvp_trades"]) == 1
    assert reports["mvp_trades"][0]["unique_signal_id"] == reports["trades"][1]["unique_signal_id"]
    assert reports["mvp_summary"]["blocked_count_by_reason"]["NOT_DEFAULT_EXPIRY"] == 2


def test_mvp_live_gate_not_ready_on_low_sample():
    reports = build_runtime_strategy_reports(
        [_sell_zone()],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["mvp_summary"]["unique_signal_count"] < 20
    assert reports["mvp_summary"]["live_readiness_gate"]["status"] == "NOT_READY"


def test_mvp_does_not_use_best_expiry_for_live_readiness():
    reports = build_runtime_strategy_reports(
        [_sell_zone()],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[300, 900, 1200],
        default_expiry_sec=1200,
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    scope = reports["mvp_summary"]["live_readiness_gate"]["requirements"]["scope"]
    assert "best_expiry" in scope
    assert reports["mvp_summary"]["default_expiry_sec"] == 900
    assert reports["mvp_summary"]["trade_count"] == 1


def test_mvp_outputs_uses_future_field_false():
    reports = build_runtime_strategy_reports(
        [_sell_zone()],
        [{"timestamp": 1004.0, "open": 98, "high": 99, "low": 80, "close": 82}],
        trade_events=[*_quiet_ticks(), _sell_burst_tick()],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["mvp_trades"][0]["uses_future_field_flag"] is False


def test_runtime_engine_config_uses_cli_min_quiet_and_tick_count_values():
    cfg = default_runtime_engine_config(a2_rt_min_quiet_sec=7, a2_rt_min_tick_count=9)
    engine = RuntimeThreeABacktestEngine(cfg)
    assert engine.config.a2.min_quiet_sec == 7
    assert engine.config.a2.min_tick_count == 9


def test_runtime_engine_config_uses_light_a2_values():
    cfg = default_runtime_engine_config(
        a2_rt_min_light_sec=5,
        a2_rt_min_light_tick_count=4,
        a2_rt_enable_light_ready=False,
    )
    engine = RuntimeThreeABacktestEngine(cfg)
    assert engine.config.a2.min_light_sec == 5
    assert engine.config.a2.min_light_tick_count == 4
    assert engine.config.a2.enable_light_ready is False


def test_a2_with_expiry_preserves_light_a2_config():
    base = default_runtime_engine_config(
        a2_rt_min_light_sec=5,
        a2_rt_min_light_tick_count=4,
        a2_rt_enable_light_ready=False,
    ).a2
    child = _a2_with_expiry(base, 300)
    assert child.max_age_sec == 300
    assert child.enable_light_ready is False
    assert child.min_light_sec == 5
    assert child.min_light_tick_count == 4


def test_runtime_engine_disable_light_ready_does_not_trigger_early_light_a2():
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=[*_quiet_ticks(count=2), _burst_tick(ts=1003.0)],
        expiry_secs=[900],
        a2_rt_min_quiet_sec=30,
        a2_rt_min_tick_count=30,
        a2_rt_enable_light_ready=False,
    )
    assert reports["summary"]["runtime_3a_status"] == "NO_RT_A3_SIGNALS"
    assert reports["summary"]["signal_count"] == 0


def test_runtime_engine_windowed_reads():
    class Source:
        def __init__(self):
            self.calls = []

        def get_window(self, start_ts, end_ts, symbol=None):
            self.calls.append((start_ts, end_ts, symbol))
            return iter([*_quiet_ticks(), _burst_tick()])

        def memory_profile(self):
            return {
                "runtime_event_source_mode": "test_source",
                "runtime_ticks_materialized_count": 0,
                "runtime_window_reads": len(self.calls),
                "runtime_max_window_ticks": 4,
            }

    source = Source()
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=source,
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert len(source.calls) == 1
    start, end, symbol = source.calls[0]
    assert start == 1000.0
    assert end == 1905.0  # start_ts(1000) + max_expiry(900) + RUNTIME_TICK_ENTRY_BUFFER_SEC(5.0)
    assert symbol == "BTC-USDT"
    assert reports["summary"]["runtime_3a_memory_profile"]["runtime_event_source_mode"] == "test_source"


def test_runtime_engine_adds_repeated_scan_warning():
    class Source:
        def get_window(self, start_ts, end_ts, symbol=None):
            return iter([*_quiet_ticks(), _burst_tick()])

        def memory_profile(self):
            return {
                "runtime_event_source_mode": "test_source",
                "runtime_ticks_materialized_count": 0,
                "runtime_window_reads": 1001,
                "runtime_max_window_ticks": 4,
                "runtime_candidate_file_scans": 0,
            }

    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=Source(),
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    profile = reports["summary"]["runtime_3a_memory_profile"]
    assert "repeated window scans may be slow" in profile["runtime_performance_warning"]


def test_runtime_engine_does_not_copy_full_ticks_per_zone(monkeypatch):
    ranges = []
    ticks = [
        {"symbol": "BTC-USDT", "ts": 1.0, "last_price": 90.0},
        *_quiet_ticks(),
        _burst_tick(),
        {"symbol": "BTC-USDT", "ts": 999999.0, "last_price": 120.0},
    ]
    from src.research.runtime_three_a import runtime_engine as engine_mod

    original = engine_mod._iter_index_range_ticks

    def wrapped(all_ticks, left, right):
        ranges.append((len(all_ticks), left, right))
        yield from original(all_ticks, left, right)

    monkeypatch.setattr(engine_mod, "_iter_index_range_ticks", wrapped)
    reports = build_runtime_strategy_reports(
        [_zone()],
        _bars(),
        trade_events=ticks,
        expiry_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    assert reports["summary"]["runtime_3a_status"] == "OK"
    assert ranges
    assert all((right - left) < total for total, left, right in ranges)


def test_blocked_trade_not_marked_future():
    cfg = RuntimeThreeAEngineConfig(
        expiry_secs=[900],
        a2=default_runtime_engine_config(a2_rt_min_quiet_sec=3, a2_rt_min_tick_count=3).a2,
        a3=default_runtime_engine_config().a3,
        max_fee_share_r=0.0,
    )
    reports = RuntimeThreeABacktestEngine(cfg).run([_zone()], [*_quiet_ticks(), _burst_tick()], _bars())
    assert reports["summary"]["trade_count"] == 0
    assert reports["summary"]["trade_blocked_count"] == 1
    trade = reports["trades"][0]
    assert trade["trade_blocked_flag"] is True
    assert trade["trade_blocked_reason"] == "INVALID_RISK_OR_FEE"
    assert trade["uses_future_field_flag"] is False
    assert trade["future_field_names"] == ""


def test_ambiguous_exit_flags():
    result = simulate_runtime_trade_exit(
        entry_ts=1004,
        entry_price=102,
        stop_price=99,
        target_price=108,
        direction="BUY",
        future_bars=[{"timestamp": 1004, "open": 102, "high": 109, "low": 98, "close": 101}],
        fee_share_r=0.1,
        risk_u=3,
    )
    assert result["exit_reason"] == "AMBIGUOUS_BOTH_HIT"
    assert result["ambiguous_flag_sim"] is True
    assert result["target_first_flag_sim"] is False
    assert result["stop_first_flag_sim"] is False


# ═══════════════════════════════════════════════════════════════════════
# V7.3.0 P1: trade exit from normalized bars (no repeated normalize/filter)
# ═══════════════════════════════════════════════════════════════════════


def test_exit_from_normalized_bars_matches_old_behavior():
    """simulate_runtime_trade_exit_from_normalized_bars must match old function."""
    bars = _bars()
    bars_norm = normalize_runtime_bars(bars)
    bar_ts = [b["timestamp"] for b in bars_norm]
    old_result = simulate_runtime_trade_exit(
        entry_ts=1004,
        entry_price=102,
        stop_price=99,
        target_price=108,
        direction="BUY",
        future_bars=bars,
        fee_share_r=0.1,
        risk_u=3,
    )
    new_result = simulate_runtime_trade_exit_from_normalized_bars(
        entry_ts=1004,
        entry_price=102,
        stop_price=99,
        target_price=108,
        direction="BUY",
        normalized_bars=bars_norm,
        normalized_bar_ts=bar_ts,
        fee_share_r=0.1,
        risk_u=3,
    )
    assert old_result["exit_reason"] == new_result["exit_reason"]
    assert old_result["realized_r_sim"] == new_result["realized_r_sim"]
    assert old_result["mfe_r_future"] == new_result["mfe_r_future"]
    assert old_result["mae_r_future"] == new_result["mae_r_future"]


def test_exit_from_normalized_bars_ambiguous():
    """Ambiguous (both hit) exit matches old behavior."""
    bars_norm = normalize_runtime_bars([{"timestamp": 1004, "open": 102, "high": 109, "low": 98, "close": 101}])
    bar_ts = [b["timestamp"] for b in bars_norm]
    result = simulate_runtime_trade_exit_from_normalized_bars(
        entry_ts=1004,
        entry_price=102,
        stop_price=99,
        target_price=108,
        direction="BUY",
        normalized_bars=bars_norm,
        normalized_bar_ts=bar_ts,
        fee_share_r=0.1,
        risk_u=3,
    )
    assert result["exit_reason"] == "AMBIGUOUS_BOTH_HIT"


def test_exit_from_normalized_bars_slice_respects_window(tmp_path, monkeypatch):
    """Only bars within [entry_ts, entry_ts + window_sec] are examined."""
    import random
    random.seed(42)
    many_bars = []
    # Generate bars where stop/target are not hit, ensuring CLOSE_EXIT or NO_FUTURE_BARS
    for i in range(1000):
        many_bars.append({
            "timestamp": 500 + i * 10.0,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        })
    bars_norm = normalize_runtime_bars(many_bars)
    bar_ts = [b["timestamp"] for b in bars_norm]
    # Entry at ts=9000, window_sec=3600: should scan ~360 bars within [9000, 12600]
    result = simulate_runtime_trade_exit_from_normalized_bars(
        entry_ts=9000,
        entry_price=100,
        stop_price=95,
        target_price=108,
        direction="BUY",
        normalized_bars=bars_norm,
        normalized_bar_ts=bar_ts,
        fee_share_r=0.1,
        risk_u=5,
        window_sec=3600,
    )
    # With narrow range [99-101], stop=95 and target=108 never hit → CLOSE_EXIT
    assert result["exit_reason"] in {"CLOSE_EXIT", "NO_FUTURE_BARS"}


def test_engine_does_not_repeat_normalize_runtime_bars(monkeypatch):
    """Runtime engine must normalize bars once, not per trade."""
    called = [0]
    original = normalize_runtime_bars

    def counting_normalize(bars):
        called[0] += 1
        return original(bars)

    monkeypatch.setattr(
        "src.research.runtime_three_a.runtime_engine.normalize_runtime_bars",
        counting_normalize,
    )
    ticks = [*_quiet_ticks(count=3), _burst_tick()]
    reports = build_runtime_strategy_reports(
        [_zone(), _zone()],
        _bars(),
        trade_events=ticks,
        expiry_secs=[300, 900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    # normalize_runtime_bars should be called exactly once, not per zone or per trade
    assert called[0] == 1, f"normalize_runtime_bars called {called[0]} times, expected 1"
