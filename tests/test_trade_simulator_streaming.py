#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.trade_simulator import SimulatorStats, iter_3a_proxy_trades, simulate_3a_proxy_trades


def _row(**overrides):
    row = {
        "zone_id": "z1",
        "direction": "BUY",
        "result": "ICEBERG",
        "a3_preview_breakout_raw_flag": True,
        "a3_preview_entry_ts": 1000,
        "a3_preview_entry_price": 100,
        "zone_lower": 99,
        "zone_upper": 100,
    }
    row.update(overrides)
    return row


def test_iter_3a_proxy_trades_defaults_skip_unavailable_rows():
    rows = [_row(a3_preview_breakout_raw_flag=False)]
    stats = SimulatorStats()
    trades = list(
        iter_3a_proxy_trades(
            rows,
            [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
            entry_models=["BREAKOUT"],
            stop_models=["V1_ZONE_WIDTH"],
            target_r_list=[1.0],
            stats=stats,
        )
    )
    assert trades == []
    assert stats.unavailable_entry_count == 1
    assert stats.written_trade_count == 0


def test_iter_3a_proxy_trades_can_include_unavailable_rows():
    stats = SimulatorStats()
    trades = list(
        iter_3a_proxy_trades(
            [_row(a3_preview_breakout_raw_flag=False)],
            [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
            entry_models=["BREAKOUT"],
            stop_models=["V1_ZONE_WIDTH"],
            target_r_list=[1.0],
            include_unavailable=True,
            stats=stats,
        )
    )
    assert len(trades) == 1
    assert trades[0]["realized_outcome_1h"] == "NO_BREAKOUT_ENTRY"
    assert stats.written_trade_count == 1


def test_iter_3a_proxy_trades_max_trades_caps_written_rows():
    stats = SimulatorStats()
    trades = list(
        iter_3a_proxy_trades(
            [_row()],
            [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
            entry_models=["BREAKOUT"],
            stop_models=["V1_ZONE_WIDTH"],
            target_r_list=[1.0, 1.5],
            max_trades=1,
            stats=stats,
        )
    )
    assert len(trades) == 1
    assert stats.written_trade_count == 1
    assert stats.capped is True


def test_simulate_3a_proxy_trades_keeps_legacy_unavailable_behavior():
    trades = simulate_3a_proxy_trades(
        [_row(a3_preview_breakout_raw_flag=False)],
        [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
        entry_models=["BREAKOUT"],
        stop_models=["V1_ZONE_WIDTH"],
        target_r_list=[1.0],
    )
    assert len(trades) == 1
    assert trades[0]["target_r"] == 0.0
