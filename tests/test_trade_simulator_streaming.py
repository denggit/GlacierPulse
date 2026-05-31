#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.trade_simulator import SimulatorStats, iter_3a_proxy_trades, simulate_3a_proxy_trades


def _row(**overrides):
    row = {
        "zone_id": "z1",
        "direction": "BUY",
        "result": "ICEBERG",
        "a3_future_breakout_seen_flag": True,
        "a3_future_breakout_entry_ts": 1000,
        "a3_future_breakout_entry_price": 100,
        "zone_lower": 99,
        "zone_upper": 100,
    }
    row.update(overrides)
    return row


def test_iter_3a_proxy_trades_defaults_skip_unavailable_rows():
    rows = [_row(a3_future_breakout_seen_flag=False)]
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
            [_row(a3_future_breakout_seen_flag=False)],
            [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
            entry_models=["BREAKOUT"],
            stop_models=["V1_ZONE_WIDTH"],
            target_r_list=[1.0],
            include_unavailable=True,
            stats=stats,
        )
    )
    assert len(trades) == 1
    assert trades[0]["realized_outcome_1h_sim"] == "NO_BREAKOUT_ENTRY"
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


def test_iter_3a_proxy_trades_max_trades_cap_csv_valid_trade_count_matches_written():
    """When max_trades=1 caps output, v7_3a_simulated_trade_count must reflect actual CSV rows, not the theoretical combo count."""
    rows = [_row(a3_quality_future_type_v2="STRONG_ORDERFLOW_AGGRESSION")]
    bars = [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}]
    stats = SimulatorStats()
    trades = list(
        iter_3a_proxy_trades(
            rows,
            bars,
            entry_models=["BREAKOUT"],
            stop_models=["V1_ZONE_WIDTH"],
            target_r_list=[1.0, 1.5, 2.0],
            max_trades=1,
            stats=stats,
        )
    )
    # stats.valid_trade_count counts every (entry, stop, target_r) combo attempted before cap — 2 here
    # (target_r=1.0 yielded; target_r=1.5 incremented then capped; target_r=2.0 never reached)
    assert stats.valid_trade_count == 2
    # But max_trades=1 caps written rows
    assert stats.written_trade_count == 1
    assert stats.capped is True
    assert len(trades) == 1
    from src.research.zone_truth.combo_matrix import is_valid_simulated_trade

    valid = [t for t in trades if is_valid_simulated_trade(t)]
    assert len(valid) == 1


def test_row_identity_preserves_original_a1_evidence_types():
    """a1_primary_evidence_type is mainline ICEBERG, but a1_evidence_types preserves original shadow evidence."""
    from src.research.zone_truth.trade_simulator import _row_identity

    row = {
        "a1_primary_evidence_type": "LADDER_ABSORPTION",
        "a1_evidence_types": "HIDDEN_RELOAD_ICEBERG|LADDER_ABSORPTION",
        "iceberg_pie_count": 1,
    }
    identity = _row_identity(row)
    assert identity["a1_primary_evidence_type"] == "ICEBERG"
    assert identity["a1_evidence_types"] == "HIDDEN_RELOAD_ICEBERG|LADDER_ABSORPTION"


def test_simulate_3a_proxy_trades_keeps_legacy_unavailable_behavior():
    trades = simulate_3a_proxy_trades(
        [_row(a3_future_breakout_seen_flag=False)],
        [{"timestamp": 1000, "high": 102, "low": 99.5, "close": 101, "open": 100}],
        entry_models=["BREAKOUT"],
        stop_models=["V1_ZONE_WIDTH"],
        target_r_list=[1.0],
    )
    assert len(trades) == 1
    assert trades[0]["target_r"] == 0.0
