from src.research.zone_truth.forward import (
    ROUNDTRIP_FEE_PCT,
    _latency_bucket,
    _first_hit,
    compute_a2_fee_metrics,
    compute_a2_pre_ignition_metrics,
    compute_a3_preview_breakout,
)


def test_fee_calculation():
    row = compute_a2_fee_metrics({"zone_lower": 2995, "zone_upper": 3005, "zone_width": 10, "trigger_price": 3000, "mfe_15m_u_future": 0, "mae_15m_u_future": 0, "mfe_1h_u_future": 0, "mae_1h_u_future": 0, "mfe_4h_u_future": 0, "mae_4h_u_future": 0})
    assert ROUNDTRIP_FEE_PCT == 0.001
    assert row["a2_fee_u"] == 3
    assert row["a2_fee_share_r"] == 0.3


def test_buy_sell_net_and_first_hit_and_latency():
    zone = {"direction": "BUY", "reaction_event_ts": 1000.0, "zone_lower": 99, "zone_upper": 101, "zone_width": 2}
    bars = [
        {"timestamp": 1000, "open": 100, "high": 100.5, "low": 99.5, "close": 100, "volume": 10},
        {"timestamp": 1060, "open": 100, "high": 102.2, "low": 99.0, "close": 101.2, "volume": 20},
        {"timestamp": 1120, "open": 101.2, "high": 104.0, "low": 99.0, "close": 103.0, "volume": 30},
    ]
    out = compute_a3_preview_breakout(zone, bars)
    assert out["a3_future_net_mfe_15m_r"] > 0.9
    assert out["a3_future_net_mae_15m_r"] < -1.5
    assert _latency_bucket(True, 60) == "FAST_IGNITION"
    assert _latency_bucket(True, 61) == "NORMAL_IGNITION"
    assert _latency_bucket(True, 901) == "LATE_IGNITION"
    assert _latency_bucket(False, 0) == "NO_IGNITION"


def test_first_hit_outcomes():
    r, o, _ = _first_hit("BUY", [{"high": 3, "low": 1, "close": 2}], 2, 1, 0.1)
    assert o == "AMBIGUOUS_BOTH_HIT" and r == -1.1
    r, o, _ = _first_hit("BUY", [{"high": 3.1, "low": 2.2, "close": 3}], 2, 1, 0.1)
    assert o == "TARGET_1R_FIRST" and r == 0.9
    r, o, _ = _first_hit("BUY", [{"high": 2.1, "low": 0.9, "close": 1}], 2, 1, 0.1)
    assert o == "STOP_1R_FIRST" and r == -1.1


def test_pre_ignition_states():
    bars = [{"timestamp": t, "high": 101, "low": 99, "close": 100} for t in [0, 60]]
    out = compute_a2_pre_ignition_metrics({"reaction_event_ts": 0, "a2_risk_u": 2}, bars)
    assert out["a2_pre_ignition_compression_state_future"] == "INSUFFICIENT_BARS"


def test_sell_net_mfe_mae():
    zone = {"direction": "SELL", "reaction_event_ts": 1000.0, "zone_lower": 99, "zone_upper": 101, "zone_width": 2}
    bars = [
        {"timestamp": 1000, "open": 100, "high": 100.5, "low": 99.5, "close": 100},
        {"timestamp": 1060, "open": 100, "high": 101.0, "low": 94.0, "close": 97},
    ]
    out = compute_a3_preview_breakout(zone, bars)
    fee_share = out["a3_future_fee_share_r"]
    assert out["a3_future_net_mfe_15m_r"] == round((4/2)-fee_share, 8)
    assert out["a3_future_net_mae_15m_r"] == round((-3/2)-fee_share, 8)

def test_pre_ignition_window_not_truncated_by_entry_ts():
    bars = [{"timestamp": t, "high": 101, "low": 99, "close": 100} for t in range(0, 3660, 60)]
    out = compute_a2_pre_ignition_metrics({"reaction_event_ts": 60, "a2_risk_u": 2, "a3_future_breakout_entry_ts": 120}, bars)
    assert out["a2_pre_ignition_window_sec_future"] >= 3540
