from src.research.zone_truth.forward import A3_PREVIEW_BREAKOUT_WINDOW_SEC, compute_a3_preview_breakout


def test_a3_preview_breakout_buy_latency():
    zone = {
        "direction": "BUY",
        "reaction_event_ts": 1_000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
    }
    bars = [
        {"timestamp": 1_000.0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        {"timestamp": 1_060.0, "open": 100.0, "high": 101.5, "low": 99.5, "close": 101.0},
        {"timestamp": 1_120.0, "open": 101.0, "high": 101.8, "low": 100.0, "close": 101.5},
        {"timestamp": 1_180.0, "open": 101.5, "high": 102.1, "low": 100.5, "close": 102.0},
    ]

    preview = compute_a3_preview_breakout(zone, bars)

    assert preview["a3_future_breakout_seen_flag"] is True
    assert preview["a3_future_breakout_latency_sec"] == 180
    assert preview["a3_future_breakout_direction"] == "BUY"
    assert preview["a3_future_breakout_threshold_u"] == 1.0
    assert preview["a3_future_breakout_price"] == 102.0


def test_a3_preview_breakout_sell_latency():
    zone = {
        "direction": "SELL",
        "reaction_event_ts": 2_000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
    }
    bars = [
        {"timestamp": 2_000.0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        {"timestamp": 2_060.0, "open": 100.0, "high": 100.5, "low": 98.5, "close": 99.0},
        {"timestamp": 2_120.0, "open": 99.0, "high": 99.5, "low": 97.9, "close": 98.0},
    ]

    preview = compute_a3_preview_breakout(zone, bars)

    assert preview["a3_future_breakout_seen_flag"] is True
    assert preview["a3_future_breakout_latency_sec"] == 120
    assert preview["a3_future_breakout_direction"] == "SELL"
    assert preview["a3_future_breakout_threshold_u"] == 1.0
    assert preview["a3_future_breakout_price"] == 98.0


def test_a3_preview_breakout_unknown_direction_is_raw_false():
    zone = {
        "direction": "UNKNOWN",
        "reaction_event_ts": 3_000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
    }
    bars = [{"timestamp": 3_060.0, "open": 100.0, "high": 110.0, "low": 90.0, "close": 100.0}]

    preview = compute_a3_preview_breakout(zone, bars)

    assert preview["a3_future_breakout_seen_flag"] is False
    assert preview["a3_future_breakout_latency_sec"] == 0
    assert preview["a3_future_breakout_direction"] == "UNKNOWN"


def test_a3_preview_breakout_after_one_hour_is_ignored():
    zone = {
        "direction": "BUY",
        "reaction_event_ts": 1_000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
    }
    bars = [
        {"timestamp": 1_000.0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        {"timestamp": 1_000.0 + A3_PREVIEW_BREAKOUT_WINDOW_SEC, "open": 100.0, "high": 101.9, "low": 99.0, "close": 101.0},
        {"timestamp": 1_000.0 + A3_PREVIEW_BREAKOUT_WINDOW_SEC + 60, "open": 101.0, "high": 102.1, "low": 100.0, "close": 102.0},
    ]

    preview = compute_a3_preview_breakout(zone, bars)

    assert preview["a3_future_breakout_seen_flag"] is False
    assert preview["a3_future_breakout_latency_sec"] == 0


def test_a3_preview_breakout_with_anchor_before_first_bar_is_raw_false():
    zone = {
        "direction": "BUY",
        "reaction_event_ts": 1_000.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
        "zone_width": 2.0,
    }
    bars = [{"timestamp": 1_060.0, "open": 100.0, "high": 102.1, "low": 99.0, "close": 102.0}]

    preview = compute_a3_preview_breakout(zone, bars)

    assert preview["a3_future_breakout_seen_flag"] is False
    assert preview["a3_future_breakout_latency_sec"] == 0
