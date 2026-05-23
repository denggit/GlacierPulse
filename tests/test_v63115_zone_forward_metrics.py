#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.forward import compute_zone_forward_metric


BASE_TS = 1_779_000_000.0


def _bars(count=20):
    rows = []
    for i in range(count):
        rows.append(
            {
                "timestamp": BASE_TS + i * 60,
                "open": 100 + i,
                "high": 101 + i,
                "low": 99 + i,
                "close": 100 + i,
                "volume": 1,
            }
        )
    return rows


def test_buy_zone_mfe_mae_calculation():
    metric = compute_zone_forward_metric({"direction": "BUY", "reaction_event_ts": BASE_TS + 30, "zone_mid": 100}, _bars(), 900)
    assert metric["mfe_u"] == 16
    assert metric["mae_u"] == 0


def test_sell_zone_mfe_mae_calculation():
    metric = compute_zone_forward_metric({"direction": "SELL", "reaction_event_ts": BASE_TS + 30, "zone_mid": 100}, _bars(), 900)
    assert metric["mfe_u"] == 0
    assert metric["mae_u"] == -16


def test_start_bar_uses_next_complete_one_minute_bar():
    bars = [
        {"timestamp": BASE_TS, "open": 100, "high": 500, "low": 1, "close": 100, "volume": 1},
        {"timestamp": BASE_TS + 60, "open": 100, "high": 102, "low": 98, "close": 101, "volume": 1},
    ]
    metric = compute_zone_forward_metric({"direction": "BUY", "reaction_event_ts": BASE_TS, "zone_mid": 100}, bars, 60)
    assert metric["mfe_u"] == 2
    assert metric["mae_u"] == -2


def test_incomplete_windows_are_marked_false():
    metric = compute_zone_forward_metric({"direction": "BUY", "reaction_event_ts": BASE_TS + 30, "zone_mid": 100}, _bars(3), 900)
    assert metric["is_complete"] is False


def test_mfe_min_zero_and_mae_max_zero():
    bars = [
        {"timestamp": BASE_TS + 60, "open": 110, "high": 111, "low": 109, "close": 110, "volume": 1},
    ]
    buy = compute_zone_forward_metric({"direction": "BUY", "reaction_event_ts": BASE_TS + 1, "zone_mid": 100}, bars, 60)
    sell = compute_zone_forward_metric({"direction": "SELL", "reaction_event_ts": BASE_TS + 1, "zone_mid": 100}, bars, 60)
    assert buy["mae_u"] == 0
    assert sell["mfe_u"] == 0
