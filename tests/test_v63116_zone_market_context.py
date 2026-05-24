#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.market_context import ZoneMarketContextCalculator


BASE_TS = 1_779_000_000.0


def _bars(count=300, start_price=100.0, step=0.01, volume=10.0):
    return [
        {
            "timestamp": BASE_TS + i * 60,
            "open": start_price + i * step,
            "high": start_price + i * step + 0.2,
            "low": start_price + i * step - 0.2,
            "close": start_price + i * step,
            "volume": volume + (i % 20),
        }
        for i in range(count)
    ]


def test_pre_1h_return_and_pct_are_calculated():
    row = ZoneMarketContextCalculator().attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, _bars(step=0.1))
    assert row["pre_1h_return_u"] == 6.0
    assert round(row["pre_1h_return_pct"], 6) == round(6.0 / 106.0 * 100, 6)


def test_trend_regime_1h_up_down_range():
    calc = ZoneMarketContextCalculator()
    up = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, _bars(step=0.1))
    down = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, _bars(start_price=130, step=-0.1))
    flat = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, _bars(step=0.001))
    assert up["trend_regime_1h"] == "UP"
    assert down["trend_regime_1h"] == "DOWN"
    assert flat["trend_regime_1h"] == "RANGE"


def test_volatility_regime_1h_thresholds():
    calc = ZoneMarketContextCalculator()
    low = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, _bars(step=0.001))
    mid_bars = _bars(step=0.0)
    for bar in mid_bars:
        bar["high"] = bar["close"] + 0.4
        bar["low"] = bar["close"] - 0.4
    high_bars = _bars(step=0.0)
    for bar in high_bars:
        bar["high"] = bar["close"] + 1.0
        bar["low"] = bar["close"] - 1.0
    mid = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, mid_bars)
    high = calc.attach_to_row({"forward_anchor_ts": BASE_TS + 120 * 60}, high_bars)
    assert low["volatility_regime_1h"] == "LOW_VOL"
    assert mid["volatility_regime_1h"] == "MID_VOL"
    assert high["volatility_regime_1h"] == "HIGH_VOL"


def test_volume_regime_has_reasonable_output():
    row = ZoneMarketContextCalculator().attach_to_row({"forward_anchor_ts": BASE_TS + 180 * 60}, _bars())
    assert row["volume_regime_1h"] in {"HIGH_VOLUME", "MID_VOLUME", "LOW_VOLUME"}


def test_market_context_anchor_prefers_forward_anchor_ts():
    row = ZoneMarketContextCalculator().attach_to_row(
        {
            "forward_anchor_ts": BASE_TS + 120 * 60,
            "reaction_event_ts": BASE_TS + 60 * 60,
            "frozen_ts": BASE_TS + 30 * 60,
        },
        _bars(),
    )
    assert row["market_context_anchor_ts"] == BASE_TS + 120 * 60


def test_insufficient_kline_outputs_unknown_and_zero():
    row = ZoneMarketContextCalculator().attach_to_row({"forward_anchor_ts": BASE_TS + 30 * 60}, _bars(count=20))
    assert row["pre_1h_return_u"] == 0.0
    assert row["trend_regime_1h"] == "UNKNOWN"
    assert row["volatility_regime_1h"] == "UNKNOWN"
    assert row["volume_regime_1h"] == "UNKNOWN"
