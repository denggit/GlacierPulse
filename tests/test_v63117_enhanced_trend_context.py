#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.market_context import ZoneMarketContextCalculator, _trend_alignment


BASE_TS = 1_779_000_000.0
ANCHOR_TS = BASE_TS + 120 * 60


def _bars_from_closes(closes, volume=10.0):
    return [
        {
            "timestamp": BASE_TS + idx * 60,
            "open": close,
            "high": close + 0.05,
            "low": close - 0.05,
            "close": close,
            "volume": volume,
        }
        for idx, close in enumerate(closes)
    ]


def _linear_bars(count=300, start=100.0, step=0.1):
    return _bars_from_closes([start + idx * step for idx in range(count)])


def _range_bars(count=300):
    return _bars_from_closes([100.0 + (0.2 if idx % 2 else 0.0) for idx in range(count)])


def _attach(bars, anchor_ts=ANCHOR_TS):
    return ZoneMarketContextCalculator().attach_to_row({"forward_anchor_ts": anchor_ts}, bars)


def test_up_trend_score_and_ema_context():
    row = _attach(_linear_bars(140, start=100.0, step=0.1))

    assert row["trend_score_1h"] > 0
    assert row["trend_regime_enhanced_1h"] == "UP"
    assert row["trend_confidence_1h"] > 0
    assert row["ema20_1m"] > row["ema60_1m"]
    assert row["ema20_slope_15m_pct"] > 0


def test_down_trend_score_and_ema_context():
    row = _attach(_linear_bars(140, start=130.0, step=-0.1))

    assert row["trend_score_1h"] < 0
    assert row["trend_regime_enhanced_1h"] == "DOWN"
    assert row["ema20_1m"] < row["ema60_1m"]
    assert row["ema20_slope_15m_pct"] < 0


def test_range_trend_score_stays_range():
    row = _attach(_range_bars(140))

    assert row["trend_regime_enhanced_1h"] == "RANGE"


def test_trend_efficiency_distinguishes_clean_trend_from_chop():
    clean_closes = [100.0] * 119 + [100.0 + idx * 0.1 for idx in range(61)]
    choppy_tail = [100.0]
    current = 100.0
    for idx in range(1, 61):
        if idx == 60:
            current = 106.0
        elif idx % 2:
            current += 1.1
        else:
            current -= 0.9
        choppy_tail.append(current)
    choppy_closes = [100.0] * 119 + choppy_tail

    clean = _attach(_bars_from_closes(clean_closes), anchor_ts=BASE_TS + 180 * 60)
    choppy = _attach(_bars_from_closes(choppy_closes), anchor_ts=BASE_TS + 180 * 60)

    assert clean["pre_1h_return_u"] == choppy["pre_1h_return_u"]
    assert clean["trend_efficiency_1h"] > 0.95
    assert choppy["trend_efficiency_1h"] < clean["trend_efficiency_1h"]


def test_enhanced_trend_context_ignores_unfinished_anchor_minute():
    completed = _linear_bars(120, start=100.0, step=0.1)
    normal_current = dict(completed[-1])
    normal_current["timestamp"] = BASE_TS + 120 * 60
    extreme_current = {
        "timestamp": BASE_TS + 120 * 60,
        "open": 999.0,
        "high": 9999.0,
        "low": 1.0,
        "close": 999.0,
        "volume": 999999.0,
    }

    anchor_ts = BASE_TS + 120 * 60 + 30
    normal = _attach(completed + [normal_current], anchor_ts=anchor_ts)
    extreme = _attach(completed + [extreme_current], anchor_ts=anchor_ts)

    fields = [
        "ema20_1m",
        "ema60_1m",
        "ema20_slope_15m_pct",
        "trend_efficiency_1h",
        "price_location_1h",
        "trend_score_1h",
        "trend_regime_enhanced_1h",
    ]
    for field in fields:
        assert extreme[field] == normal[field]


def test_trend_alignment_cases():
    up = _attach(_linear_bars(320, start=100.0, step=0.1), anchor_ts=BASE_TS + 300 * 60)
    down = _attach(_linear_bars(320, start=160.0, step=-0.1), anchor_ts=BASE_TS + 300 * 60)

    ranging = _attach(_range_bars(320), anchor_ts=BASE_TS + 300 * 60)
    conflict_label, conflict_score = _trend_alignment("UP", "DOWN")

    assert up["trend_alignment"] == "ALIGNED_UP"
    assert up["trend_alignment_score"] == 2
    assert down["trend_alignment"] == "ALIGNED_DOWN"
    assert down["trend_alignment_score"] == 2
    assert conflict_label == "CONFLICT"
    assert conflict_score == -2
    assert ranging["trend_regime_enhanced_1h"] == "RANGE"
    assert ranging["trend_regime_enhanced_4h"] == "RANGE"
    assert ranging["trend_alignment"] == "RANGE_ALIGNED"
    assert ranging["trend_alignment_score"] == 1
