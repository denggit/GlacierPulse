from src.research.a1_edge.forward_metrics import A1ForwardMetricsAnalyzer
from src.research.a1_edge.schema import A1EdgeEvent


def _klines():
    return [
        {"timestamp": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
        {"timestamp": 60, "open": 100, "high": 103, "low": 99, "close": 102, "volume": 1},
        {"timestamp": 120, "open": 102, "high": 104, "low": 98, "close": 99, "volume": 1},
    ]


def test_buy_sell_hits_partial_and_insufficient():
    analyzer = A1ForwardMetricsAnalyzer(windows_sec=[60, 300], min_risk_u=1)
    buy = A1EdgeEvent.from_mapping({"zone_id": "b", "direction": "BUY", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    sell = A1EdgeEvent.from_mapping({"zone_id": "s", "direction": "SELL", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    late = A1EdgeEvent.from_mapping({"zone_id": "late", "direction": "BUY", "reaction_event_ts": 999, "last_price": 100})
    rows = analyzer.analyze([buy, sell, late], _klines())
    buy_60 = next(r for r in rows if r.zone_id == "b" and r.window_sec == 60)
    sell_60 = next(r for r in rows if r.zone_id == "s" and r.window_sec == 60)
    late_60 = next(r for r in rows if r.zone_id == "late")
    assert buy_60.future_bar_count == 1
    assert buy_60.directional_mfe_u == 3
    assert buy_60.directional_mae_u == 1
    assert buy_60.hit_plus_1r is True
    assert sell_60.directional_mfe_u == 1
    assert sell_60.hit_minus_1r is True
    assert next(r for r in rows if r.zone_id == "b" and r.window_sec == 300).partial_window is True
    assert late_60.insufficient_future_data is True
