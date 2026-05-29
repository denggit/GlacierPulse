#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from datetime import datetime, timezone
from pathlib import Path

from src.research.context import ContextCacheSimulator, IcebergContextConfig, label_iceberg_contexts
from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_372_000.0


def _candidate(key="c1", ts=BASE_TS, direction="BUY", **extra):
    row = {
        "record_type": "candidate_finalized",
        "event_key": key,
        "result": "ICEBERG",
        "direction": direction,
        "settle_ts": ts,
        "trigger_ts": ts,
        "settle_price": 100.0,
        "trigger_price": 100.0,
        "zone_lower": 99.0,
        "zone_upper": 101.0,
    }
    row.update(extra)
    return row


def _minute_rows(start, count, open_price=100.0, close_price=100.0, high=None, low=None, volume=10.0):
    rows = []
    for i in range(count):
        ts = start + i * 60
        rows.append(
            {
                "timestamp": ts,
                "open": open_price,
                "high": high if high is not None else max(open_price, close_price),
                "low": low if low is not None else min(open_price, close_price),
                "close": close_price,
                "volume": volume,
            }
        )
    return rows


def _expand_15m(start, bars):
    rows = []
    for idx, bar in enumerate(bars):
        bar_start = start + idx * 900
        for minute in range(15):
            ts = bar_start + minute * 60
            rows.append(
                {
                    "timestamp": ts,
                    "open": bar["open"] if minute == 0 else bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar.get("volume", 10.0),
                }
            )
    return rows


def test_context_cache_simulator_advances_by_1m_and_labels_candidates_at_arrival():
    rows = _minute_rows(BASE_TS, 3, close_price=100)
    candidates = [_candidate("early", BASE_TS), _candidate("late", BASE_TS + 120)]
    labels = ContextCacheSimulator(rows, candidates).run()
    assert set(labels) == {"early", "late"}
    assert labels["early"]["context_labels_status"] == "SUCCESS"


def test_boll_15m_no_future_uses_previous_closed_bar_only():
    rows = _minute_rows(BASE_TS, 20 * 15, close_price=100, high=101, low=99)
    rows += _minute_rows(BASE_TS + 20 * 900, 1, open_price=100, close_price=200, high=200, low=100)
    labels = label_iceberg_contexts([_candidate("c1", BASE_TS + 20 * 900, settle_price=100, zone_lower=100)], rows)
    assert labels["c1"]["boll_15m_mid"] == 100.0
    assert labels["c1"]["boll_15m_position"] == "BOLL_UNAVAILABLE" or labels["c1"]["boll_15m_position"] == "MID_AREA"


def test_boll_1h_no_future_uses_previous_closed_hour_only():
    rows = _minute_rows(BASE_TS, 20 * 60, close_price=100, high=101, low=99)
    rows += _minute_rows(BASE_TS + 20 * 3600, 1, open_price=100, close_price=200, high=200, low=100)
    labels = label_iceberg_contexts([_candidate("c1", BASE_TS + 20 * 3600, settle_price=100, zone_lower=100)], rows)
    assert labels["c1"]["boll_1h_mid"] == 100.0


def test_context_cache_simulator_does_not_use_open_1m_bar_until_close():
    rows = [
        {"timestamp": BASE_TS, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 10},
        {"timestamp": BASE_TS + 60, "open": 200, "high": 200, "low": 200, "close": 200, "volume": 1000},
    ]
    labels = label_iceberg_contexts(
        [
            _candidate("inside_second_bar", BASE_TS + 90, zone_lower=100, settle_price=100),
            _candidate("at_second_close", BASE_TS + 120, zone_lower=200, settle_price=200),
        ],
        rows,
    )
    assert labels["inside_second_bar"]["vp1h_proxy_poc"] == 100
    assert labels["inside_second_bar"]["vp1h_proxy_total_volume"] == 10
    assert labels["at_second_close"]["vp1h_proxy_poc"] == 200
    assert labels["at_second_close"]["vp1h_proxy_total_volume"] == 1010


def test_vpsession_resets_at_utc_session_boundary_before_first_new_session_bar_closes():
    # UTC session boundary: Asia 00:00-08:00, Europe 08:00-16:00.
    asia_last_open = datetime(2026, 1, 1, 7, 59, tzinfo=timezone.utc).timestamp()
    europe_first_open = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc).timestamp()
    rows = [
        {"timestamp": asia_last_open, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1000},
        {"timestamp": europe_first_open, "open": 200, "high": 200, "low": 200, "close": 200, "volume": 2000},
    ]
    labels = label_iceberg_contexts(
        [
            _candidate("at_boundary", europe_first_open, zone_lower=200, settle_price=200, trigger_price=200),
            _candidate("before_europe_close", europe_first_open + 30, zone_lower=200, settle_price=200, trigger_price=200),
            _candidate("at_europe_close", europe_first_open + 60, zone_lower=200, settle_price=200, trigger_price=200),
        ],
        rows,
    )

    assert labels["at_boundary"]["vp1h_proxy_poc"] == 100
    assert labels["at_boundary"]["vp1h_proxy_total_volume"] == 1000
    assert labels["at_boundary"]["vpsession_proxy_total_volume"] == 0
    assert labels["at_boundary"]["vpsession_proxy_location"] == "VP_INSUFFICIENT_DATA"

    assert labels["before_europe_close"]["vp1h_proxy_poc"] == 100
    assert labels["before_europe_close"]["vp1h_proxy_total_volume"] == 1000
    assert labels["before_europe_close"]["vpsession_proxy_total_volume"] == 0
    assert labels["before_europe_close"]["vpsession_proxy_location"] == "VP_INSUFFICIENT_DATA"

    assert labels["at_europe_close"]["vp1h_proxy_poc"] == 200
    assert labels["at_europe_close"]["vp1h_proxy_total_volume"] == 3000
    assert labels["at_europe_close"]["vpsession_proxy_poc"] == 200
    assert labels["at_europe_close"]["vpsession_proxy_total_volume"] == 2000


def test_vp_value_area_expands_contiguously_from_poc():
    rows = [
        {"timestamp": BASE_TS + idx * 60, "open": price, "high": price, "low": price, "close": price, "volume": volume}
        for idx, (price, volume) in enumerate([(100, 1000), (101, 10), (102, 10), (103, 900), (104, 10)])
    ]
    labels = label_iceberg_contexts(
        [_candidate("vp", BASE_TS + 5 * 60, zone_lower=100, settle_price=100)],
        rows,
        IcebergContextConfig(vp_bin_size_u=1.0, vp_value_area_ratio=0.52),
    )
    assert labels["vp"]["vp1h_proxy_poc"] == 100
    assert labels["vp"]["vp1h_proxy_val"] == 100
    assert labels["vp"]["vp1h_proxy_vah"] == 101
    assert labels["vp"]["vp1h_proxy_value_area_volume"] == 1010


def test_buy_context_price_uses_min_extreme_and_sell_uses_max_extreme():
    buy = _candidate(
        "buy",
        BASE_TS,
        "BUY",
        first_iceberg_pie_min_trade_price=98,
        first_pie_min_trade_price=97,
        min_trade_price=96,
        zone_lower=99,
    )
    sell = _candidate(
        "sell",
        BASE_TS,
        "SELL",
        first_iceberg_pie_max_trade_price=102,
        first_pie_max_trade_price=103,
        max_trade_price=104,
        zone_upper=101,
    )
    labels = label_iceberg_contexts([buy, sell], _minute_rows(BASE_TS, 1))
    assert labels["buy"]["iceberg_context_price"] == 96
    assert labels["buy"]["iceberg_context_price_source"] == "min_trade_price"
    assert labels["sell"]["iceberg_context_price"] == 104
    assert labels["sell"]["iceberg_context_price_source"] == "max_trade_price"


def test_boll_position_classification_boundaries():
    rows = []
    closes = [100.0] * 10 + [110.0] * 10
    for idx, close in enumerate(closes):
        rows += _minute_rows(BASE_TS + idx * 900, 15, close_price=close, high=close + 1, low=close - 1)
    labels = label_iceberg_contexts(
        [
            _candidate("below", BASE_TS + 20 * 900, zone_lower=90, settle_price=90),
            _candidate("mid", BASE_TS + 20 * 900, zone_lower=105, settle_price=105, trigger_price=105),
            _candidate("above", BASE_TS + 20 * 900, direction="SELL", zone_upper=120, settle_price=120),
        ],
        rows + _minute_rows(BASE_TS + 20 * 900, 1, close_price=105),
    )
    assert labels["below"]["boll_15m_position"] == "BELOW_LOWER"
    assert labels["mid"]["boll_15m_position"] == "MID_AREA"
    assert labels["above"]["boll_15m_position"] == "ABOVE_UPPER"


def test_vp_rolling_histogram_changes_with_candidate_time_without_candidate_scan():
    rows = _minute_rows(BASE_TS, 70, close_price=100, high=100, low=100, volume=10)
    rows += _minute_rows(BASE_TS + 70 * 60, 70, close_price=200, high=200, low=200, volume=20)
    labels = label_iceberg_contexts(
        [
            _candidate("first", BASE_TS + 59 * 60, zone_lower=100, settle_price=100),
            _candidate("second", BASE_TS + 139 * 60, zone_lower=200, settle_price=200),
        ],
        rows,
    )
    assert labels["first"]["vp1h_proxy_poc"] == 100
    assert labels["second"]["vp1h_proxy_poc"] == 200


def test_vp_poc_val_vah_and_locations():
    rows = []
    rows += _minute_rows(BASE_TS, 5, close_price=100, high=100, low=100, volume=100)
    rows += _minute_rows(BASE_TS + 5 * 60, 3, close_price=101, high=101, low=101, volume=50)
    rows += _minute_rows(BASE_TS + 8 * 60, 1, close_price=105, high=105, low=105, volume=5)
    candidates = [
        _candidate("val", BASE_TS + 9 * 60, zone_lower=100, settle_price=100),
        _candidate("inside", BASE_TS + 9 * 60, zone_lower=101, settle_price=101, trigger_price=101),
        _candidate("above", BASE_TS + 9 * 60, direction="SELL", zone_upper=110, settle_price=110),
    ]
    labels = label_iceberg_contexts(candidates, rows, IcebergContextConfig(vp_bin_size_u=1.0))
    assert labels["val"]["vp1h_proxy_poc"] == 100
    assert labels["val"]["vp1h_proxy_val"] <= labels["val"]["vp1h_proxy_vah"]
    assert labels["val"]["vp1h_proxy_location"] in {"NEAR_VAL", "NEAR_POC"}
    assert labels["inside"]["vp1h_proxy_location"] in {"NEAR_VAL", "NEAR_VAH", "INSIDE_VALUE_AREA", "NEAR_POC"}
    assert labels["above"]["vp1h_proxy_location"] == "OUTSIDE_VALUE_ABOVE"


def test_local_high_low_no_future():
    bars = [{"open": 100, "high": 110, "low": 90, "close": 100} for _ in range(16)]
    rows = _expand_15m(BASE_TS, bars)
    rows += _minute_rows(BASE_TS + 16 * 900, 1, open_price=100, close_price=100, high=100, low=50)
    labels = label_iceberg_contexts([_candidate("c1", BASE_TS + 16 * 900, zone_lower=90, settle_price=90)], rows)
    assert labels["c1"]["local_15m_low_16"] == 90
    assert labels["c1"]["near_local_15m_low_flag"] is True


def test_order_block_bullish_and_buy_inside_flag():
    bars = [{"open": 100, "high": 100, "low": 95, "close": 96}]
    bars += [{"open": 100, "high": 104, "low": 96, "close": 101} for _ in range(8)]
    bars += [{"open": 100, "high": 110, "low": 99, "close": 108}]
    rows = _expand_15m(BASE_TS, bars) + _minute_rows(BASE_TS + len(bars) * 900, 1, close_price=98, high=99, low=97)
    labels = label_iceberg_contexts([_candidate("buy", BASE_TS + len(bars) * 900, zone_lower=98, settle_price=98)], rows)
    assert labels["buy"]["order_block_15m_type"] == "BULLISH_OB"
    assert labels["buy"]["inside_order_block_15m_flag"] is True


def test_order_block_bearish_and_sell_inside_flag():
    bars = [{"open": 100, "high": 105, "low": 100, "close": 104}]
    bars += [{"open": 100, "high": 104, "low": 96, "close": 99} for _ in range(8)]
    bars += [{"open": 100, "high": 101, "low": 90, "close": 92}]
    rows = _expand_15m(BASE_TS, bars) + _minute_rows(BASE_TS + len(bars) * 900, 1, close_price=103, high=104, low=102)
    labels = label_iceberg_contexts([_candidate("sell", BASE_TS + len(bars) * 900, "SELL", zone_upper=103, settle_price=103)], rows)
    assert labels["sell"]["order_block_15m_type"] == "BEARISH_OB"
    assert labels["sell"]["inside_order_block_15m_flag"] is True


def test_book_liquidity_proxy_unavailable_medium_and_strong():
    labels = label_iceberg_contexts(
        [
            _candidate("none", BASE_TS),
            _candidate("medium", BASE_TS, zone_v2_reload_level_count=2, active_notional=1_000_000, absorption_rate=0.8),
            _candidate("strong", BASE_TS, local_depth_usdt=2_000_000, zone_v2_reload_level_count=2, active_notional=1_000_000, hidden_volume=2_000_000, absorption_rate=0.9),
        ],
        [],
    )
    assert labels["none"]["book_blocking_liquidity_proxy_strength"] == "UNAVAILABLE"
    assert labels["medium"]["book_blocking_liquidity_proxy_strength"] == "MEDIUM"
    assert labels["strong"]["book_blocking_liquidity_proxy_strength"] == "STRONG"


def test_book_liquidity_proxy_does_not_treat_hidden_volume_as_notional():
    labels = label_iceberg_contexts(
        [_candidate("volume_only", BASE_TS, hidden_volume=2_000_000, absorption_rate=0.9)],
        [],
    )
    assert labels["volume_only"]["reload_wall_proxy_flag"] is False
    assert labels["volume_only"]["book_blocking_liquidity_proxy_flag"] is False
    assert labels["volume_only"]["book_blocking_liquidity_proxy_strength"] == "NONE"


def test_context_labels_do_not_change_zone_truth_events_row_count_and_no_kline_does_not_fail(tmp_path):
    phase1 = [
        _candidate("pie-1", BASE_TS, zone_id="z1", hidden_volume=2_000_000, absorption_rate=0.8, active_notional=1_000_000),
        _candidate("pie-2", BASE_TS + 60, zone_id="z2", hidden_volume=2_000_000, absorption_rate=0.8, active_notional=1_000_000),
    ]
    reactions = [
        {"zone_id": "z1", "direction": "BUY", "frozen_ts": BASE_TS, "reaction_event_ts": BASE_TS + 60, "frozen_low": 99, "frozen_high": 101},
        {"zone_id": "z2", "direction": "BUY", "frozen_ts": BASE_TS + 60, "reaction_event_ts": BASE_TS + 120, "frozen_low": 99, "frozen_high": 101},
    ]
    out = tmp_path / "out"
    summary = ZoneTruthAnalyzer().export(phase1, reactions, [], out)
    assert summary["context_labels_status"] == "KLINE_UNAVAILABLE"
    with (out / "zone_truth_events.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert {row["context_labels_status"] for row in rows} == {"KLINE_UNAVAILABLE"}


def test_truth_score_is_not_required_for_context_label_inputs():
    row = _candidate("no_truth", BASE_TS, zone_lower=100, settle_price=100)
    labels = label_iceberg_contexts([row], _minute_rows(BASE_TS, 1))
    assert labels["no_truth"]["context_labels_status"] == "SUCCESS"


def test_deleted_boll_strategy_and_martingale_tests_do_not_exist():
    root = Path(__file__).resolve().parents[1]
    assert not (root / "tools" / "backtest_iceberg_boll_mid_strategy.py").exists()
    assert not (root / "tests" / "test_iceberg_boll_mid_strategy.py").exists()
    assert not (root / "tests" / "test_iceberg_boll_martingale_strategy.py").exists()


def test_a1_and_a2_pre_pool_existing_tests_still_present():
    root = Path(__file__).resolve().parents[1]
    assert (root / "tests" / "test_v6311_phase1_truth_scorer.py").exists()
    assert (root / "tests" / "test_v6312_zone_truth_a2_fields.py").exists()


def test_v721_vp_nearest_hvn_lvn_node_and_value_area_width():
    from src.research.context.iceberg_context_labels import _classify_vp_price, _compute_vp_cache

    cache = _compute_vp_cache({98: 10, 99: 20, 100: 100, 101: 15, 102: 80}, 0.70)
    labels = _classify_vp_price(cache, 102.0, threshold=0.25, bin_size=1.0, atr_15m=2.0)
    assert labels["nearest_hvn"] == 102
    assert labels["nearest_lvn"] in {98, 101}
    assert labels["near_hvn_flag"] is True
    assert labels["node_context"] == "NEAR_VAH"
    assert labels["value_area_width_u"] == cache["vah"] - cache["val"]
    assert labels["value_area_width_pct"] == round((cache["vah"] - cache["val"]) / 102.0, 8)
    assert labels["value_area_width_atr"] == round((cache["vah"] - cache["val"]) / 2.0, 8)


def test_v721_value_edge_reclaim_and_sweep_failed_auction_labels():
    analyzer = ZoneTruthAnalyzer()
    future = [
        {"timestamp": BASE_TS + 60, "close": 99.5},
        {"timestamp": BASE_TS + 120, "close": 100.5},
    ]
    buy = {"vpsession_proxy_val": 100, "vpsession_proxy_vah": 110, "previous_local_15m_low_16": 99, "zone_v2_structural_risk_u": 2}
    value = analyzer._value_edge_labels(buy, future, "BUY", 98.5, "vpsession")
    sweep = analyzer._sweep_failed_auction_labels(buy, future, "BUY", 98.5, "15m", 16)
    assert value["vpsession_value_edge_side"] == "BELOW_VAL"
    assert value["vpsession_reclaim_value_post_event_flag"] is True
    assert value["vpsession_bars_to_reclaim"] == 2
    assert sweep["post_sweep_reclaim_15m_post_event_flag"] is True
    assert sweep["failed_auction_15m_post_event_flag"] is True

    sell = {"vp24h_proxy_val": 90, "vp24h_proxy_vah": 100, "previous_local_1h_high_12": 101, "zone_v2_structural_risk_u": 2}
    future_sell = [{"timestamp": BASE_TS + 60, "close": 99.5}]
    value_sell = analyzer._value_edge_labels(sell, future_sell, "SELL", 101.5, "vp24h")
    sweep_sell = analyzer._sweep_failed_auction_labels(sell, future_sell, "SELL", 101.5, "1h", 12)
    assert value_sell["vp24h_value_edge_side"] == "ABOVE_VAH"
    assert value_sell["vp24h_reclaim_value_post_event_flag"] is True
    assert sweep_sell["failed_auction_1h_post_event_flag"] is True


def test_v721_aggression_quality_boll_session_and_ob_quality():
    from src.research.context.iceberg_context_labels import _aggression_quality_labels, _boll_labels, _compute_boll, _ob_labels, _session_labels

    recent = _minute_rows(BASE_TS, 20, open_price=100, close_price=100, high=100.5, low=99.5, volume=10)
    strong_bar = {"timestamp": BASE_TS + 1200, "open": 100, "high": 110, "low": 99, "close": 109.5, "volume": 100}
    strong = _aggression_quality_labels(strong_bar, recent + [strong_bar], "BUY")
    weak_bar = {"timestamp": BASE_TS + 1260, "open": 100, "high": 101, "low": 99, "close": 100.1, "volume": 10}
    weak = _aggression_quality_labels(weak_bar, recent + [weak_bar], "BUY")
    assert strong["a3_aggression_quality"] == "STRONG"
    assert weak["a3_aggression_quality"] in {"WEAK", "MEDIUM"}

    bars = [{"timestamp": BASE_TS + i * 900, "open": 100, "high": 100 + (i % 5), "low": 99, "close": 100 + (i % 5), "volume": 10} for i in range(120)]
    boll = _boll_labels(_compute_boll(bars, IcebergContextConfig(), "15m"), 100, "15m")
    assert 0 <= boll["boll_15m_band_width_percentile"] <= 1
    assert "boll_15m_squeeze_flag" in boll and "boll_15m_expansion_flag" in boll

    session = _session_labels(BASE_TS, BASE_TS - 60 * 30)
    assert session["session_utc"] in {"ASIA", "EUROPE", "US"}
    assert session["session_bucket"].endswith("EARLY")

    ob = {"bullish": {"type": "BULLISH_OB", "low": 99, "high": 101, "created_bar_index": 10, "displacement_strength": 1.5, "invalidated_flag": False}}
    labels = _ob_labels(ob, 100, "BUY", 1.0, "15m", 1.0, current_index=20, fresh_bars=32)
    assert labels["order_block_15m_age_bars"] == 10
    assert labels["order_block_15m_fresh_flag"] is True
    ob["bullish"]["invalidated_flag"] = True
    assert _ob_labels(ob, 100, "BUY", 1.0, "15m", 1.0, 50, 32)["order_block_15m_invalidated_flag"] is True


def test_v721_vp_labels_filter_internal_histogram_keys():
    labels = label_iceberg_contexts([_candidate("vp_internal", BASE_TS + 120)], _minute_rows(BASE_TS, 4, close_price=100, volume=10))
    row = labels["vp_internal"]
    assert not any(key.endswith(("__hist", "__hvn_bins", "__lvn_bins")) for key in row)


def test_v721_post_event_aggression_uses_first_future_bar_not_pre_event_bar():
    analyzer = ZoneTruthAnalyzer()
    bars = _minute_rows(BASE_TS, 19, open_price=100, close_price=100, high=100.5, low=99.5, volume=10)
    bars.append({"timestamp": BASE_TS + 19 * 60, "open": 100, "high": 112, "low": 99, "close": 111.5, "volume": 500})
    bars.append({"timestamp": BASE_TS + 20 * 60, "open": 100, "high": 101, "low": 99, "close": 100.1, "volume": 10})
    bars.append({"timestamp": BASE_TS + 21 * 60, "open": 100, "high": 150, "low": 90, "close": 149, "volume": 10_000})
    rows = [{"zone_id": "z", "direction": "BUY", "settle_ts": BASE_TS + 20 * 60, "iceberg_context_price": 100}]
    out = analyzer.attach_post_event_context_labels(rows, bars)[0]
    assert out["a3_aggression_quality"] != "STRONG"


def test_v721_post_event_aggression_strong_and_ignores_later_future_baseline():
    analyzer = ZoneTruthAnalyzer()
    baseline = _minute_rows(BASE_TS, 20, open_price=100, close_price=100, high=100.5, low=99.5, volume=10)
    strong_post = {"timestamp": BASE_TS + 20 * 60, "open": 100, "high": 112, "low": 99, "close": 111.5, "volume": 500}
    later_future = {"timestamp": BASE_TS + 21 * 60, "open": 100, "high": 200, "low": 50, "close": 190, "volume": 100_000}
    rows = [{"zone_id": "z", "direction": "BUY", "settle_ts": BASE_TS + 20 * 60, "iceberg_context_price": 100}]
    without_later = analyzer.attach_post_event_context_labels(rows, baseline + [strong_post])[0]
    with_later = analyzer.attach_post_event_context_labels(rows, baseline + [strong_post, later_future])[0]
    assert with_later["a3_aggression_quality"] == "STRONG"
    assert with_later["a3_volume_zscore"] == without_later["a3_volume_zscore"]
    assert with_later["a3_range_expansion_ratio"] == without_later["a3_range_expansion_ratio"]
