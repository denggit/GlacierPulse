from src.research.zone_truth.a2_state import ZoneA2StateClassifier
from src.research.zone_truth.aggregator import ZoneTruthAggregator
from src.research.zone_truth.forward import compute_a3_structural_proxy_metrics


def _structural(row, bars=None):
    return compute_a3_structural_proxy_metrics(
        {
            "a3_preview_breakout_raw_flag": True,
            "a3_preview_entry_ts": 1000.0,
            "zone_lower": 2998.0,
            "zone_upper": 3002.0,
            "zone_width": 4.0,
            **row,
        },
        bars or [{"timestamp": 1000.0, "open": 3000.0, "high": 3001.0, "low": 2999.0, "close": 3000.0}],
    )


def _classify(row):
    return ZoneA2StateClassifier().classify_row(row)


def test_buy_structural_stop_uses_iceberg_sweep_low_with_buffer():
    out = _structural({"direction": "BUY", "a3_preview_entry_price": 3001.0, "iceberg_trade_sweep_low": 2996.0})

    assert out["a3_structural_stop_price"] == 2995.5
    assert out["a3_structural_risk_u"] == 5.5


def test_sell_structural_stop_uses_iceberg_sweep_high_with_buffer():
    out = _structural({"direction": "SELL", "a3_preview_entry_price": 2999.0, "iceberg_trade_sweep_high": 3004.0})

    assert out["a3_structural_stop_price"] == 3004.5
    assert out["a3_structural_risk_u"] == 5.5


def test_structural_fee_share_uses_roundtrip_fee_over_structural_risk():
    out = _structural({"direction": "BUY", "a3_preview_entry_price": 3000.0, "iceberg_trade_sweep_low": 2994.5})

    assert out["a3_structural_fee_u"] == 3.0
    assert out["a3_structural_fee_share_r"] == 0.5


def test_buy_structural_first_hit_target_first():
    bars = [{"timestamp": 1000.0, "open": 3001.0, "high": 3006.6, "low": 3000.0, "close": 3006.0}]
    out = _structural({"direction": "BUY", "a3_preview_entry_price": 3001.0, "iceberg_trade_sweep_low": 2996.0}, bars)

    assert out["a3_structural_realized_outcome_1h"] == "TARGET_1R_FIRST"
    assert out["a3_structural_realized_r_proxy_1h"] == round(1.0 - out["a3_structural_fee_share_r"], 8)


def test_buy_structural_first_hit_stop_first():
    bars = [{"timestamp": 1000.0, "open": 3001.0, "high": 3002.0, "low": 2995.4, "close": 2996.0}]
    out = _structural({"direction": "BUY", "a3_preview_entry_price": 3001.0, "iceberg_trade_sweep_low": 2996.0}, bars)

    assert out["a3_structural_realized_outcome_1h"] == "STOP_1R_FIRST"
    assert out["a3_structural_realized_r_proxy_1h"] == round(-1.0 - out["a3_structural_fee_share_r"], 8)


def test_buy_structural_ambiguous_both_hit_is_conservative_stop():
    bars = [{"timestamp": 1000.0, "open": 3001.0, "high": 3006.6, "low": 2995.4, "close": 3000.0}]
    out = _structural({"direction": "BUY", "a3_preview_entry_price": 3001.0, "iceberg_trade_sweep_low": 2996.0}, bars)

    assert out["a3_structural_realized_outcome_1h"] == "AMBIGUOUS_BOTH_HIT"
    assert out["a3_structural_realized_r_proxy_1h"] == round(-1.0 - out["a3_structural_fee_share_r"], 8)


def test_after_a2_structural_gate_blocks_raw_structural_fields():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "a2_book_depth_state": "BOOK_DEPTH_MISSING",
            "a3_preview_breakout_raw_flag": True,
            "a3_structural_realized_r_proxy_1h": 0.8,
            "a3_structural_realized_outcome_1h": "TARGET_1R_FIRST",
        }
    )

    assert row["a3_preview_breakout_after_a2_flag"] is False
    assert row["a3_after_a2_structural_realized_r_proxy_1h"] == 0.0
    assert row["a3_after_a2_structural_realized_outcome_1h"] == "NO_BREAKOUT"


def test_after_a2_structural_gate_copies_raw_fields_when_ready():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "ALIGNED",
            "a3_preview_breakout_raw_flag": True,
            "a3_preview_realized_r_proxy_1h": 0.2,
            "a3_preview_fee_share_r": 0.6,
            "a3_structural_stop_price": 2995.5,
            "a3_structural_risk_u": 5.5,
            "a3_structural_fee_share_r": 0.3,
            "a3_structural_realized_r_proxy_15m": 0.4,
            "a3_structural_realized_r_proxy_1h": 0.7,
            "a3_structural_realized_outcome_15m": "CLOSE_EXIT",
            "a3_structural_realized_outcome_1h": "TARGET_1R_FIRST",
            "a3_structural_realized_r_proxy_1h_bucket": "STRUCT_REALIZED_0_TO_1R",
        }
    )

    assert row["a3_preview_breakout_after_a2_flag"] is True
    assert row["a3_after_a2_structural_stop_price"] == 2995.5
    assert row["a3_after_a2_structural_risk_u"] == 5.5
    assert row["a3_after_a2_structural_fee_share_r"] == 0.3
    assert row["a3_after_a2_structural_realized_r_proxy_15m"] == 0.4
    assert row["a3_after_a2_structural_realized_r_proxy_1h"] == 0.7
    assert row["a3_after_a2_structural_realized_outcome_1h"] == "TARGET_1R_FIRST"
    assert row["a3_after_a2_structural_realized_r_proxy_1h_bucket"] == "STRUCT_REALIZED_0_TO_1R"


def test_after_a2_structural_improved_flag_compares_against_v1():
    row = _classify(
        {
            "a2_pre_pool_eligible": True,
            "has_clean_hold": True,
            "a2_book_depth_state": "BOOK_DEPTH_VALID",
            "a2_context_alignment": "ALIGNED",
            "a3_preview_breakout_raw_flag": True,
            "a3_preview_realized_r_proxy_1h": 0.2,
            "a3_preview_fee_share_r": 0.6,
            "a3_structural_fee_share_r": 0.3,
            "a3_structural_realized_r_proxy_1h": 0.5,
        }
    )

    assert row["a3_after_a2_structural_improved_flag"] is True
    assert row["a3_after_a2_structural_vs_v1_delta_r_1h"] == 0.3
    assert row["a3_after_a2_structural_fee_share_delta_r"] == -0.3


def test_aggregator_sweep_prefers_iceberg_pies_over_all_pies():
    rows = ZoneTruthAggregator().aggregate(
        [
            {
                "record_type": "candidate_finalized",
                "event_key": "iceberg",
                "zone_id": "zone-1",
                "direction": "BUY",
                "result": "ICEBERG",
                "settle_ts": 1000,
                "settle_price": 3000,
                "zone_lower": 2990,
                "zone_upper": 3010,
                "min_trade_price": 2996,
                "max_trade_price": 3004,
                "truth_score": {"truth_score_total": 80, "truth_label": "HIGH_CONFIDENCE_ICEBERG"},
            },
            {
                "record_type": "candidate_finalized",
                "event_key": "ignore",
                "zone_id": "zone-1",
                "direction": "BUY",
                "result": "IGNORE",
                "settle_ts": 1001,
                "settle_price": 3000,
                "zone_lower": 2990,
                "zone_upper": 3010,
                "min_trade_price": 2980,
                "max_trade_price": 3020,
            },
        ],
        [],
    )

    row = rows[0]
    assert row["trade_sweep_low"] == 2980
    assert row["trade_sweep_high"] == 3020
    assert row["iceberg_trade_sweep_low"] == 2996
    assert row["iceberg_trade_sweep_high"] == 3004
    assert row["structural_proxy_available"] is True
    assert row["structural_proxy_reason"] == "ICEBERG_PIE_SWEEP"
