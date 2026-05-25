import csv

from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_100_000.0


def _phase1():
    return [
        {
            "record_type": "candidate_finalized",
            "event_key": "pie-1",
            "zone_id": "zone-a2",
            "symbol": "ETH-USDT-SWAP",
            "direction": "BUY",
            "result": "ICEBERG",
            "settle_ts": BASE_TS,
            "settle_price": 100.0,
            "zone_lower": 99.0,
            "zone_upper": 101.0,
            "active_notional": 1_500_000,
            "hidden_volume": 2_000_000,
            "absorption_rate": 0.7,
            "truth_score": {"truth_score_total": 80, "truth_label": "HIGH_CONFIDENCE_ICEBERG"},
        }
    ]


def _reactions(reaction_ts):
    return [
        {
            "zone_id": "zone-a2",
            "direction": "BUY",
            "frozen_ts": BASE_TS,
            "reaction_event_ts": reaction_ts,
            "reaction_event_ts_valid": True,
            "frozen_low": 99.0,
            "frozen_high": 101.0,
            "reaction_type": "CLEAN_HOLD",
            "a1_reaction_type": "CLEAN_HOLD",
            "relevant_book_depth_available": True,
        }
    ]


def _klines():
    return [
        {
            "timestamp": BASE_TS + i * 60,
            "open": 100.0 + i * 0.1,
            "high": 101.0 + i * 0.1,
            "low": 99.0 + i * 0.1,
            "close": 100.5 + i * 0.1,
            "volume": 10,
        }
        for i in range(90)
    ]


def test_zone_truth_export_includes_a2_fields_and_group_reports(tmp_path):
    out = tmp_path / "zone_truth"
    summary = ZoneTruthAnalyzer().export(_phase1(), _reactions(BASE_TS + 60), _klines(), out)

    with (out / "zone_truth_events.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    expected_fields = {
        "a2_state",
        "a2_book_depth_state",
        "a2_context_alignment",
        "strong_a1_tier",
        "a2_validated_candidate_flag",
        "reaction_event_ts_valid",
        "reaction_event_ts_outside_kline_range",
    }
    assert expected_fields.issubset(rows[0])
    assert rows[0]["a2_state"] == "A2_CLEAN_HOLD"
    assert rows[0]["a2_book_depth_state"] == "BOOK_DEPTH_VALID"
    assert rows[0]["strong_a1_tier"] == "STRONG_A1_RAW"
    assert rows[0]["a2_validated_candidate_flag"] == "True"
    assert summary["a2_clean_hold_count"] == 1
    assert summary["reaction_event_ts_invalid_count"] == 0

    for name in (
        "zone_truth_by_a2_state.csv",
        "zone_truth_by_a2_book_depth_state.csv",
        "zone_truth_by_a2_context_alignment.csv",
        "zone_truth_by_strong_a1_tier.csv",
        "zone_truth_by_a2_validated_candidate.csv",
    ):
        assert (out / name).exists()


def test_zone_truth_marks_reaction_event_outside_kline_range(tmp_path):
    out = tmp_path / "zone_truth"
    summary = ZoneTruthAnalyzer().export(_phase1(), _reactions(BASE_TS + 999_999), _klines(), out)

    with (out / "zone_truth_events.csv").open("r", encoding="utf-8", newline="") as f:
        row = next(csv.DictReader(f))

    assert row["reaction_event_ts_outside_kline_range"] == "True"
    assert row["is_complete_15m"] == "False"
    assert row["mfe_15m_u"] == "0.0"
    assert summary["reaction_events_outside_kline_range_count"] == 1
