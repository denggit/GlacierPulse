#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json

from src.research.phase1_truth.analyzer import Phase1TruthAnalyzer


def _record(
    key,
    record_type,
    result="ICEBERG",
    score=80,
    active=600000,
    hidden=1200000,
    absorption=0.8,
    label="HIGH_CONFIDENCE_ICEBERG",
):
    return {
        "record_type": record_type,
        "event_key": key,
        "result": result,
        "direction": "BUY",
        "quality": "HIGH",
        "behavior": "ICEBERG_ABSORPTION",
        "session_tag": "US_OPEN",
        "active_notional": active,
        "hidden_volume": hidden,
        "absorption_rate": absorption,
        "start_thickness_usdt": 600000,
        "truth_score": {
            "truth_score_total": score,
            "truth_label": label,
            "post_data_coverage": {
                "has_any_post_trade": label != "INSUFFICIENT_POST_DATA",
                "has_5s_trade_data": label != "INSUFFICIENT_POST_DATA",
                "has_30s_trade_data": label != "INSUFFICIENT_POST_DATA",
                "observed_through_5s": label != "INSUFFICIENT_POST_DATA",
                "observed_through_30s": label != "INSUFFICIENT_POST_DATA",
                "observed_through_120s": label != "INSUFFICIENT_POST_DATA",
                "has_5s_trade_window": label != "INSUFFICIENT_POST_DATA",
                "has_30s_trade_window": label != "INSUFFICIENT_POST_DATA",
                "has_120s_observation": label != "INSUFFICIENT_POST_DATA",
                "has_book_recovery_data": label != "INSUFFICIENT_POST_DATA",
                "has_cvd_data": label != "INSUFFICIENT_POST_DATA",
                "has_sweep_data": label != "INSUFFICIENT_POST_DATA",
                "has_post_price_data": label != "INSUFFICIENT_POST_DATA",
            },
        },
    }


def test_analyzer_reads_jsonl_and_uses_finalized_for_core_stats(tmp_path):
    events = tmp_path / "events.jsonl"
    rows = [
        _record("s1", "candidate_settled", score=10),
        _record("f1", "candidate_finalized", "ICEBERG", 90),
        _record("f2", "candidate_finalized", "IGNORE", 85, active=200000, hidden=600000, absorption=0.55),
    ]
    events.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    out = tmp_path / "out"
    summary = Phase1TruthAnalyzer(min_sample=1).analyze_file(events, out)
    assert summary["settled_total"] == 1
    assert summary["finalized_total"] == 2
    assert summary["by_result"]["ICEBERG"]["avg_truth_score"] == 90
    assert (out / "phase1_truth_summary.md").exists()


def test_analyzer_counts_insufficient_post_data(tmp_path):
    finalized = [
        _record("ok", "candidate_finalized", "ICEBERG", 88),
        _record("insufficient", "candidate_finalized", "ICEBERG", 49, label="INSUFFICIENT_POST_DATA"),
    ]
    summary = Phase1TruthAnalyzer(min_sample=1).export(finalized, tmp_path / "out")
    assert summary["insufficient_post_data_total"] == 1
    assert summary["insufficient_post_data_ratio"] > 0
    assert summary["by_result"]["ICEBERG"]["insufficient_post_data_count"] == 1


def test_parameter_grid_outputs_required_fields_and_missed_high_truth(tmp_path):
    finalized = [
        _record("ice", "candidate_finalized", "ICEBERG", 88, active=900000, hidden=2000000, absorption=0.9),
        _record("miss", "candidate_finalized", "IGNORE", 92, active=200000, hidden=600000, absorption=0.55),
    ]
    out = tmp_path / "out"
    Phase1TruthAnalyzer(min_sample=1).export(finalized, out)
    with (out / "phase1_parameter_grid.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert {"selected_count", "avg_truth_score", "pct_truth_ge_80"}.issubset(rows[0])
    assert any(int(row["missed_high_truth_count"]) >= 1 for row in rows)
    assert (out / "phase1_parameter_grid_by_session.csv").exists()
    assert (out / "phase1_dynamic_preview_summary.json").exists()


def test_parameter_grid_outputs_insufficient_post_data_ratio(tmp_path):
    finalized = [
        _record("ice", "candidate_finalized", "ICEBERG", 88, active=900000, hidden=2000000, absorption=0.9),
        _record(
            "insufficient",
            "candidate_finalized",
            "ICEBERG",
            49,
            active=900000,
            hidden=2000000,
            absorption=0.9,
            label="INSUFFICIENT_POST_DATA",
        ),
    ]
    out = tmp_path / "out"
    Phase1TruthAnalyzer(min_sample=1).export(finalized, out)
    with (out / "phase1_parameter_grid.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert "selected_insufficient_post_data_count" in rows[0]
    assert "selected_insufficient_post_data_ratio" in rows[0]
    assert any(float(row["selected_insufficient_post_data_ratio"]) > 0 for row in rows)


def test_parameter_grid_insufficient_coverage_label():
    analyzer = Phase1TruthAnalyzer(min_sample=30)
    label = analyzer.parameter_score_label(
        {
            "selected_count": 40,
            "selected_insufficient_post_data_ratio": 0.45,
            "pct_truth_ge_80": 0.90,
            "pct_truth_ge_65": 0.90,
            "avg_truth_score": 90,
        }
    )
    assert label == "INSUFFICIENT_COVERAGE"


def test_high_quality_param_set_requires_low_insufficient_ratio():
    analyzer = Phase1TruthAnalyzer(min_sample=30)
    label = analyzer.parameter_score_label(
        {
            "selected_count": 40,
            "selected_insufficient_post_data_ratio": 0.30,
            "pct_truth_ge_80": 0.90,
            "pct_truth_ge_65": 0.90,
            "avg_truth_score": 90,
        }
    )
    assert label != "HIGH_QUALITY_PARAM_SET"
