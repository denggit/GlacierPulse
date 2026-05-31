#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
from pathlib import Path

from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_373_200


def _inputs(root: Path) -> tuple[Path, Path, Path]:
    phase1 = root / "phase1.jsonl"
    phase1.write_text(
        json.dumps(
            {
                "record_type": "candidate_finalized",
                "event_key": "e1",
                "zone_id": "z1",
                "direction": "BUY",
                "result": "ICEBERG",
                "settle_ts": BASE_TS,
                "trigger_ts": BASE_TS,
                "settle_price": 100,
                "zone_lower": 99,
                "zone_upper": 101,
                "truth_score": {"truth_score_total": 80, "truth_label": "HIGH_CONFIDENCE_ICEBERG"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    reactions = root / "a1.jsonl"
    reactions.write_text(
        json.dumps(
            {
                "zone_id": "z1",
                "direction": "BUY",
                "reaction_event_ts": BASE_TS,
                "frozen_ts": BASE_TS,
                "frozen_low": 99,
                "frozen_high": 101,
                "reaction_type": "CLEAN_HOLD",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    kline = root / "kline.csv"
    lines = ["timestamp,open,high,low,close,volume"]
    for i in range(80):
        ts = BASE_TS + i * 60
        lines.append(f"{ts},100,102,99,101,10")
    kline.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return phase1, reactions, kline


def test_v73_zone_truth_outputs_future_offline_and_rt_report_files(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out"
    summary = ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    with (out / "zone_truth_events.csv").open(encoding="utf-8", newline="") as handle:
        fieldnames = csv.DictReader(handle).fieldnames or []
    assert "mfe_15m_u_future" in fieldnames
    assert "truth_score_avg_offline" in fieldnames
    assert "a2_compression_state_future" in fieldnames
    assert "a3_future_ignition_quality" in fieldnames
    assert "vp24h_a1_vp_setup_rt" in fieldnames
    assert "mfe_15m_u" not in fieldnames
    assert "truth_score_avg" not in fieldnames
    for name in (
        "zone_truth_3a_rt_signals.csv",
        "zone_truth_3a_rt_trades.csv",
        "zone_truth_3a_rt_by_strategy.csv",
        "zone_truth_3a_rt_by_vp_setup.csv",
        "zone_truth_3a_rt_by_expiry.csv",
        "zone_truth_3a_rt_by_target_candidate.csv",
        "zone_truth_3a_rt_summary.json",
    ):
        assert (out / name).exists()
    assert summary["no_future_field_hygiene_version"] == "v7.3.0.no_future_field_registry"

