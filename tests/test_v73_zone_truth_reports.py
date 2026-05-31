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
                "active_notional": 1_000_000,
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
    with (out / "zone_truth_3a_rt_summary.json").open(encoding="utf-8") as handle:
        rt_summary = json.load(handle)
    assert rt_summary["runtime_3a_status"] == "SKIPPED_NO_TRADE_EVENTS"
    assert "runtime_3a_memory_profile" in rt_summary
    assert summary["no_future_field_hygiene_version"] == "v7.3.0.no_future_field_registry"


def test_v73_zone_truth_runtime_reports_use_supplied_trade_events(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_rt"
    ticks = [
        {"ts": BASE_TS + 1, "last_price": 100.0, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 2, "last_price": 100.1, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 3, "last_price": 100.2, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 4, "last_price": 102.0, "active_buy_notional_3s": 250_000, "active_sell_notional_3s": 20_000, "cvd_delta_3s": 50_000, "price_velocity_u_per_sec": 0.5},
    ]
    analyzer = ZoneTruthAnalyzer(
        enable_3a_simulator=False,
        a2_rt_expiry_sweep_secs=[900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    analyzer.analyze_files(phase1, reactions, kline, out, runtime_events=ticks)
    with (out / "zone_truth_3a_rt_summary.json").open(encoding="utf-8") as handle:
        rt_summary = json.load(handle)
    assert rt_summary["runtime_3a_status"] == "OK"
    with (out / "zone_truth_3a_rt_trades.csv").open(encoding="utf-8", newline="") as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["entry_ts"] == str(float(BASE_TS + 4))
    assert "trade_blocked_flag" in trades[0]
    assert "ambiguous_flag_sim" in trades[0]
