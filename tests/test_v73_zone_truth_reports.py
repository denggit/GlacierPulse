#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
from pathlib import Path

from src.research.zone_truth.analyzer import ZoneTruthAnalyzer


BASE_TS = 1_779_373_200


def _runtime_ticks() -> list[dict[str, float]]:
    return [
        {"ts": BASE_TS + 1, "last_price": 100.0, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 2, "last_price": 100.1, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 3, "last_price": 100.2, "active_buy_notional_3s": 10_000, "active_sell_notional_3s": 10_000, "cvd_delta_3s": 0, "price_velocity_u_per_sec": 0.01},
        {"ts": BASE_TS + 4, "last_price": 102.0, "active_buy_notional_3s": 250_000, "active_sell_notional_3s": 20_000, "cvd_delta_3s": 50_000, "price_velocity_u_per_sec": 0.5},
    ]


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
    assert "a3_aggression_quality_future" in fieldnames
    assert "vp24h_a1_vp_setup_rt" in fieldnames
    assert "mfe_15m_u" not in fieldnames
    assert "truth_score_avg" not in fieldnames
    for name in (
        "zone_truth_3a_rt_signals.csv",
        "zone_truth_3a_rt_trades.csv",
        "zone_truth_3a_rt_by_strategy.csv",
        "zone_truth_3a_rt_by_strategy_all_expiry_variants.csv",
        "zone_truth_3a_rt_by_strategy_default_expiry.csv",
        "zone_truth_3a_rt_by_vp_setup.csv",
        "zone_truth_3a_rt_by_expiry.csv",
        "zone_truth_3a_rt_by_target_candidate.csv",
        "zone_truth_3a_rt_summary.json",
        "zone_truth_3a_mvp_trades.csv",
        "zone_truth_3a_mvp_summary.json",
        "zone_truth_3a_mvp_decision.md",
        "zone_truth_3a_mvp_by_vp_setup.csv",
        "zone_truth_3a_mvp_by_a2_quality.csv",
        "zone_truth_by_a3_quality_future_type_v2.csv",
        "zone_truth_by_a3_aggression_type_v2.csv",
        "zone_truth_by_aggression_quality_future_context.csv",
        "zone_truth_by_aggression_quality_context.csv",
    ):
        assert (out / name).exists()
    with (out / "zone_truth_3a_rt_summary.json").open(encoding="utf-8") as handle:
        rt_summary = json.load(handle)
    assert rt_summary["runtime_3a_status"] == "SKIPPED_NO_TRADE_EVENTS"
    assert "runtime_3a_memory_profile" in rt_summary
    assert "default_expiry_trade_count" in rt_summary
    with (out / "zone_truth_3a_mvp_summary.json").open(encoding="utf-8") as handle:
        mvp_summary = json.load(handle)
    assert mvp_summary["strategy_name"] == "MVP_VP_3A_LITE"
    assert mvp_summary["live_readiness_gate"]["status"] == "NOT_READY"
    assert summary["no_future_field_hygiene_version"] == "v7.3.0.no_future_field_registry"
    assert "zone_truth_by_a3_aggression_type_v2.csv" in summary["deprecated_report_aliases"]
    assert "zone_truth_by_aggression_quality_context.csv" in summary["deprecated_report_aliases"]


def test_deprecated_report_aliases_marked(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_aliases"
    summary = ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    assert summary["deprecated_report_aliases"] == [
        "zone_truth_by_a3_aggression_type_v2.csv",
        "zone_truth_by_aggression_quality_context.csv",
    ]
    md = (out / "zone_truth_summary.md").read_text(encoding="utf-8")
    assert "Deprecated Report Aliases" in md
    assert "zone_truth_by_a3_quality_future_type_v2.csv" in md
    assert "zone_truth_by_aggression_quality_future_context.csv" in md


def test_v73_zone_truth_runtime_reports_use_supplied_trade_events(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_rt"
    ticks = _runtime_ticks()
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
    assert "unique_signal_id" in trades[0]
    assert "a2_rt_quality" in trades[0]
    assert "a2_rt_light_ready_for_a3_flag" in trades[0]
    assert "a2_rt_confirmed_ready_for_a3_flag" in trades[0]
    assert trades[0]["entry_ts"] == str(float(BASE_TS + 4))
    assert "trade_blocked_flag" in trades[0]
    assert "ambiguous_flag_sim" in trades[0]
    with (out / "zone_truth_3a_rt_signals.csv").open(encoding="utf-8", newline="") as handle:
        signals = list(csv.DictReader(handle))
    assert "unique_signal_id" in signals[0]
    assert "a2_rt_quality" in signals[0]
    assert "a2_rt_light_ready_for_a3_flag" in signals[0]
    assert "a2_rt_confirmed_ready_for_a3_flag" in signals[0]


def test_mvp_decision_md_is_short(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_mvp_decision"
    ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    lines = (out / "zone_truth_3a_mvp_decision.md").read_text(encoding="utf-8").splitlines()
    assert len(lines) <= 80


def test_mvp_trades_file_exists(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_mvp_trades"
    ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    assert (out / "zone_truth_3a_mvp_trades.csv").exists()


def test_mvp_summary_file_exists(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_mvp_summary"
    ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    assert (out / "zone_truth_3a_mvp_summary.json").exists()


def test_mvp_decision_file_exists(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_mvp_decision_exists"
    ZoneTruthAnalyzer(enable_3a_simulator=False).analyze_files(phase1, reactions, kline, out)
    assert (out / "zone_truth_3a_mvp_decision.md").exists()


def test_default_expiry_sec_plumbed_from_analyzer(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_default_expiry"
    analyzer = ZoneTruthAnalyzer(
        enable_3a_simulator=False,
        a2_rt_max_age_sec=1200,
        a2_rt_expiry_sweep_secs=[180, 900, 1200],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    analyzer.analyze_files(phase1, reactions, kline, out, runtime_events=_runtime_ticks())
    with (out / "zone_truth_3a_rt_summary.json").open(encoding="utf-8") as handle:
        rt_summary = json.load(handle)
    assert rt_summary["default_expiry_sec"] == 1200


def test_by_strategy_uses_default_expiry_not_all_variants(tmp_path):
    phase1, reactions, kline = _inputs(tmp_path)
    out = tmp_path / "out_strategy_scope"
    analyzer = ZoneTruthAnalyzer(
        enable_3a_simulator=False,
        a2_rt_expiry_sweep_secs=[300, 900],
        a2_rt_min_quiet_sec=3,
        a2_rt_min_tick_count=3,
    )
    analyzer.analyze_files(phase1, reactions, kline, out, runtime_events=_runtime_ticks())
    with (out / "zone_truth_3a_rt_by_strategy.csv").open(encoding="utf-8", newline="") as handle:
        by_strategy = list(csv.DictReader(handle))
    with (out / "zone_truth_3a_rt_by_strategy_all_expiry_variants.csv").open(encoding="utf-8", newline="") as handle:
        by_strategy_all = list(csv.DictReader(handle))
    baseline = next(row for row in by_strategy if row["strategy_variant"] == "A_CORE_NO_VP")
    baseline_all = next(row for row in by_strategy_all if row["strategy_variant"] == "A_CORE_NO_VP")
    assert baseline["trade_count"] == "1"
    assert baseline_all["trade_count"] == "2"
