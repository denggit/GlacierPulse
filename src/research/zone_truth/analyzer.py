#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import bisect
import csv
import gc
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.io_utils import normalize_klines, read_csv, read_jsonl, write_csv, write_json
from src.research.a1_edge.schema import parse_bool, parse_float
from src.research.context import IcebergContextConfig, build_context_summary_rows, label_iceberg_contexts
from src.research.context.iceberg_context_labels import _aggression_quality_labels
from src.research.field_registry import field_hygiene_summary
from src.research.no_future_audit import audit_report_schema

from .a2_accumulation_v2 import attach_a2_accumulation_path_v2
from .a3_quality_future_v2 import attach_a3_quality_future_v2
from .aggregator import ZoneTruthAggregator
from .a2_state import ZoneA2StateClassifier
from .combo_matrix import COMBO_KEY_FIELDS, COMBO_METRIC_FIELDS, ComboStatsAccumulator, TradeGroupStatsAccumulator, bad_combos, build_combo_matrix, combo_summary, group_stats, is_valid_simulated_trade, top_combos
from .forward import ZoneForwardMetricsCalculator
from .market_context import ZoneMarketContextCalculator
from .models import SOURCE_SYNTHETIC, ZONE_TRUTH_MAIN_EVENT_WITH_CONTEXT_FIELDS
from .trade_simulator import SimulatorStats, iter_3a_proxy_trades
from src.research.runtime_three_a.three_a_strategy_backtest import build_runtime_strategy_reports


FEE_AWARE_GROUP_METRIC_FIELDS = [
    "a2_net_mfe_15m_r_future_avg",
    "a2_net_mfe_1h_r_future_avg",
    "a2_net_mae_15m_r_future_avg",
    "a2_net_mae_1h_r_future_avg",
    "a2_net_hit_1r_15m_future_rate",
    "a2_net_hit_1r_1h_future_rate",
    "a3_future_net_mfe_15m_r_avg",
    "a3_future_net_mfe_1h_r_avg",
    "a3_future_net_mae_15m_r_avg",
    "a3_future_net_mae_1h_r_avg",
    "a3_future_realized_r_proxy_15m_avg",
    "a3_future_realized_r_proxy_1h_avg",
    "a3_future_fee_positive_1h_rate",
    "a3_future_target_1r_first_1h_rate",
    "a3_future_stop_1r_first_1h_rate",
    "a3_future_ambiguous_both_hit_1h_rate",
    "a3_after_a2_future_net_mfe_1h_r_avg",
    "a3_after_a2_future_realized_r_proxy_1h_avg",
    "a3_after_a2_future_fee_positive_1h_rate",
    "a3_after_a2_future_target_1r_first_1h_rate",
    "a3_after_a2_future_stop_1r_first_1h_rate",
    "a3_after_a2_future_ambiguous_both_hit_1h_rate",
    "a3_structural_risk_u_avg",
    "a3_structural_fee_share_r_avg",
    "a3_structural_realized_r_proxy_1h_avg",
    "a3_structural_fee_positive_1h_rate",
    "a3_after_a2_future_structural_risk_u_avg",
    "a3_after_a2_future_structural_fee_share_r_avg",
    "a3_after_a2_future_structural_realized_r_proxy_1h_avg",
    "a3_after_a2_future_structural_fee_positive_1h_rate",
    "a3_after_a2_future_structural_improved_rate",
    "a3_after_a2_future_structural_vs_v1_delta_r_1h_avg",
    "a3_after_a2_future_structural_fee_share_delta_r_avg",
]

V7_SIMULATED_TRADE_FIELDS = [
    "zone_id", "symbol", "direction",
    "a1_primary_evidence_type", "a1_evidence_types", "a1_strength_tier", "a1_best_horizon",
    "a2_accumulation_path_v2", "a3_quality_future_type_v2", "market_context_bucket",
    "entry_model", "stop_model", "target_r", "entry_ts", "entry_bar_ts", "entry_price_source",
    "entry_price", "stop_price", "target_price",
    "stop_basis_reason", "risk_u", "fee_share_r", "realized_r_1h_sim", "realized_outcome_1h_sim", "target_first_flag_sim",
    "stop_first_flag_sim", "ambiguous_flag_sim", "complete_flag_sim", "mfe_r_1h_sim", "mae_r_1h_sim",
]

V73_RT_SIGNAL_FIELDS = [
    "zone_id", "direction", "a1_ts", "a1_price", "a1_vp_setup_rt", "a2_rt_ready_ts",
    "a2_rt_expiry_sec", "entry_ts", "entry_price", "entry_reason",
    "condition_available_ts_max", "uses_future_field_flag", "future_field_names",
]

V73_RT_TRADE_FIELDS = [
    "trade_id", "zone_id", "direction", "a1_ts", "a1_price", "a1_vp_setup_rt",
    "a1_vp_context_prefix", "a2_rt_start_ts", "a2_rt_ready_ts", "a2_rt_expiry_sec",
    "a2_rt_state", "entry_ts", "entry_price", "entry_reason", "condition_available_ts_max",
    "condition_source", "uses_future_field_flag", "future_field_names", "trade_blocked_flag", "trade_blocked_reason",
    "stop_model", "stop_price", "risk_u",
    "stop_reason", "fee_share_r", "target_model", "target_price", "target_r", "exit_ts",
    "exit_price", "exit_reason", "realized_r_sim", "mfe_r_future", "mae_r_future",
    "target_first_flag_sim", "stop_first_flag_sim", "ambiguous_flag_sim", "complete_flag_sim",
    "a3_quality_future_type_v2", "a3_quality_future_score_v2",
    "target_fixed_2r_price_sim", "target_poc_price_rt", "target_hvn_directional_price_rt",
    "target_opposite_value_edge_price_rt", "target_next_lvn_price_rt", "target_poc_r_rt",
    "target_hvn_r_rt", "target_opposite_value_edge_r_rt", "target_next_lvn_r_rt",
    "target_hybrid_min_2r_available_rt", "target_hybrid_min_2r_price_rt", "target_hybrid_min_2r_r_rt",
]

V73_RT_SUMMARY_METRIC_FIELDS = [
    "trade_count", "avg_realized_r_sim", "median_realized_r_sim", "win_rate",
    "profit_factor", "max_drawdown_r", "max_consecutive_losses", "trades_per_day",
    "fee_share_r_avg", "long_short_split", "a1_vp_setup_split", "a2_expiry_split",
    "target_candidate_split",
]

V73_RT_BY_STRATEGY_FIELDS = ["strategy_variant"] + V73_RT_SUMMARY_METRIC_FIELDS
V73_RT_BY_STRATEGY_ALL_EXPIRY_FIELDS = V73_RT_BY_STRATEGY_FIELDS
V73_RT_BY_STRATEGY_DEFAULT_EXPIRY_FIELDS = V73_RT_BY_STRATEGY_FIELDS
V73_RT_BY_VP_SETUP_FIELDS = ["a1_vp_setup_rt"] + V73_RT_SUMMARY_METRIC_FIELDS
V73_RT_BY_EXPIRY_FIELDS = ["expiry_sec"] + V73_RT_SUMMARY_METRIC_FIELDS + [
    "expired_count", "invalidated_count", "a3_triggered_count",
]
V73_RT_BY_TARGET_CANDIDATE_FIELDS = ["target_model"] + V73_RT_SUMMARY_METRIC_FIELDS

GROUP_METRIC_FIELDS = [
    "count",
    "truth_score_avg_offline",
    "truth_score_max_offline_avg",
    "truth_ge65_offline_avg",
    "truth_ge80_offline_avg",
    "mfe_15m_future_avg",
    "mae_15m_future_avg",
    "mfe_15m_future_complete_avg",
    "mae_15m_future_complete_avg",
    "mfe_1h_future_avg",
    "mae_1h_future_avg",
    "mfe_1h_future_complete_avg",
    "mae_1h_future_complete_avg",
    "mfe_4h_future_avg",
    "mae_4h_future_avg",
    "mfe_4h_future_complete_avg",
    "mae_4h_future_complete_avg",
    "complete_15m_future_count",
    "complete_1h_future_count",
    "complete_4h_future_count",
] + FEE_AWARE_GROUP_METRIC_FIELDS

CONTEXT_SUMMARY_METRIC_FIELDS = [
    "zone_count",
    "iceberg_zone_count",
    "avg_truth_score",
    "median_truth_score",
    "a2_pre_pool_count",
    "a2_ready_count",
    "a3_count",
    "avg_mfe_r",
    "avg_mae_r",
    "fee_positive_rate",
    "avg_realized_r_proxy",
    "median_realized_r_proxy",
]

CONTEXT_COMBO_FIELDS = [
    "direction",
    "vp24h_proxy_node_context",
    "vpsession_proxy_node_context",
    "vpsession_reclaim_value_future_flag",
    "vp24h_reclaim_value_future_flag",
    "failed_auction_15m_future_flag",
    "failed_auction_1h_future_flag",
    "order_block_15m_type",
    "order_block_15m_fresh_flag",
    "a3_aggression_quality_future",
]

SHADOW_EVIDENCE_EVENT_FIELDS = [
    "zone_id", "symbol", "direction", "a1_primary_evidence_type", "a1_evidence_types",
    "visible_wall_absorption_flag", "cluster_absorption_flag", "ladder_absorption_flag",
    "failed_wall_flag", "spoofing_withdrawal_flag", "visible_wall_start_depth_usdt",
    "visible_wall_end_depth_usdt", "visible_wall_consumption_ratio", "visible_wall_survival_ratio",
    "visible_wall_withdrawal_excess_ratio", "visible_wall_absorbed_notional_proxy",
    "cluster_best_window_sec", "cluster_best_active_notional", "cluster_best_event_count",
    "cluster_best_price_efficiency", "ladder_level_count", "ladder_core_low", "ladder_core_high",
    "ladder_sweep_extreme", "ladder_absorption_score", "a1_evidence_v2_reason",
]



def write_simulated_trades_streaming(
    path: Path | str,
    trade_iter: Iterable[Mapping[str, Any]],
    fieldnames: Iterable[str],
    combo_accumulator: ComboStatsAccumulator,
    group_accumulators: Mapping[str, TradeGroupStatsAccumulator],
) -> dict[str, Any]:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = list(fieldnames)
    written = 0
    valid = 0
    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for trade in trade_iter:
            writer.writerow({name: trade.get(name, "") for name in fields})
            written += 1
            if is_valid_simulated_trade(trade):
                valid += 1
            combo_accumulator.add(trade)
            for accumulator in group_accumulators.values():
                accumulator.add(trade)
    return {"written_trade_count": written, "valid_trade_count": valid}


def _normalize_simulator_input_scope(value: object) -> str:
    text = str(value or "ICEBERG_ONLY").strip().upper()
    if text in {"ALL", "FULL"}:
        return "ALL"
    return "ICEBERG_ONLY"


def _new_memory_profile() -> dict[str, float]:
    keys = [
        "peak_rss_mb",
        "after_read_inputs_rss_mb",
        "after_aggregate_rss_mb",
        "after_forward_metrics_rss_mb",
        "after_market_context_rss_mb",
        "after_a2_a3_rss_mb",
        "after_context_labels_rss_mb",
        "after_post_event_labels_rss_mb",
        "after_simulator_rss_mb",
        "after_csv_write_rss_mb",
    ]
    return {key: 0.0 for key in keys}


def _peak_rss_mb() -> float:
    try:
        import resource

        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return 0.0
    if sys.platform == "darwin":
        rss /= 1024.0 * 1024.0
    else:
        rss /= 1024.0
    return round(rss, 4)

class ZoneTruthAnalyzer:
    def __init__(
        self,
        price_tolerance_usdt: float = 1.5,
        time_tolerance_sec: float = 300.0,
        windows_sec: Iterable[int] | None = None,
        timezone: str = "Asia/Shanghai",
        enable_context_labels: bool = True,
        vp_bin_size_u: float = 1.0,
        vp_value_area_ratio: float = 0.70,
        enable_3a_simulator: bool | None = None,
        simulator_input_scope: str = "ICEBERG_ONLY",
        simulator_include_unavailable: bool | None = None,
        simulator_max_trades: int | None = None,
        enable_3a_rt_backtest: bool | None = None,
        a2_rt_max_age_sec: float | None = None,
        a2_rt_expiry_sweep_secs: Iterable[int] | None = None,
        a2_rt_min_quiet_sec: float | None = None,
        a2_rt_min_tick_count: int | None = None,
        a3_rt_target_model: str | None = None,
        a3_rt_stop_model: str | None = None,
        a3_rt_next_tick_entry: bool | None = None,
        enable_no_future_audit: bool | None = None,
    ) -> None:
        self.price_tolerance_usdt = float(price_tolerance_usdt)
        self.time_tolerance_sec = float(time_tolerance_sec)
        self.windows_sec = list(windows_sec or [900, 3600, 14400])
        self.timezone = timezone
        self.enable_context_labels = bool(enable_context_labels)
        self.context_config = IcebergContextConfig(
            timezone=timezone,
            vp_bin_size_u=float(vp_bin_size_u),
            vp_value_area_ratio=float(vp_value_area_ratio),
        )
        self.enable_3a_simulator = bool(getattr(cfg, "V7_3A_SIMULATOR_ENABLED", True)) if enable_3a_simulator is None else bool(enable_3a_simulator)
        self.simulator_input_scope = _normalize_simulator_input_scope(simulator_input_scope or getattr(cfg, "V7_3A_SIMULATOR_INPUT_SCOPE", "ICEBERG_ONLY"))
        self.simulator_include_unavailable = bool(getattr(cfg, "V7_3A_SIMULATOR_INCLUDE_UNAVAILABLE", False)) if simulator_include_unavailable is None else bool(simulator_include_unavailable)
        self.simulator_max_trades = max(0, int(getattr(cfg, "V7_3A_SIMULATOR_MAX_TRADES", 0) if simulator_max_trades is None else simulator_max_trades))
        self.enable_3a_rt_backtest = bool(getattr(cfg, "V7_3A_RT_ENABLED", True)) if enable_3a_rt_backtest is None else bool(enable_3a_rt_backtest)
        self.a2_rt_max_age_sec = float(getattr(cfg, "A2_RT_MAX_AGE_SEC", 900.0) if a2_rt_max_age_sec is None else a2_rt_max_age_sec)
        self.a2_rt_expiry_sweep_secs = [int(x) for x in (a2_rt_expiry_sweep_secs or getattr(cfg, "A2_RT_EXPIRY_SWEEP_SECS", [180, 300, 600, 900, 1200, 1800]))]
        if int(self.a2_rt_max_age_sec) not in self.a2_rt_expiry_sweep_secs:
            self.a2_rt_expiry_sweep_secs.append(int(self.a2_rt_max_age_sec))
        self.a2_rt_min_quiet_sec = float(getattr(cfg, "A2_RT_MIN_QUIET_SEC", 3.0) if a2_rt_min_quiet_sec is None else a2_rt_min_quiet_sec)
        self.a2_rt_min_tick_count = int(getattr(cfg, "A2_RT_MIN_TICK_COUNT", 20) if a2_rt_min_tick_count is None else a2_rt_min_tick_count)
        self.a3_rt_target_model = str(a3_rt_target_model or getattr(cfg, "V7_3A_RT_TARGET_MODEL", "TARGET_FIXED_2R"))
        self.a3_rt_stop_model = str(a3_rt_stop_model or getattr(cfg, "V7_3A_RT_STOP_MODEL", "STOP_STRUCTURAL_ZONE_V2"))
        self.a3_rt_next_tick_entry = bool(getattr(cfg, "V7_3A_RT_NEXT_TICK_ENTRY", False)) if a3_rt_next_tick_entry is None else bool(a3_rt_next_tick_entry)
        self.enable_no_future_audit = bool(getattr(cfg, "V7_3A_RT_ENABLE_NO_FUTURE_AUDIT", True)) if enable_no_future_audit is None else bool(enable_no_future_audit)

    def analyze_files(
        self,
        phase1_candidates: str | Path,
        a1_reactions: str | Path,
        kline: str | Path | None,
        out_dir: str | Path,
        runtime_events: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self.export(
            read_jsonl(phase1_candidates),
            read_jsonl(a1_reactions),
            read_csv(kline) if kline else [],
            out_dir,
            runtime_events=runtime_events,
        )

    def export(
        self,
        phase1_records: Iterable[Mapping[str, Any]],
        reaction_records: Iterable[Mapping[str, Any]],
        kline_records: Iterable[Mapping[str, Any]],
        out_dir: str | Path,
        runtime_events: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        memory_profile = _new_memory_profile()
        kline_records = list(kline_records or [])
        memory_profile["after_read_inputs_rss_mb"] = _peak_rss_mb()
        aggregator = ZoneTruthAggregator(
            price_tolerance_usdt=self.price_tolerance_usdt,
            time_tolerance_sec=self.time_tolerance_sec,
            timezone=self.timezone,
        )
        rows = aggregator.aggregate(phase1_records, reaction_records)
        memory_profile["after_aggregate_rss_mb"] = _peak_rss_mb()
        phase1_records = None
        reaction_records = None
        gc.collect()
        rows = ZoneForwardMetricsCalculator(self.windows_sec, kline_timezone=self.timezone).attach_forward_metrics(rows, kline_records)
        memory_profile["after_forward_metrics_rss_mb"] = _peak_rss_mb()
        rows = ZoneMarketContextCalculator(kline_timezone=self.timezone).attach_market_context(rows, kline_records)
        memory_profile["after_market_context_rss_mb"] = _peak_rss_mb()
        rows = ZoneA2StateClassifier().attach_a2_state(rows)
        rows = [attach_a2_accumulation_path_v2(row) for row in rows]
        rows = [attach_a3_quality_future_v2(row) for row in rows]
        memory_profile["after_a2_a3_rss_mb"] = _peak_rss_mb()
        rows = self.attach_context_labels(rows, kline_records)
        memory_profile["after_context_labels_rss_mb"] = _peak_rss_mb()
        normalized_bars = normalize_klines(kline_records, kline_timezone=self.timezone) if kline_records else []
        kline_records = None
        gc.collect()
        rows = self.attach_post_event_context_labels(rows, normalized_bars)
        memory_profile["after_post_event_labels_rss_mb"] = _peak_rss_mb()
        iceberg_rows = self.iceberg_context_rows(rows)
        simulator_rows = rows if self.simulator_input_scope == "ALL" else iceberg_rows
        combo_accumulator = ComboStatsAccumulator()
        group_accumulators = {
            "entry_model": TradeGroupStatsAccumulator("entry_model"),
            "stop_model": TradeGroupStatsAccumulator("stop_model"),
            "target_r": TradeGroupStatsAccumulator("target_r"),
        }
        simulator_stats = SimulatorStats(max_trades=self.simulator_max_trades)
        if self.enable_3a_simulator:
            trade_iter = iter_3a_proxy_trades(
                simulator_rows,
                normalized_bars,
                include_unavailable=self.simulator_include_unavailable,
                max_trades=self.simulator_max_trades,
                stats=simulator_stats,
            )
        else:
            trade_iter = iter(())
        stream_summary = write_simulated_trades_streaming(
            out / "zone_truth_3a_simulated_trades.csv",
            trade_iter,
            V7_SIMULATED_TRADE_FIELDS,
            combo_accumulator,
            group_accumulators,
        )
        memory_profile["after_simulator_rss_mb"] = _peak_rss_mb()
        combo_matrix_rows = combo_accumulator.to_rows()
        top_combo_rows = top_combos(combo_matrix_rows)
        bad_combo_rows = bad_combos(combo_matrix_rows)
        runtime_3a_memory_profile = {
            "before_runtime_events_rss_mb": _peak_rss_mb(),
        }
        rt_reports = build_runtime_strategy_reports(
            iceberg_rows if self.enable_3a_rt_backtest else [],
            normalized_bars,
            trade_events=runtime_events,
            expiry_secs=self.a2_rt_expiry_sweep_secs,
            stop_model=self.a3_rt_stop_model,
            target_model=self.a3_rt_target_model,
            next_tick_entry=self.a3_rt_next_tick_entry,
            enable_audit=self.enable_no_future_audit,
            a2_rt_max_age_sec=self.a2_rt_max_age_sec,
            a2_rt_min_quiet_sec=self.a2_rt_min_quiet_sec,
            a2_rt_min_tick_count=self.a2_rt_min_tick_count,
            default_expiry_sec=int(self.a2_rt_max_age_sec),
        )
        runtime_3a_memory_profile.update(rt_reports["summary"].get("runtime_3a_memory_profile", {}))
        runtime_3a_memory_profile["after_runtime_engine_rss_mb"] = _peak_rss_mb()
        rt_reports["summary"]["runtime_3a_memory_profile"] = runtime_3a_memory_profile
        write_csv(out / "zone_truth_events.csv", rows, ZONE_TRUTH_MAIN_EVENT_WITH_CONTEXT_FIELDS)
        write_csv(out / "zone_truth_by_reaction.csv", self.group_rows(rows, "reaction_type"), ["reaction_type"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_final_reaction.csv", self.group_rows(rows, "final_reaction_type"), ["final_reaction_type"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_direction.csv", self.group_rows(rows, "direction"), ["direction"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_session.csv", self.group_rows(rows, "session_tag"), ["session_tag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_truth_bucket.csv", self.group_by_truth_bucket(rows), ["truth_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_pre_pool.csv", self.group_rows(rows, "a2_pre_pool_eligible"), ["a2_pre_pool_eligible"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_state.csv", self.group_rows(rows, "a2_state"), ["a2_state"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_book_depth_state.csv", self.group_rows(rows, "a2_book_depth_state"), ["a2_book_depth_state"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_context_alignment.csv", self.group_rows(rows, "a2_context_alignment"), ["a2_context_alignment"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_strong_a1_tier.csv", self.group_rows(rows, "strong_a1_tier"), ["strong_a1_tier"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_validated_candidate.csv", self.group_rows(rows, "a2_validated_candidate_flag"), ["a2_validated_candidate_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_observe_priority.csv", self.group_rows(rows, "a2_observe_priority"), ["a2_observe_priority"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_risk_tier.csv", self.group_rows(rows, "a2_risk_tier"), ["a2_risk_tier"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_block_reason.csv", self.group_rows(rows, "a2_block_reason"), ["a2_block_reason"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_sweep_reclaim_quality.csv", self.group_rows(rows, "a2_sweep_reclaim_quality"), ["a2_sweep_reclaim_quality"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_compression_state.csv", self.group_rows(rows, "a2_compression_state_future"), ["a2_compression_state_future"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_ready_for_a3_watch.csv", self.group_rows(rows, "a2_ready_for_a3_watch_flag"), ["a2_ready_for_a3_watch_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_watch_priority.csv", self.group_rows(rows, "a3_watch_priority"), ["a3_watch_priority"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_breakout_after_a2.csv", self.group_rows(rows, "a3_future_breakout_after_a2_flag"), ["a3_future_breakout_after_a2_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_latency_bucket.csv", self.group_rows(rows, "a3_future_latency_bucket"), ["a3_future_latency_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_ignition_quality.csv", self.group_rows(rows, "a3_future_ignition_quality"), ["a3_future_ignition_quality"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_pre_ignition_compression_state.csv", self.group_rows(rows, "a2_pre_ignition_compression_state_future"), ["a2_pre_ignition_compression_state_future"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_realized_outcome_15m.csv", self.group_rows(rows, "a3_future_realized_outcome_15m"), ["a3_future_realized_outcome_15m"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_realized_outcome_1h.csv", self.group_rows(rows, "a3_future_realized_outcome_1h"), ["a3_future_realized_outcome_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_net_mfe_1h_bucket.csv", self.group_rows(rows, "a3_future_net_mfe_1h_bucket"), ["a3_future_net_mfe_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_future_realized_r_proxy_1h_bucket.csv", self.group_rows(rows, "a3_future_realized_r_proxy_1h_bucket"), ["a3_future_realized_r_proxy_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_realized_outcome_1h.csv", self.group_rows(rows, "a3_after_a2_future_realized_outcome_1h"), ["a3_after_a2_future_realized_outcome_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_net_mfe_1h_bucket.csv", self.group_rows(rows, "a3_after_a2_future_net_mfe_1h_bucket"), ["a3_after_a2_future_net_mfe_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_realized_r_proxy_1h_bucket.csv", self.group_rows(rows, "a3_after_a2_future_realized_r_proxy_1h_bucket"), ["a3_after_a2_future_realized_r_proxy_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_structural_proxy_reason.csv", self.group_rows(rows, "structural_proxy_reason"), ["structural_proxy_reason"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_structural_realized_outcome_1h.csv", self.group_rows(rows, "a3_structural_realized_outcome_1h_future"), ["a3_structural_realized_outcome_1h_future"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_structural_realized_r_proxy_1h_bucket.csv", self.group_rows(rows, "a3_structural_realized_r_proxy_1h_bucket_future"), ["a3_structural_realized_r_proxy_1h_bucket_future"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_structural_realized_outcome_1h.csv", self.group_rows(rows, "a3_after_a2_future_structural_realized_outcome_1h"), ["a3_after_a2_future_structural_realized_outcome_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_structural_realized_r_proxy_1h_bucket.csv", self.group_rows(rows, "a3_after_a2_future_structural_realized_r_proxy_1h_bucket"), ["a3_after_a2_future_structural_realized_r_proxy_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_after_a2_structural_improved.csv", self.group_rows(rows, "a3_after_a2_future_structural_improved_flag"), ["a3_after_a2_future_structural_improved_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_shadow_evidence.csv", self.group_rows(rows, "a1_evidence_types"), ["a1_evidence_types"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_shadow_evidence_events.csv", self.shadow_evidence_rows(rows), SHADOW_EVIDENCE_EVENT_FIELDS)
        write_csv(out / "zone_truth_by_a2_accumulation_path_v2.csv", self.group_rows(rows, "a2_accumulation_path_v2"), ["a2_accumulation_path_v2"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_quality_future_type_v2.csv", self.group_rows(rows, "a3_quality_future_type_v2"), ["a3_quality_future_type_v2"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_aggression_type_v2.csv", self.group_rows(rows, "a3_quality_future_type_v2"), ["a3_quality_future_type_v2"] + GROUP_METRIC_FIELDS)
        write_csv(
            out / "zone_truth_by_boll_context.csv",
            build_context_summary_rows(iceberg_rows, ["direction", "boll_15m_position", "boll_1h_position"]),
            ["direction", "boll_15m_position", "boll_1h_position"] + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(
            out / "zone_truth_by_vp_context.csv",
            build_context_summary_rows(iceberg_rows, ["direction", "vp24h_proxy_location", "vp4h_proxy_location", "vp1h_proxy_location", "vpsession_proxy_location"]),
            ["direction", "vp24h_proxy_location", "vp4h_proxy_location", "vp1h_proxy_location", "vpsession_proxy_location"] + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(
            out / "zone_truth_by_local_structure_context.csv",
            build_context_summary_rows(iceberg_rows, ["direction", "near_local_15m_low_flag", "near_local_15m_high_flag", "sweep_local_15m_low_flag", "sweep_local_15m_high_flag", "near_local_1h_low_flag", "near_local_1h_high_flag"]),
            ["direction", "near_local_15m_low_flag", "near_local_15m_high_flag", "sweep_local_15m_low_flag", "sweep_local_15m_high_flag", "near_local_1h_low_flag", "near_local_1h_high_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(
            out / "zone_truth_by_order_block_context.csv",
            build_context_summary_rows(iceberg_rows, ["direction", "order_block_15m_type", "inside_order_block_15m_flag", "near_order_block_15m_flag", "order_block_1h_type", "inside_order_block_1h_flag", "near_order_block_1h_flag"]),
            ["direction", "order_block_15m_type", "inside_order_block_15m_flag", "near_order_block_15m_flag", "order_block_1h_type", "inside_order_block_1h_flag", "near_order_block_1h_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(
            out / "zone_truth_by_book_liquidity_proxy_context.csv",
            build_context_summary_rows(iceberg_rows, ["direction", "book_blocking_liquidity_proxy_strength", "visible_depth_proxy_flag", "reload_wall_proxy_flag", "passive_absorption_proxy_flag"]),
            ["direction", "book_blocking_liquidity_proxy_strength", "visible_depth_proxy_flag", "reload_wall_proxy_flag", "passive_absorption_proxy_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(
            out / "zone_truth_by_context_combo.csv",
            build_context_summary_rows(iceberg_rows, CONTEXT_COMBO_FIELDS, min_count=5),
            CONTEXT_COMBO_FIELDS + CONTEXT_SUMMARY_METRIC_FIELDS,
        )
        write_csv(out / "zone_truth_by_vp_node_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "vp24h_proxy_node_context", "vpsession_proxy_node_context"]), ["direction", "vp24h_proxy_node_context", "vpsession_proxy_node_context"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_value_edge_reclaim_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "vpsession_value_edge_side", "vpsession_reclaim_value_future_flag", "vp24h_value_edge_side", "vp24h_reclaim_value_future_flag"]), ["direction", "vpsession_value_edge_side", "vpsession_reclaim_value_future_flag", "vp24h_value_edge_side", "vp24h_reclaim_value_future_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_sweep_failed_auction_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "failed_auction_15m_future_flag", "failed_auction_1h_future_flag"]), ["direction", "failed_auction_15m_future_flag", "failed_auction_1h_future_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_aggression_quality_future_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "a3_aggression_quality_future"]), ["direction", "a3_aggression_quality_future"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_aggression_quality_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "a3_aggression_quality_future"]), ["direction", "a3_aggression_quality_future"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_session_context.csv", build_context_summary_rows(iceberg_rows, ["session_utc", "session_bucket", "is_weekend_flag"]), ["session_utc", "session_bucket", "is_weekend_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_ob_quality_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "order_block_15m_type", "order_block_15m_fresh_flag", "order_block_15m_invalidated_flag", "order_block_1h_type", "order_block_1h_fresh_flag", "order_block_1h_invalidated_flag"]), ["direction", "order_block_15m_type", "order_block_15m_fresh_flag", "order_block_15m_invalidated_flag", "order_block_1h_type", "order_block_1h_fresh_flag", "order_block_1h_invalidated_flag"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_poc_risk_context.csv", build_context_summary_rows(iceberg_rows, ["direction", "vp24h_proxy_location", "vp24h_proxy_nearest_node_type", "vpsession_proxy_location", "vpsession_proxy_nearest_node_type"]), ["direction", "vp24h_proxy_location", "vp24h_proxy_nearest_node_type", "vpsession_proxy_location", "vpsession_proxy_nearest_node_type"] + CONTEXT_SUMMARY_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_1h.csv", self.group_rows(rows, "trend_regime_1h"), ["trend_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_4h.csv", self.group_rows(rows, "trend_regime_4h"), ["trend_regime_4h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_enhanced_1h.csv", self.group_rows(rows, "trend_regime_enhanced_1h"), ["trend_regime_enhanced_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_enhanced_4h.csv", self.group_rows(rows, "trend_regime_enhanced_4h"), ["trend_regime_enhanced_4h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_alignment.csv", self.group_rows(rows, "trend_alignment"), ["trend_alignment"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_volume_regime_1h.csv", self.group_rows(rows, "volume_regime_1h"), ["volume_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_volatility_regime_1h.csv", self.group_rows(rows, "volatility_regime_1h"), ["volatility_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_top_cases.csv", self.top_cases(rows), ZONE_TRUTH_MAIN_EVENT_WITH_CONTEXT_FIELDS)
        write_csv(out / "zone_truth_match_quality.csv", self.match_quality(rows, aggregator.unmatched_pie_count), ["match_quality"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_entry_model.csv", group_accumulators["entry_model"].to_rows(), ["entry_model"] + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_stop_model.csv", group_accumulators["stop_model"].to_rows(), ["stop_model"] + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_target_r.csv", group_accumulators["target_r"].to_rows(), ["target_r"] + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_3a_combo_matrix.csv", combo_matrix_rows, COMBO_KEY_FIELDS + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_3a_combo_top.csv", top_combo_rows, COMBO_KEY_FIELDS + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_3a_combo_bad.csv", bad_combo_rows, COMBO_KEY_FIELDS + COMBO_METRIC_FIELDS)
        write_csv(out / "zone_truth_3a_rt_signals.csv", rt_reports["signals"], V73_RT_SIGNAL_FIELDS)
        write_csv(out / "zone_truth_3a_rt_trades.csv", rt_reports["trades"], V73_RT_TRADE_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_strategy.csv", rt_reports["by_strategy"], V73_RT_BY_STRATEGY_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_strategy_all_expiry_variants.csv", rt_reports["by_strategy_all_expiry_variants"], V73_RT_BY_STRATEGY_ALL_EXPIRY_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_strategy_default_expiry.csv", rt_reports["by_strategy_default_expiry"], V73_RT_BY_STRATEGY_DEFAULT_EXPIRY_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_vp_setup.csv", rt_reports["by_vp_setup"], V73_RT_BY_VP_SETUP_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_expiry.csv", rt_reports["by_expiry"], V73_RT_BY_EXPIRY_FIELDS)
        write_csv(out / "zone_truth_3a_rt_by_target_candidate.csv", rt_reports["by_target_candidate"], V73_RT_BY_TARGET_CANDIDATE_FIELDS)
        write_json(out / "zone_truth_3a_rt_summary.json", rt_reports["summary"])
        memory_profile["after_csv_write_rss_mb"] = _peak_rss_mb()
        memory_profile["peak_rss_mb"] = _peak_rss_mb()
        summary = self.summary(rows, aggregator.unmatched_pie_count)
        summary.update(combo_summary(combo_matrix_rows, valid_trade_count=stream_summary["valid_trade_count"]))
        summary.update({
            "v7_enabled": self.enable_3a_simulator,
            "simulator_enabled": self.enable_3a_simulator,
            "simulator_input_scope": self.simulator_input_scope,
            "simulator_input_rows": len(simulator_rows),
            "total_rows": len(rows),
            "iceberg_rows": len(iceberg_rows),
            "simulator_include_unavailable": self.simulator_include_unavailable,
            "simulator_max_trades": self.simulator_max_trades,
            "simulator_capped": simulator_stats.capped,
            "simulator_unavailable_entry_count": simulator_stats.unavailable_entry_count,
            "simulator_unavailable_stop_count": simulator_stats.unavailable_stop_count,
            "simulator_valid_trade_count": simulator_stats.valid_trade_count,
            "simulator_written_trade_count": simulator_stats.written_trade_count,
            "simulator_combo_valid_trade_count": stream_summary["valid_trade_count"],
            "memory_profile": memory_profile,
            "deprecated_report_aliases": {
                "zone_truth_by_a3_aggression_type_v2.csv": "zone_truth_by_a3_quality_future_type_v2.csv",
                "zone_truth_by_aggression_quality_context.csv": "zone_truth_by_aggression_quality_future_context.csv",
                "a3_aggression_quality": "a3_aggression_quality_future",
            },
            **field_hygiene_summary(ZONE_TRUTH_MAIN_EVENT_WITH_CONTEXT_FIELDS),
            "no_future_schema_audit": audit_report_schema(ZONE_TRUTH_MAIN_EVENT_WITH_CONTEXT_FIELDS),
            "runtime_3a_report_summary": rt_reports["summary"],
            "runtime_3a_memory_profile": runtime_3a_memory_profile,
        })
        write_json(out / "summary.json", summary)
        self._write_summary_md(out / "zone_truth_summary.md", summary)
        return summary

    def attach_context_labels(
        self,
        rows: list[Mapping[str, Any]],
        kline_records: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        enriched = [dict(row) for row in rows]
        if not self.enable_context_labels:
            for row in enriched:
                row["context_labels_status"] = "DISABLED"
            return enriched
        kline_list = list(kline_records or [])
        labels_by_key = label_iceberg_contexts(enriched, kline_list, self.context_config)
        for row in enriched:
            key = self._context_join_key(row)
            labels = labels_by_key.get(key)
            if labels:
                row.update(labels)
            elif parse_float(row.get("iceberg_pie_count")) > 0:
                row["context_labels_status"] = "KLINE_UNAVAILABLE" if not kline_list else "CONTEXT_UNMATCHED"
            else:
                row["context_labels_status"] = "NON_ICEBERG_ZONE"
        return enriched


    @staticmethod
    def iceberg_context_rows(rows: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        return [
            row for row in rows
            if parse_float(row.get("iceberg_pie_count")) > 0
            or str(row.get("a1_primary_evidence_type") or "").upper() == "ICEBERG"
        ]


    def attach_post_event_context_labels(self, rows: list[Mapping[str, Any]], bars: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        out = [dict(row) for row in rows]
        close_ts = [float(bar["timestamp"]) + 60.0 for bar in bars]
        for row in out:
            ts = self._row_context_ts(row)
            idx = bisect.bisect_right(close_ts, ts)
            future = bars[idx:idx + int(self.context_config.value_reclaim_lookahead_bars)] if idx < len(bars) else []
            sweep_future = bars[idx:idx + int(self.context_config.sweep_reclaim_lookahead_bars)] if idx < len(bars) else []
            direction = str(row.get("direction") or "").upper()
            price = parse_float(row.get("iceberg_context_price"))
            aggression_bar = future[0] if future else None
            aggression_history = bars[max(0, idx - 20):idx] if aggression_bar else []
            row.update(_aggression_quality_labels(aggression_bar, [*aggression_history, aggression_bar] if aggression_bar else [], direction))
            for prefix in ("vpsession", "vp24h"):
                row.update(self._value_edge_labels(row, future, direction, price, prefix))
            for label, lookback in (("15m", 16), ("1h", 12)):
                row.update(self._sweep_failed_auction_labels(row, sweep_future, direction, price, label, lookback))
            self._attach_quick_return_labels(row, sweep_future, direction, price)
        return out

    def _value_edge_labels(self, row: dict[str, Any], future: list[Mapping[str, Any]], direction: str, price: float, prefix: str) -> dict[str, Any]:
        val = parse_float(row.get(f"{prefix}_proxy_val")); vah = parse_float(row.get(f"{prefix}_proxy_vah"))
        side = "INSIDE_VALUE"
        level = 0.0
        if direction == "BUY" and val > 0 and price > 0 and price < val:
            side = "BELOW_VAL"; level = val
        elif direction == "SELL" and vah > 0 and price > 0 and price > vah:
            side = "ABOVE_VAH"; level = vah
        outside = side in {"BELOW_VAL", "ABOVE_VAH"}
        bars_to = 0; reclaimed = False
        if outside:
            for i, bar in enumerate(future, start=1):
                close = parse_float(bar.get("close"))
                if (direction == "BUY" and close >= level) or (direction == "SELL" and close <= level):
                    bars_to = i; reclaimed = True; break
        return {
            f"{prefix}_value_edge_side": side if val > 0 or vah > 0 else "VP_UNAVAILABLE",
            f"{prefix}_outside_value_flag": outside,
            f"{prefix}_reclaim_value_future_flag": reclaimed,
            f"{prefix}_reject_value_future_flag": False,
            f"{prefix}_bars_to_reclaim_future": bars_to,
            f"{prefix}_reclaim_level": round(level, 8),
        }

    def _sweep_failed_auction_labels(self, row: dict[str, Any], future: list[Mapping[str, Any]], direction: str, price: float, label: str, lookback: int) -> dict[str, Any]:
        low = parse_float(row.get(f"previous_local_{label}_low_{lookback}"))
        high = parse_float(row.get(f"previous_local_{label}_high_{lookback}"))
        atr = parse_float(row.get("a3_structural_risk_u")) or parse_float(row.get("zone_v2_structural_risk_u"))
        swept = False; level = 0.0; magnitude = 0.0
        if direction == "BUY" and low > 0 and price > 0 and price < low:
            swept = True; level = low; magnitude = low - price
        elif direction == "SELL" and high > 0 and price > 0 and price > high:
            swept = True; level = high; magnitude = price - high
        reclaimed = False; bars_to = 0
        if swept:
            for i, bar in enumerate(future, start=1):
                close = parse_float(bar.get("close"))
                if (direction == "BUY" and close >= level) or (direction == "SELL" and close <= level):
                    reclaimed = True; bars_to = i; break
        return {
            f"future_sweep_reclaim_{label}_future_flag": reclaimed,
            f"bars_to_sweep_reclaim_future_{label}": bars_to,
            f"sweep_reclaim_level_{label}": round(level, 8),
            f"failed_auction_{label}_future_flag": reclaimed,
            f"sweep_magnitude_{label}_u": round(magnitude, 8),
            f"sweep_magnitude_{label}_atr": round(magnitude / atr, 8) if atr > 0 else 0.0,
        }

    def _attach_quick_return_labels(self, row: dict[str, Any], future: list[Mapping[str, Any]], direction: str, price: float) -> None:
        if price <= 0 or not future or direction not in {"BUY", "SELL"}:
            return
        failed = any((direction == "BUY" and parse_float(bar.get("close")) < price) or (direction == "SELL" and parse_float(bar.get("close")) > price) for bar in future)
        row["a3_failed_quick_return_future_flag"] = failed
        row["a3_no_quick_return_future_flag"] = not failed

    def shadow_evidence_rows(self, rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [{field: row.get(field, "") for field in SHADOW_EVIDENCE_EVENT_FIELDS} for row in rows if row.get("a1_evidence_types") or parse_bool(row.get("visible_wall_absorption_flag")) or parse_bool(row.get("cluster_absorption_flag")) or parse_bool(row.get("ladder_absorption_flag"))]

    def _row_context_ts(self, row: Mapping[str, Any]) -> float:
        for field in ("settle_ts", "settle_recv_ts", "trigger_ts", "first_iceberg_pie_ts", "best_pie_ts", "first_seen_ts"):
            value = parse_float(row.get(field))
            if value > 0:
                return value
        return 0.0

    def summary(self, rows: list[Mapping[str, Any]], unmatched_pie_count: int = 0) -> dict[str, Any]:
        total = len(rows)
        exact = sum(1 for row in rows if str(row.get("zone_match_method")) == "exact")
        fuzzy = sum(1 for row in rows if str(row.get("zone_match_method")) == "fuzzy")
        synthetic = sum(1 for row in rows if str(row.get("zone_source")) == SOURCE_SYNTHETIC)
        a2_count = sum(1 for row in rows if parse_bool(row.get("a2_pre_pool_eligible")))
        reaction_distribution = dict(Counter(str(row.get("reaction_type") or "UNKNOWN") for row in rows))
        context_status_distribution = dict(Counter(str(row.get("context_labels_status") or "UNKNOWN") for row in rows))
        enhanced_trend_1h_distribution = dict(Counter(str(row.get("trend_regime_enhanced_1h") or "UNKNOWN") for row in rows))
        enhanced_trend_4h_distribution = dict(Counter(str(row.get("trend_regime_enhanced_4h") or "UNKNOWN") for row in rows))
        trend_alignment_distribution = dict(Counter(str(row.get("trend_alignment") or "MIXED_OR_UNKNOWN") for row in rows))
        a2_state_distribution = dict(Counter(str(row.get("a2_state") or "UNKNOWN") for row in rows))
        a2_book_depth_state_distribution = dict(Counter(str(row.get("a2_book_depth_state") or "UNKNOWN") for row in rows))
        a2_context_alignment_distribution = dict(Counter(str(row.get("a2_context_alignment") or "MIXED_OR_UNKNOWN") for row in rows))
        strong_a1_tier_distribution = dict(Counter(str(row.get("strong_a1_tier") or "UNKNOWN") for row in rows))
        a2_observe_priority_distribution = dict(Counter(str(row.get("a2_observe_priority") or "UNKNOWN") for row in rows))
        a2_risk_tier_distribution = dict(Counter(str(row.get("a2_risk_tier") or "UNKNOWN") for row in rows))
        a2_block_reason_distribution = dict(Counter(str(row.get("a2_block_reason") if row.get("a2_block_reason") not in (None, "") else "NONE") for row in rows))
        a2_sweep_reclaim_quality_distribution = dict(Counter(str(row.get("a2_sweep_reclaim_quality") or "UNKNOWN") for row in rows))
        a2_compression_state_distribution = dict(Counter(str(row.get("a2_compression_state_future") or "UNKNOWN") for row in rows))
        a3_watch_priority_distribution = dict(Counter(str(row.get("a3_watch_priority") or "NONE") for row in rows))
        a3_future_latency_bucket_distribution = dict(Counter(str(row.get("a3_future_latency_bucket") or "NO_IGNITION") for row in rows))
        a3_future_ignition_quality_distribution = dict(Counter(str(row.get("a3_future_ignition_quality") or "NO_IGNITION") for row in rows))
        a2_pre_ignition_compression_state_distribution = dict(Counter(str(row.get("a2_pre_ignition_compression_state_future") or "INSUFFICIENT_BARS") for row in rows))
        a3_future_realized_outcome_15m_distribution = dict(Counter(str(row.get("a3_future_realized_outcome_15m") or "NO_BREAKOUT") for row in rows))
        a3_future_realized_outcome_1h_distribution = dict(Counter(str(row.get("a3_future_realized_outcome_1h") or "NO_BREAKOUT") for row in rows))
        a3_after_a2_future_realized_outcome_1h_distribution = dict(Counter(str(row.get("a3_after_a2_future_realized_outcome_1h") or "NO_BREAKOUT") for row in rows))
        structural_proxy_reason_distribution = dict(Counter(str(row.get("structural_proxy_reason") or "UNAVAILABLE") for row in rows))
        structural_positive_count = sum(1 for row in rows if parse_bool(row.get("a3_structural_fee_positive_1h_future")))
        after_a2_structural_positive_count = sum(1 for row in rows if parse_bool(row.get("a3_after_a2_future_structural_fee_positive_1h")))
        after_a2_structural_improved_count = sum(1 for row in rows if parse_bool(row.get("a3_after_a2_future_structural_improved_flag")))
        reaction_rows = [row for row in rows if self._is_reaction_row(row)]
        reaction_rows_without_reaction_event_ts_count = sum(1 for row in reaction_rows if parse_float(row.get("reaction_event_ts")) <= 0)
        reaction_event_ts_invalid_count_on_reaction_rows = sum(
            1 for row in reaction_rows if not parse_bool(row.get("reaction_event_ts_valid"), default=True)
        )
        forward_summary = {
            "15m": self._forward_summary(rows, "15m"),
            "1h": self._forward_summary(rows, "1h"),
            "4h": self._forward_summary(rows, "4h"),
        }
        return {
            "total_zones": total,
            "exact_matched_zones": exact,
            "fuzzy_matched_zones": fuzzy,
            "synthetic_zones": synthetic,
            "unmatched_pie_count": int(unmatched_pie_count),
            "a2_pre_pool_zone_count": a2_count,
            "context_labels_status": self._context_status(rows),
            "context_labels_status_distribution": context_status_distribution,
            "reaction_distribution": reaction_distribution,
            "trend_regime_enhanced_1h_distribution": enhanced_trend_1h_distribution,
            "trend_regime_enhanced_4h_distribution": enhanced_trend_4h_distribution,
            "trend_alignment_distribution": trend_alignment_distribution,
            "a2_state_distribution": a2_state_distribution,
            "a2_book_depth_state_distribution": a2_book_depth_state_distribution,
            "a2_context_alignment_distribution": a2_context_alignment_distribution,
            "strong_a1_tier_distribution": strong_a1_tier_distribution,
            "a2_observe_priority_distribution": a2_observe_priority_distribution,
            "a2_risk_tier_distribution": a2_risk_tier_distribution,
            "a2_block_reason_distribution": a2_block_reason_distribution,
            "a2_sweep_reclaim_quality_distribution": a2_sweep_reclaim_quality_distribution,
            "a2_compression_state_distribution": a2_compression_state_distribution,
            "a3_watch_priority_distribution": a3_watch_priority_distribution,
            "a3_future_latency_bucket_distribution": a3_future_latency_bucket_distribution,
            "a3_future_ignition_quality_distribution": a3_future_ignition_quality_distribution,
            "a2_pre_ignition_compression_state_distribution": a2_pre_ignition_compression_state_distribution,
            "a3_future_realized_outcome_15m_distribution": a3_future_realized_outcome_15m_distribution,
            "a3_future_realized_outcome_1h_distribution": a3_future_realized_outcome_1h_distribution,
            "a3_after_a2_future_realized_outcome_1h_distribution": a3_after_a2_future_realized_outcome_1h_distribution,
            "structural_proxy_available_count": sum(1 for row in rows if parse_bool(row.get("structural_proxy_available"))),
            "structural_proxy_reason_distribution": structural_proxy_reason_distribution,
            "a3_future_strong_ignition_count": sum(1 for row in rows if str(row.get("a3_future_ignition_quality")) == "STRONG_IGNITION"),
            "a3_future_medium_ignition_count": sum(1 for row in rows if str(row.get("a3_future_ignition_quality")) == "MEDIUM_IGNITION"),
            "a3_future_fee_aware_positive_1h_count": sum(1 for row in rows if parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0),
            "a3_future_fee_aware_positive_1h_rate": round(sum(1 for row in rows if parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0) / total, 6) if total else 0.0,
            "a3_watch_high_fee_aware_positive_1h_count": sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH" and parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0),
            "a3_watch_high_fee_aware_positive_1h_rate": round(sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH" and parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0) / max(1, sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH")), 6),
            "a2_ready_a3_breakout_fee_positive_1h_count": sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_future_breakout_after_a2_flag")) and parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0),
            "a2_ready_a3_breakout_fee_positive_1h_rate": round(sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_future_breakout_after_a2_flag")) and parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0) / max(1, sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_future_breakout_after_a2_flag")))), 6),
            "a3_after_a2_future_fee_positive_1h_count": sum(1 for row in rows if parse_bool(row.get("a3_after_a2_future_fee_positive_1h"))),
            "a3_after_a2_future_fee_positive_1h_rate": round(sum(1 for row in rows if parse_bool(row.get("a3_after_a2_future_fee_positive_1h")))/total, 6) if total else 0.0,
            "a3_after_a2_future_realized_r_proxy_1h": self._avg(rows, "a3_after_a2_future_realized_r_proxy_1h"),
            "a3_after_a2_future_realized_r_proxy_1h_avg": self._avg(rows, "a3_after_a2_future_realized_r_proxy_1h"),
            "a3_structural_fee_positive_1h_count": structural_positive_count,
            "a3_structural_fee_positive_1h_rate": round(structural_positive_count / total, 6) if total else 0.0,
            "a3_after_a2_future_structural_fee_positive_1h_count": after_a2_structural_positive_count,
            "a3_after_a2_future_structural_fee_positive_1h_rate": round(after_a2_structural_positive_count / total, 6) if total else 0.0,
            "a3_after_a2_future_structural_improved_count": after_a2_structural_improved_count,
            "a3_after_a2_future_structural_improved_rate": round(after_a2_structural_improved_count / total, 6) if total else 0.0,
            "a3_after_a2_future_structural_realized_r_proxy_1h": self._avg(rows, "a3_after_a2_future_structural_realized_r_proxy_1h"),
            "a3_after_a2_future_structural_realized_r_proxy_1h_avg": self._avg(rows, "a3_after_a2_future_structural_realized_r_proxy_1h"),
            "a3_after_a2_future_structural_vs_v1_delta_r_1h_avg": self._avg(rows, "a3_after_a2_future_structural_vs_v1_delta_r_1h"),
            "a3_after_a2_future_structural_fee_share_delta_r_avg": self._avg(rows, "a3_after_a2_future_structural_fee_share_delta_r"),
            "a2_validated_candidate_count": sum(1 for row in rows if parse_bool(row.get("a2_validated_candidate_flag"))),
            "a2_clean_hold_count": sum(1 for row in rows if parse_bool(row.get("a2_clean_hold_flag"))),
            "a2_failed_reclaim_count": sum(1 for row in rows if parse_bool(row.get("a2_failed_reclaim_flag"))),
            "a2_ready_for_a3_watch_count": sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag"))),
            "a3_future_breakout_after_a2_count": sum(1 for row in rows if parse_bool(row.get("a3_future_breakout_after_a2_flag"))),
            "a2_book_depth_missing_count": sum(1 for row in rows if str(row.get("a2_book_depth_state")) == "BOOK_DEPTH_MISSING"),
            "reaction_events_outside_kline_range_count": sum(1 for row in rows if parse_bool(row.get("reaction_event_ts_outside_kline_range"))),
            "reaction_rows_count": len(reaction_rows),
            "non_reaction_rows_count": total - len(reaction_rows),
            "reaction_rows_without_reaction_event_ts_count": reaction_rows_without_reaction_event_ts_count,
            "reaction_event_ts_invalid_count_on_reaction_rows": reaction_event_ts_invalid_count_on_reaction_rows,
            "reaction_event_ts_invalid_count": reaction_event_ts_invalid_count_on_reaction_rows,
            "forward_metrics": forward_summary,
            "clean_hold_count": self._reaction_contains(rows, "CLEAN_HOLD"),
            "failed_reclaim_count": self._reaction_contains(rows, "FAILED_RECLAIM"),
            "has_clean_hold_count": sum(1 for row in rows if parse_bool(row.get("has_clean_hold"))),
            "has_failed_reclaim_count": sum(1 for row in rows if parse_bool(row.get("has_failed_reclaim"))),
            "truth_ge65_zone_count": sum(1 for row in rows if parse_float(row.get("truth_ge65_count_offline")) > 0),
            "hard_cap_warning_zone_count": sum(1 for row in rows if parse_bool(row.get("has_any_hard_cap"))),
        }

    def group_rows(self, rows: Iterable[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
        groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows or []:
            groups[str(row.get(field) if row.get(field) not in (None, "") else "UNKNOWN")].append(row)
        return [self._group_stats(key, groups[key], output_field=field) for key in sorted(groups)]

    def group_by_truth_bucket(self, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows or []:
            score = parse_float(row.get("truth_score_max_offline", row.get("truth_score_avg_offline")))
            if score < 50:
                bucket = "<50"
            elif score < 65:
                bucket = "50-65"
            elif score < 80:
                bucket = "65-80"
            else:
                bucket = ">=80"
            groups[bucket].append(row)
        order = ["<50", "50-65", "65-80", ">=80"]
        return [self._group_stats(key, groups[key], output_field="truth_bucket") for key in order if key in groups]

    def match_quality(self, rows: list[Mapping[str, Any]], unmatched_pie_count: int = 0) -> list[dict[str, Any]]:
        groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            key = SOURCE_SYNTHETIC if str(row.get("zone_source")) == SOURCE_SYNTHETIC else str(row.get("zone_match_method") or "unmatched")
            groups[key].append(row)
        result = []
        for key in ("exact", "fuzzy", SOURCE_SYNTHETIC, "unmatched"):
            result.append(self._group_stats(key, groups[key], output_field="match_quality"))
        if unmatched_pie_count:
            result[-1]["count"] = int(result[-1].get("count") or 0) + unmatched_pie_count
        return result

    def group_simulated_trades(self, trades: Iterable[Mapping[str, Any]], field: str, mainline_only: bool = True) -> list[dict[str, Any]]:
        groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for trade in trades or []:
            if mainline_only and not is_valid_simulated_trade(trade):
                continue
            if not mainline_only and parse_float(trade.get("target_r")) < 1.0:
                continue
            key = str(trade.get(field) if trade.get(field) not in (None, "") else "UNKNOWN")
            groups[key].append(trade)
        return [{field: key, **group_stats(groups[key])} for key in sorted(groups)]

    def top_cases(self, rows: list[Mapping[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
        return sorted(
            [dict(row) for row in rows],
            key=lambda row: (
                parse_float(row.get("mfe_1h_u_future")),
                parse_float(row.get("truth_score_max_offline")),
                parse_float(row.get("sum_active_notional")),
            ),
            reverse=True,
        )[:limit]

    def _group_stats(self, key: str, rows: list[Mapping[str, Any]], output_field: str = "group") -> dict[str, Any]:
        count = len(rows)
        return {
            output_field: key,
            "count": count,
            "truth_score_avg_offline": self._avg(rows, "truth_score_avg_offline"),
            "truth_score_max_offline_avg": self._avg(rows, "truth_score_max_offline"),
            "truth_ge65_offline_avg": self._avg(rows, "truth_ge65_count_offline"),
            "truth_ge80_offline_avg": self._avg(rows, "truth_ge80_count_offline"),
            "mfe_15m_future_avg": self._avg(rows, "mfe_15m_u_future"),
            "mae_15m_future_avg": self._avg(rows, "mae_15m_u_future"),
            "mfe_15m_future_complete_avg": self._complete_avg(rows, "mfe_15m_u_future", "is_complete_15m_future"),
            "mae_15m_future_complete_avg": self._complete_avg(rows, "mae_15m_u_future", "is_complete_15m_future"),
            "mfe_1h_future_avg": self._avg(rows, "mfe_1h_u_future"),
            "mae_1h_future_avg": self._avg(rows, "mae_1h_u_future"),
            "mfe_1h_future_complete_avg": self._complete_avg(rows, "mfe_1h_u_future", "is_complete_1h_future"),
            "mae_1h_future_complete_avg": self._complete_avg(rows, "mae_1h_u_future", "is_complete_1h_future"),
            "mfe_4h_future_avg": self._avg(rows, "mfe_4h_u_future"),
            "mae_4h_future_avg": self._avg(rows, "mae_4h_u_future"),
            "mfe_4h_future_complete_avg": self._complete_avg(rows, "mfe_4h_u_future", "is_complete_4h_future"),
            "mae_4h_future_complete_avg": self._complete_avg(rows, "mae_4h_u_future", "is_complete_4h_future"),
            "complete_15m_future_count": sum(1 for row in rows if parse_bool(row.get("is_complete_15m_future"))),
            "complete_1h_future_count": sum(1 for row in rows if parse_bool(row.get("is_complete_1h_future"))),
            "complete_4h_future_count": sum(1 for row in rows if parse_bool(row.get("is_complete_4h_future"))),
            "a2_net_mfe_15m_r_future_avg": self._avg(rows, "a2_net_mfe_15m_r_future"),
            "a2_net_mfe_1h_r_future_avg": self._avg(rows, "a2_net_mfe_1h_r_future"),
            "a2_net_mae_15m_r_future_avg": self._avg(rows, "a2_net_mae_15m_r_future"),
            "a2_net_mae_1h_r_future_avg": self._avg(rows, "a2_net_mae_1h_r_future"),
            "a2_net_hit_1r_15m_future_rate": self._rate(rows, lambda row: parse_bool(row.get("a2_net_hit_1r_15m_future"))),
            "a2_net_hit_1r_1h_future_rate": self._rate(rows, lambda row: parse_bool(row.get("a2_net_hit_1r_1h_future"))),
            "a3_future_net_mfe_15m_r_avg": self._avg(rows, "a3_future_net_mfe_15m_r"),
            "a3_future_net_mfe_1h_r_avg": self._avg(rows, "a3_future_net_mfe_1h_r"),
            "a3_future_net_mae_15m_r_avg": self._avg(rows, "a3_future_net_mae_15m_r"),
            "a3_future_net_mae_1h_r_avg": self._avg(rows, "a3_future_net_mae_1h_r"),
            "a3_future_realized_r_proxy_15m_avg": self._avg(rows, "a3_future_realized_r_proxy_15m"),
            "a3_future_realized_r_proxy_1h_avg": self._avg(rows, "a3_future_realized_r_proxy_1h"),
            "a3_future_fee_positive_1h_rate": self._rate(rows, lambda row: parse_float(row.get("a3_future_realized_r_proxy_1h")) > 0),
            "a3_future_target_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_future_realized_outcome_1h")) == "TARGET_1R_FIRST"),
            "a3_future_stop_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_future_realized_outcome_1h")) == "STOP_1R_FIRST"),
            "a3_future_ambiguous_both_hit_1h_rate": self._rate(rows, lambda row: str(row.get("a3_future_realized_outcome_1h")) == "AMBIGUOUS_BOTH_HIT"),
            "a3_after_a2_future_net_mfe_1h_r_avg": self._avg(rows, "a3_after_a2_future_net_mfe_1h_r"),
            "a3_after_a2_future_realized_r_proxy_1h_avg": self._avg(rows, "a3_after_a2_future_realized_r_proxy_1h"),
            "a3_after_a2_future_fee_positive_1h_rate": self._rate(rows, lambda row: parse_bool(row.get("a3_after_a2_future_fee_positive_1h"))),
            "a3_after_a2_future_target_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_after_a2_future_realized_outcome_1h")) == "TARGET_1R_FIRST"),
            "a3_after_a2_future_stop_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_after_a2_future_realized_outcome_1h")) == "STOP_1R_FIRST"),
            "a3_after_a2_future_ambiguous_both_hit_1h_rate": self._rate(rows, lambda row: str(row.get("a3_after_a2_future_realized_outcome_1h")) == "AMBIGUOUS_BOTH_HIT"),
            "a3_structural_risk_u_avg": self._avg(rows, "a3_structural_risk_u"),
            "a3_structural_fee_share_r_avg": self._avg(rows, "a3_structural_fee_share_r"),
            "a3_structural_realized_r_proxy_1h_avg": self._avg(rows, "a3_structural_realized_r_proxy_1h_future"),
            "a3_structural_fee_positive_1h_rate": self._rate(rows, lambda row: parse_bool(row.get("a3_structural_fee_positive_1h_future"))),
            "a3_after_a2_future_structural_risk_u_avg": self._avg(rows, "a3_after_a2_future_structural_risk_u"),
            "a3_after_a2_future_structural_fee_share_r_avg": self._avg(rows, "a3_after_a2_future_structural_fee_share_r"),
            "a3_after_a2_future_structural_realized_r_proxy_1h_avg": self._avg(rows, "a3_after_a2_future_structural_realized_r_proxy_1h"),
            "a3_after_a2_future_structural_fee_positive_1h_rate": self._rate(rows, lambda row: parse_bool(row.get("a3_after_a2_future_structural_fee_positive_1h"))),
            "a3_after_a2_future_structural_improved_rate": self._rate(rows, lambda row: parse_bool(row.get("a3_after_a2_future_structural_improved_flag"))),
            "a3_after_a2_future_structural_vs_v1_delta_r_1h_avg": self._avg(rows, "a3_after_a2_future_structural_vs_v1_delta_r_1h"),
            "a3_after_a2_future_structural_fee_share_delta_r_avg": self._avg(rows, "a3_after_a2_future_structural_fee_share_delta_r"),
        }

    @staticmethod
    def _avg(rows: list[Mapping[str, Any]], field: str) -> float:
        values = [parse_float(row.get(field)) for row in rows if row.get(field) not in (None, "")]
        return round(sum(values) / len(values), 6) if values else 0.0

    @staticmethod
    def _rate(rows: list[Mapping[str, Any]], predicate) -> float:
        total = len(rows)
        if total <= 0:
            return 0.0
        return round(sum(1 for row in rows if predicate(row)) / total, 6)

    @staticmethod
    def _complete_avg(rows: list[Mapping[str, Any]], field: str, complete_field: str) -> float:
        complete_rows = [row for row in rows if parse_bool(row.get(complete_field))]
        return ZoneTruthAnalyzer._avg(complete_rows, field)

    @staticmethod
    def _forward_summary(rows: list[Mapping[str, Any]], label: str) -> dict[str, Any]:
        return {
            "mfe_avg": ZoneTruthAnalyzer._avg(rows, f"mfe_{label}_u_future"),
            "mae_avg": ZoneTruthAnalyzer._avg(rows, f"mae_{label}_u_future"),
            "mfe_complete_avg": ZoneTruthAnalyzer._complete_avg(rows, f"mfe_{label}_u_future", f"is_complete_{label}_future"),
            "mae_complete_avg": ZoneTruthAnalyzer._complete_avg(rows, f"mae_{label}_u_future", f"is_complete_{label}_future"),
            "complete_count": sum(1 for row in rows if parse_bool(row.get(f"is_complete_{label}_future"))),
        }

    @staticmethod
    def _reaction_contains(rows: list[Mapping[str, Any]], token: str) -> int:
        return sum(1 for row in rows if token in str(row.get("reaction_type") or row.get("a1_reaction_type") or ""))

    @staticmethod
    def _is_reaction_row(row: Mapping[str, Any]) -> bool:
        def is_known_reaction_type(value: Any) -> bool:
            text = str(value or "").strip().upper()
            return bool(text and text not in {"UNKNOWN", "SYNTHETIC"})

        return (
            parse_float(row.get("reaction_count")) > 0
            or is_known_reaction_type(row.get("reaction_type"))
            or is_known_reaction_type(row.get("final_reaction_type"))
            or is_known_reaction_type(row.get("a1_reaction_type"))
        )

    @staticmethod
    def _context_join_key(row: Mapping[str, Any]) -> str:
        for field in ("iceberg_pie_event_keys", "best_pie_event_key", "event_key", "zone_id"):
            value = str(row.get(field) or "").strip()
            if value:
                return value.split("|")[0]
        return ""

    @staticmethod
    def _context_status(rows: list[Mapping[str, Any]]) -> str:
        statuses = {str(row.get("context_labels_status") or "") for row in rows}
        if "SUCCESS" in statuses:
            return "SUCCESS"
        if "KLINE_UNAVAILABLE" in statuses:
            return "KLINE_UNAVAILABLE"
        if "DISABLED" in statuses:
            return "DISABLED"
        return "UNAVAILABLE"

    @staticmethod
    def _write_summary_md(path: Path, summary: Mapping[str, Any]) -> None:
        lines = [
            "# V7.2.1 ICEBERG 3A Context Research",
            "",
            f"- total_zones: {summary.get('total_zones')}",
            f"- exact_matched_zones: {summary.get('exact_matched_zones')}",
            f"- fuzzy_matched_zones: {summary.get('fuzzy_matched_zones')}",
            f"- synthetic_zones: {summary.get('synthetic_zones')}",
            f"- unmatched_pie_count: {summary.get('unmatched_pie_count')}",
            f"- a2_pre_pool_zone_count: {summary.get('a2_pre_pool_zone_count')}",
            f"- context_labels_status: {summary.get('context_labels_status')}",
            "",
            "## Reaction Distribution",
            "",
        ]
        for key, value in dict(summary.get("reaction_distribution") or {}).items():
            lines.append(f"- {key}: {value}")
        lines.extend(["", "## Enhanced Trend Context", ""])
        lines.append("- trend_regime_enhanced_1h distribution:")
        for key, value in dict(summary.get("trend_regime_enhanced_1h_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- trend_regime_enhanced_4h distribution:")
        for key, value in dict(summary.get("trend_regime_enhanced_4h_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- trend_alignment distribution:")
        for key, value in dict(summary.get("trend_alignment_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.extend(["", "## A2 State Machine Research Fields", ""])
        lines.append("- a2_state distribution:")
        for key, value in dict(summary.get("a2_state_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_book_depth_state distribution:")
        for key, value in dict(summary.get("a2_book_depth_state_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_context_alignment distribution:")
        for key, value in dict(summary.get("a2_context_alignment_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- strong_a1_tier distribution:")
        for key, value in dict(summary.get("strong_a1_tier_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_observe_priority distribution:")
        for key, value in dict(summary.get("a2_observe_priority_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_risk_tier distribution:")
        for key, value in dict(summary.get("a2_risk_tier_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_block_reason distribution:")
        for key, value in dict(summary.get("a2_block_reason_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_sweep_reclaim_quality distribution:")
        for key, value in dict(summary.get("a2_sweep_reclaim_quality_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a2_compression_state_future distribution:")
        for key, value in dict(summary.get("a2_compression_state_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a3_watch_priority distribution:")
        for key, value in dict(summary.get("a3_watch_priority_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append(f"- a2_validated_candidate_count: {summary.get('a2_validated_candidate_count')}")
        lines.append(f"- a2_clean_hold_count: {summary.get('a2_clean_hold_count')}")
        lines.append(f"- a2_failed_reclaim_count: {summary.get('a2_failed_reclaim_count')}")
        lines.append(f"- a2_ready_for_a3_watch_count: {summary.get('a2_ready_for_a3_watch_count')}")
        lines.append(f"- a3_future_breakout_after_a2_count: {summary.get('a3_future_breakout_after_a2_count')}")
        lines.append(f"- a2_book_depth_missing_count: {summary.get('a2_book_depth_missing_count')}")
        lines.append(f"- reaction_events_outside_kline_range_count: {summary.get('reaction_events_outside_kline_range_count')}")
        lines.append(f"- reaction_rows_count: {summary.get('reaction_rows_count')}")
        lines.append(f"- non_reaction_rows_count: {summary.get('non_reaction_rows_count')}")
        lines.append(f"- reaction_rows_without_reaction_event_ts_count: {summary.get('reaction_rows_without_reaction_event_ts_count')}")
        lines.append(f"- reaction_event_ts_invalid_count_on_reaction_rows: {summary.get('reaction_event_ts_invalid_count_on_reaction_rows')}")
        lines.append(f"- reaction_event_ts_invalid_count: {summary.get('reaction_event_ts_invalid_count')}")
        lines.extend(["", "## A3 Structural Stop Proxy", ""])
        lines.append(f"- structural_proxy_available_count: {summary.get('structural_proxy_available_count')}")
        lines.append("- structural_proxy_reason distribution:")
        for key, value in dict(summary.get("structural_proxy_reason_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append(f"- a3_structural_fee_positive_1h_count: {summary.get('a3_structural_fee_positive_1h_count')}")
        lines.append(f"- a3_structural_fee_positive_1h_rate: {summary.get('a3_structural_fee_positive_1h_rate')}")
        rt_memory = dict(summary.get("runtime_3a_memory_profile") or {})
        if rt_memory:
            lines.extend(["", "## Runtime 3A Memory Profile", ""])
            lines.append(f"- runtime_event_source_mode: {rt_memory.get('runtime_event_source_mode')}")
            lines.append(f"- runtime_window_reads: {rt_memory.get('runtime_window_reads')}")
            lines.append(f"- runtime_candidate_file_scans: {rt_memory.get('runtime_candidate_file_scans')}")
            lines.append(f"- runtime_max_window_ticks: {rt_memory.get('runtime_max_window_ticks')}")
            if rt_memory.get("runtime_performance_warning"):
                lines.append(f"- WARNING: {rt_memory.get('runtime_performance_warning')}")
        deprecated_aliases = dict(summary.get("deprecated_report_aliases") or {})
        if deprecated_aliases:
            lines.extend(["", "## Deprecated Report Aliases", ""])
            for old, new in deprecated_aliases.items():
                lines.append(f"- `{old}` -> `{new}`")
        lines.append(f"- a3_after_a2_future_fee_positive_1h_count: {summary.get('a3_after_a2_future_fee_positive_1h_count')}")
        lines.append(f"- a3_after_a2_future_fee_positive_1h_rate: {summary.get('a3_after_a2_future_fee_positive_1h_rate')}")
        lines.append(f"- a3_after_a2_future_realized_r_proxy_1h_avg: {summary.get('a3_after_a2_future_realized_r_proxy_1h_avg')}")
        lines.append(f"- a3_after_a2_future_structural_fee_positive_1h_count: {summary.get('a3_after_a2_future_structural_fee_positive_1h_count')}")
        lines.append(f"- a3_after_a2_future_structural_fee_positive_1h_rate: {summary.get('a3_after_a2_future_structural_fee_positive_1h_rate')}")
        lines.append(f"- a3_after_a2_future_structural_realized_r_proxy_1h_avg: {summary.get('a3_after_a2_future_structural_realized_r_proxy_1h_avg')}")
        lines.append(f"- a3_after_a2_future_structural_improved_count: {summary.get('a3_after_a2_future_structural_improved_count')}")
        lines.append(f"- a3_after_a2_future_structural_improved_rate: {summary.get('a3_after_a2_future_structural_improved_rate')}")
        lines.append(f"- a3_after_a2_future_structural_vs_v1_delta_r_1h_avg: {summary.get('a3_after_a2_future_structural_vs_v1_delta_r_1h_avg')}")
        lines.append(f"- a3_after_a2_future_structural_fee_share_delta_r_avg: {summary.get('a3_after_a2_future_structural_fee_share_delta_r_avg')}")
        lines.extend(["", "## V7 3A Shadow Matrix", ""])
        lines.append(f"- v7_enabled: {summary.get('v7_enabled')}")
        lines.append(f"- v7_3a_simulated_trade_count: {summary.get('v7_3a_simulated_trade_count')}")
        lines.append(f"- v7_top_combo_count: {summary.get('v7_top_combo_count')}")
        lines.append(f"- v7_positive_combo_count: {summary.get('v7_positive_combo_count')}")
        lines.append(f"- v7_bad_combo_count: {summary.get('v7_bad_combo_count')}")
        ZoneTruthAnalyzer._append_combo_preview(lines, "Top Realized R", summary.get("top_3a_combos_by_realized_r"))
        ZoneTruthAnalyzer._append_combo_preview(lines, "Top Profit Factor", summary.get("top_3a_combos_by_profit_factor"))
        ZoneTruthAnalyzer._append_combo_preview(lines, "Bad Combos To Delete", summary.get("bad_3a_combos_to_delete"))
        lines.extend(
            [
                "",
                "## Runtime-safe vs Future Labels",
                "",
                "- `PRICE_BREAKOUT_PERSISTENT` is a future quality label under `a3_quality_future_type_v2`, not a runtime entry condition.",
                "- `A2_COMPRESSION` is now `A2_COMPRESSION_FUTURE_PROXY`, because the old compression proxy uses post-zone future windows.",
                "- Runtime strategy entry uses `a2_rt_*` and `a3_entry_rt_*` fields only.",
                "- VP fields with `a1_vp_setup_rt` describe A1 absorption location, not a mandatory A3 entry location.",
            ]
        )
        lines.extend(["", "## Forward Metrics", ""])
        lines.append(
            "Zone forward metrics start from `forward_anchor_ts`; `forward_anchor_source` and "
            "`forward_entry_price_source` identify the exact timestamp and entry price used."
        )
        lines.append("")
        for label, stats in dict(summary.get("forward_metrics") or {}).items():
            lines.append(
                f"- {label}: mfe_avg={stats.get('mfe_avg')} mae_avg={stats.get('mae_avg')} "
                f"mfe_complete_avg={stats.get('mfe_complete_avg')} mae_complete_avg={stats.get('mae_complete_avg')} "
                f"complete_count={stats.get('complete_count')}"
            )
        lines.extend(
            [
                "",
                "## Key Findings",
                "",
                f"- CLEAN_HOLD zone count: {summary.get('clean_hold_count')}",
                f"- FAILED_RECLAIM zone count: {summary.get('failed_reclaim_count')}",
                f"- has_clean_hold_count: {summary.get('has_clean_hold_count')}",
                f"- has_failed_reclaim_count: {summary.get('has_failed_reclaim_count')}",
                f"- synthetic vs reaction zone: synthetic={summary.get('synthetic_zones')} reaction={int(summary.get('total_zones') or 0) - int(summary.get('synthetic_zones') or 0)}",
                f"- truth_ge65_count_offline positive zones: {summary.get('truth_ge65_zone_count')}",
                f"- hard cap warning zones: {summary.get('hard_cap_warning_zone_count')}",
                "",
                "Synthetic zones are currently emitted per unmatched ICEBERG pie and do not represent real merged zones. A later version may add synthetic_merge_enabled.",
                "",
                "For zones with multiple reactions, `final_reaction_type` is the final observed reaction label. It does not mean forward metrics start from `final_reaction_ts`; the default anchor remains reaction_event_ts -> frozen_ts -> best_pie_ts -> first_seen_ts. A later version may add final_reaction_forward_metrics.",
                "",
                "A2_PRE_POOL eligibility is based only on iceberg_pie_count >= 1. Truth Score and forward MFE/MAE are offline research fields.",
            ]
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    @staticmethod
    def _append_combo_preview(lines: list[str], title: str, rows: Any) -> None:
        fields = [
            "a1_primary_evidence_type",
            "a2_accumulation_path_v2",
            "a3_quality_future_type_v2",
            "entry_model",
            "stop_model",
            "target_r",
            "count",
            "avg_realized_r_sim",
            "profit_factor_proxy",
            "fee_positive_rate",
        ]
        lines.extend(["", f"### {title}", ""])
        rows = list(rows or [])[:5]
        if not rows:
            lines.append("- none")
            return
        lines.append("| " + " | ".join(fields) + " |")
        lines.append("| " + " | ".join(["---"] * len(fields)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
