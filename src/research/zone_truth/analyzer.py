#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.research.a1_edge.io_utils import read_csv, read_jsonl, write_csv
from src.research.a1_edge.schema import parse_bool, parse_float

from .aggregator import ZoneTruthAggregator
from .a2_state import ZoneA2StateClassifier
from .forward import ZoneForwardMetricsCalculator
from .market_context import ZoneMarketContextCalculator
from .models import SOURCE_SYNTHETIC, ZONE_TRUTH_EVENT_WITH_CONTEXT_FIELDS


FEE_AWARE_GROUP_METRIC_FIELDS = [
    "a2_net_mfe_15m_r_avg",
    "a2_net_mfe_1h_r_avg",
    "a2_net_mae_15m_r_avg",
    "a2_net_mae_1h_r_avg",
    "a2_net_hit_1r_15m_rate",
    "a2_net_hit_1r_1h_rate",
    "a3_preview_net_mfe_15m_r_avg",
    "a3_preview_net_mfe_1h_r_avg",
    "a3_preview_net_mae_15m_r_avg",
    "a3_preview_net_mae_1h_r_avg",
    "a3_preview_realized_r_proxy_15m_avg",
    "a3_preview_realized_r_proxy_1h_avg",
    "a3_preview_fee_positive_1h_rate",
    "a3_preview_target_1r_first_1h_rate",
    "a3_preview_stop_1r_first_1h_rate",
    "a3_preview_ambiguous_both_hit_1h_rate",
]

GROUP_METRIC_FIELDS = [
    "count",
    "truth_score_avg",
    "truth_score_max_avg",
    "truth_ge65_avg",
    "truth_ge80_avg",
    "mfe_15m_avg",
    "mae_15m_avg",
    "mfe_15m_complete_avg",
    "mae_15m_complete_avg",
    "mfe_1h_avg",
    "mae_1h_avg",
    "mfe_1h_complete_avg",
    "mae_1h_complete_avg",
    "mfe_4h_avg",
    "mae_4h_avg",
    "mfe_4h_complete_avg",
    "mae_4h_complete_avg",
    "complete_15m_count",
    "complete_1h_count",
    "complete_4h_count",
] + FEE_AWARE_GROUP_METRIC_FIELDS


class ZoneTruthAnalyzer:
    def __init__(
        self,
        price_tolerance_usdt: float = 1.5,
        time_tolerance_sec: float = 300.0,
        windows_sec: Iterable[int] | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self.price_tolerance_usdt = float(price_tolerance_usdt)
        self.time_tolerance_sec = float(time_tolerance_sec)
        self.windows_sec = list(windows_sec or [900, 3600, 14400])
        self.timezone = timezone

    def analyze_files(
        self,
        phase1_candidates: str | Path,
        a1_reactions: str | Path,
        kline: str | Path,
        out_dir: str | Path,
    ) -> dict[str, Any]:
        phase1_records = read_jsonl(phase1_candidates)
        reaction_records = read_jsonl(a1_reactions)
        kline_records = read_csv(kline)
        return self.export(phase1_records, reaction_records, kline_records, out_dir)

    def export(
        self,
        phase1_records: Iterable[Mapping[str, Any]],
        reaction_records: Iterable[Mapping[str, Any]],
        kline_records: Iterable[Mapping[str, Any]],
        out_dir: str | Path,
    ) -> dict[str, Any]:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        aggregator = ZoneTruthAggregator(
            price_tolerance_usdt=self.price_tolerance_usdt,
            time_tolerance_sec=self.time_tolerance_sec,
            timezone=self.timezone,
        )
        rows = aggregator.aggregate(phase1_records, reaction_records)
        rows = ZoneForwardMetricsCalculator(self.windows_sec, kline_timezone=self.timezone).attach_forward_metrics(rows, kline_records)
        rows = ZoneMarketContextCalculator(kline_timezone=self.timezone).attach_market_context(rows, kline_records)
        rows = ZoneA2StateClassifier().attach_a2_state(rows)
        write_csv(out / "zone_truth_events.csv", rows, ZONE_TRUTH_EVENT_WITH_CONTEXT_FIELDS)
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
        write_csv(out / "zone_truth_by_a2_compression_state.csv", self.group_rows(rows, "a2_compression_state"), ["a2_compression_state"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_ready_for_a3_watch.csv", self.group_rows(rows, "a2_ready_for_a3_watch_flag"), ["a2_ready_for_a3_watch_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_watch_priority.csv", self.group_rows(rows, "a3_watch_priority"), ["a3_watch_priority"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_breakout_after_a2.csv", self.group_rows(rows, "a3_preview_breakout_after_a2_flag"), ["a3_preview_breakout_after_a2_flag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_latency_bucket.csv", self.group_rows(rows, "a3_preview_latency_bucket"), ["a3_preview_latency_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_ignition_quality.csv", self.group_rows(rows, "a3_preview_ignition_quality"), ["a3_preview_ignition_quality"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_pre_ignition_compression_state.csv", self.group_rows(rows, "a2_pre_ignition_compression_state"), ["a2_pre_ignition_compression_state"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_realized_outcome_15m.csv", self.group_rows(rows, "a3_preview_realized_outcome_15m"), ["a3_preview_realized_outcome_15m"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_realized_outcome_1h.csv", self.group_rows(rows, "a3_preview_realized_outcome_1h"), ["a3_preview_realized_outcome_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_net_mfe_1h_bucket.csv", self.group_rows(rows, "a3_preview_net_mfe_1h_bucket"), ["a3_preview_net_mfe_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a3_preview_realized_r_proxy_1h_bucket.csv", self.group_rows(rows, "a3_preview_realized_r_proxy_1h_bucket"), ["a3_preview_realized_r_proxy_1h_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_1h.csv", self.group_rows(rows, "trend_regime_1h"), ["trend_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_4h.csv", self.group_rows(rows, "trend_regime_4h"), ["trend_regime_4h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_enhanced_1h.csv", self.group_rows(rows, "trend_regime_enhanced_1h"), ["trend_regime_enhanced_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_regime_enhanced_4h.csv", self.group_rows(rows, "trend_regime_enhanced_4h"), ["trend_regime_enhanced_4h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_trend_alignment.csv", self.group_rows(rows, "trend_alignment"), ["trend_alignment"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_volume_regime_1h.csv", self.group_rows(rows, "volume_regime_1h"), ["volume_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_volatility_regime_1h.csv", self.group_rows(rows, "volatility_regime_1h"), ["volatility_regime_1h"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_top_cases.csv", self.top_cases(rows), ZONE_TRUTH_EVENT_WITH_CONTEXT_FIELDS)
        write_csv(out / "zone_truth_match_quality.csv", self.match_quality(rows, aggregator.unmatched_pie_count), ["match_quality"] + GROUP_METRIC_FIELDS)
        summary = self.summary(rows, aggregator.unmatched_pie_count)
        self._write_summary_md(out / "zone_truth_summary.md", summary)
        return summary

    def summary(self, rows: list[Mapping[str, Any]], unmatched_pie_count: int = 0) -> dict[str, Any]:
        total = len(rows)
        exact = sum(1 for row in rows if str(row.get("zone_match_method")) == "exact")
        fuzzy = sum(1 for row in rows if str(row.get("zone_match_method")) == "fuzzy")
        synthetic = sum(1 for row in rows if str(row.get("zone_source")) == SOURCE_SYNTHETIC)
        a2_count = sum(1 for row in rows if parse_bool(row.get("a2_pre_pool_eligible")))
        reaction_distribution = dict(Counter(str(row.get("reaction_type") or "UNKNOWN") for row in rows))
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
        a2_compression_state_distribution = dict(Counter(str(row.get("a2_compression_state") or "UNKNOWN") for row in rows))
        a3_watch_priority_distribution = dict(Counter(str(row.get("a3_watch_priority") or "NONE") for row in rows))
        a3_preview_latency_bucket_distribution = dict(Counter(str(row.get("a3_preview_latency_bucket") or "NO_IGNITION") for row in rows))
        a3_preview_ignition_quality_distribution = dict(Counter(str(row.get("a3_preview_ignition_quality") or "NO_IGNITION") for row in rows))
        a2_pre_ignition_compression_state_distribution = dict(Counter(str(row.get("a2_pre_ignition_compression_state") or "INSUFFICIENT_BARS") for row in rows))
        a3_preview_realized_outcome_15m_distribution = dict(Counter(str(row.get("a3_preview_realized_outcome_15m") or "NO_BREAKOUT") for row in rows))
        a3_preview_realized_outcome_1h_distribution = dict(Counter(str(row.get("a3_preview_realized_outcome_1h") or "NO_BREAKOUT") for row in rows))
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
            "a3_preview_latency_bucket_distribution": a3_preview_latency_bucket_distribution,
            "a3_preview_ignition_quality_distribution": a3_preview_ignition_quality_distribution,
            "a2_pre_ignition_compression_state_distribution": a2_pre_ignition_compression_state_distribution,
            "a3_preview_realized_outcome_15m_distribution": a3_preview_realized_outcome_15m_distribution,
            "a3_preview_realized_outcome_1h_distribution": a3_preview_realized_outcome_1h_distribution,
            "a3_preview_strong_ignition_count": sum(1 for row in rows if str(row.get("a3_preview_ignition_quality")) == "STRONG_IGNITION"),
            "a3_preview_medium_ignition_count": sum(1 for row in rows if str(row.get("a3_preview_ignition_quality")) == "MEDIUM_IGNITION"),
            "a3_preview_fee_aware_positive_1h_count": sum(1 for row in rows if parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0),
            "a3_preview_fee_aware_positive_1h_rate": round(sum(1 for row in rows if parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0) / total, 6) if total else 0.0,
            "a3_watch_high_fee_aware_positive_1h_count": sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH" and parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0),
            "a3_watch_high_fee_aware_positive_1h_rate": round(sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH" and parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0) / max(1, sum(1 for row in rows if str(row.get("a3_watch_priority")) == "HIGH")), 6),
            "a2_ready_a3_breakout_fee_positive_1h_count": sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_preview_breakout_after_a2_flag")) and parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0),
            "a2_ready_a3_breakout_fee_positive_1h_rate": round(sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_preview_breakout_after_a2_flag")) and parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0) / max(1, sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) and parse_bool(row.get("a3_preview_breakout_after_a2_flag")))), 6),
            "a2_validated_candidate_count": sum(1 for row in rows if parse_bool(row.get("a2_validated_candidate_flag"))),
            "a2_clean_hold_count": sum(1 for row in rows if parse_bool(row.get("a2_clean_hold_flag"))),
            "a2_failed_reclaim_count": sum(1 for row in rows if parse_bool(row.get("a2_failed_reclaim_flag"))),
            "a2_ready_for_a3_watch_count": sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag"))),
            "a3_preview_breakout_after_a2_count": sum(1 for row in rows if parse_bool(row.get("a3_preview_breakout_after_a2_flag"))),
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
            "truth_ge65_zone_count": sum(1 for row in rows if parse_float(row.get("truth_ge65_count")) > 0),
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
            score = parse_float(row.get("truth_score_max", row.get("truth_score_avg")))
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

    def top_cases(self, rows: list[Mapping[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
        return sorted(
            [dict(row) for row in rows],
            key=lambda row: (
                parse_float(row.get("mfe_1h_u")),
                parse_float(row.get("truth_score_max")),
                parse_float(row.get("sum_active_notional")),
            ),
            reverse=True,
        )[:limit]

    def _group_stats(self, key: str, rows: list[Mapping[str, Any]], output_field: str = "group") -> dict[str, Any]:
        count = len(rows)
        return {
            output_field: key,
            "count": count,
            "truth_score_avg": self._avg(rows, "truth_score_avg"),
            "truth_score_max_avg": self._avg(rows, "truth_score_max"),
            "truth_ge65_avg": self._avg(rows, "truth_ge65_count"),
            "truth_ge80_avg": self._avg(rows, "truth_ge80_count"),
            "mfe_15m_avg": self._avg(rows, "mfe_15m_u"),
            "mae_15m_avg": self._avg(rows, "mae_15m_u"),
            "mfe_15m_complete_avg": self._complete_avg(rows, "mfe_15m_u", "is_complete_15m"),
            "mae_15m_complete_avg": self._complete_avg(rows, "mae_15m_u", "is_complete_15m"),
            "mfe_1h_avg": self._avg(rows, "mfe_1h_u"),
            "mae_1h_avg": self._avg(rows, "mae_1h_u"),
            "mfe_1h_complete_avg": self._complete_avg(rows, "mfe_1h_u", "is_complete_1h"),
            "mae_1h_complete_avg": self._complete_avg(rows, "mae_1h_u", "is_complete_1h"),
            "mfe_4h_avg": self._avg(rows, "mfe_4h_u"),
            "mae_4h_avg": self._avg(rows, "mae_4h_u"),
            "mfe_4h_complete_avg": self._complete_avg(rows, "mfe_4h_u", "is_complete_4h"),
            "mae_4h_complete_avg": self._complete_avg(rows, "mae_4h_u", "is_complete_4h"),
            "complete_15m_count": sum(1 for row in rows if parse_bool(row.get("is_complete_15m"))),
            "complete_1h_count": sum(1 for row in rows if parse_bool(row.get("is_complete_1h"))),
            "complete_4h_count": sum(1 for row in rows if parse_bool(row.get("is_complete_4h"))),
            "a2_net_mfe_15m_r_avg": self._avg(rows, "a2_net_mfe_15m_r"),
            "a2_net_mfe_1h_r_avg": self._avg(rows, "a2_net_mfe_1h_r"),
            "a2_net_mae_15m_r_avg": self._avg(rows, "a2_net_mae_15m_r"),
            "a2_net_mae_1h_r_avg": self._avg(rows, "a2_net_mae_1h_r"),
            "a2_net_hit_1r_15m_rate": self._rate(rows, lambda row: parse_bool(row.get("a2_net_hit_1r_15m"))),
            "a2_net_hit_1r_1h_rate": self._rate(rows, lambda row: parse_bool(row.get("a2_net_hit_1r_1h"))),
            "a3_preview_net_mfe_15m_r_avg": self._avg(rows, "a3_preview_net_mfe_15m_r"),
            "a3_preview_net_mfe_1h_r_avg": self._avg(rows, "a3_preview_net_mfe_1h_r"),
            "a3_preview_net_mae_15m_r_avg": self._avg(rows, "a3_preview_net_mae_15m_r"),
            "a3_preview_net_mae_1h_r_avg": self._avg(rows, "a3_preview_net_mae_1h_r"),
            "a3_preview_realized_r_proxy_15m_avg": self._avg(rows, "a3_preview_realized_r_proxy_15m"),
            "a3_preview_realized_r_proxy_1h_avg": self._avg(rows, "a3_preview_realized_r_proxy_1h"),
            "a3_preview_fee_positive_1h_rate": self._rate(rows, lambda row: parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0),
            "a3_preview_target_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_preview_realized_outcome_1h")) == "TARGET_1R_FIRST"),
            "a3_preview_stop_1r_first_1h_rate": self._rate(rows, lambda row: str(row.get("a3_preview_realized_outcome_1h")) == "STOP_1R_FIRST"),
            "a3_preview_ambiguous_both_hit_1h_rate": self._rate(rows, lambda row: str(row.get("a3_preview_realized_outcome_1h")) == "AMBIGUOUS_BOTH_HIT"),
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
            "mfe_avg": ZoneTruthAnalyzer._avg(rows, f"mfe_{label}_u"),
            "mae_avg": ZoneTruthAnalyzer._avg(rows, f"mae_{label}_u"),
            "mfe_complete_avg": ZoneTruthAnalyzer._complete_avg(rows, f"mfe_{label}_u", f"is_complete_{label}"),
            "mae_complete_avg": ZoneTruthAnalyzer._complete_avg(rows, f"mae_{label}_u", f"is_complete_{label}"),
            "complete_count": sum(1 for row in rows if parse_bool(row.get(f"is_complete_{label}"))),
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
    def _write_summary_md(path: Path, summary: Mapping[str, Any]) -> None:
        lines = [
            "# V6.3.12.2 Zone Truth A2 Diagnostics and A3 Watch Preview",
            "",
            f"- total_zones: {summary.get('total_zones')}",
            f"- exact_matched_zones: {summary.get('exact_matched_zones')}",
            f"- fuzzy_matched_zones: {summary.get('fuzzy_matched_zones')}",
            f"- synthetic_zones: {summary.get('synthetic_zones')}",
            f"- unmatched_pie_count: {summary.get('unmatched_pie_count')}",
            f"- a2_pre_pool_zone_count: {summary.get('a2_pre_pool_zone_count')}",
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
        lines.append("- a2_compression_state distribution:")
        for key, value in dict(summary.get("a2_compression_state_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append("- a3_watch_priority distribution:")
        for key, value in dict(summary.get("a3_watch_priority_distribution") or {}).items():
            lines.append(f"  - {key}: {value}")
        lines.append(f"- a2_validated_candidate_count: {summary.get('a2_validated_candidate_count')}")
        lines.append(f"- a2_clean_hold_count: {summary.get('a2_clean_hold_count')}")
        lines.append(f"- a2_failed_reclaim_count: {summary.get('a2_failed_reclaim_count')}")
        lines.append(f"- a2_ready_for_a3_watch_count: {summary.get('a2_ready_for_a3_watch_count')}")
        lines.append(f"- a3_preview_breakout_after_a2_count: {summary.get('a3_preview_breakout_after_a2_count')}")
        lines.append(f"- a2_book_depth_missing_count: {summary.get('a2_book_depth_missing_count')}")
        lines.append(f"- reaction_events_outside_kline_range_count: {summary.get('reaction_events_outside_kline_range_count')}")
        lines.append(f"- reaction_rows_count: {summary.get('reaction_rows_count')}")
        lines.append(f"- non_reaction_rows_count: {summary.get('non_reaction_rows_count')}")
        lines.append(f"- reaction_rows_without_reaction_event_ts_count: {summary.get('reaction_rows_without_reaction_event_ts_count')}")
        lines.append(f"- reaction_event_ts_invalid_count_on_reaction_rows: {summary.get('reaction_event_ts_invalid_count_on_reaction_rows')}")
        lines.append(f"- reaction_event_ts_invalid_count: {summary.get('reaction_event_ts_invalid_count')}")
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
                f"- truth_ge65_count positive zones: {summary.get('truth_ge65_zone_count')}",
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
