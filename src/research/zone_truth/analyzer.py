#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.research.a1_edge.io_utils import read_csv, read_jsonl, write_csv
from src.research.a1_edge.schema import parse_bool, parse_float

from .aggregator import ZoneTruthAggregator
from .forward import ZoneForwardMetricsCalculator
from .models import FORWARD_FIELDS, SOURCE_SYNTHETIC, ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS


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
]


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
        write_csv(out / "zone_truth_events.csv", rows, ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS)
        write_csv(out / "zone_truth_by_reaction.csv", self.group_rows(rows, "reaction_type"), ["reaction_type"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_final_reaction.csv", self.group_rows(rows, "final_reaction_type"), ["final_reaction_type"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_direction.csv", self.group_rows(rows, "direction"), ["direction"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_session.csv", self.group_rows(rows, "session_tag"), ["session_tag"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_truth_bucket.csv", self.group_by_truth_bucket(rows), ["truth_bucket"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_by_a2_pre_pool.csv", self.group_rows(rows, "a2_pre_pool_eligible"), ["a2_pre_pool_eligible"] + GROUP_METRIC_FIELDS)
        write_csv(out / "zone_truth_top_cases.csv", self.top_cases(rows), ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS)
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
        }

    @staticmethod
    def _avg(rows: list[Mapping[str, Any]], field: str) -> float:
        values = [parse_float(row.get(field)) for row in rows if row.get(field) not in (None, "")]
        return round(sum(values) / len(values), 6) if values else 0.0

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
    def _write_summary_md(path: Path, summary: Mapping[str, Any]) -> None:
        lines = [
            "# V6.3.11.5.1 Zone Truth Aggregation Cleanup",
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
