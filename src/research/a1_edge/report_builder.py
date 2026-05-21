#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from .io_utils import read_csv, write_json
from .schema import A1EdgeReport, parse_bool, parse_float, parse_int


def _avg(rows: Iterable[Mapping[str, Any]], field: str) -> float:
    values = [parse_float(row.get(field)) for row in rows]
    return sum(values) / len(values) if values else 0.0


def _rate(rows: Iterable[Mapping[str, Any]], field: str) -> float:
    data = list(rows)
    return sum(1 for row in data if parse_bool(row.get(field))) / len(data) if data else 0.0


class A1EdgeReportBuilder:
    def __init__(self, min_group_sample_size: int = 30, min_total_events: int = 200):
        self.min_group_sample_size = int(min_group_sample_size)
        self.min_total_events = int(min_total_events)

    def _decision(
        self,
        total_events: int,
        total_random: int,
        random_summary: List[Dict[str, Any]],
        hypothesis_summary: List[Dict[str, Any]],
    ) -> str:
        if total_events < self.min_total_events or total_random < total_events * 5:
            return "INSUFFICIENT_SAMPLE"
        overall = next((r for r in random_summary if r.get("dimension") == "ALL"), {})
        overall_edge = overall.get("edge_label") in {"STRONG_DIRECTIONAL_EDGE", "VOLATILITY_EDGE"}
        subgroup_edge = any(
            r.get("edge_label") in {"STRONG_DIRECTIONAL_EDGE", "VOLATILITY_EDGE"}
            and parse_int(r.get("a1_sample_count")) >= self.min_group_sample_size
            for r in random_summary
            if r.get("dimension") != "ALL"
        )
        hypothesis_edge = any(
            parse_float(r.get("avg_realized_r_proxy")) > 0
            and parse_int(r.get("valid_count")) >= self.min_group_sample_size
            for r in hypothesis_summary
        )
        if overall_edge and hypothesis_edge:
            return "A1_GO"
        if subgroup_edge or hypothesis_edge:
            return "A1_PARTIAL_GO"
        return "A1_NO_GO"

    def _recommendation(self, decision: str) -> str:
        if decision == "A1_GO":
            return "Proceed to V6.4 A2 design using the strongest A1 groups."
        if decision == "A1_PARTIAL_GO":
            return "Proceed to V6.4 only for selected A1 subtypes. Exclude weak/no-edge A1 groups."
        if decision == "A1_NO_GO":
            return "Do not proceed to A2. Rework A1 detection."
        return "Continue data collection. Do not make strategic conclusion."

    def build_from_dir(self, out_dir: Path | str) -> A1EdgeReport:
        out = Path(out_dir)
        return self.build(
            events=read_csv(out / "a1_edge_events.csv"),
            forward_metrics=read_csv(out / "a1_forward_metrics.csv"),
            random_baseline=read_csv(out / "a1_random_baseline.csv"),
            random_summary=read_csv(out / "a1_vs_random_summary.csv"),
            hypothesis_results=read_csv(out / "a1_hypothesis_results.csv"),
            hypothesis_summary=read_csv(out / "a1_hypothesis_summary.csv"),
            out_dir=out,
        )

    def build(
        self,
        events: Iterable[Mapping[str, Any]],
        forward_metrics: Iterable[Mapping[str, Any]],
        random_baseline: Iterable[Mapping[str, Any]],
        random_summary: Iterable[Mapping[str, Any]],
        hypothesis_results: Iterable[Mapping[str, Any]],
        hypothesis_summary: Iterable[Mapping[str, Any]],
        out_dir: Path | str,
    ) -> A1EdgeReport:
        event_rows = [dict(row) for row in events or []]
        forward_rows = [dict(row) for row in forward_metrics or []]
        random_rows = [dict(row) for row in random_baseline or []]
        random_summary_rows = [dict(row) for row in random_summary or []]
        hypothesis_rows = [dict(row) for row in hypothesis_results or []]
        hypothesis_summary_rows = [dict(row) for row in hypothesis_summary or []]
        total_random_samples = len({row.get("baseline_id") for row in random_rows})
        decision = self._decision(len(event_rows), total_random_samples, random_summary_rows, hypothesis_summary_rows)
        recommendation = self._recommendation(decision)
        report = A1EdgeReport(
            decision=decision,
            total_a1_events=len(event_rows),
            total_random_baseline_samples=total_random_samples,
            total_hypothesis_rows=len(hypothesis_rows),
            recommended_next_step=recommendation,
        )
        summary = self._summary_json(report, event_rows, random_summary_rows, hypothesis_summary_rows)
        out = Path(out_dir)
        write_json(out / "a1_edge_summary.json", summary)
        (out / "a1_go_no_go_report.md").write_text(
            self._markdown(report, event_rows, forward_rows, random_summary_rows, hypothesis_summary_rows),
            encoding="utf-8",
        )
        return report

    def _summary_json(
        self,
        report: A1EdgeReport,
        events: List[Dict[str, Any]],
        random_summary: List[Dict[str, Any]],
        hypothesis_summary: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        event_ts = [parse_float(row.get("event_ts")) for row in events if parse_float(row.get("event_ts")) > 0]
        return {
            **report.to_dict(),
            "total_events": len(events),
            "time_range": {"min_event_ts": min(event_ts) if event_ts else 0.0, "max_event_ts": max(event_ts) if event_ts else 0.0},
            "symbols": sorted({str(row.get("symbol") or "") for row in events if row.get("symbol")}),
            "by_direction": dict(Counter(str(row.get("direction") or "UNKNOWN") for row in events)),
            "by_a1_reaction_type": dict(Counter(str(row.get("a1_reaction_type") or "UNKNOWN") for row in events)),
            "by_reaction_event_kind": dict(Counter(str(row.get("reaction_event_kind") or "UNKNOWN") for row in events)),
            "by_legacy_phase2_type": dict(Counter(str(row.get("legacy_phase2_type") or "UNKNOWN") for row in events)),
            "by_frozen_reason": dict(Counter(str(row.get("frozen_reason") or "UNKNOWN") for row in events)),
            "by_frozen_state": dict(Counter(str(row.get("frozen_state") or "UNKNOWN") for row in events)),
            "confirmed_count": sum(1 for row in events if parse_bool(row.get("has_confirmed"))),
            "failed_count": sum(1 for row in events if parse_bool(row.get("has_failed"))),
            "book_depth_available_count": sum(1 for row in events if parse_bool(row.get("relevant_book_depth_available"))),
            "unknown_reaction_type_count": sum(
                1 for row in events if not row.get("a1_reaction_type") or "UNKNOWN" in str(row.get("a1_reaction_type"))
            ),
            "unknown_frozen_reason_count": sum(
                1 for row in events if not row.get("frozen_reason") or str(row.get("frozen_reason")) == "UNKNOWN"
            ),
            "best_random_summary": random_summary[:5],
            "best_hypothesis_summary": sorted(
                hypothesis_summary,
                key=lambda row: parse_float(row.get("avg_realized_r_proxy")),
                reverse=True,
            )[:5],
        }

    def _table_counts(self, title: str, rows: List[Dict[str, Any]], field: str) -> List[str]:
        counts = Counter(str(row.get(field) or "UNKNOWN") for row in rows)
        lines = [f"## {title}", "", "| Value | Count |", "|---|---:|"]
        for value, count in counts.most_common():
            lines.append(f"| {value} | {count} |")
        if not counts:
            lines.append("| EMPTY | 0 |")
        lines.append("")
        return lines

    def _markdown(
        self,
        report: A1EdgeReport,
        events: List[Dict[str, Any]],
        forward_metrics: List[Dict[str, Any]],
        random_summary: List[Dict[str, Any]],
        hypothesis_summary: List[Dict[str, Any]],
    ) -> str:
        event_ts = [parse_float(row.get("event_ts")) for row in events if parse_float(row.get("event_ts")) > 0]
        symbols = ", ".join(sorted({str(row.get("symbol") or "") for row in events if row.get("symbol")})) or "UNKNOWN"
        overall_random = next((row for row in random_summary if row.get("dimension") == "ALL"), {})
        best_groups = sorted(random_summary, key=lambda row: parse_float(row.get("mfe_edge_15m")), reverse=True)[:5]
        worst_groups = sorted(random_summary, key=lambda row: parse_float(row.get("mfe_edge_15m")))[:5]
        best_hyp = sorted(hypothesis_summary, key=lambda row: parse_float(row.get("avg_realized_r_proxy")), reverse=True)[:5]
        worst_hyp = sorted(hypothesis_summary, key=lambda row: parse_float(row.get("avg_realized_r_proxy")))[:5]
        buy_15 = [r for r in forward_metrics if r.get("direction") == "BUY" and parse_int(r.get("window_sec")) == 900]
        sell_15 = [r for r in forward_metrics if r.get("direction") == "SELL" and parse_int(r.get("window_sec")) == 900]
        lines = [
            "# V6.3.10 A1 Edge Validation Report",
            "",
            "## Run Summary",
            "",
            f"- total A1 events: {report.total_a1_events}",
            f"- total random baseline samples: {report.total_random_baseline_samples}",
            f"- total hypothesis rows: {report.total_hypothesis_rows}",
            f"- time range: {(min(event_ts) if event_ts else 0.0)} - {(max(event_ts) if event_ts else 0.0)}",
            f"- symbol: {symbols}",
            f"- sample sufficiency: {report.decision != 'INSUFFICIENT_SAMPLE'}",
            "",
            "## A1 Event Distribution",
            "",
        ]
        for title, field in (
            ("By Reaction Type", "a1_reaction_type"),
            ("By Event Kind", "reaction_event_kind"),
            ("By Frozen Reason", "frozen_reason"),
            ("By Frozen State", "frozen_state"),
            ("By Direction", "direction"),
        ):
            lines.extend(self._table_counts(title, events, field))
        lines.extend(
            [
                "## A1 vs Random",
                "",
                f"- directional edge: {overall_random.get('mfe_edge_15m', 0)}",
                f"- volatility edge: {overall_random.get('volatility_edge_15m', 0)}",
                f"- 15m / 60m comparison: {overall_random.get('a1_avg_mfe_r_15m', 0)} / {overall_random.get('a1_avg_mfe_r_60m', 0)}",
                "",
                "### Best Groups",
                "",
            ]
        )
        lines.extend(self._summary_table(best_groups, ["dimension", "group", "edge_label", "mfe_edge_15m"]))
        lines.extend(["### Worst Groups", ""])
        lines.extend(self._summary_table(worst_groups, ["dimension", "group", "edge_label", "mfe_edge_15m"]))
        lines.extend(
            [
                "## Directionality",
                "",
                f"- BUY A1 forward MFE 15m: {_avg(buy_15, 'directional_mfe_r')}",
                f"- SELL A1 forward MFE 15m: {_avg(sell_15, 'directional_mfe_r')}",
                f"- first hit +1R rate 15m: {_rate([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'first_hit_plus_1r')}",
                f"- first hit -1R rate 15m: {_rate([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'first_hit_minus_1r')}",
                "",
                "## Volatility Expansion",
                "",
                f"- total range after A1 15m: {_avg([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'total_range_u')}",
                f"- A1 vs random total range 15m: {overall_random.get('a1_avg_total_range_15m', 0)} / {overall_random.get('random_avg_total_range_15m', 0)}",
                "",
                "## Hypothesis Results",
                "",
                "### Best Hypothesis Overall",
                "",
            ]
        )
        lines.extend(self._summary_table(best_hyp, ["dimension", "group", "avg_realized_r_proxy", "edge_label"]))
        lines.extend(["### Worst Hypothesis", ""])
        lines.extend(self._summary_table(worst_hyp, ["dimension", "group", "avg_realized_r_proxy", "edge_label"]))
        lines.extend(
            [
                "## Go / No-Go Decision",
                "",
                f"Decision: {report.decision}",
                "",
                "Recommended next step:",
                "",
            ]
        )
        for recommendation_line in report.recommended_next_step.split(". "):
            if recommendation_line:
                suffix = "" if recommendation_line.endswith(".") else "."
                lines.append(f"- {recommendation_line}{suffix}")
        return "\n".join(lines) + "\n"

    def _summary_table(self, rows: List[Dict[str, Any]], fields: List[str]) -> List[str]:
        lines = ["| " + " | ".join(fields) + " |", "|" + "|".join("---" for _ in fields) + "|"]
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
        if not rows:
            lines.append("| " + " | ".join("EMPTY" if i == 0 else "0" for i, _ in enumerate(fields)) + " |")
        lines.append("")
        return lines
