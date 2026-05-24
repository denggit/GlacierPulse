#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
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


def _metric(row: Mapping[str, Any], primary: str, fallback: str = "", default: float = 0.0) -> float:
    value = row.get(primary)
    if value not in (None, ""):
        return parse_float(value)
    if fallback:
        return parse_float(row.get(fallback), default)
    return default


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
        data_quality: Mapping[str, Any] | None = None,
    ) -> str:
        quality = dict(data_quality or {})
        if total_events > 0:
            if parse_int(quality.get("events_outside_kline_range")) > total_events * 0.5:
                return "INSUFFICIENT_KLINE_COVERAGE"
            if parse_int(quality.get("events_insufficient_15m_count")) > total_events * 0.5:
                return "INSUFFICIENT_KLINE_COVERAGE"
            if parse_int(quality.get("events_insufficient_60m_count")) > total_events * 0.5:
                return "INSUFFICIENT_KLINE_COVERAGE"
        if total_events < self.min_total_events or total_random < total_events * 5:
            return "INSUFFICIENT_SAMPLE"
        overall = next((r for r in random_summary if r.get("dimension") == "ALL"), {})
        overall_edge = overall.get("edge_label") == "STRONG_DIRECTIONAL_EDGE"
        subgroup_edge = any(
            r.get("edge_label") == "STRONG_DIRECTIONAL_EDGE"
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
        if decision == "INSUFFICIENT_KLINE_COVERAGE":
            return "Fix kline timestamp alignment and future-bar coverage, then rerun the offline analysis."
        return "Continue data collection. Do not make strategic conclusion."

    def build_from_dir(self, out_dir: Path | str) -> A1EdgeReport:
        out = Path(out_dir)
        metadata_path = out / "a1_run_metadata.json"
        metadata = {}
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        fee_model = {
            "fee_model": "roundtrip_notional_pct",
            "roundtrip_fee_pct": parse_float(metadata.get("roundtrip_fee_pct"), 0.001),
            "description": "Fee-aware net R subtracts entry_price * roundtrip_fee_pct from favorable movement.",
        }
        return self.build(
            events=read_csv(out / "a1_edge_events.csv"),
            forward_metrics=read_csv(out / "a1_forward_metrics.csv"),
            random_baseline=read_csv(out / "a1_random_baseline.csv"),
            random_summary=read_csv(out / "a1_vs_random_summary.csv"),
            hypothesis_results=read_csv(out / "a1_hypothesis_results.csv"),
            hypothesis_summary=read_csv(out / "a1_hypothesis_summary.csv"),
            out_dir=out,
            metadata=metadata,
            fee_model=fee_model,
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
        data_quality: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        fee_model: Mapping[str, Any] | None = None,
    ) -> A1EdgeReport:
        event_rows = [dict(row) for row in events or []]
        forward_rows = [dict(row) for row in forward_metrics or []]
        random_rows = [dict(row) for row in random_baseline or []]
        random_summary_rows = [dict(row) for row in random_summary or []]
        hypothesis_rows = [dict(row) for row in hypothesis_results or []]
        hypothesis_summary_rows = [dict(row) for row in hypothesis_summary or []]
        quality = self._data_quality(event_rows, forward_rows, data_quality)
        total_random_samples = len({row.get("baseline_id") for row in random_rows})
        decision = self._decision(len(event_rows), total_random_samples, random_summary_rows, hypothesis_summary_rows, quality)
        recommendation = self._recommendation(decision)
        report = A1EdgeReport(
            decision=decision,
            total_a1_events=len(event_rows),
            total_random_baseline_samples=total_random_samples,
            total_hypothesis_rows=len(hypothesis_rows),
            recommended_next_step=recommendation,
        )
        fee = dict(
            fee_model
            or {
                "fee_model": "roundtrip_notional_pct",
                "roundtrip_fee_pct": 0.001,
                "description": "Fee-aware net R subtracts entry_price * roundtrip_fee_pct from favorable movement.",
            }
        )
        summary = self._summary_json(
            report,
            event_rows,
            random_summary_rows,
            hypothesis_summary_rows,
            quality,
            metadata=metadata,
            fee_model=fee,
        )
        out = Path(out_dir)
        write_json(out / "a1_edge_summary.json", summary)
        (out / "a1_go_no_go_report.md").write_text(
            self._markdown(report, event_rows, forward_rows, random_summary_rows, hypothesis_summary_rows, quality, metadata, fee),
            encoding="utf-8",
        )
        return report

    def _data_quality(
        self,
        events: List[Dict[str, Any]],
        forward_metrics: List[Dict[str, Any]],
        data_quality: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        quality = dict(data_quality or {})
        event_ts = [parse_float(row.get("event_ts")) for row in events if parse_float(row.get("event_ts")) > 0]
        quality.setdefault("event_min_ts", min(event_ts) if event_ts else 0.0)
        quality.setdefault("event_max_ts", max(event_ts) if event_ts else 0.0)
        for window_sec, suffix in ((900, "15m"), (3600, "60m")):
            rows = [row for row in forward_metrics if parse_int(row.get("window_sec")) == window_sec]
            insufficient = [row for row in rows if parse_bool(row.get("insufficient_future_data"))]
            valid = [row for row in rows if not parse_bool(row.get("insufficient_future_data"))]
            quality.setdefault(f"events_insufficient_{suffix}_count", len({row.get("event_key") for row in insufficient}))
            quality.setdefault(f"valid_events_{suffix}_count", len({row.get("event_key") for row in valid}))
        for name in ("kline_min_ts", "kline_max_ts", "events_outside_kline_range"):
            quality.setdefault(name, 0.0 if name.endswith("_ts") else 0)
        return quality

    def _summary_json(
        self,
        report: A1EdgeReport,
        events: List[Dict[str, Any]],
        random_summary: List[Dict[str, Any]],
        hypothesis_summary: List[Dict[str, Any]],
        data_quality: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
        fee_model: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        event_ts = [parse_float(row.get("event_ts")) for row in events if parse_float(row.get("event_ts")) > 0]
        return {
            **report.to_dict(),
            "total_events": len(events),
            "time_range": {"min_event_ts": min(event_ts) if event_ts else 0.0, "max_event_ts": max(event_ts) if event_ts else 0.0},
            "metadata": dict(metadata or {}),
            "data_quality": dict(data_quality),
            "fee_model": dict(fee_model or {}),
            "fee_model_name": dict(fee_model or {}).get("fee_model", "roundtrip_notional_pct"),
            "roundtrip_fee_pct": dict(fee_model or {}).get("roundtrip_fee_pct", 0.001),
            "decision_basis": {
                "uses_fee_aware_metrics": True,
                "raw_metrics_are_diagnostic_only": True,
                "primary_edge_fields": [
                    "a1_avg_net_mfe_r_15m",
                    "random_avg_net_mfe_r_15m",
                    "a1_net_hit_1r_rate_15m",
                    "random_net_hit_1r_rate_15m",
                ],
            },
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
        data_quality: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
        fee_model: Mapping[str, Any] | None = None,
    ) -> str:
        event_ts = [parse_float(row.get("event_ts")) for row in events if parse_float(row.get("event_ts")) > 0]
        symbols = ", ".join(sorted({str(row.get("symbol") or "") for row in events if row.get("symbol")})) or "UNKNOWN"
        overall_random = next((row for row in random_summary if row.get("dimension") == "ALL"), {})
        best_groups = sorted(random_summary, key=lambda row: _metric(row, "net_mfe_edge_15m", "mfe_edge_15m"), reverse=True)[:5]
        worst_groups = sorted(random_summary, key=lambda row: _metric(row, "net_mfe_edge_15m", "mfe_edge_15m"))[:5]
        best_hyp = sorted(hypothesis_summary, key=lambda row: parse_float(row.get("avg_realized_r_proxy")), reverse=True)[:5]
        worst_hyp = sorted(hypothesis_summary, key=lambda row: parse_float(row.get("avg_realized_r_proxy")))[:5]
        buy_15 = [r for r in forward_metrics if r.get("direction") == "BUY" and parse_int(r.get("window_sec")) == 900]
        sell_15 = [r for r in forward_metrics if r.get("direction") == "SELL" and parse_int(r.get("window_sec")) == 900]
        meta = dict(metadata or {})
        fee = dict(fee_model or {})
        events_hash = str(meta.get("events_file_sha256") or "")
        klines_hash = str(meta.get("klines_file_sha256") or "")
        git_commit = str(meta.get("git_commit") or "UNKNOWN")
        analysis_params = dict(meta.get("analysis_parameters") or {})
        roundtrip_fee_pct = fee.get("roundtrip_fee_pct", analysis_params.get("roundtrip_fee_pct", 0.001))
        fee_model_name = fee.get("fee_model", "roundtrip_notional_pct")
        lines = [
            "# A1 Edge Validation Report",
            "",
            "## Reproducibility Metadata",
            "",
            f"- events_file_name: {meta.get('events_file_name', 'UNKNOWN')}",
            f"- events_file_sha256: {events_hash[:12] if events_hash else 'UNKNOWN'}",
            f"- events_file_line_count: {meta.get('events_file_line_count', 'UNKNOWN')}",
            f"- klines_file_name: {meta.get('klines_file_name', 'UNKNOWN')}",
            f"- klines_file_sha256: {klines_hash[:12] if klines_hash else 'UNKNOWN'}",
            f"- klines_file_row_count: {meta.get('klines_file_row_count', 'UNKNOWN')}",
            f"- git_commit: {git_commit[:12] if git_commit and git_commit != 'UNKNOWN' else 'UNKNOWN'}",
            f"- kline_timezone: {analysis_params.get('kline_timezone', 'UNKNOWN')}",
            f"- roundtrip_fee_pct: {roundtrip_fee_pct}",
            f"- fee_model: {fee_model_name}",
            "",
            "## Run Summary",
            "",
            f"- total A1 events: {report.total_a1_events}",
            f"- total random baseline samples: {report.total_random_baseline_samples}",
            f"- total hypothesis rows: {report.total_hypothesis_rows}",
            f"- time range: {(min(event_ts) if event_ts else 0.0)} - {(max(event_ts) if event_ts else 0.0)}",
            f"- symbol: {symbols}",
            f"- sample sufficiency: {report.decision != 'INSUFFICIENT_SAMPLE'}",
            f"- decision basis: fee-aware net R; raw metrics are diagnostic only",
            "",
            "## Fee Model",
            "",
            f"- roundtrip_fee_pct: {roundtrip_fee_pct}",
            "- Raw +1R only means price moved one risk unit before fees.",
            "- Net +1R requires price movement to exceed 1R plus fee_share_r.",
            "- 裸 +1R 只是价格走了一个风险单位，不代表扣费后赚到 +1R。",
            "- 净 +1R 必须覆盖 1R + 手续费折算成的 R。",
            "",
            "## Data Quality",
            "",
            f"- kline_min_ts: {data_quality.get('kline_min_ts', 0.0)}",
            f"- kline_max_ts: {data_quality.get('kline_max_ts', 0.0)}",
            f"- event_min_ts: {data_quality.get('event_min_ts', 0.0)}",
            f"- event_max_ts: {data_quality.get('event_max_ts', 0.0)}",
            f"- events_outside_kline_range: {data_quality.get('events_outside_kline_range', 0)}",
            f"- events_insufficient_15m_count: {data_quality.get('events_insufficient_15m_count', 0)}",
            f"- events_insufficient_60m_count: {data_quality.get('events_insufficient_60m_count', 0)}",
            f"- valid_events_15m_count: {data_quality.get('valid_events_15m_count', 0)}",
            f"- valid_events_60m_count: {data_quality.get('valid_events_60m_count', 0)}",
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
                "### Fee-Aware Metrics",
                "",
                f"- roundtrip_fee_pct: {roundtrip_fee_pct}",
                f"- avg_fee_share_r_15m: {overall_random.get('a1_avg_fee_share_r_15m', 0)}",
                f"- a1_avg_net_mfe_r_15m: {overall_random.get('a1_avg_net_mfe_r_15m', 0)}",
                f"- random_avg_net_mfe_r_15m: {overall_random.get('random_avg_net_mfe_r_15m', 0)}",
                f"- net_mfe_edge_15m: {overall_random.get('net_mfe_edge_15m', 0)}",
                f"- a1_net_hit_1r_rate_15m: {overall_random.get('a1_net_hit_1r_rate_15m', 0)}",
                f"- random_net_hit_1r_rate_15m: {overall_random.get('random_net_hit_1r_rate_15m', 0)}",
                f"- net_hit_1r_edge_15m: {overall_random.get('net_hit_1r_edge_15m', 0)}",
                "",
                "### Raw Metrics",
                "",
                "- raw metrics are diagnostic only",
                f"- a1_avg_raw_mfe_r_15m: {overall_random.get('a1_avg_raw_mfe_r_15m', overall_random.get('a1_avg_mfe_r_15m', 0))}",
                f"- random_avg_raw_mfe_r_15m: {overall_random.get('random_avg_raw_mfe_r_15m', overall_random.get('random_avg_mfe_r_15m', 0))}",
                f"- raw_mfe_edge_15m: {overall_random.get('raw_mfe_edge_15m', overall_random.get('mfe_edge_15m', 0))}",
                f"- volatility edge: {overall_random.get('volatility_edge_15m', 0)}",
                "",
                "### Best Groups",
                "",
            ]
        )
        lines.extend(self._summary_table(best_groups, ["dimension", "group", "edge_label", "net_mfe_edge_15m", "net_hit_1r_edge_15m"]))
        lines.extend(["### Worst Groups", ""])
        lines.extend(self._summary_table(worst_groups, ["dimension", "group", "edge_label", "net_mfe_edge_15m", "net_hit_1r_edge_15m"]))
        lines.extend(
            [
                "## Directionality",
                "",
                f"- BUY A1 forward net MFE 15m: {_avg(buy_15, 'net_directional_mfe_r')}",
                f"- SELL A1 forward net MFE 15m: {_avg(sell_15, 'net_directional_mfe_r')}",
                f"- raw BUY A1 forward MFE 15m: {_avg(buy_15, 'directional_mfe_r')}",
                f"- raw SELL A1 forward MFE 15m: {_avg(sell_15, 'directional_mfe_r')}",
                f"- net hit +1R rate 15m: {_rate([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'net_hit_plus_1r')}",
                f"- first hit -1R rate 15m: {_rate([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'first_hit_minus_1r')}",
                "",
                "## Volatility Expansion",
                "",
                f"- total range after A1 15m: {_avg([r for r in forward_metrics if parse_int(r.get('window_sec')) == 900], 'total_range_u')}",
                f"- A1 vs random total range 15m: {overall_random.get('a1_avg_total_range_15m', 0)} / {overall_random.get('random_avg_total_range_15m', 0)}",
                "",
                "## Hypothesis Results",
                "",
                "- avg_realized_r_proxy is fee-aware.",
                "- hit_1r / hit_2r / hit_3r are raw hit diagnostics; net_hit_* fields are fee-aware.",
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
