#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import safe_float
from .scorer import IcebergTruthScorer


GRID_START = [150000, 300000, 500000, 700000, 900000]
GRID_HIDDEN = [500000, 750000, 1000000, 1500000, 2000000]
GRID_ABS = [0.50, 0.60, 0.70, 0.80, 0.90]
GRID_DEPTH = [150000, 300000, 500000, 700000, 900000]


class Phase1TruthAnalyzer:
    def __init__(self, min_sample: int = 30) -> None:
        self.min_sample = int(min_sample)

    def load_jsonl(self, path: str | Path) -> list[dict[str, Any]]:
        p = Path(path)
        records: list[dict[str, Any]] = []
        if not p.exists() or p.stat().st_size == 0:
            return records
        with p.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    value = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSONL at {p}:{line_no}: {exc}") from exc
                if isinstance(value, dict):
                    records.append(value)
        return records

    def analyze_file(self, events_path: str | Path, out_dir: str | Path) -> dict[str, Any]:
        records = self.load_jsonl(events_path)
        return self.export(records, out_dir)

    def export(self, records: list[dict[str, Any]], out_dir: str | Path) -> dict[str, Any]:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        settled = [r for r in records if r.get("record_type") == "candidate_settled"]
        finalized = [normalize_record(r) for r in records if r.get("record_type") == "candidate_finalized"]
        summary = self.summary(records, settled, finalized)
        self._write_csv(out / "phase1_candidate_events.csv", finalized, candidate_fields(finalized))
        self._write_csv(out / "phase1_truth_by_result.csv", self.group_rows(finalized, "result"), group_fields())
        self._write_csv(out / "phase1_truth_by_session.csv", self.group_rows(finalized, "session_tag"), group_fields("session_tag"))
        self._write_csv(out / "phase1_truth_by_quality.csv", self.group_rows(finalized, "quality"), group_fields("quality"))
        self._write_csv(out / "phase1_truth_by_behavior.csv", self.group_rows(finalized, "behavior"), group_fields("behavior"))
        grid = self.parameter_grid(finalized)
        self._write_csv(out / "phase1_parameter_grid.csv", grid, parameter_grid_fields())
        grid_by_session = self.parameter_grid_by_session(finalized)
        self._write_csv(out / "phase1_parameter_grid_by_session.csv", grid_by_session, ["session_tag"] + parameter_grid_fields())
        self._write_json(out / "phase1_dynamic_preview_summary.json", self.dynamic_preview_summary(finalized, grid))
        self._write_summary_md(out / "phase1_truth_summary.md", summary)
        return summary

    def summary(
        self,
        records: list[Mapping[str, Any]],
        settled: list[Mapping[str, Any]],
        finalized: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        by_result = {row["result"]: row for row in self.group_rows(finalized, "result")}
        iceberg = by_result.get("ICEBERG", {})
        ignore = by_result.get("IGNORE", {})
        spoofing = by_result.get("SPOOFING", {})
        cancel = by_result.get("CANCEL", {})
        result_distribution = dict(Counter(str(r.get("result") or "UNKNOWN") for r in finalized))
        summary = {
            "candidate_total": len(records),
            "settled_total": len(settled),
            "finalized_total": len(finalized),
            "settled_but_not_finalized_total": max(0, len(settled) - len(finalized)),
            "result_distribution": result_distribution,
            "direction_distribution": dict(Counter(str(r.get("direction") or "UNKNOWN") for r in finalized)),
            "session_distribution": dict(Counter(str(r.get("session_tag") or "UNKNOWN") for r in finalized)),
            "quality_distribution": dict(Counter(str(r.get("quality") or "UNKNOWN") for r in finalized)),
            "behavior_distribution": dict(Counter(str(r.get("behavior") or "UNKNOWN") for r in finalized)),
            "by_result": by_result,
            "current_iceberg_precision_proxy": safe_float(iceberg.get("pct_score_ge_65")),
            "current_iceberg_high_conf_proxy": safe_float(iceberg.get("pct_score_ge_80")),
            "missed_high_score_proxy": {
                "count": int(safe_float(ignore.get("count")) * safe_float(ignore.get("pct_score_ge_80"))),
                "ratio": safe_float(ignore.get("pct_score_ge_80")),
            },
            "iceberg_high_score_ratio": safe_float(iceberg.get("pct_score_ge_65")),
            "ignore_high_score_ratio": safe_float(ignore.get("pct_score_ge_65")),
            "spoofing_high_score_ratio": safe_float(spoofing.get("pct_score_ge_65")),
            "cancel_high_score_ratio": safe_float(cancel.get("pct_score_ge_65")),
        }
        summary["too_loose_warning"] = (
            safe_float(iceberg.get("avg_truth_score")) < 50.0
            or safe_float(iceberg.get("pct_score_ge_65")) < 0.35
        )
        summary["too_strict_warning"] = safe_float(ignore.get("pct_score_ge_80")) > 0.10
        return summary

    def group_rows(self, records: Iterable[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for record in records:
            groups[str(record.get(field) or "UNKNOWN")].append(record)
        rows = []
        for key in sorted(groups):
            scores = [truth_score(r) for r in groups[key]]
            row = score_stats(scores)
            row[field] = key
            rows.append(row)
        return rows

    def parameter_grid(self, records: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        high_truth_keys = {
            str(r.get("event_key"))
            for r in records
            if truth_score(r) >= 80
        }
        for start in GRID_START:
            for hidden in GRID_HIDDEN:
                for abs_rate in GRID_ABS:
                    for depth in GRID_DEPTH:
                        selected = [
                            r for r in records
                            if safe_float(r.get("active_notional")) >= start
                            and safe_float(r.get("hidden_volume")) >= hidden
                            and safe_float(r.get("absorption_rate")) >= abs_rate
                            and safe_float(r.get("start_thickness_usdt")) >= depth
                        ]
                        selected_keys = {str(r.get("event_key")) for r in selected}
                        scores = [truth_score(r) for r in selected]
                        row = {
                            "min_event_start_notional_usdt": start,
                            "min_hidden_notional_usdt": hidden,
                            "min_absorption_rate": abs_rate,
                            "min_local_depth_usdt": depth,
                            **score_stats(scores, selected_count_name="selected_count"),
                            "selected_iceberg_count": count_result(selected, "ICEBERG"),
                            "selected_ignore_count": count_result(selected, "IGNORE"),
                            "selected_spoofing_count": count_result(selected, "SPOOFING"),
                            "selected_cancel_count": count_result(selected, "CANCEL"),
                            "missed_high_truth_count": len(high_truth_keys - selected_keys),
                        }
                        row["score_label"] = self.parameter_score_label(row)
                        rows.append(row)
        return rows

    def parameter_grid_by_session(self, records: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        sessions = sorted({str(r.get("session_tag") or "UNKNOWN") for r in records})
        for session in sessions:
            session_records = [r for r in records if str(r.get("session_tag") or "UNKNOWN") == session]
            for row in self.parameter_grid(session_records):
                row = dict(row)
                row["session_tag"] = session
                rows.append(row)
        return rows

    def parameter_score_label(self, row: Mapping[str, Any]) -> str:
        if safe_float(row.get("selected_count")) < self.min_sample:
            return "INSUFFICIENT_SAMPLE"
        if safe_float(row.get("pct_truth_ge_80")) >= 0.35 and safe_float(row.get("avg_truth_score")) >= 65:
            return "HIGH_QUALITY_PARAM_SET"
        if safe_float(row.get("pct_truth_ge_65")) >= 0.50 and safe_float(row.get("avg_truth_score")) >= 60:
            return "PROMISING_PARAM_SET"
        return "WEAK_PARAM_SET"

    def dynamic_preview_summary(self, records: list[Mapping[str, Any]], grid: list[Mapping[str, Any]]) -> dict[str, Any]:
        promising = [r for r in grid if r.get("score_label") in {"HIGH_QUALITY_PARAM_SET", "PROMISING_PARAM_SET"}]
        return {
            "mode": "preview_only",
            "active_params_source": "static",
            "dynamic_params_active": False,
            "finalized_count": len(records),
            "promising_param_set_count": len(promising),
            "top_param_sets": sorted(
                promising,
                key=lambda r: (safe_float(r.get("avg_truth_score")), safe_float(r.get("selected_count"))),
                reverse=True,
            )[:10],
        }

    @staticmethod
    def _write_csv(path: Path, rows: list[Mapping[str, Any]], fieldnames: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({name: row.get(name, "") for name in fieldnames})

    @staticmethod
    def _write_json(path: Path, data: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(dict(data), f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")

    @staticmethod
    def _write_summary_md(path: Path, summary: Mapping[str, Any]) -> None:
        lines = [
            "# V6.3.11 Phase1 Truth Summary",
            "",
            f"- candidate_total: {summary.get('candidate_total')}",
            f"- settled_total: {summary.get('settled_total')}",
            f"- finalized_total: {summary.get('finalized_total')}",
            f"- settled_but_not_finalized_total: {summary.get('settled_but_not_finalized_total')}",
            f"- current_iceberg_precision_proxy: {summary.get('current_iceberg_precision_proxy')}",
            f"- current_iceberg_high_conf_proxy: {summary.get('current_iceberg_high_conf_proxy')}",
            f"- too_loose_warning: {summary.get('too_loose_warning')}",
            f"- too_strict_warning: {summary.get('too_strict_warning')}",
            "",
            "## Result Distribution",
            "",
        ]
        for key, value in dict(summary.get("result_distribution") or {}).items():
            lines.append(f"- {key}: {value}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(record)
    session = row.get("session")
    if isinstance(session, Mapping):
        for key in ("timezone", "local_time", "session_tag", "is_weekend"):
            row.setdefault(key, session.get(key))
    score = row.get("truth_score")
    if isinstance(score, Mapping):
        row.setdefault("truth_score_total", score.get("truth_score_total"))
        row.setdefault("truth_label", score.get("truth_label"))
    return row


def truth_score(record: Mapping[str, Any]) -> float:
    score = record.get("truth_score")
    if isinstance(score, Mapping):
        return safe_float(score.get("truth_score_total"))
    return safe_float(record.get("truth_score_total", record.get("truth_score")))


def score_stats(scores: list[float], selected_count_name: str = "count") -> dict[str, Any]:
    if not scores:
        return {
            selected_count_name: 0,
            "avg_truth_score": 0.0,
            "median_truth_score": 0.0,
            "p75_truth_score": 0.0,
            "p90_truth_score": 0.0,
            "pct_score_ge_65": 0.0,
            "pct_score_ge_80": 0.0,
            "pct_truth_ge_65": 0.0,
            "pct_truth_ge_80": 0.0,
            "pct_truth_lt_50": 0.0,
        }
    values = sorted(float(s) for s in scores)
    count = len(values)
    p75 = percentile(values, 0.75)
    p90 = percentile(values, 0.90)
    ge65 = sum(1 for s in values if s >= 65) / count
    ge80 = sum(1 for s in values if s >= 80) / count
    lt50 = sum(1 for s in values if s < 50) / count
    return {
        selected_count_name: count,
        "avg_truth_score": round(sum(values) / count, 4),
        "median_truth_score": round(statistics.median(values), 4),
        "p75_truth_score": round(p75, 4),
        "p90_truth_score": round(p90, 4),
        "pct_score_ge_65": round(ge65, 6),
        "pct_score_ge_80": round(ge80, 6),
        "pct_truth_ge_65": round(ge65, 6),
        "pct_truth_ge_80": round(ge80, 6),
        "pct_truth_lt_50": round(lt50, 6),
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    idx = (len(values) - 1) * pct
    lower = int(idx)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    ratio = idx - lower
    return values[lower] * (1 - ratio) + values[upper] * ratio


def count_result(records: Iterable[Mapping[str, Any]], result: str) -> int:
    return sum(1 for r in records if str(r.get("result") or "").upper() == result)


def candidate_fields(records: list[Mapping[str, Any]]) -> list[str]:
    base = [
        "schema_version", "record_type", "event_key", "symbol", "direction", "result",
        "behavior", "quality", "cancel_reason", "trigger_ts", "settle_ts", "wait_ms",
        "trigger_price", "settle_price", "zone_lower", "zone_upper", "active_notional",
        "hidden_volume", "absorption_rate", "truth_score_total", "truth_label",
        "session_tag", "is_weekend",
    ]
    extras = sorted({k for r in records for k in r.keys() if isinstance(k, str) and k not in base and not isinstance(r.get(k), (dict, list))})
    return base + extras


def group_fields(group_name: str = "result") -> list[str]:
    return [
        group_name, "count", "avg_truth_score", "median_truth_score",
        "p75_truth_score", "p90_truth_score", "pct_score_ge_65", "pct_score_ge_80",
        "pct_truth_ge_65", "pct_truth_ge_80", "pct_truth_lt_50",
    ]


def parameter_grid_fields() -> list[str]:
    return [
        "min_event_start_notional_usdt", "min_hidden_notional_usdt", "min_absorption_rate",
        "min_local_depth_usdt", "selected_count", "avg_truth_score", "median_truth_score",
        "pct_truth_ge_65", "pct_truth_ge_80", "pct_truth_lt_50", "selected_iceberg_count",
        "selected_ignore_count", "selected_spoofing_count", "selected_cancel_count",
        "missed_high_truth_count", "score_label",
    ]
