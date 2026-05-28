#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float


COMBO_KEY_FIELDS = [
    "a1_primary_evidence_type",
    "a1_evidence_types",
    "a1_strength_tier",
    "a1_best_horizon",
    "a2_accumulation_path_v2",
    "a3_aggression_type_v2",
    "entry_model",
    "stop_model",
    "target_r",
    "market_context_bucket",
    "direction",
]

COMBO_METRIC_FIELDS = [
    "count",
    "count_per_day",
    "avg_realized_r",
    "median_realized_r",
    "sum_positive_r",
    "sum_negative_r_abs",
    "profit_factor_proxy",
    "fee_positive_rate",
    "target_first_rate",
    "stop_first_rate",
    "ambiguous_rate",
    "avg_fee_share_r",
    "avg_risk_u",
    "avg_mfe_r",
    "avg_mae_r",
    "complete_rate",
]
INVALID_SIMULATED_OUTCOMES = {
    "NO_ENTRY",
    "INVALID_STOP",
    "UNKNOWN_ENTRY_MODEL",
    "UNKNOWN_STOP_MODEL",
    "STRUCTURAL_PROXY_UNAVAILABLE",
    "ZONE_BOUNDARY_V2_FALLBACK_STRUCTURAL_PROXY_UNAVAILABLE",
}


def combo_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(_key_value(row, field) for field in COMBO_KEY_FIELDS)


def build_combo_matrix(trades: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades or []:
        if not is_valid_simulated_trade(trade):
            continue
        groups[combo_key(trade)].append(trade)
    rows = []
    for key, group in groups.items():
        row = {field: key[idx] for idx, field in enumerate(COMBO_KEY_FIELDS)}
        row.update(group_stats(group))
        rows.append(row)
    rows.sort(key=lambda r: (parse_float(r.get("avg_realized_r")), parse_float(r.get("profit_factor_proxy")), parse_float(r.get("count"))), reverse=True)
    return rows


def is_valid_simulated_trade(trade: Mapping[str, Any]) -> bool:
    return (
        parse_float(trade.get("target_r")) >= 1.0
        and parse_float(trade.get("entry_ts")) > 0
        and parse_float(trade.get("entry_price")) > 0
        and parse_float(trade.get("risk_u")) > 0
        and str(trade.get("realized_outcome_1h") or "").upper() not in INVALID_SIMULATED_OUTCOMES
    )


def group_stats(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    values = [parse_float(row.get("realized_r_1h")) for row in rows]
    positive = [v for v in values if v > 0]
    negative = [abs(v) for v in values if v < 0]
    first_ts = min([parse_float(row.get("entry_ts")) for row in rows if parse_float(row.get("entry_ts")) > 0] or [0.0])
    last_ts = max([parse_float(row.get("entry_ts")) for row in rows if parse_float(row.get("entry_ts")) > 0] or [0.0])
    days = max((last_ts - first_ts) / 86400.0, 1.0) if first_ts and last_ts else 1.0
    return {
        "count": len(rows),
        "count_per_day": round(len(rows) / days, 8),
        "avg_realized_r": round(sum(values) / len(values), 8) if values else 0.0,
        "median_realized_r": round(statistics.median(values), 8) if values else 0.0,
        "sum_positive_r": round(sum(positive), 8),
        "sum_negative_r_abs": round(sum(negative), 8),
        "profit_factor_proxy": round(sum(positive) / sum(negative), 8) if negative else (round(sum(positive), 8) if positive else 0.0),
        "fee_positive_rate": _rate(rows, lambda row: parse_float(row.get("realized_r_1h")) > 0),
        "target_first_rate": _rate(rows, lambda row: parse_bool(row.get("target_first_flag"))),
        "stop_first_rate": _rate(rows, lambda row: parse_bool(row.get("stop_first_flag"))),
        "ambiguous_rate": _rate(rows, lambda row: parse_bool(row.get("ambiguous_flag"))),
        "avg_fee_share_r": _avg(rows, "fee_share_r"),
        "avg_risk_u": _avg(rows, "risk_u"),
        "avg_mfe_r": _avg(rows, "mfe_r_1h"),
        "avg_mae_r": _avg(rows, "mae_r_1h"),
        "complete_rate": _rate(rows, lambda row: parse_bool(row.get("complete_flag"))),
    }


def top_combos(matrix: list[Mapping[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    min_sample = int(getattr(cfg, "V7_3A_MIN_SAMPLE", 10))
    cap = int(limit or getattr(cfg, "V7_3A_TOP_COMBO_LIMIT", 100))
    filtered = [dict(row) for row in matrix if int(parse_float(row.get("count"))) >= min_sample]
    filtered.sort(
        key=lambda row: (
            parse_float(row.get("avg_realized_r")),
            parse_float(row.get("profit_factor_proxy")),
            parse_float(row.get("count_per_day")),
        ),
        reverse=True,
    )
    return filtered[:cap]


def bad_combos(matrix: list[Mapping[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    min_sample = int(getattr(cfg, "V7_3A_MIN_SAMPLE", 10))
    cap = int(limit or getattr(cfg, "V7_3A_TOP_COMBO_LIMIT", 100))
    rows = [
        dict(row) for row in matrix
        if int(parse_float(row.get("count"))) >= min_sample
        and parse_float(row.get("avg_realized_r")) < 0
        and parse_float(row.get("profit_factor_proxy")) < 1
        and parse_float(row.get("fee_positive_rate")) < 0.4
    ]
    rows.sort(key=lambda row: (parse_float(row.get("avg_realized_r")), parse_float(row.get("profit_factor_proxy"))))
    return rows[:cap]


def combo_summary(matrix: list[Mapping[str, Any]], trades: list[Mapping[str, Any]]) -> dict[str, Any]:
    top = top_combos(matrix, limit=10)
    bad = bad_combos(matrix, limit=10)
    positive = [row for row in matrix if parse_float(row.get("avg_realized_r")) > 0 and int(parse_float(row.get("count"))) >= int(getattr(cfg, "V7_3A_MIN_SAMPLE", 10))]
    return {
        "v7_enabled": bool(getattr(cfg, "V7_3A_SIMULATOR_ENABLED", True)),
        "v7_3a_simulated_trade_count": len([t for t in trades if parse_float(t.get("target_r")) >= 1.0]),
        "v7_top_combo_count": len(top_combos(matrix)),
        "v7_positive_combo_count": len(positive),
        "v7_bad_combo_count": len(bad_combos(matrix)),
        "top_3a_combos_by_realized_r": _compact_top(top),
        "top_3a_combos_by_profit_factor": _compact_top(sorted(top, key=lambda row: parse_float(row.get("profit_factor_proxy")), reverse=True)[:10]),
        "top_3a_combos_by_frequency": _compact_top(sorted(top, key=lambda row: parse_float(row.get("count_per_day")), reverse=True)[:10]),
        "bad_3a_combos_to_delete": _compact_top(bad),
    }


def _compact_top(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows[:10]:
        result.append(
            {
                **{field: row.get(field, "") for field in COMBO_KEY_FIELDS},
                "count": int(parse_float(row.get("count"))),
                "avg_realized_r": parse_float(row.get("avg_realized_r")),
                "profit_factor_proxy": parse_float(row.get("profit_factor_proxy")),
                "fee_positive_rate": parse_float(row.get("fee_positive_rate")),
            }
        )
    return result


def _key_value(row: Mapping[str, Any], field: str) -> Any:
    value = row.get(field)
    if value in (None, ""):
        return "UNKNOWN"
    if field == "target_r":
        return parse_float(value)
    return value


def _avg(rows: list[Mapping[str, Any]], field: str) -> float:
    values = [parse_float(row.get(field)) for row in rows if row.get(field) not in (None, "")]
    return round(sum(values) / len(values), 8) if values else 0.0


def _rate(rows: list[Mapping[str, Any]], predicate) -> float:
    return round(sum(1 for row in rows if predicate(row)) / len(rows), 8) if rows else 0.0


def day_bucket(ts: float) -> str:
    if ts <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")
