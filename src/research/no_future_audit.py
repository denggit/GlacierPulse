#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from src.research.a1_edge.schema import parse_bool, parse_float
from src.research.field_registry import (
    DEPRECATED_LOOKAHEAD_ALIASES,
    field_hygiene_summary,
    infer_availability,
    is_deprecated_lookahead_alias,
)


INVALID_LOOKAHEAD = "INVALID_LOOKAHEAD"


def validate_entry_conditions(fields: list[str]) -> None:
    invalid = invalid_entry_fields(fields)
    if invalid:
        raise ValueError(f"{INVALID_LOOKAHEAD}: strategy entry uses non-runtime fields: {', '.join(invalid)}")


def invalid_entry_fields(fields: list[str]) -> list[str]:
    invalid: list[str] = []
    for field in fields or []:
        name = str(field)
        availability = infer_availability(name)
        if availability in {"future", "offline", "sim"} or is_deprecated_lookahead_alias(name):
            invalid.append(name)
    return invalid


def validate_trade_row(row: Mapping[str, Any]) -> dict[str, Any]:
    fields = _row_condition_fields(row)
    invalid = invalid_entry_fields(fields)
    entry_ts = parse_float(row.get("entry_ts") or row.get("a3_entry_rt_ts"))
    available_ts = parse_float(row.get("condition_available_ts_max"))
    if available_ts > 0 and entry_ts > 0 and available_ts > entry_ts:
        invalid.append("condition_available_ts_max")
    out = dict(row)
    out["uses_future_field_flag"] = bool(invalid) or parse_bool(out.get("uses_future_field_flag"))
    existing = [part.strip() for part in str(out.get("future_field_names") or "").replace(",", "|").split("|") if part.strip()]
    out["future_field_names"] = "|".join(sorted(set([*existing, *invalid])))
    if invalid:
        out["audit_status"] = INVALID_LOOKAHEAD
    else:
        out.setdefault("audit_status", "OK")
    return out


def audit_report_schema(fieldnames: list[str]) -> dict[str, Any]:
    summary = field_hygiene_summary(list(fieldnames or []))
    disallowed = invalid_entry_fields([name for name in fieldnames or [] if _looks_like_entry_condition(name)])
    summary["entry_schema_invalid_fields"] = disallowed
    summary["entry_schema_valid"] = not disallowed
    return summary


def _row_condition_fields(row: Mapping[str, Any]) -> list[str]:
    raw = row.get("entry_condition_fields") or row.get("condition_fields") or ""
    if isinstance(raw, str):
        return [part.strip() for part in raw.replace(",", "|").split("|") if part.strip()]
    if isinstance(raw, (list, tuple, set)):
        return [str(part).strip() for part in raw if str(part).strip()]
    return []


def _looks_like_entry_condition(name: str) -> bool:
    text = str(name)
    if text in DEPRECATED_LOOKAHEAD_ALIASES:
        return True
    return text.startswith(("a2_rt_", "a3_entry_rt_", "a1_vp_", "vp24h_a1_vp_"))
