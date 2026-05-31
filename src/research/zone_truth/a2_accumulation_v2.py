#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from src.research.a1_edge.schema import parse_bool


def classify_a2_accumulation_path_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    text = _reaction_text(row)
    failed = (
        parse_bool(row.get("a2_failed_reclaim_flag"))
        or parse_bool(row.get("has_failed_reclaim"))
        or "FAILED_RECLAIM" in text
        or "SWEEP_NO_RECLAIM" in text
    )
    if failed:
        return _result("A2_FAILED_RECLAIM", 0.0, "failed_reclaim")

    sweep = parse_bool(row.get("a2_sweep_flag")) or parse_bool(row.get("has_swept_boundary")) or "SWEEP" in text
    absorb = parse_bool(row.get("has_absorbed_after_sweep")) or parse_bool(row.get("hidden_reload_iceberg_flag")) or parse_bool(row.get("visible_wall_absorption_flag"))
    reclaim = parse_bool(row.get("a2_reclaim_flag")) or parse_bool(row.get("has_reclaimed_boundary")) or "RECLAIM" in text
    retest = parse_bool(row.get("a2_retest_flag")) or parse_bool(row.get("has_retested_inside_zone")) or "RETEST" in text
    if sweep and absorb and reclaim and retest:
        return _result("A2_SWEEP_RECLAIM_RETEST", 0.95, "sweep|absorb|reclaim|retest")
    if sweep and absorb and reclaim:
        return _result("A2_SWEEP_RECLAIM_NO_RETEST", 0.80, "sweep|absorb|reclaim")

    phase2_type = str(row.get("phase2_type") or row.get("reaction_type") or row.get("a1_reaction_type") or "").upper()
    if "BELOW_ZONE_ABSORPTION" in phase2_type or "BELOW_ZONE_ABSORPTION" in text:
        return _result("A2_BELOW_ZONE_ABSORPTION", 0.70, "below_zone_absorption")
    if parse_bool(row.get("has_clean_hold")) or parse_bool(row.get("a2_clean_hold_flag")) or "CLEAN_HOLD" in text:
        return _result("A2_CLEAN_HOLD", 0.75, "clean_hold")
    if parse_bool(row.get("ladder_absorption_flag")):
        return _result("A2_LADDER_HOLD", 0.65, "ladder_absorption")
    if parse_bool(row.get("visible_wall_absorption_flag")):
        return _result("A2_VISIBLE_WALL_HOLD", 0.60, "visible_wall_absorption")

    compression = str(row.get("a2_compression_state_future") or row.get("a2_pre_ignition_compression_state_future") or "").upper()
    if compression in {"COMPRESSING", "RANGING", "PRE_IGNITION_COMPRESSED", "PRE_IGNITION_RANGING"}:
        return _result("A2_COMPRESSION_FUTURE_PROXY", 0.55, compression.lower())
    if parse_bool(row.get("a2_pre_pool_eligible")):
        return _result("A2_NO_ACCUMULATION", 0.20, "a2_pre_pool_without_v2_path")
    return _result("A2_UNKNOWN", 0.0, "not_a2_pre_pool_or_missing_fields")


def attach_a2_accumulation_path_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.update(classify_a2_accumulation_path_v2(out))
    return out


def _reaction_text(row: Mapping[str, Any]) -> str:
    parts = [
        row.get("reaction_type"),
        row.get("a1_reaction_type"),
        row.get("final_reaction_type"),
        row.get("reaction_types"),
        row.get("a2_sweep_reclaim_quality"),
        row.get("a2_state"),
    ]
    return "|".join(str(part or "").upper() for part in parts)


def _result(path: str, score: float, reason: str) -> dict[str, Any]:
    return {
        "a2_accumulation_path_v2": path,
        "a2_accumulation_path_score_v2": round(float(score), 6),
        "a2_accumulation_path_reason_v2": reason,
    }
