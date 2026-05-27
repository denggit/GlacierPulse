#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float, parse_timestamp

from .models import first_present


EVIDENCE_PRIORITY = (
    "LADDER_ABSORPTION",
    "CLUSTER_ABSORPTION",
    "HIDDEN_RELOAD_ICEBERG",
    "VISIBLE_WALL_ABSORPTION",
    "FAILED_WALL",
    "SPOOFING_WITHDRAWAL",
)


def classify_a1_evidence_event(
    record: Mapping[str, Any],
    *,
    current_price: float = 0.0,
) -> dict[str, Any]:
    result = str(record.get("result") or "").upper()
    behavior = str(record.get("behavior") or "").upper()
    direction = str(record.get("direction") or "").upper()
    active = parse_float(record.get("active_notional"), parse_float(record.get("active_volume")))
    start = parse_float(record.get("start_thickness_usdt"))
    end = parse_float(record.get("end_thickness_usdt"))
    if end <= 0 and start > 0 and record.get("book_reduction") not in (None, ""):
        end = max(0.0, start - parse_float(record.get("book_reduction")))
    book_reduction = parse_float(record.get("book_reduction"), max(0.0, start - end))
    trigger = parse_float(record.get("trigger_price"))
    settle = parse_float(first_present(record, "settle_price", "current_price"), current_price)
    min_trade = parse_float(record.get("min_trade_price"), trigger)
    max_trade = parse_float(record.get("max_trade_price"), trigger)

    consumption = book_reduction / active if active > 0 else 0.0
    survival = end / start if start > 0 else 0.0
    withdrawal_excess = max(0.0, book_reduction - active)
    withdrawal_ratio = withdrawal_excess / active if active > 0 else 0.0
    absorbed_proxy = min(active, max(0.0, book_reduction))

    visible = (
        start >= float(getattr(cfg, "A1_VISIBLE_WALL_MIN_START_DEPTH_USDT", 500000.0))
        and active >= float(getattr(cfg, "A1_VISIBLE_WALL_MIN_ACTIVE_NOTIONAL_USDT", 300000.0))
        and book_reduction >= 0
        and float(getattr(cfg, "A1_VISIBLE_WALL_MIN_CONSUMPTION_RATIO", 0.45)) <= consumption <= float(getattr(cfg, "A1_VISIBLE_WALL_MAX_CONSUMPTION_RATIO", 1.35))
        and withdrawal_ratio <= float(getattr(cfg, "A1_VISIBLE_WALL_MAX_WITHDRAWAL_EXCESS_RATIO", 0.35))
        and survival >= float(getattr(cfg, "A1_VISIBLE_WALL_MIN_SURVIVAL_RATIO", 0.20))
        and not _price_broke_far(direction, trigger, settle, min_trade, max_trade)
    )
    spoofing = (
        "SPOOFING" in result
        or "SPOOFING_WITHDRAWAL" in behavior
        or (
            active > 0
            and book_reduction > active
            and withdrawal_ratio > float(getattr(cfg, "A1_VISIBLE_WALL_MAX_WITHDRAWAL_EXCESS_RATIO", 0.35))
        )
    )
    failed = (
        (result == "CANCEL" and "PRICE_BROKE" in str(record.get("cancel_reason") or "").upper())
        or (
            start > 0
            and active > 0
            and consumption >= float(getattr(cfg, "A1_VISIBLE_WALL_MIN_CONSUMPTION_RATIO", 0.45))
            and survival < float(getattr(cfg, "A1_VISIBLE_WALL_MIN_SURVIVAL_RATIO", 0.20))
            and _price_broke_far(direction, trigger, settle, min_trade, max_trade)
        )
    )
    hidden = result == "ICEBERG"

    types = []
    if hidden:
        types.append("HIDDEN_RELOAD_ICEBERG")
    if visible:
        types.append("VISIBLE_WALL_ABSORPTION")
    if failed:
        types.append("FAILED_WALL")
    if spoofing:
        types.append("SPOOFING_WITHDRAWAL")

    return {
        "a1_evidence_v2_available": True,
        "hidden_reload_iceberg_flag": hidden,
        "hidden_reload_hidden_notional": parse_float(record.get("hidden_volume")),
        "hidden_reload_absorption_rate": parse_float(record.get("absorption_rate")),
        "hidden_reload_confidence": parse_float(record.get("confidence")),
        "visible_wall_absorption_flag": visible,
        "failed_wall_flag": failed,
        "spoofing_withdrawal_flag": spoofing,
        "visible_wall_start_depth_usdt": round(start, 8),
        "visible_wall_end_depth_usdt": round(end, 8),
        "visible_wall_book_reduction_usdt": round(book_reduction, 8),
        "visible_wall_consumption_ratio": round(consumption, 8),
        "visible_wall_survival_ratio": round(survival, 8),
        "visible_wall_withdrawal_excess_ratio": round(withdrawal_ratio, 8),
        "visible_wall_absorbed_notional_proxy": round(absorbed_proxy, 8),
        "a1_evidence_types": "|".join(types),
        "a1_primary_evidence_type": _primary(types),
        "a1_evidence_score_v2": _event_score(hidden, visible, failed, spoofing),
        "a1_evidence_v2_reason": "|".join(types) if types else "NO_A1_EVIDENCE_V2",
    }


def attach_a1_evidence_v2(
    row: Mapping[str, Any],
    pies: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    out = dict(row)
    pie_list = [dict(p) for p in pies or []]
    event_evidence = [classify_a1_evidence_event(p) for p in pie_list]
    hidden = any(parse_bool(e.get("hidden_reload_iceberg_flag")) for e in event_evidence)
    visible = any(parse_bool(e.get("visible_wall_absorption_flag")) for e in event_evidence)
    failed = any(parse_bool(e.get("failed_wall_flag")) for e in event_evidence)
    spoofing = any(parse_bool(e.get("spoofing_withdrawal_flag")) for e in event_evidence)
    cluster = compute_cluster_absorption(pie_list)
    ladder = compute_ladder_absorption(pie_list, event_evidence)

    types = set()
    for evidence in event_evidence:
        types.update(_split_types(evidence.get("a1_evidence_types")))
    if cluster["cluster_absorption_flag"]:
        types.add("CLUSTER_ABSORPTION")
    if ladder["ladder_absorption_flag"]:
        types.add("LADDER_ABSORPTION")

    out.update(
        {
            "a1_evidence_v2_available": bool(pie_list),
            "hidden_reload_iceberg_flag": hidden,
            "visible_wall_absorption_flag": visible,
            "cluster_absorption_flag": cluster["cluster_absorption_flag"],
            "ladder_absorption_flag": ladder["ladder_absorption_flag"],
            "failed_wall_flag": failed,
            "spoofing_withdrawal_flag": spoofing,
            "a1_evidence_types": "|".join(t for t in EVIDENCE_PRIORITY if t in types),
            "a1_primary_evidence_type": _primary(types),
            "a1_evidence_score_v2": _zone_score(types),
            "cluster_best_window_sec": cluster["cluster_best_window_sec"],
            "cluster_best_active_notional": cluster["cluster_best_active_notional"],
            "cluster_best_event_count": cluster["cluster_best_event_count"],
            "cluster_best_price_efficiency": cluster["cluster_best_price_efficiency"],
            "ladder_level_count": ladder["ladder_level_count"],
            "ladder_core_low": ladder["ladder_core_low"],
            "ladder_core_high": ladder["ladder_core_high"],
            "ladder_sweep_extreme": ladder["ladder_sweep_extreme"],
            "ladder_absorption_score": ladder["ladder_absorption_score"],
            "a1_evidence_v2_reason": "|".join(t for t in EVIDENCE_PRIORITY if t in types) if types else "NO_A1_EVIDENCE_V2",
        }
    )
    _copy_best_wall_metrics(out, event_evidence)
    return out


def compute_cluster_absorption(pies: list[Mapping[str, Any]]) -> dict[str, Any]:
    sorted_pies = sorted(pies, key=lambda p: parse_timestamp(first_present(p, "settle_ts", "trigger_ts")))
    best = {"window": 0, "active": 0.0, "count": 0, "eff": 0.0, "flag": False}
    windows = list(getattr(cfg, "A1_CLUSTER_WINDOWS_SEC", [3, 10, 30, 120]) or [3, 10, 30, 120])
    for window in windows:
        for idx, pie in enumerate(sorted_pies):
            start_ts = parse_timestamp(first_present(pie, "settle_ts", "trigger_ts"))
            if start_ts <= 0:
                continue
            group = [
                p for p in sorted_pies[idx:]
                if 0 <= parse_timestamp(first_present(p, "settle_ts", "trigger_ts")) - start_ts <= float(window)
            ]
            active = sum(parse_float(p.get("active_notional"), parse_float(p.get("active_volume"))) for p in group)
            prices = []
            for p in group:
                prices.extend([
                    parse_float(first_present(p, "min_trade_price", "trigger_price", "settle_price")),
                    parse_float(first_present(p, "max_trade_price", "trigger_price", "settle_price")),
                ])
            prices = [p for p in prices if p > 0]
            displacement = max(prices) - min(prices) if prices else 0.0
            eff = displacement / max(active / 1_000_000.0, 1.0)
            flag = (
                len(group) >= int(getattr(cfg, "A1_CLUSTER_MIN_EVENT_COUNT", 2))
                and active >= float(getattr(cfg, "A1_CLUSTER_MIN_ACTIVE_NOTIONAL_USDT", 1000000.0))
                and eff <= float(getattr(cfg, "A1_CLUSTER_MAX_PRICE_EFFICIENCY", 1.0))
            )
            if flag or active > best["active"]:
                best = {"window": int(window), "active": active, "count": len(group), "eff": eff, "flag": flag}
    return {
        "cluster_absorption_flag": bool(best["flag"]),
        "cluster_best_window_sec": int(best["window"]),
        "cluster_best_active_notional": round(float(best["active"]), 8),
        "cluster_best_event_count": int(best["count"]),
        "cluster_best_price_efficiency": round(float(best["eff"]), 8),
    }


def compute_ladder_absorption(
    pies: list[Mapping[str, Any]],
    event_evidence: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    bucket_size = max(float(getattr(cfg, "A1_LADDER_BUCKET_SIZE_U", 0.5)), 0.000001)
    active_by_bucket: defaultdict[int, float] = defaultdict(float)
    direction = str(first_present(pies[0], "direction") or "").upper() if pies else "UNKNOWN"
    eligible_keys = set()
    for idx, evidence in enumerate(event_evidence or []):
        if parse_bool(evidence.get("hidden_reload_iceberg_flag")) or parse_bool(evidence.get("visible_wall_absorption_flag")):
            eligible_keys.add(idx)
    if not eligible_keys and pies:
        eligible_keys = set(range(len(pies)))
    for idx, pie in enumerate(pies):
        if idx not in eligible_keys:
            continue
        low = parse_float(first_present(pie, "min_trade_price", "trigger_price", "settle_price"))
        high = parse_float(first_present(pie, "max_trade_price", "trigger_price", "settle_price"))
        price = low if direction == "BUY" else high or low
        if price <= 0:
            continue
        bucket = int(price / bucket_size)
        active_by_bucket[bucket] += parse_float(pie.get("active_notional"), parse_float(pie.get("active_volume")))
    longest = _longest_adjacent_run(sorted(active_by_bucket))
    level_count = len(longest)
    low = min(longest) * bucket_size if longest else 0.0
    high = (max(longest) + 1) * bucket_size if longest else 0.0
    active = sum(active_by_bucket[b] for b in longest)
    flag = level_count >= int(getattr(cfg, "A1_LADDER_MIN_LEVEL_COUNT", 3))
    return {
        "ladder_absorption_flag": flag,
        "ladder_level_count": level_count,
        "ladder_core_low": round(low, 8),
        "ladder_core_high": round(high, 8),
        "ladder_sweep_extreme": round(low if direction == "BUY" else high, 8),
        "ladder_absorption_score": round(level_count + min(active / 1_000_000.0, 5.0), 8) if flag else 0.0,
    }


def _price_broke_far(direction: str, trigger: float, settle: float, low: float, high: float) -> bool:
    if trigger <= 0:
        return False
    if direction == "BUY":
        probe = min(v for v in (settle, low) if v > 0) if any(v > 0 for v in (settle, low)) else trigger
        return probe < trigger - 1.5
    if direction == "SELL":
        probe = max(v for v in (settle, high) if v > 0) if any(v > 0 for v in (settle, high)) else trigger
        return probe > trigger + 1.5
    return False


def _primary(types: Iterable[str]) -> str:
    type_set = set(types or [])
    for evidence_type in EVIDENCE_PRIORITY:
        if evidence_type in type_set:
            return evidence_type
    return "NO_A1_EVIDENCE_V2"


def _event_score(hidden: bool, visible: bool, failed: bool, spoofing: bool) -> float:
    score = 0.0
    if hidden:
        score += 0.35
    if visible:
        score += 0.25
    if failed:
        score -= 0.25
    if spoofing:
        score -= 0.25
    return round(max(0.0, min(1.0, score)), 6)


def _zone_score(types: Iterable[str]) -> float:
    type_set = set(types or [])
    score = 0.0
    weights = {
        "LADDER_ABSORPTION": 0.30,
        "CLUSTER_ABSORPTION": 0.25,
        "HIDDEN_RELOAD_ICEBERG": 0.25,
        "VISIBLE_WALL_ABSORPTION": 0.20,
        "FAILED_WALL": -0.25,
        "SPOOFING_WITHDRAWAL": -0.20,
    }
    for evidence_type, weight in weights.items():
        if evidence_type in type_set:
            score += weight
    return round(max(0.0, min(1.0, score)), 6)


def _split_types(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").split("|") if part.strip()]


def _longest_adjacent_run(values: list[int]) -> list[int]:
    if not values:
        return []
    best = current = [values[0]]
    for value in values[1:]:
        if value == current[-1] + 1:
            current.append(value)
        else:
            if len(current) > len(best):
                best = current
            current = [value]
    return current if len(current) > len(best) else best


def _copy_best_wall_metrics(out: dict[str, Any], evidence: list[Mapping[str, Any]]) -> None:
    best = max(evidence or [{}], key=lambda e: parse_float(e.get("visible_wall_absorbed_notional_proxy")))
    for field in (
        "visible_wall_start_depth_usdt",
        "visible_wall_end_depth_usdt",
        "visible_wall_consumption_ratio",
        "visible_wall_survival_ratio",
        "visible_wall_withdrawal_excess_ratio",
        "visible_wall_absorbed_notional_proxy",
    ):
        out[field] = best.get(field, 0.0)
