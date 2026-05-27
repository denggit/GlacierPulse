#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
from typing import Any, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float


def bucket_price(price: float, bucket_size: float | None = None) -> float:
    size = max(float(bucket_size or getattr(cfg, "ZONE_BOUNDARY_V2_BUCKET_SIZE_U", 0.5)), 0.000001)
    return round(math.floor(float(price) / size) * size, 8)


def build_book_profile(
    book_levels: Mapping[Any, Any],
    lower: float,
    upper: float,
    *,
    bucket_size: float | None = None,
) -> dict[float, float]:
    size = max(float(bucket_size or getattr(cfg, "ZONE_BOUNDARY_V2_BUCKET_SIZE_U", 0.5)), 0.000001)
    profile: dict[float, float] = {}
    for raw_price, raw_size in (book_levels or {}).items():
        price = parse_float(raw_price)
        qty = parse_float(raw_size)
        if price <= 0 or qty <= 0 or price < lower or price > upper:
            continue
        bucket = bucket_price(price, size)
        profile[bucket] = profile.get(bucket, 0.0) + price * qty
    return {k: round(v, 8) for k, v in sorted(profile.items())}


def initialize_pending_event_profile(
    event: dict[str, Any],
    book_levels: Mapping[Any, Any],
    *,
    trigger_price: float | None = None,
) -> None:
    if not bool(getattr(cfg, "ZONE_BOUNDARY_V2_ENABLED", True)):
        return
    trigger = parse_float(trigger_price, parse_float(event.get("trigger_price")))
    scan = float(getattr(cfg, "ZONE_BOUNDARY_V2_SCAN_RANGE_U", 8.0))
    lower = trigger - scan
    upper = trigger + scan
    profile = build_book_profile(book_levels, lower, upper)
    event["observation_zone_lower"] = parse_float(event.get("zone_lower"))
    event["observation_zone_upper"] = parse_float(event.get("zone_upper"))
    event["trade_sweep_low"] = parse_float(event.get("min_trade_price"), trigger)
    event["trade_sweep_high"] = parse_float(event.get("max_trade_price"), trigger)
    event["trade_notional_by_bucket"] = {}
    event["book_profile_start"] = dict(profile)
    event["book_profile_min"] = dict(profile)
    event["book_profile_end"] = dict(profile)


def record_trade_bucket(event: dict[str, Any], price: float, active_notional: float) -> None:
    if not bool(getattr(cfg, "ZONE_BOUNDARY_V2_ENABLED", True)):
        return
    bucket = bucket_price(price)
    trade_map = event.setdefault("trade_notional_by_bucket", {})
    trade_map[bucket] = parse_float(trade_map.get(bucket)) + parse_float(active_notional)
    event["trade_sweep_low"] = min(parse_float(event.get("trade_sweep_low"), price), price)
    event["trade_sweep_high"] = max(parse_float(event.get("trade_sweep_high"), price), price)


def update_pending_event_profile(event: dict[str, Any], book_levels: Mapping[Any, Any]) -> None:
    if not bool(getattr(cfg, "ZONE_BOUNDARY_V2_ENABLED", True)):
        return
    if "book_profile_start" not in event:
        return
    keys = set(event.get("book_profile_start") or {})
    if not keys:
        return
    lower = min(keys)
    upper = max(keys) + float(getattr(cfg, "ZONE_BOUNDARY_V2_BUCKET_SIZE_U", 0.5))
    current = build_book_profile(book_levels, lower, upper)
    min_profile = event.setdefault("book_profile_min", {})
    for bucket in keys:
        depth = parse_float(current.get(bucket))
        min_profile[bucket] = min(parse_float(min_profile.get(bucket), depth), depth)
    event["book_profile_end"] = current


def compute_zone_boundary_v2(
    event: Mapping[str, Any],
    direction: str | None = None,
    current_price: float = 0.0,
) -> dict[str, Any]:
    direction = str(direction or event.get("direction") or "").upper()
    obs_low = parse_float(event.get("observation_zone_lower"), parse_float(event.get("zone_lower")))
    obs_high = parse_float(event.get("observation_zone_upper"), parse_float(event.get("zone_upper")))
    if obs_low > obs_high:
        obs_low, obs_high = obs_high, obs_low
    sweep_low = parse_float(event.get("trade_sweep_low"), parse_float(event.get("min_trade_price"), obs_low))
    sweep_high = parse_float(event.get("trade_sweep_high"), parse_float(event.get("max_trade_price"), obs_high))
    start = _numeric_map(event.get("book_profile_start"))
    min_profile = _numeric_map(event.get("book_profile_min"))
    end = _numeric_map(event.get("book_profile_end"))
    trades = _numeric_map(event.get("trade_notional_by_bucket"))

    coverage = bool(start and end and min_profile)
    core_buckets: list[float] = []
    recovery_values: list[float] = []
    end_vs_start_values: list[float] = []
    over_reload_count = 0
    for bucket in sorted(set(start) | set(trades)):
        start_depth = parse_float(start.get(bucket))
        min_depth = parse_float(min_profile.get(bucket), start_depth)
        end_depth = parse_float(end.get(bucket), min_depth)
        trade_notional = parse_float(trades.get(bucket))
        depletion = max(0.0, start_depth - min_depth)
        recovery = max(0.0, end_depth - min_depth)
        recovery_ratio = recovery / depletion if depletion > 0 else 0.0
        end_vs_start = end_depth / start_depth if start_depth > 0 else 0.0
        over_reload = end_vs_start >= float(getattr(cfg, "ZONE_BOUNDARY_V2_OVER_RELOAD_RATIO", 1.05))
        if over_reload:
            over_reload_count += 1
        if (
            trade_notional >= float(getattr(cfg, "ZONE_BOUNDARY_V2_MIN_TRADE_NOTIONAL_PER_BUCKET", 20000.0))
            and (
                recovery_ratio >= float(getattr(cfg, "ZONE_BOUNDARY_V2_MIN_RECOVERY_RATIO", 0.70))
                or end_vs_start >= float(getattr(cfg, "ZONE_BOUNDARY_V2_MIN_END_VS_START", 0.90))
                or over_reload
            )
        ):
            core_buckets.append(bucket)
            recovery_values.append(recovery_ratio)
            end_vs_start_values.append(end_vs_start)

    if core_buckets:
        core_low = min(core_buckets)
        core_high = max(core_buckets) + float(getattr(cfg, "ZONE_BOUNDARY_V2_BUCKET_SIZE_U", 0.5))
        reason = "CORE_RECOVERY_BUCKETS"
    else:
        core_low = obs_low
        core_high = obs_high
        reason = "FALLBACK_OBSERVATION_ZONE" if coverage else "FALLBACK_BOOK_COVERAGE_INSUFFICIENT"

    buffer_u = float(getattr(cfg, "ZONE_BOUNDARY_V2_STRUCTURAL_STOP_BUFFER_U", 0.5))
    if direction == "BUY":
        structural_stop = (sweep_low or obs_low) - buffer_u if (sweep_low or obs_low) > 0 else 0.0
        structural_risk = max(0.0, parse_float(current_price) - structural_stop) if current_price > 0 else 0.0
        defended_low, defended_high = core_low, core_high
    elif direction == "SELL":
        structural_stop = (sweep_high or obs_high) + buffer_u if (sweep_high or obs_high) > 0 else 0.0
        structural_risk = max(0.0, structural_stop - parse_float(current_price)) if current_price > 0 else 0.0
        defended_low, defended_high = core_low, core_high
    else:
        structural_stop = 0.0
        structural_risk = 0.0
        defended_low, defended_high = core_low, core_high

    out = {
        "observation_zone_lower": round(obs_low, 8),
        "observation_zone_upper": round(obs_high, 8),
        "absorption_core_lower": round(core_low, 8),
        "absorption_core_upper": round(core_high, 8),
        "absorption_core_width": round(max(0.0, core_high - core_low), 8),
        "sweep_extreme_low": round(sweep_low, 8),
        "sweep_extreme_high": round(sweep_high, 8),
        "defended_low": round(defended_low, 8),
        "defended_high": round(defended_high, 8),
        "zone_v2_structural_stop_price": round(structural_stop, 8),
        "zone_v2_structural_risk_u": round(structural_risk, 8),
        "zone_v2_boundary_reason": reason,
        "zone_v2_core_recovery_ratio_avg": round(sum(recovery_values) / len(recovery_values), 8) if recovery_values else 0.0,
        "zone_v2_core_recovery_ratio_min": round(min(recovery_values), 8) if recovery_values else 0.0,
        "zone_v2_core_end_vs_start_avg": round(sum(end_vs_start_values) / len(end_vs_start_values), 8) if end_vs_start_values else 0.0,
        "zone_v2_over_reload_count": int(over_reload_count),
        "zone_v2_reload_level_count": len(core_buckets),
        "zone_v2_layered_absorption_flag": len(core_buckets) >= 2,
        "zone_v2_book_coverage_low": round(min(start) if start else 0.0, 8),
        "zone_v2_book_coverage_high": round(max(start) if start else 0.0, 8),
        "zone_v2_book_coverage_sufficient_flag": coverage,
    }
    if parse_bool(getattr(cfg, "ZONE_BOUNDARY_V2_WRITE_PROFILE_MAPS", False)):
        out["book_profile_start"] = dict(start)
        out["book_profile_min"] = dict(min_profile)
        out["book_profile_end"] = dict(end)
        out["trade_notional_by_bucket"] = dict(trades)
    return out


def aggregate_zone_boundary_v2(row: Mapping[str, Any], pies: list[Mapping[str, Any]]) -> dict[str, Any]:
    out = dict(row)
    event_bounds = [
        compute_zone_boundary_v2(pie, direction=str(out.get("direction") or ""), current_price=parse_float(out.get("zone_mid")))
        for pie in pies or []
    ]
    if not event_bounds:
        event_bounds = [compute_zone_boundary_v2(out, direction=str(out.get("direction") or ""), current_price=parse_float(out.get("zone_mid")))]
    core_lows = [parse_float(e.get("absorption_core_lower")) for e in event_bounds if parse_float(e.get("absorption_core_lower")) > 0]
    core_highs = [parse_float(e.get("absorption_core_upper")) for e in event_bounds if parse_float(e.get("absorption_core_upper")) > 0]
    sweep_lows = [parse_float(e.get("sweep_extreme_low")) for e in event_bounds if parse_float(e.get("sweep_extreme_low")) > 0]
    sweep_highs = [parse_float(e.get("sweep_extreme_high")) for e in event_bounds if parse_float(e.get("sweep_extreme_high")) > 0]
    direction = str(out.get("direction") or "").upper()
    buffer_u = float(getattr(cfg, "ZONE_BOUNDARY_V2_STRUCTURAL_STOP_BUFFER_U", 0.5))
    stop = 0.0
    if direction == "BUY" and sweep_lows:
        stop = min(sweep_lows) - buffer_u
    elif direction == "SELL" and sweep_highs:
        stop = max(sweep_highs) + buffer_u
    first = event_bounds[0]
    out.update(
        {
            "observation_zone_lower": parse_float(out.get("zone_lower")),
            "observation_zone_upper": parse_float(out.get("zone_upper")),
            "absorption_core_lower": min(core_lows) if core_lows else parse_float(out.get("zone_lower")),
            "absorption_core_upper": max(core_highs) if core_highs else parse_float(out.get("zone_upper")),
            "absorption_core_width": max(0.0, (max(core_highs) if core_highs else parse_float(out.get("zone_upper"))) - (min(core_lows) if core_lows else parse_float(out.get("zone_lower")))),
            "sweep_extreme_low": min(sweep_lows) if sweep_lows else parse_float(out.get("trade_sweep_low")),
            "sweep_extreme_high": max(sweep_highs) if sweep_highs else parse_float(out.get("trade_sweep_high")),
            "defended_low": min(core_lows) if core_lows else parse_float(out.get("zone_lower")),
            "defended_high": max(core_highs) if core_highs else parse_float(out.get("zone_upper")),
            "zone_v2_structural_stop_price": stop or parse_float(first.get("zone_v2_structural_stop_price")),
            "zone_v2_boundary_reason": first.get("zone_v2_boundary_reason", ""),
            "zone_v2_core_recovery_ratio_avg": _avg(event_bounds, "zone_v2_core_recovery_ratio_avg"),
            "zone_v2_core_recovery_ratio_min": min([parse_float(e.get("zone_v2_core_recovery_ratio_min")) for e in event_bounds if parse_float(e.get("zone_v2_core_recovery_ratio_min")) > 0] or [0.0]),
            "zone_v2_core_end_vs_start_avg": _avg(event_bounds, "zone_v2_core_end_vs_start_avg"),
            "zone_v2_over_reload_count": sum(int(parse_float(e.get("zone_v2_over_reload_count"))) for e in event_bounds),
            "zone_v2_reload_level_count": max(int(parse_float(e.get("zone_v2_reload_level_count"))) for e in event_bounds),
            "zone_v2_layered_absorption_flag": any(parse_bool(e.get("zone_v2_layered_absorption_flag")) for e in event_bounds),
            "zone_v2_book_coverage_low": min([parse_float(e.get("zone_v2_book_coverage_low")) for e in event_bounds if parse_float(e.get("zone_v2_book_coverage_low")) > 0] or [0.0]),
            "zone_v2_book_coverage_high": max([parse_float(e.get("zone_v2_book_coverage_high")) for e in event_bounds if parse_float(e.get("zone_v2_book_coverage_high")) > 0] or [0.0]),
            "zone_v2_book_coverage_sufficient_flag": any(parse_bool(e.get("zone_v2_book_coverage_sufficient_flag")) for e in event_bounds),
        }
    )
    out["zone_v2_structural_risk_u"] = _risk_from_stop(out, direction)
    return out


def _numeric_map(value: Any) -> dict[float, float]:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for key, val in value.items():
        result[parse_float(key)] = parse_float(val)
    return result


def _avg(rows: list[Mapping[str, Any]], field: str) -> float:
    vals = [parse_float(row.get(field)) for row in rows if parse_float(row.get(field)) > 0]
    return round(sum(vals) / len(vals), 8) if vals else 0.0


def _risk_from_stop(row: Mapping[str, Any], direction: str) -> float:
    stop = parse_float(row.get("zone_v2_structural_stop_price"))
    entry = parse_float(row.get("a3_preview_entry_price"), parse_float(row.get("zone_mid")))
    if stop <= 0 or entry <= 0:
        return 0.0
    if direction == "BUY":
        return round(max(0.0, entry - stop), 8)
    if direction == "SELL":
        return round(max(0.0, stop - entry), 8)
    return 0.0
