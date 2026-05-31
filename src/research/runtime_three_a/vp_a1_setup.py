#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from src.research.a1_edge.schema import parse_float


VP_ADVANTAGE_SETUPS = {
    "BUY_BELOW_VAL_ABSORB",
    "BUY_NEAR_VAL_ABSORB",
    "BUY_LVN_BELOW_HVN_ABSORB",
    "BUY_INSIDE_VALUE_BELOW_POC_ABSORB",
    "SELL_ABOVE_VAH_ABSORB",
    "SELL_NEAR_VAH_ABSORB",
    "SELL_LVN_ABOVE_HVN_ABSORB",
    "SELL_INSIDE_VALUE_ABOVE_POC_ABSORB",
}


def classify_a1_vp_setup(row: Mapping[str, Any], prefix: str) -> dict[str, Any]:
    direction = str(row.get("direction") or row.get("iceberg_context_side") or "").upper()
    price = parse_float(row.get("iceberg_context_price") or row.get("a1_price") or row.get("zone_mid"))
    poc = parse_float(row.get(f"{prefix}_poc_rt") or row.get(f"{prefix}_proxy_poc"))
    val = parse_float(row.get(f"{prefix}_val_rt") or row.get(f"{prefix}_proxy_val"))
    vah = parse_float(row.get(f"{prefix}_vah_rt") or row.get(f"{prefix}_proxy_vah"))
    hvn_above = parse_float(row.get(f"{prefix}_hvn_above_rt"))
    hvn_below = parse_float(row.get(f"{prefix}_hvn_below_rt"))
    lvn_above = parse_float(row.get(f"{prefix}_lvn_above_rt"))
    lvn_below = parse_float(row.get(f"{prefix}_lvn_below_rt"))
    threshold = max(parse_float(row.get(f"{prefix}_near_threshold_rt")), 1.0)
    if price <= 0 or poc <= 0 or val <= 0 or vah <= 0:
        setup = "VP_UNAVAILABLE"
        value_location = "UNKNOWN"
        node_location = "NONE"
    else:
        value_location = _value_location(price, poc, val, vah, threshold)
        node_location = _node_location(price, poc, hvn_above, hvn_below, lvn_above, lvn_below, threshold)
        setup = _setup(direction, value_location, node_location, price, poc, hvn_above, hvn_below, lvn_above, lvn_below)
    targets = _targets(direction, price, poc, val, vah, hvn_above, hvn_below, lvn_above, lvn_below)
    return {
        f"{prefix}_a1_value_location_rt": value_location,
        f"{prefix}_a1_node_location_rt": node_location,
        f"{prefix}_a1_vp_setup_rt": setup,
        **targets,
    }


def _value_location(price: float, poc: float, val: float, vah: float, threshold: float) -> str:
    if abs(price - poc) <= threshold:
        return "NEAR_POC"
    if price < val - threshold:
        return "BELOW_VAL"
    if abs(price - val) <= threshold:
        return "NEAR_VAL"
    if price > vah + threshold:
        return "ABOVE_VAH"
    if abs(price - vah) <= threshold:
        return "NEAR_VAH"
    if val < price < poc:
        return "INSIDE_VALUE_BELOW_POC"
    if poc < price < vah:
        return "INSIDE_VALUE_ABOVE_POC"
    return "UNKNOWN"


def _node_location(price: float, poc: float, hvn_above: float, hvn_below: float, lvn_above: float, lvn_below: float, threshold: float) -> str:
    if abs(price - poc) <= threshold:
        return "NEAR_POC"
    if _near(price, lvn_above, threshold) or _near(price, lvn_below, threshold):
        return "NEAR_LVN"
    if _near(price, hvn_above, threshold) or _near(price, hvn_below, threshold):
        return "NEAR_HVN"
    return "NONE"


def _setup(direction: str, value_location: str, node_location: str, price: float, poc: float, hvn_above: float, hvn_below: float, lvn_above: float, lvn_below: float) -> str:
    if direction == "BUY":
        if value_location == "BELOW_VAL":
            return "BUY_BELOW_VAL_ABSORB"
        if value_location == "NEAR_VAL":
            return "BUY_NEAR_VAL_ABSORB"
        if node_location == "NEAR_LVN" and hvn_above > price:
            return "BUY_LVN_BELOW_HVN_ABSORB"
        if value_location == "INSIDE_VALUE_BELOW_POC":
            return "BUY_INSIDE_VALUE_BELOW_POC_ABSORB"
        if node_location in {"NEAR_HVN", "NEAR_POC"}:
            return "BUY_NEAR_HVN_DANGER"
        return "BUY_NO_VP_EDGE"
    if direction == "SELL":
        if value_location == "ABOVE_VAH":
            return "SELL_ABOVE_VAH_ABSORB"
        if value_location == "NEAR_VAH":
            return "SELL_NEAR_VAH_ABSORB"
        if node_location == "NEAR_LVN" and hvn_below > 0 and hvn_below < price:
            return "SELL_LVN_ABOVE_HVN_ABSORB"
        if value_location == "INSIDE_VALUE_ABOVE_POC":
            return "SELL_INSIDE_VALUE_ABOVE_POC_ABSORB"
        if node_location in {"NEAR_HVN", "NEAR_POC"}:
            return "SELL_NEAR_HVN_DANGER"
        return "SELL_NO_VP_EDGE"
    return "NO_DIRECTIONAL_VP_SETUP"


def _targets(direction: str, price: float, poc: float, val: float, vah: float, hvn_above: float, hvn_below: float, lvn_above: float, lvn_below: float) -> dict[str, Any]:
    if direction == "BUY":
        hvn = hvn_above
        edge = vah
        lvn = lvn_above
    elif direction == "SELL":
        hvn = hvn_below
        edge = val
        lvn = lvn_below
    else:
        hvn = edge = lvn = 0.0
    return {
        "a1_target_poc_price_rt": poc if _directional(direction, price, poc) else 0.0,
        "a1_target_hvn_price_rt": hvn if _directional(direction, price, hvn) else 0.0,
        "a1_target_value_edge_price_rt": edge if _directional(direction, price, edge) else 0.0,
        "a1_target_lvn_price_rt": lvn if _directional(direction, price, lvn) else 0.0,
        "a1_target_poc_distance_u_rt": abs(poc - price) if _directional(direction, price, poc) else 0.0,
        "a1_target_hvn_distance_u_rt": abs(hvn - price) if _directional(direction, price, hvn) else 0.0,
        "a1_target_value_edge_distance_u_rt": abs(edge - price) if _directional(direction, price, edge) else 0.0,
        "a1_target_lvn_distance_u_rt": abs(lvn - price) if _directional(direction, price, lvn) else 0.0,
    }


def _near(price: float, level: float, threshold: float) -> bool:
    return level > 0 and abs(price - level) <= threshold


def _directional(direction: str, price: float, target: float) -> bool:
    return target > 0 and ((direction == "BUY" and target > price) or (direction == "SELL" and target < price))

