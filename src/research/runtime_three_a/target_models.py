#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from src.research.a1_edge.schema import parse_float


def build_target_candidates(row: Mapping[str, Any], entry_price: float, direction: str, risk_u: float) -> dict[str, Any]:
    side = str(direction or "").upper()
    fixed = entry_price + 2.0 * risk_u if side == "BUY" else entry_price - 2.0 * risk_u if side == "SELL" else 0.0
    poc = _first_positive(row, "a1_target_poc_price_rt", "vp24h_a1_target_poc_price_rt", "vpsession_a1_target_poc_price_rt", "vp24h_proxy_poc")
    hvn = _first_positive(row, "a1_target_hvn_price_rt", "vp24h_a1_target_hvn_price_rt", "vpsession_a1_target_hvn_price_rt")
    edge = _first_positive(row, "a1_target_value_edge_price_rt", "vp24h_a1_target_value_edge_price_rt", "vpsession_a1_target_value_edge_price_rt")
    lvn = _first_positive(row, "a1_target_lvn_price_rt", "vp24h_a1_target_lvn_price_rt", "vpsession_a1_target_lvn_price_rt")
    poc = poc if _directional(side, entry_price, poc) else 0.0
    hvn = hvn if _directional(side, entry_price, hvn) else 0.0
    edge = edge if _directional(side, entry_price, edge) else 0.0
    lvn = lvn if _directional(side, entry_price, lvn) else 0.0
    candidates = [price for price in (poc, hvn, edge, lvn) if _target_r(side, entry_price, price, risk_u) >= 2.0]
    hybrid = min(candidates, key=lambda price: abs(price - entry_price), default=0.0)
    return {
        "target_fixed_2r_price_sim": round(fixed, 8),
        "target_poc_price_rt": round(poc, 8),
        "target_hvn_directional_price_rt": round(hvn, 8),
        "target_opposite_value_edge_price_rt": round(edge, 8),
        "target_next_lvn_price_rt": round(lvn, 8),
        "target_poc_r_rt": round(_target_r(side, entry_price, poc, risk_u), 8),
        "target_hvn_r_rt": round(_target_r(side, entry_price, hvn, risk_u), 8),
        "target_opposite_value_edge_r_rt": round(_target_r(side, entry_price, edge, risk_u), 8),
        "target_next_lvn_r_rt": round(_target_r(side, entry_price, lvn, risk_u), 8),
        "target_hybrid_min_2r_available_rt": hybrid > 0,
        "target_hybrid_min_2r_price_rt": round(hybrid, 8),
        "target_hybrid_min_2r_r_rt": round(_target_r(side, entry_price, hybrid, risk_u), 8),
    }


def _target_r(direction: str, entry: float, target: float, risk: float) -> float:
    if target <= 0 or risk <= 0:
        return 0.0
    if direction == "BUY":
        return max(0.0, (target - entry) / risk)
    if direction == "SELL":
        return max(0.0, (entry - target) / risk)
    return 0.0


def _directional(direction: str, entry: float, target: float) -> bool:
    return target > 0 and ((direction == "BUY" and target > entry) or (direction == "SELL" and target < entry))


def _first_positive(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0

