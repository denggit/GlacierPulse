#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_float


def build_stop(row: Mapping[str, Any], entry_price: float, direction: str, stop_model: str = "STOP_STRUCTURAL_ZONE_V2") -> dict[str, Any]:
    model = str(stop_model or "STOP_STRUCTURAL_ZONE_V2").upper()
    side = str(direction or "").upper()
    buffer_u = parse_float(row.get("a2_rt_invalidation_buffer_u")) or parse_float(getattr(cfg, "A2_RT_INVALIDATION_BUFFER_U", 0.5))
    if model == "STOP_STRUCTURAL_ZONE_V2":
        basis = _first_positive(row, "defended_low", "zone_v2_structural_stop_price", "zone_lower") if side == "BUY" else _first_positive(row, "defended_high", "zone_v2_structural_stop_price", "zone_upper")
        stop = basis - buffer_u if side == "BUY" and basis > 0 else basis + buffer_u if side == "SELL" and basis > 0 else 0.0
        reason = "structural_zone_v2"
    elif model == "STOP_A2_BOX":
        basis = parse_float(row.get("a2_rt_box_low")) if side == "BUY" else parse_float(row.get("a2_rt_box_high"))
        stop = basis - buffer_u if side == "BUY" and basis > 0 else basis + buffer_u if side == "SELL" and basis > 0 else 0.0
        reason = "a2_box"
    elif model == "STOP_ABSORPTION_CORE":
        basis = parse_float(row.get("absorption_core_lower")) if side == "BUY" else parse_float(row.get("absorption_core_upper"))
        stop = basis - buffer_u if side == "BUY" and basis > 0 else basis + buffer_u if side == "SELL" and basis > 0 else 0.0
        reason = "absorption_core"
    elif model == "STOP_FIXED_U":
        fixed = parse_float(row.get("stop_fixed_u")) or 3.0
        stop = entry_price - fixed if side == "BUY" else entry_price + fixed if side == "SELL" else 0.0
        reason = "fixed_u"
    else:
        stop = 0.0
        reason = "unknown_stop_model"
    risk = entry_price - stop if side == "BUY" else stop - entry_price if side == "SELL" else 0.0
    fee_share = (entry_price * parse_float(getattr(cfg, "V7_3A_ROUNDTRIP_FEE_PCT", 0.001)) / risk) if risk > 0 else 0.0
    return {
        "stop_model": model,
        "stop_price": round(stop, 8),
        "risk_u": round(risk, 8),
        "stop_reason": reason,
        "fee_share_r": round(fee_share, 8),
        "stop_available_rt": risk > 0,
    }


def _first_positive(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0

