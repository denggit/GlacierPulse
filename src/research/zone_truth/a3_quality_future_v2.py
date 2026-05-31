#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from src.research.a1_edge.schema import parse_bool, parse_float


def classify_a3_quality_future_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    direction = str(row.get("direction") or row.get("a3_future_breakout_direction") or "").upper()
    price_breakout = _first_bool(row, "a3_future_breakout_seen_flag", "a3_preview_breakout_raw_flag")
    volume_boost = _first_float(row, "a3_future_volume_boost", "a3_preview_volume_boost") >= 2.0
    body_strength = _first_float(row, "a3_future_body_strength", "a3_preview_body_strength") >= 0.5
    no_quick_return = (
        _first_bool(row, "a3_future_no_quick_return_3m_flag", "a3_preview_no_quick_return_3m_flag")
        or _first_bool(row, "a3_future_no_quick_return_5m_flag", "a3_preview_no_quick_return_5m_flag")
    )
    persistence = (
        _first_bool(row, "a3_future_persistence_3m_flag", "a3_preview_persistence_3m_flag")
        or _first_bool(row, "a3_future_persistence_5m_flag", "a3_preview_persistence_5m_flag")
    )
    orderflow = _orderflow_aligned(row, direction)
    reclaim = (parse_bool(row.get("has_reclaimed_boundary")) or parse_bool(row.get("a2_reclaim_flag"))) and orderflow

    if orderflow and (volume_boost or body_strength):
        label = "STRONG_ORDERFLOW_AGGRESSION"
        score = 0.95
        reason = "future_quality:orderflow_aligned|volume_or_body"
    elif reclaim:
        label = "RECLAIM_AGGRESSION"
        score = 0.85
        reason = "future_quality:reclaim|orderflow_aligned"
    elif price_breakout and persistence and no_quick_return:
        label = "PRICE_BREAKOUT_PERSISTENT"
        score = 0.75
        reason = "future_quality:price_breakout|persistence|no_quick_return"
    elif price_breakout:
        label = "PRICE_BREAKOUT_WEAK"
        score = 0.45
        reason = "future_quality:price_breakout_only"
    else:
        label = "NO_AGGRESSION"
        score = 0.0
        reason = "future_quality:no_aggression_evidence"

    return {
        "a3_quality_future_type_v2": label,
        "a3_quality_future_score_v2": round(score, 6),
        "a3_quality_future_reason_v2": reason,
        "a3_price_breakout_flag_future": price_breakout,
        "a3_orderflow_aggression_flag_future": orderflow,
        "a3_cvd_flip_flag_future": _cvd_aligned(row, direction),
        "a3_reclaim_aggression_flag_future": reclaim,
        "a3_no_quick_return_flag_future": no_quick_return,
        "a3_persistence_flag_future": persistence,
        "a3_volume_boost_flag_future": volume_boost,
        "a3_body_strength_flag_future": body_strength,
    }


def attach_a3_quality_future_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.update(classify_a3_quality_future_v2(out))
    return out


def _orderflow_aligned(row: Mapping[str, Any], direction: str) -> bool:
    buy = _first_float(row, "active_buy_notional_3s", "active_buy_notional", "buy_active_notional_3s")
    sell = _first_float(row, "active_sell_notional_3s", "active_sell_notional", "sell_active_notional_3s")
    if buy > 0 or sell > 0:
        if direction == "BUY":
            return buy >= max(sell * 1.5, sell + 100000.0)
        if direction == "SELL":
            return sell >= max(buy * 1.5, buy + 100000.0)
    return _cvd_aligned(row, direction)


def _cvd_aligned(row: Mapping[str, Any], direction: str) -> bool:
    cvd = _first_float(row, "cvd_delta_3s", "cvd_delta", "cvd_flip_delta")
    if direction == "BUY":
        return cvd > 0
    if direction == "SELL":
        return cvd < 0
    return False


def _first_float(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value != 0:
            return value
    return 0.0


def _first_bool(row: Mapping[str, Any], *names: str) -> bool:
    for name in names:
        if name in row:
            return parse_bool(row.get(name))
    return False
