#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.research.a1_edge.schema import parse_float
from src.research.no_future_audit import invalid_entry_fields


@dataclass(frozen=True)
class A3RuntimeConfig:
    breakout_buffer_u: float = 0.5
    active_flow_ratio: float = 1.5
    min_delta_notional: float = 100000.0
    min_burst_notional: float = 150000.0
    burst_multiplier: float = 2.0
    price_velocity_min_u_per_sec: float = 0.2


ENTRY_CONDITION_FIELDS = [
    "active_buy_notional_3s",
    "active_sell_notional_3s",
    "cvd_delta_3s",
    "price_velocity_u_per_sec",
    "a2_rt_ready_for_a3_flag",
    "a2_rt_box_high",
    "a2_rt_box_low",
    "a1_vp_setup_rt",
]


def evaluate_a3_runtime_entry(
    a2_snapshot: Mapping[str, Any],
    tick: Mapping[str, Any],
    *,
    direction: str | None = None,
    inherited_a1_vp_setup: str = "",
    config: A3RuntimeConfig | None = None,
) -> dict[str, Any]:
    cfg = config or A3RuntimeConfig()
    side = str(direction or tick.get("direction") or a2_snapshot.get("direction") or "").upper()
    ts = _first_float(tick, "ts", "timestamp", "event_ts", "recv_ts")
    price = _first_float(tick, "last_price", "price", "trade_price", "close")
    buy = _first_float(tick, "active_buy_notional_3s", "active_buy_notional_1s", "buy_notional")
    sell = _first_float(tick, "active_sell_notional_3s", "active_sell_notional_1s", "sell_notional")
    quiet_buy = max(_first_float(a2_snapshot, "a2_rt_quiet_buy_avg", "quiet_buy_avg"), 1.0)
    quiet_sell = max(_first_float(a2_snapshot, "a2_rt_quiet_sell_avg", "quiet_sell_avg"), 1.0)
    cvd = _first_float(tick, "cvd_delta_3s", "cvd_delta_1s", "cvd_delta")
    velocity = _first_float(tick, "price_velocity_u_per_sec", "velocity_u_per_sec")
    box_high = parse_float(a2_snapshot.get("a2_rt_box_high"))
    box_low = parse_float(a2_snapshot.get("a2_rt_box_low"))
    ready = str(a2_snapshot.get("a2_rt_state") or "").upper() == "A2_READY_FOR_A3" or bool(a2_snapshot.get("a2_rt_ready_for_a3_flag"))

    if side == "BUY":
        breakout = price > box_high + cfg.breakout_buffer_u
        burst = buy >= max(quiet_buy * cfg.burst_multiplier, cfg.min_burst_notional)
        active_dominance = buy >= max(sell * cfg.active_flow_ratio, sell + cfg.min_delta_notional)
        cvd_aligned = cvd > 0
        velocity_ok = velocity > cfg.price_velocity_min_u_per_sec
        box_side = "BOX_HIGH"
    elif side == "SELL":
        breakout = price < box_low - cfg.breakout_buffer_u
        burst = sell >= max(quiet_sell * cfg.burst_multiplier, cfg.min_burst_notional)
        active_dominance = sell >= max(buy * cfg.active_flow_ratio, buy + cfg.min_delta_notional)
        cvd_aligned = cvd < 0
        velocity_ok = velocity < -cfg.price_velocity_min_u_per_sec
        box_side = "BOX_LOW"
    else:
        breakout = burst = active_dominance = cvd_aligned = velocity_ok = False
        box_side = "UNKNOWN"

    flag = bool(ready and breakout and burst and active_dominance and cvd_aligned and velocity_ok)
    reasons = []
    if ready:
        reasons.append("a2_ready")
    if breakout:
        reasons.append("box_breakout")
    if burst and active_dominance:
        reasons.append("orderflow_burst")
    if cvd_aligned:
        reasons.append("cvd_aligned")
    if velocity_ok:
        reasons.append("price_velocity")
    invalid_fields = invalid_entry_fields(ENTRY_CONDITION_FIELDS)
    source_ts = _first_float(tick, "condition_available_ts", "field_available_ts", "ts", "timestamp", "event_ts", "recv_ts")
    return {
        "a3_entry_rt_flag": flag,
        "a3_entry_rt_ts": round(ts, 8) if flag else 0.0,
        "a3_entry_rt_price": round(price, 8) if flag else 0.0,
        "a3_entry_rt_direction": side if flag else "UNKNOWN",
        "a3_entry_rt_reason": "|".join(reasons) if flag else "conditions_not_met",
        "a3_entry_rt_breakout_box_side": box_side if breakout else "NONE",
        "a3_entry_rt_orderflow_burst_flag": bool(burst and active_dominance),
        "a3_entry_rt_cvd_aligned_flag": bool(cvd_aligned),
        "a3_entry_rt_volume_boost": round((buy / quiet_buy) if side == "BUY" else (sell / quiet_sell if quiet_sell > 0 else 0.0), 8),
        "a3_entry_rt_price_velocity_u_per_sec": round(velocity, 8),
        "a3_entry_rt_inherited_a1_vp_setup": inherited_a1_vp_setup,
        "a3_entry_rt_uses_future_field_flag": bool(invalid_fields),
        "a3_entry_rt_condition_fields": "|".join(ENTRY_CONDITION_FIELDS),
        "a3_entry_rt_condition_available_ts_max": round(source_ts, 8),
        "a3_entry_rt_condition_source": "tick_at_entry",
    }


def _first_float(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value != 0:
            return value
    return 0.0
