#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.research.a1_edge.schema import parse_float


A2_A1_DETECTED = "A1_DETECTED"
A2_FORMING = "A2_FORMING"
A2_QUIET_HOLD = "A2_QUIET_HOLD"
A2_LIGHT_READY = "A2_LIGHT_READY"
A2_READY_FOR_A3 = "A2_READY_FOR_A3"
A2_A3_TRIGGERED = "A3_TRIGGERED"
A2_INVALIDATED = "A2_INVALIDATED"
A2_EXPIRED = "A2_EXPIRED"


@dataclass(frozen=True)
class A2RuntimeConfig:
    min_quiet_sec: float = 3.0
    min_tick_count: int = 20
    max_age_sec: float = 900.0
    max_box_width_u: float = 3.0
    max_box_width_multiplier: float = 2.0
    quiet_volume_ratio_max: float = 0.45
    cvd_stall_ratio_max: float = 0.35
    invalidation_buffer_u: float = 0.5
    enable_light_ready: bool = True
    min_light_sec: float = 2.0
    min_light_tick_count: int = 2


class A2RuntimeStateMachine:
    def __init__(self, zone: Mapping[str, Any], expiry_sec: float = 900.0, config: A2RuntimeConfig | None = None) -> None:
        self.zone = dict(zone)
        self.config = config or A2RuntimeConfig(max_age_sec=float(expiry_sec))
        self.expiry_sec = float(expiry_sec)
        self.direction = str(zone.get("direction") or "").upper()
        self.start_ts = _first_ts(zone, "reaction_event_ts", "first_iceberg_pie_ts", "best_pie_ts", "first_seen_ts")
        self.state = A2_A1_DETECTED
        self.reason = "a1_detected"
        self.last_update_ts = self.start_ts
        self.tick_count = 0
        self.box_low = 0.0
        self.box_high = 0.0
        self.buy_notional_sum = 0.0
        self.sell_notional_sum = 0.0
        self.cvd_min = 0.0
        self.cvd_max = 0.0
        self.defended_low = _defended_low(zone)
        self.defended_high = _defended_high(zone)
        self.ready_quality = "NONE"
        self.a1_buy_peak = max(parse_float(zone.get("active_buy_notional_3s")), parse_float(zone.get("max_active_notional")), 1.0)
        self.a1_sell_peak = max(parse_float(zone.get("active_sell_notional_3s")), parse_float(zone.get("max_active_notional")), 1.0)

    def update(self, tick: Mapping[str, Any]) -> dict[str, Any]:
        ts = _tick_ts(tick)
        price = _tick_price(tick)
        if self.state in {A2_INVALIDATED, A2_EXPIRED, A2_A3_TRIGGERED}:
            return self.snapshot()
        if self.start_ts <= 0:
            self.start_ts = ts
        self.last_update_ts = max(self.last_update_ts, ts)
        if self.expiry_sec > 0 and ts - self.start_ts > self.expiry_sec:
            self.state = A2_EXPIRED
            self.reason = "max_age_exceeded"
            return self.snapshot()
        if self._invalidated(price):
            self.state = A2_INVALIDATED
            self.reason = "defended_level_broken"
            return self.snapshot()
        if self.state == A2_READY_FOR_A3:
            self.reason = "confirmed_ready_waiting_for_a3"
            return self.snapshot()

        self.tick_count += 1
        self.box_low = price if self.box_low <= 0 else min(self.box_low, price)
        self.box_high = max(self.box_high, price)
        buy = parse_float(tick.get("active_buy_notional_3s") or tick.get("active_buy_notional_1s") or tick.get("buy_notional"))
        sell = parse_float(tick.get("active_sell_notional_3s") or tick.get("active_sell_notional_1s") or tick.get("sell_notional"))
        cvd = parse_float(tick.get("cvd_delta_3s") or tick.get("cvd_delta_1s") or tick.get("cvd_delta"))
        self.buy_notional_sum += buy
        self.sell_notional_sum += sell
        if self.tick_count == 1:
            self.cvd_min = cvd
            self.cvd_max = cvd
        else:
            self.cvd_min = min(self.cvd_min, cvd)
            self.cvd_max = max(self.cvd_max, cvd)

        if self.tick_count == 1:
            self.state = A2_FORMING
            self.reason = "first_runtime_tick"
        elif self._confirmed_ready():
            self.state = A2_READY_FOR_A3
            self.ready_quality = "CONFIRMED"
            self.reason = "confirmed_quiet_hold|box_width_ok|flow_exhaustion|cvd_stall"
        elif self._light_ready():
            self.state = A2_LIGHT_READY
            if self.ready_quality != "CONFIRMED":
                self.ready_quality = "LIGHT"
            self.reason = "light_ready|defended_level_hold|box_width_ok"
        else:
            self.state = A2_QUIET_HOLD
            self.reason = "collecting_quiet_hold_evidence"
        return self.snapshot()

    def mark_a3_triggered(self) -> dict[str, Any]:
        if self.ready_quality not in {"LIGHT", "CONFIRMED"}:
            self.ready_quality = "LIGHT"
        self.state = A2_A3_TRIGGERED
        self.reason = "runtime_a3_triggered"
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        width = max(0.0, self.box_high - self.box_low) if self.box_high > 0 and self.box_low > 0 else 0.0
        duration = max(0.0, self.last_update_ts - self.start_ts) if self.last_update_ts and self.start_ts else 0.0
        buy_avg = self.buy_notional_sum / max(self.tick_count, 1)
        sell_avg = self.sell_notional_sum / max(self.tick_count, 1)
        quiet_ratio = self._quiet_volume_ratio()
        quality = self.ready_quality if self.ready_quality in {"LIGHT", "CONFIRMED"} else "NONE"
        light_ready = quality in {"LIGHT", "CONFIRMED"}
        confirmed_ready = quality == "CONFIRMED"
        return {
            "a2_rt_state": self.state,
            "a2_rt_state_reason": self.reason,
            "a2_rt_start_ts": round(self.start_ts, 8),
            "a2_rt_last_update_ts": round(self.last_update_ts, 8),
            "a2_rt_box_low": round(self.box_low, 8),
            "a2_rt_box_high": round(self.box_high, 8),
            "a2_rt_box_mid": round((self.box_low + self.box_high) / 2.0, 8) if self.box_low and self.box_high else 0.0,
            "a2_rt_box_width_u": round(width, 8),
            "a2_rt_duration_sec": round(duration, 8),
            "a2_rt_tick_count": int(self.tick_count),
            "a2_rt_quiet_volume_ratio": round(quiet_ratio, 8),
            "a2_rt_quiet_buy_avg": round(buy_avg, 8),
            "a2_rt_quiet_sell_avg": round(sell_avg, 8),
            "a2_rt_quiet_total_avg": round(buy_avg + sell_avg, 8),
            "a2_rt_a1_buy_peak": round(self.a1_buy_peak, 8),
            "a2_rt_a1_sell_peak": round(self.a1_sell_peak, 8),
            "a2_rt_active_buy_sum": round(self.buy_notional_sum, 8),
            "a2_rt_active_sell_sum": round(self.sell_notional_sum, 8),
            "a2_rt_active_sell_exhaustion_flag": self.direction == "BUY" and sell_avg <= self.a1_sell_peak * self.config.quiet_volume_ratio_max,
            "a2_rt_active_buy_exhaustion_flag": self.direction == "SELL" and buy_avg <= self.a1_buy_peak * self.config.quiet_volume_ratio_max,
            "a2_rt_cvd_stall_flag": self._cvd_stall(),
            "a2_rt_invalidated_flag": self.state == A2_INVALIDATED,
            "a2_rt_expired_flag": self.state == A2_EXPIRED,
            "a2_rt_light_ready_for_a3_flag": light_ready,
            "a2_rt_confirmed_ready_for_a3_flag": confirmed_ready,
            "a2_rt_ready_for_a3_flag": light_ready,
            "a2_rt_quality": quality,
            "a2_rt_defended_low": round(self.defended_low, 8),
            "a2_rt_defended_high": round(self.defended_high, 8),
            "a2_rt_expiry_sec": round(self.expiry_sec, 8),
            "a2_rt_expiry_bucket": _expiry_bucket(self.expiry_sec),
        }

    def _confirmed_ready(self) -> bool:
        if self.tick_count < self.config.min_tick_count:
            return False
        if self.last_update_ts - self.start_ts < self.config.min_quiet_sec:
            return False
        if not self._box_width_ok():
            return False
        if self.direction == "BUY" and not self.snapshot()["a2_rt_active_sell_exhaustion_flag"]:
            return False
        if self.direction == "SELL" and not self.snapshot()["a2_rt_active_buy_exhaustion_flag"]:
            return False
        return self._cvd_stall()

    def _light_ready(self) -> bool:
        if not self.config.enable_light_ready:
            return False
        if self.tick_count < max(1, int(self.config.min_light_tick_count)):
            return False
        if self.last_update_ts - self.start_ts < max(float(self.config.min_light_sec), 0.0):
            return False
        return self._box_width_ok()

    def _box_width_ok(self) -> bool:
        zone_width = max(parse_float(self.zone.get("zone_width")), abs(parse_float(self.zone.get("zone_upper")) - parse_float(self.zone.get("zone_lower"))), 1.0)
        max_width = max(zone_width * self.config.max_box_width_multiplier, self.config.max_box_width_u)
        return not (self.box_high <= 0 or self.box_low <= 0 or self.box_high - self.box_low > max_width)

    def _ready(self) -> bool:
        return self._confirmed_ready()

    def _quiet_volume_ratio(self) -> float:
        if self.direction == "BUY":
            return (self.sell_notional_sum / max(self.tick_count, 1)) / max(self.a1_sell_peak, 1.0)
        if self.direction == "SELL":
            return (self.buy_notional_sum / max(self.tick_count, 1)) / max(self.a1_buy_peak, 1.0)
        return 0.0

    def _cvd_stall(self) -> bool:
        span = abs(self.cvd_max - self.cvd_min)
        peak = max(abs(self.cvd_max), abs(self.cvd_min), 1.0)
        return span / peak <= max(self.config.cvd_stall_ratio_max, 0.0)

    def _invalidated(self, price: float) -> bool:
        if price <= 0:
            return False
        if self.direction == "BUY" and self.defended_low > 0:
            return price < self.defended_low - self.config.invalidation_buffer_u
        if self.direction == "SELL" and self.defended_high > 0:
            return price > self.defended_high + self.config.invalidation_buffer_u
        return False


def _defended_low(zone: Mapping[str, Any]) -> float:
    return _first_positive(zone, "defended_low", "sweep_extreme_low", "first_iceberg_pie_min_trade_price", "absorption_core_lower", "zone_lower")


def _defended_high(zone: Mapping[str, Any]) -> float:
    return _first_positive(zone, "defended_high", "sweep_extreme_high", "first_iceberg_pie_max_trade_price", "absorption_core_upper", "zone_upper")


def _first_positive(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0


def _first_ts(row: Mapping[str, Any], *names: str) -> float:
    return _first_positive(row, *names)


def _tick_ts(tick: Mapping[str, Any]) -> float:
    return _first_positive(tick, "ts", "timestamp", "event_ts", "recv_ts")


def _tick_price(tick: Mapping[str, Any]) -> float:
    return _first_positive(tick, "last_price", "price", "trade_price", "close")


def _expiry_bucket(expiry_sec: float) -> str:
    minutes = int(round(float(expiry_sec) / 60.0))
    return f"{minutes}m"
