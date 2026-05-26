#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
from statistics import median
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from src.research.a1_edge.io_utils import normalize_klines
from src.research.a1_edge.schema import parse_bool, parse_float


DEFAULT_WINDOWS_SEC = (900, 3600, 14400)
A3_PREVIEW_BREAKOUT_WINDOW_SEC = 3600
WINDOW_LABELS = {
    900: "15m",
    3600: "1h",
    14400: "4h",
}


class ZoneForwardMetricsCalculator:
    def __init__(self, windows_sec: Iterable[int] | None = None, kline_timezone: str = "Asia/Shanghai") -> None:
        self.windows_sec = [int(w) for w in (windows_sec or DEFAULT_WINDOWS_SEC)]
        self.kline_timezone = kline_timezone

    def attach_forward_metrics(
        self,
        zone_rows: Iterable[Mapping[str, Any]],
        kline_rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        bars = normalize_klines(kline_rows, kline_timezone=self.kline_timezone)
        return [self.attach_to_row(row, bars) for row in zone_rows or []]

    def attach_to_row(self, row: Mapping[str, Any], bars: list[dict[str, float]]) -> dict[str, Any]:
        result = dict(row)
        anchor_ts, anchor_source = _resolve_event_ts(result)
        entry_price, entry_source = _resolve_entry_price(result)
        reaction_event_ts = parse_float(result.get("reaction_event_ts"))
        outside_kline = _outside_kline_range(reaction_event_ts, bars) if reaction_event_ts > 0 else False
        result["reaction_event_ts_valid"] = parse_bool(
            result.get("reaction_event_ts_valid"),
            default=reaction_event_ts > 0,
        )
        result["reaction_event_ts_outside_kline_range"] = outside_kline
        result["forward_anchor_ts"] = anchor_ts
        result["forward_anchor_source"] = anchor_source
        result["forward_anchor_local_time"] = _local_time(anchor_ts, self.kline_timezone)
        result["forward_entry_price"] = entry_price
        result["forward_entry_price_source"] = entry_source
        for window_sec in self.windows_sec:
            label = WINDOW_LABELS.get(int(window_sec), f"{int(window_sec)}s")
            metric = compute_zone_forward_metric(result, bars, int(window_sec))
            result[f"mfe_{label}_u"] = metric["mfe_u"]
            result[f"mae_{label}_u"] = metric["mae_u"]
            result[f"end_{label}_u"] = metric["end_u"]
            result[f"is_complete_{label}"] = metric["is_complete"]
        result.update(compute_a3_preview_breakout(result, bars))
        return result


def compute_zone_forward_metric(zone: Mapping[str, Any], bars: list[dict[str, float]], window_sec: int) -> dict[str, Any]:
    event_ts, _anchor_source = _resolve_event_ts(zone)
    entry, _entry_source = _resolve_entry_price(zone)
    direction = str(zone.get("direction") or "").upper()
    if not bars or event_ts <= 0 or entry <= 0 or direction not in {"BUY", "SELL"}:
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}
    if _outside_kline_range(event_ts, bars):
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    timestamps = [float(bar["timestamp"]) for bar in bars]
    start_idx = bisect_right(timestamps, event_ts)
    if start_idx >= len(bars):
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    interval = infer_bar_interval_sec(bars)
    expected_count = max(1, int(round(float(window_sec) / max(interval, 1.0))))
    start_ts = float(bars[start_idx]["timestamp"])
    end_ts = start_ts + float(window_sec)
    future = [bar for bar in bars[start_idx:] if float(bar["timestamp"]) < end_ts]
    if len(future) > expected_count:
        future = future[:expected_count]
    is_complete = len(future) >= expected_count
    if not future:
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    high = max(float(bar["high"]) for bar in future)
    low = min(float(bar["low"]) for bar in future)
    close = float(future[-1]["close"])
    if direction == "BUY":
        mfe = max(0.0, high - entry)
        mae = min(0.0, low - entry)
        end = close - entry
    else:
        mfe = max(0.0, entry - low)
        mae = min(0.0, entry - high)
        end = entry - close
    return {
        "mfe_u": round(mfe, 8),
        "mae_u": round(mae, 8),
        "end_u": round(end, 8),
        "is_complete": bool(is_complete),
        "future_bar_count": len(future),
    }


def infer_bar_interval_sec(bars: list[dict[str, float]], default: float = 60.0) -> float:
    diffs = [
        float(bars[i]["timestamp"]) - float(bars[i - 1]["timestamp"])
        for i in range(1, len(bars))
        if float(bars[i]["timestamp"]) > float(bars[i - 1]["timestamp"])
    ]
    return float(median(diffs)) if diffs else float(default)


def compute_a3_preview_breakout(zone: Mapping[str, Any], bars: list[dict[str, float]]) -> dict[str, Any]:
    """Offline A3 watch preview only; not a runtime signal."""
    default = {
        "a3_preview_breakout_raw_flag": False,
        "a3_preview_breakout_raw_latency_sec": 0.0,
        "a3_preview_breakout_direction": "UNKNOWN",
        "a3_preview_breakout_threshold_u": 0.0,
        "a3_preview_breakout_price": 0.0,
        "a3_preview_max_extension_15m_u": 0.0,
        "a3_preview_max_extension_1h_u": 0.0,
    }
    anchor_ts, _anchor_source = _resolve_event_ts(zone)
    direction = str(zone.get("direction") or "").upper()
    zone_low = parse_float(zone.get("zone_lower"))
    zone_high = parse_float(zone.get("zone_upper"))
    if zone_low > zone_high:
        zone_low, zone_high = zone_high, zone_low
    zone_width = max(
        parse_float(zone.get("zone_width")),
        zone_high - zone_low,
        1.0,
    )
    if not bars or anchor_ts <= 0 or direction not in {"BUY", "SELL"} or zone_low <= 0 or zone_high <= 0:
        return default
    if anchor_ts < float(bars[0]["timestamp"]) or anchor_ts > float(bars[-1]["timestamp"]):
        return default

    threshold_u = max(zone_width * 0.5, 1.0)
    timestamps = [float(bar["timestamp"]) for bar in bars]
    start_idx = bisect_right(timestamps, anchor_ts)
    if start_idx >= len(bars):
        return {
            **default,
            "a3_preview_breakout_direction": direction,
            "a3_preview_breakout_threshold_u": round(threshold_u, 8),
        }

    breakout_price = zone_high + threshold_u if direction == "BUY" else zone_low - threshold_u
    future = bars[start_idx:]
    future_window = [
        bar for bar in future
        if float(bar["timestamp"]) <= anchor_ts + A3_PREVIEW_BREAKOUT_WINDOW_SEC
    ]
    breakout_ts = 0.0
    for bar in future_window:
        if direction == "BUY" and float(bar["high"]) >= breakout_price:
            breakout_ts = float(bar["timestamp"])
            break
        if direction == "SELL" and float(bar["low"]) <= breakout_price:
            breakout_ts = float(bar["timestamp"])
            break

    return {
        "a3_preview_breakout_raw_flag": breakout_ts > 0,
        "a3_preview_breakout_raw_latency_sec": round(max(0.0, breakout_ts - anchor_ts), 6) if breakout_ts > 0 else 0.0,
        "a3_preview_breakout_direction": direction,
        "a3_preview_breakout_threshold_u": round(threshold_u, 8),
        "a3_preview_breakout_price": round(breakout_price, 8),
        "a3_preview_max_extension_15m_u": _max_zone_extension(zone_low, zone_high, direction, future, anchor_ts, 900),
        "a3_preview_max_extension_1h_u": _max_zone_extension(zone_low, zone_high, direction, future, anchor_ts, 3600),
    }


def _max_zone_extension(
    zone_low: float,
    zone_high: float,
    direction: str,
    future: list[dict[str, float]],
    anchor_ts: float,
    window_sec: int,
) -> float:
    window = [bar for bar in future if float(bar["timestamp"]) <= anchor_ts + float(window_sec)]
    if not window:
        return 0.0
    if direction == "BUY":
        return round(max(0.0, max(float(bar["high"]) for bar in window) - zone_high), 8)
    if direction == "SELL":
        return round(max(0.0, zone_low - min(float(bar["low"]) for bar in window)), 8)
    return 0.0


def _resolve_event_ts(zone: Mapping[str, Any]) -> tuple[float, str]:
    for name in ("reaction_event_ts", "frozen_ts", "best_pie_ts", "first_seen_ts"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value, name
    return 0.0, "none"


def _resolve_entry_price(zone: Mapping[str, Any]) -> tuple[float, str]:
    for name in ("zone_mid", "best_pie_price", "settle_price"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value, name
    return 0.0, "none"


def _local_time(ts: float, timezone: str) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(float(ts), tz=ZoneInfo(str(timezone))).isoformat()


def _outside_kline_range(ts: float, bars: list[dict[str, float]]) -> bool:
    if not bars or ts <= 0:
        return False
    return ts < float(bars[0]["timestamp"]) or ts > float(bars[-1]["timestamp"])
