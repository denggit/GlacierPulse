#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_right
from statistics import median
from typing import Any, Iterable, Mapping

from src.research.a1_edge.io_utils import normalize_klines
from src.research.a1_edge.schema import parse_float


DEFAULT_WINDOWS_SEC = (900, 3600, 14400)
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
        for window_sec in self.windows_sec:
            label = WINDOW_LABELS.get(int(window_sec), f"{int(window_sec)}s")
            metric = compute_zone_forward_metric(result, bars, int(window_sec))
            result[f"mfe_{label}_u"] = metric["mfe_u"]
            result[f"mae_{label}_u"] = metric["mae_u"]
            result[f"end_{label}_u"] = metric["end_u"]
            result[f"is_complete_{label}"] = metric["is_complete"]
        return result


def compute_zone_forward_metric(zone: Mapping[str, Any], bars: list[dict[str, float]], window_sec: int) -> dict[str, Any]:
    event_ts = _event_ts(zone)
    entry = _entry_price(zone)
    direction = str(zone.get("direction") or "").upper()
    if not bars or event_ts <= 0 or entry <= 0 or direction not in {"BUY", "SELL"}:
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


def _event_ts(zone: Mapping[str, Any]) -> float:
    for name in ("reaction_event_ts", "frozen_ts", "best_pie_ts", "first_seen_ts"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value
    return 0.0


def _entry_price(zone: Mapping[str, Any]) -> float:
    for name in ("zone_mid", "best_pie_price", "settle_price"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value
    return 0.0
