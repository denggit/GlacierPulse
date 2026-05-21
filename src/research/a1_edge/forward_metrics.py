#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_right
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .io_utils import normalize_klines, write_csv
from .schema import A1EdgeEvent, FORWARD_METRIC_FIELDS, ForwardMetricResult


DEFAULT_WINDOWS_SEC = [60, 180, 300, 900, 1800, 3600]


def compute_proxy_risk(event: A1EdgeEvent, entry_price: float, min_risk_u: float = 1.0, min_risk_pct: float = 0.0003) -> float:
    explicit = 0.0
    for value in (getattr(event, "risk_u", 0.0), getattr(event, "risk_distance_u", 0.0)):
        try:
            explicit = max(explicit, float(value))
        except (TypeError, ValueError):
            pass
    if explicit > 0:
        return explicit
    return max(
        abs(entry_price - event.frozen_low),
        abs(entry_price - event.frozen_high),
        float(min_risk_u),
        abs(entry_price) * float(min_risk_pct),
    )


def _first_hit(event_ts: float, bars: List[Dict[str, float]], entry: float, risk: float, direction: str) -> tuple[bool, bool, float, float]:
    plus_hit = False
    minus_hit = False
    plus_time = 0.0
    minus_time = 0.0
    for bar in bars:
        high = bar["high"]
        low = bar["low"]
        if direction == "BUY":
            plus_now = high >= entry + risk
            minus_now = low <= entry - risk
        elif direction == "SELL":
            plus_now = low <= entry - risk
            minus_now = high >= entry + risk
        else:
            plus_now = False
            minus_now = False
        if plus_now and not plus_hit:
            plus_hit = True
            plus_time = max(0.0, bar["timestamp"] - event_ts)
        if minus_now and not minus_hit:
            minus_hit = True
            minus_time = max(0.0, bar["timestamp"] - event_ts)
        if plus_hit and minus_hit:
            break
    if not plus_hit:
        plus_time = 0.0
    if not minus_hit:
        minus_time = 0.0
    return plus_hit and (not minus_hit or plus_time <= minus_time), minus_hit and (not plus_hit or minus_time < plus_time), plus_time, minus_time


def compute_forward_metric(
    event: A1EdgeEvent,
    bars: List[Dict[str, float]],
    window_sec: int,
    min_risk_u: float = 1.0,
    min_risk_pct: float = 0.0003,
    entry_price: Optional[float] = None,
    event_ts: Optional[float] = None,
) -> ForwardMetricResult:
    ts = float(event.event_ts if event_ts is None else event_ts)
    entry = float(event.last_price if entry_price is None else entry_price)
    if entry <= 0 and bars:
        entry = bars[0]["open"]
    risk = compute_proxy_risk(event, entry, min_risk_u, min_risk_pct)
    risk_pct = risk / entry if entry else 0.0
    timestamps = [bar["timestamp"] for bar in bars]
    start_idx = bisect_right(timestamps, ts)
    if not bars or start_idx >= len(bars):
        return ForwardMetricResult(
            zone_id=event.zone_id,
            symbol=event.symbol,
            direction=event.direction,
            event_ts=ts,
            entry_price=entry,
            risk_u=risk,
            risk_pct=risk_pct,
            a1_reaction_type=event.a1_reaction_type,
            reaction_event_kind=event.reaction_event_kind,
            legacy_phase2_type=event.legacy_phase2_type,
            frozen_reason=event.frozen_reason,
            frozen_state=event.frozen_state,
            window_sec=int(window_sec),
            partial_window=True,
            insufficient_future_data=True,
        )
    end_ts = ts + int(window_sec)
    future = [bar for bar in bars[start_idx:] if bar["timestamp"] <= end_ts]
    if not future:
        future = [bars[start_idx]]
    high = max(bar["high"] for bar in future)
    low = min(bar["low"] for bar in future)
    close = future[-1]["close"]
    upside = max(0.0, high - entry)
    downside = max(0.0, entry - low)
    total_range = max(0.0, high - low)
    if event.direction == "BUY":
        directional_mfe = high - entry
        directional_mae = entry - low
        hit_plus_2r = high >= entry + 2 * risk
        hit_plus_3r = high >= entry + 3 * risk
        hit_minus_1r = low <= entry - risk
    elif event.direction == "SELL":
        directional_mfe = entry - low
        directional_mae = high - entry
        hit_plus_2r = low <= entry - 2 * risk
        hit_plus_3r = low <= entry - 3 * risk
        hit_minus_1r = high >= entry + risk
    else:
        directional_mfe = 0.0
        directional_mae = 0.0
        hit_plus_2r = False
        hit_plus_3r = False
        hit_minus_1r = False
    first_plus, first_minus, plus_time, minus_time = _first_hit(ts, future, entry, risk, event.direction)
    partial = bool(future[-1]["timestamp"] < end_ts)
    return ForwardMetricResult(
        zone_id=event.zone_id,
        symbol=event.symbol,
        direction=event.direction,
        event_ts=ts,
        entry_price=entry,
        risk_u=risk,
        risk_pct=risk_pct,
        a1_reaction_type=event.a1_reaction_type,
        reaction_event_kind=event.reaction_event_kind,
        legacy_phase2_type=event.legacy_phase2_type,
        frozen_reason=event.frozen_reason,
        frozen_state=event.frozen_state,
        window_sec=int(window_sec),
        future_bar_count=len(future),
        directional_mfe_u=directional_mfe,
        directional_mae_u=directional_mae,
        directional_mfe_r=directional_mfe / risk if risk else 0.0,
        directional_mae_r=directional_mae / risk if risk else 0.0,
        directional_mfe_pct=directional_mfe / entry if entry else 0.0,
        directional_mae_pct=directional_mae / entry if entry else 0.0,
        upside_move_u=upside,
        downside_move_u=downside,
        upside_move_pct=upside / entry if entry else 0.0,
        downside_move_pct=downside / entry if entry else 0.0,
        total_range_u=total_range,
        total_range_pct=total_range / entry if entry else 0.0,
        close_return_pct=(close - entry) / entry if entry else 0.0,
        hit_plus_1r=first_plus or (event.direction == "BUY" and high >= entry + risk) or (event.direction == "SELL" and low <= entry - risk),
        hit_plus_2r=hit_plus_2r,
        hit_plus_3r=hit_plus_3r,
        hit_minus_1r=hit_minus_1r,
        first_hit_plus_1r=first_plus,
        first_hit_minus_1r=first_minus,
        time_to_plus_1r_sec=plus_time,
        time_to_minus_1r_sec=minus_time,
        partial_window=partial,
        insufficient_future_data=False,
    )


class A1ForwardMetricsAnalyzer:
    def __init__(self, windows_sec: Iterable[int] | None = None, min_risk_u: float = 1.0, min_risk_pct: float = 0.0003):
        self.windows_sec = list(windows_sec or DEFAULT_WINDOWS_SEC)
        self.min_risk_u = float(min_risk_u)
        self.min_risk_pct = float(min_risk_pct)

    def analyze(self, events: Iterable[A1EdgeEvent], klines: Iterable[Mapping[str, Any]]) -> List[ForwardMetricResult]:
        bars = normalize_klines(klines)
        results: List[ForwardMetricResult] = []
        for event in events or []:
            for window_sec in self.windows_sec:
                results.append(compute_forward_metric(event, bars, window_sec, self.min_risk_u, self.min_risk_pct))
        return results

    def export(self, results: Iterable[ForwardMetricResult], out_dir: Path | str) -> None:
        rows = [result.to_dict() for result in results or []]
        write_csv(Path(out_dir) / "a1_forward_metrics.csv", rows, FORWARD_METRIC_FIELDS)
