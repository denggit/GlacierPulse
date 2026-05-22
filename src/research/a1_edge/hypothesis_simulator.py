#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_left, bisect_right
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Mapping

from .forward_metrics import compute_forward_metric
from .io_utils import normalize_klines, write_csv
from .schema import A1EdgeEvent, HYPOTHESIS_RESULT_FIELDS, HypothesisResult, parse_bool


HYPOTHESES = [
    "IMMEDIATE_ENTRY",
    "NEXT_BAR_ENTRY",
    "DELAYED_ENTRY_1M",
    "DELAYED_ENTRY_3M",
    "FAST_CLEAN_HOLD_ENTRY",
    "ZONE_RETEST_ENTRY",
    "BREAKOUT_AWAY_ENTRY",
    "FAILED_RECLAIM_REVERSE_ENTRY",
]

HYPOTHESIS_SUMMARY_FIELDS = [
    "dimension",
    "group",
    "sample_count",
    "skipped_count",
    "valid_count",
    "avg_realized_r_proxy",
    "median_realized_r_proxy",
    "hit_1r_rate",
    "hit_2r_rate",
    "hit_3r_rate",
    "net_hit_1r_rate",
    "net_hit_2r_rate",
    "net_hit_3r_rate",
    "hit_stop_rate",
    "avg_mfe_r",
    "avg_mae_r",
    "avg_fee_share_r",
    "best_realized_r_proxy",
    "worst_realized_r_proxy",
    "right_tail_3r_count",
    "edge_label",
]


def _avg(values: Iterable[float]) -> float:
    data = [float(v) for v in values]
    return sum(data) / len(data) if data else 0.0


def _rate(values: Iterable[Any]) -> float:
    data = list(values)
    return sum(1 for v in data if parse_bool(v)) / len(data) if data else 0.0


class A1HypothesisSimulator:
    def __init__(
        self,
        window_sec: int = 3600,
        stop_buffer_u: float = 0.8,
        min_risk_u: float = 1.0,
        min_risk_pct: float = 0.0003,
        roundtrip_fee_pct: float = 0.001,
        min_group_sample_size: int = 30,
    ):
        self.window_sec = int(window_sec)
        self.stop_buffer_u = float(stop_buffer_u)
        self.min_risk_u = float(min_risk_u)
        self.min_risk_pct = float(min_risk_pct)
        self.roundtrip_fee_pct = float(roundtrip_fee_pct)
        self.min_group_sample_size = int(min_group_sample_size)

    def _entry(self, hypothesis: str, event: A1EdgeEvent, bars: List[Dict[str, float]]) -> tuple[bool, str, float, float, str]:
        timestamps = [bar["timestamp"] for bar in bars]
        if hypothesis == "IMMEDIATE_ENTRY":
            return False, "", event.event_ts, event.last_price, event.direction
        if hypothesis == "NEXT_BAR_ENTRY":
            idx = bisect_right(timestamps, event.event_ts)
        elif hypothesis == "DELAYED_ENTRY_1M":
            idx = bisect_left(timestamps, event.event_ts + 60)
        elif hypothesis == "DELAYED_ENTRY_3M":
            idx = bisect_left(timestamps, event.event_ts + 180)
        elif hypothesis == "FAST_CLEAN_HOLD_ENTRY":
            if "CLEAN_HOLD" not in event.a1_reaction_type:
                return True, "requires_clean_hold_reaction", 0.0, 0.0, event.direction
            idx = bisect_right(timestamps, event.event_ts)
        elif hypothesis == "ZONE_RETEST_ENTRY":
            for bar in bars[bisect_right(timestamps, event.event_ts) :]:
                if bar["low"] <= event.frozen_high and bar["high"] >= event.frozen_low:
                    price = (event.frozen_low + event.frozen_high) / 2.0
                    return False, "", bar["timestamp"], price, event.direction
            return True, "zone_retest_not_seen", 0.0, 0.0, event.direction
        elif hypothesis == "BREAKOUT_AWAY_ENTRY":
            if "BREAKOUT_AWAY" not in event.a1_reaction_type and not event.has_swept_boundary:
                return True, "requires_breakout_or_sweep", 0.0, 0.0, event.direction
            idx = bisect_right(timestamps, event.event_ts)
        elif hypothesis == "FAILED_RECLAIM_REVERSE_ENTRY":
            if "FAILED_RECLAIM" not in event.a1_reaction_type and not event.has_failed:
                return True, "requires_failed_reclaim", 0.0, 0.0, event.direction
            idx = bisect_right(timestamps, event.event_ts)
            direction = "SELL" if event.direction == "BUY" else "BUY" if event.direction == "SELL" else "UNKNOWN"
            if idx >= len(bars):
                return True, "insufficient_future_data", 0.0, 0.0, direction
            return False, "", bars[idx]["timestamp"], bars[idx]["open"], direction
        else:
            return True, "unsupported_hypothesis", 0.0, 0.0, event.direction
        if idx >= len(bars):
            return True, "insufficient_future_data", 0.0, 0.0, event.direction
        return False, "", bars[idx]["timestamp"], bars[idx]["open"], event.direction

    def _stop_price(self, event: A1EdgeEvent, direction: str) -> float:
        if direction == "BUY":
            return event.frozen_low - self.stop_buffer_u
        if direction == "SELL":
            return event.frozen_high + self.stop_buffer_u
        return 0.0

    def simulate(self, events: Iterable[A1EdgeEvent], klines: Iterable[Mapping[str, Any]]) -> List[HypothesisResult]:
        bars = normalize_klines(klines)
        results: List[HypothesisResult] = []
        for event_idx, event in enumerate(events or []):
            for hypothesis in HYPOTHESES:
                skipped, reason, entry_ts, entry_price, direction = self._entry(hypothesis, event, bars)
                hypothesis_id = f"{event.event_key}-{hypothesis}"
                if skipped or not bars or entry_price <= 0 or direction == "UNKNOWN":
                    results.append(
                        HypothesisResult(
                            hypothesis_id=hypothesis_id,
                            zone_id=event.zone_id,
                            event_key=event.event_key,
                            symbol=event.symbol,
                            direction=direction,
                            hypothesis_type=hypothesis,
                            is_skipped=True,
                            skip_reason=reason or "invalid_entry",
                            window_sec=self.window_sec,
                            a1_reaction_type=event.a1_reaction_type,
                            reaction_event_kind=event.reaction_event_kind,
                            legacy_phase2_type=event.legacy_phase2_type,
                            frozen_reason=event.frozen_reason,
                            frozen_state=event.frozen_state,
                        )
                    )
                    continue
                stop_price = self._stop_price(event, direction)
                risk = max(abs(entry_price - stop_price), self.min_risk_u, abs(entry_price) * self.min_risk_pct)
                fee_u = entry_price * self.roundtrip_fee_pct
                fee_share_r = fee_u / risk if risk else 0.0
                metric_event = A1EdgeEvent(
                    zone_id=event.zone_id,
                    event_key=event.event_key,
                    symbol=event.symbol,
                    direction=direction,
                    event_ts=entry_ts,
                    frozen_low=event.frozen_low,
                    frozen_high=event.frozen_high,
                    last_price=entry_price,
                    a1_reaction_type=event.a1_reaction_type,
                    reaction_event_kind=event.reaction_event_kind,
                    legacy_phase2_type=event.legacy_phase2_type,
                    frozen_reason=event.frozen_reason,
                    frozen_state=event.frozen_state,
                )
                metric = compute_forward_metric(
                    metric_event,
                    bars,
                    self.window_sec,
                    self.min_risk_u,
                    self.min_risk_pct,
                    entry_price,
                    entry_ts,
                    risk_u_override=risk,
                    roundtrip_fee_pct=self.roundtrip_fee_pct,
                )
                hit_stop = metric.hit_minus_1r
                if metric.first_hit_minus_1r and (not metric.first_hit_plus_1r or metric.time_to_minus_1r_sec <= metric.time_to_plus_1r_sec):
                    realized = -1.0 - fee_share_r
                elif metric.hit_plus_2r:
                    realized = 2.0 - fee_share_r
                elif metric.hit_plus_1r:
                    realized = 1.0 - fee_share_r
                else:
                    close_return_r = metric.close_return_pct * entry_price / risk if risk else 0.0
                    if direction == "SELL":
                        close_return_r *= -1.0
                    realized = close_return_r - fee_share_r
                results.append(
                    HypothesisResult(
                        hypothesis_id=hypothesis_id,
                        zone_id=event.zone_id,
                        event_key=event.event_key,
                        symbol=event.symbol,
                        direction=direction,
                        hypothesis_type=hypothesis,
                        entry_ts=entry_ts,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        risk_u=risk,
                        risk_pct=risk / entry_price if entry_price else 0.0,
                        fee_u=fee_u,
                        roundtrip_fee_pct=self.roundtrip_fee_pct,
                        fee_share_r=fee_share_r,
                        window_sec=self.window_sec,
                        future_bar_count=metric.future_bar_count,
                        insufficient_future_data=metric.insufficient_future_data,
                        mfe_r=metric.directional_mfe_r,
                        mae_r=metric.directional_mae_r,
                        hit_1r=metric.hit_plus_1r,
                        hit_2r=metric.hit_plus_2r,
                        hit_3r=metric.hit_plus_3r,
                        net_hit_1r=metric.net_hit_plus_1r,
                        net_hit_2r=metric.net_hit_plus_2r,
                        net_hit_3r=metric.net_hit_plus_3r,
                        hit_stop=hit_stop,
                        first_hit_plus_1r=metric.first_hit_plus_1r,
                        first_hit_minus_1r=metric.first_hit_minus_1r,
                        time_to_1r_sec=metric.time_to_plus_1r_sec,
                        time_to_stop_sec=metric.time_to_minus_1r_sec,
                        realized_r_proxy=realized,
                        a1_reaction_type=event.a1_reaction_type,
                        reaction_event_kind=event.reaction_event_kind,
                        legacy_phase2_type=event.legacy_phase2_type,
                        frozen_reason=event.frozen_reason,
                        frozen_state=event.frozen_state,
                    )
                )
        return results

    def summarize(self, results: Iterable[HypothesisResult]) -> List[Dict[str, Any]]:
        rows = [r.to_dict() if hasattr(r, "to_dict") else dict(r) for r in results or []]
        group_specs: List[tuple[str, str, List[Dict[str, Any]]]] = [("ALL", "ALL", rows)]
        for dimension in ("hypothesis_type", "a1_reaction_type", "reaction_event_kind", "frozen_reason", "frozen_state", "direction"):
            values = sorted({str(row.get(dimension) or "UNKNOWN") for row in rows})
            group_specs.extend((dimension, value, [row for row in rows if str(row.get(dimension) or "UNKNOWN") == value]) for value in values)
        for left, right in (("hypothesis_type", "a1_reaction_type"), ("hypothesis_type", "frozen_reason"), ("hypothesis_type", "frozen_state")):
            values = sorted({(str(row.get(left) or "UNKNOWN"), str(row.get(right) or "UNKNOWN")) for row in rows})
            group_specs.extend((f"{left}+{right}", f"{a}|{b}", [row for row in rows if str(row.get(left) or "UNKNOWN") == a and str(row.get(right) or "UNKNOWN") == b]) for a, b in values)
        summaries: List[Dict[str, Any]] = []
        for dimension, group, group_rows in group_specs:
            valid = [
                row
                for row in group_rows
                if not parse_bool(row.get("is_skipped")) and not parse_bool(row.get("insufficient_future_data"))
            ]
            realized = [float(row.get("realized_r_proxy", 0.0)) for row in valid]
            avg_realized = _avg(realized)
            net_hit_1r_rate = _rate(row.get("net_hit_1r") for row in valid)
            if valid and not any("net_hit_1r" in row and row.get("net_hit_1r") not in (None, "") for row in valid):
                net_hit_1r_rate = _rate(row.get("hit_1r") for row in valid)
            if len(valid) < self.min_group_sample_size:
                label = "INSUFFICIENT_SAMPLE"
            elif avg_realized > 0 and net_hit_1r_rate > _rate(row.get("hit_stop") for row in valid):
                label = "PROMISING"
            else:
                label = "NO_EDGE"
            summaries.append(
                {
                    "dimension": dimension,
                    "group": group,
                    "sample_count": len(group_rows),
                    "skipped_count": sum(1 for row in group_rows if row.get("is_skipped")),
                    "valid_count": len(valid),
                    "avg_realized_r_proxy": avg_realized,
                    "median_realized_r_proxy": median(realized) if realized else 0.0,
                    "hit_1r_rate": _rate(row.get("hit_1r") for row in valid),
                    "hit_2r_rate": _rate(row.get("hit_2r") for row in valid),
                    "hit_3r_rate": _rate(row.get("hit_3r") for row in valid),
                    "net_hit_1r_rate": net_hit_1r_rate,
                    "net_hit_2r_rate": _rate(row.get("net_hit_2r") for row in valid),
                    "net_hit_3r_rate": _rate(row.get("net_hit_3r") for row in valid),
                    "hit_stop_rate": _rate(row.get("hit_stop") for row in valid),
                    "avg_mfe_r": _avg(float(row.get("mfe_r", 0.0)) for row in valid),
                    "avg_mae_r": _avg(float(row.get("mae_r", 0.0)) for row in valid),
                    "avg_fee_share_r": _avg(float(row.get("fee_share_r", 0.0)) for row in valid),
                    "best_realized_r_proxy": max(realized) if realized else 0.0,
                    "worst_realized_r_proxy": min(realized) if realized else 0.0,
                    "right_tail_3r_count": sum(1 for row in valid if row.get("hit_3r")),
                    "edge_label": label,
                }
            )
        return summaries

    def best_by_group(self, summary: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in summary or [] if row.get("dimension") == "hypothesis_type"]
        return sorted(rows, key=lambda row: float(row.get("avg_realized_r_proxy", 0.0)), reverse=True)

    def export(self, results: Iterable[HypothesisResult], summary: Iterable[Mapping[str, Any]], out_dir: Path | str) -> None:
        out = Path(out_dir)
        result_rows = [row.to_dict() for row in results or []]
        summary_rows = [dict(row) for row in summary or []]
        write_csv(out / "a1_hypothesis_results.csv", result_rows, HYPOTHESIS_RESULT_FIELDS)
        write_csv(out / "a1_hypothesis_summary.csv", summary_rows, HYPOTHESIS_SUMMARY_FIELDS)
        write_csv(out / "a1_best_hypothesis_by_group.csv", self.best_by_group(summary_rows), HYPOTHESIS_SUMMARY_FIELDS)
