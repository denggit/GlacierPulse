#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .forward_metrics import DEFAULT_WINDOWS_SEC, compute_forward_metric, compute_proxy_risk
from .io_utils import normalize_klines, write_csv
from .schema import A1EdgeEvent, RANDOM_BASELINE_FIELDS, RandomBaselineEvent, parse_bool


SUMMARY_FIELDS = [
    "dimension",
    "group",
    "a1_sample_count",
    "random_sample_count",
    "a1_avg_net_mfe_r_15m",
    "random_avg_net_mfe_r_15m",
    "a1_avg_net_mfe_r_60m",
    "random_avg_net_mfe_r_60m",
    "a1_net_hit_1r_rate_15m",
    "random_net_hit_1r_rate_15m",
    "a1_net_hit_2r_rate_15m",
    "random_net_hit_2r_rate_15m",
    "a1_net_hit_3r_rate_60m",
    "random_net_hit_3r_rate_60m",
    "net_mfe_edge_15m",
    "net_mfe_edge_60m",
    "net_hit_1r_edge_15m",
    "a1_avg_fee_share_r_15m",
    "random_avg_fee_share_r_15m",
    "a1_avg_raw_mfe_r_15m",
    "random_avg_raw_mfe_r_15m",
    "raw_mfe_edge_15m",
    "a1_raw_hit_1r_rate_15m",
    "random_raw_hit_1r_rate_15m",
    "a1_avg_mfe_r_15m",
    "random_avg_mfe_r_15m",
    "a1_avg_mfe_r_60m",
    "random_avg_mfe_r_60m",
    "a1_hit_1r_rate_15m",
    "random_hit_1r_rate_15m",
    "a1_hit_2r_rate_15m",
    "random_hit_2r_rate_15m",
    "a1_hit_3r_rate_60m",
    "random_hit_3r_rate_60m",
    "a1_avg_total_range_15m",
    "random_avg_total_range_15m",
    "mfe_edge_15m",
    "mfe_edge_60m",
    "hit_1r_edge_15m",
    "volatility_edge_15m",
    "edge_label",
]


def _day_key(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def _avg(values: Iterable[float]) -> float:
    data = [float(v) for v in values]
    return sum(data) / len(data) if data else 0.0


def _rate(values: Iterable[Any]) -> float:
    data = list(values)
    return sum(1 for v in data if parse_bool(v)) / len(data) if data else 0.0


def _value(row: Mapping[str, Any], primary: str, fallback: str, default: Any = 0.0) -> Any:
    value = row.get(primary)
    if value not in (None, ""):
        return value
    return row.get(fallback, default)


class RandomBaselineSampler:
    def __init__(
        self,
        samples_per_event: int = 10,
        random_seed: int = 42,
        exclude_near_a1_minutes: float = 5.0,
        windows_sec: Sequence[int] | None = None,
        min_risk_u: float = 1.0,
        min_risk_pct: float = 0.0003,
        baseline_risk_mode: str = "source",
        roundtrip_fee_pct: float = 0.001,
    ):
        self.samples_per_event = int(samples_per_event)
        self.random = random.Random(int(random_seed))
        self.exclude_near_a1_sec = float(exclude_near_a1_minutes) * 60.0
        self.windows_sec = list(windows_sec or DEFAULT_WINDOWS_SEC)
        self.min_risk_u = float(min_risk_u)
        self.min_risk_pct = float(min_risk_pct)
        self.roundtrip_fee_pct = float(roundtrip_fee_pct)
        normalized_mode = str(baseline_risk_mode or "source").strip().lower()
        if normalized_mode not in {"source", "local"}:
            raise ValueError("baseline_risk_mode must be 'source' or 'local'")
        self.baseline_risk_mode = normalized_mode

    def sample(self, events: Iterable[A1EdgeEvent], klines: Iterable[Mapping[str, Any]]) -> List[RandomBaselineEvent]:
        bars = normalize_klines(klines)
        if not bars:
            return []
        by_day: Dict[str, List[int]] = defaultdict(list)
        for idx, bar in enumerate(bars):
            if idx > 0:
                by_day[_day_key(bar["timestamp"])].append(idx)
        rows: List[RandomBaselineEvent] = []
        for event_idx, event in enumerate(events or []):
            source_risk_u = compute_proxy_risk(
                event,
                event.last_price,
                min_risk_u=self.min_risk_u,
                min_risk_pct=self.min_risk_pct,
            )
            candidates = [
                idx
                for idx in by_day.get(_day_key(event.event_ts), [])
                if abs(bars[idx - 1]["timestamp"] - event.event_ts) > self.exclude_near_a1_sec
            ]
            if not candidates:
                continue
            picked = [self.random.choice(candidates) for _ in range(self.samples_per_event)]
            for sample_idx, bar_idx in enumerate(picked):
                event_ts = bars[bar_idx - 1]["timestamp"]
                entry = bars[bar_idx]["open"]
                random_frozen_low = event.frozen_low
                random_frozen_high = event.frozen_high
                risk_override = source_risk_u
                risk_mode = "SOURCE_A1_RISK"
                if self.baseline_risk_mode == "local":
                    random_frozen_low = entry - source_risk_u
                    random_frozen_high = entry + source_risk_u
                    risk_override = None
                    risk_mode = "LOCAL_PROXY_RISK"
                source = A1EdgeEvent(
                    zone_id=event.zone_id,
                    event_key=event.event_key,
                    symbol=event.symbol,
                    direction=event.direction,
                    event_ts=event_ts,
                    frozen_low=random_frozen_low,
                    frozen_high=random_frozen_high,
                    last_price=entry,
                    a1_reaction_type=event.a1_reaction_type,
                    reaction_event_kind=event.reaction_event_kind,
                    legacy_phase2_type=event.legacy_phase2_type,
                    frozen_reason=event.frozen_reason,
                    frozen_state=event.frozen_state,
                )
                for window_sec in self.windows_sec:
                    metric = compute_forward_metric(
                        source,
                        bars,
                        window_sec,
                        min_risk_u=self.min_risk_u,
                        min_risk_pct=self.min_risk_pct,
                        entry_price=entry,
                        event_ts=event_ts,
                        risk_u_override=risk_override,
                        roundtrip_fee_pct=self.roundtrip_fee_pct,
                    )
                    rows.append(
                        RandomBaselineEvent(
                            baseline_id=f"{event_idx}-{sample_idx}",
                            source_zone_id=event.zone_id,
                            source_event_key=event.event_key,
                            symbol=event.symbol,
                            direction=event.direction,
                            random_event_ts=event_ts,
                            entry_price=entry,
                            source_risk_u=source_risk_u,
                            risk_mode=risk_mode,
                            fee_u=metric.fee_u,
                            roundtrip_fee_pct=metric.roundtrip_fee_pct,
                            fee_share_r=metric.fee_share_r,
                            window_sec=window_sec,
                            future_bar_count=metric.future_bar_count,
                            insufficient_future_data=metric.insufficient_future_data,
                            directional_mfe_r=metric.directional_mfe_r,
                            directional_mae_r=metric.directional_mae_r,
                            net_directional_mfe_r=metric.net_directional_mfe_r,
                            net_directional_mae_r=metric.net_directional_mae_r,
                            hit_plus_1r=metric.hit_plus_1r,
                            hit_plus_2r=metric.hit_plus_2r,
                            hit_plus_3r=metric.hit_plus_3r,
                            hit_minus_1r=metric.hit_minus_1r,
                            net_hit_plus_1r=metric.net_hit_plus_1r,
                            net_hit_plus_2r=metric.net_hit_plus_2r,
                            net_hit_plus_3r=metric.net_hit_plus_3r,
                            net_hit_minus_1r=metric.net_hit_minus_1r,
                            total_range_u=metric.total_range_u,
                            total_range_pct=metric.total_range_pct,
                            close_return_pct=metric.close_return_pct,
                        )
                    )
        return rows


class A1RandomBaselineComparator:
    def __init__(self, min_group_sample_size: int = 30):
        self.min_group_sample_size = int(min_group_sample_size)

    def _event_value(self, event: A1EdgeEvent, dimension: str) -> str:
        value = getattr(event, dimension, "")
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value or "UNKNOWN")

    def _groups(self, events: List[A1EdgeEvent]) -> List[tuple[str, str, set[str]]]:
        groups: List[tuple[str, str, set[str]]] = [("ALL", "ALL", {e.event_key for e in events})]
        for dimension in (
            "a1_reaction_type",
            "reaction_event_kind",
            "legacy_phase2_type",
            "frozen_reason",
            "frozen_state",
            "direction",
            "relevant_book_depth_available",
        ):
            values: Dict[str, set[str]] = defaultdict(set)
            for event in events:
                values[self._event_value(event, dimension)].add(event.event_key)
            groups.extend((dimension, value, zone_ids) for value, zone_ids in values.items())
        return groups

    def summarize(
        self,
        events: Iterable[A1EdgeEvent],
        a1_metrics: Iterable[Mapping[str, Any]],
        random_baseline: Iterable[RandomBaselineEvent],
    ) -> List[Dict[str, Any]]:
        event_list = list(events or [])
        a1_rows = [dict(row) for row in a1_metrics or []]
        random_rows = [row.to_dict() if hasattr(row, "to_dict") else dict(row) for row in random_baseline or []]
        valid_a1_rows = [row for row in a1_rows if not parse_bool(row.get("insufficient_future_data"))]
        valid_random_rows = [row for row in random_rows if not parse_bool(row.get("insufficient_future_data"))]
        summaries: List[Dict[str, Any]] = []
        for dimension, group, event_keys in self._groups(event_list):
            a1_15 = [r for r in valid_a1_rows if r.get("event_key") in event_keys and int(float(r.get("window_sec", 0))) == 900]
            a1_60 = [r for r in valid_a1_rows if r.get("event_key") in event_keys and int(float(r.get("window_sec", 0))) == 3600]
            rnd_15 = [r for r in valid_random_rows if r.get("source_event_key") in event_keys and int(float(r.get("window_sec", 0))) == 900]
            rnd_60 = [r for r in valid_random_rows if r.get("source_event_key") in event_keys and int(float(r.get("window_sec", 0))) == 3600]
            a1_avg_net_mfe_15 = _avg(float(_value(r, "net_directional_mfe_r", "directional_mfe_r", 0.0)) for r in a1_15)
            rnd_avg_net_mfe_15 = _avg(float(_value(r, "net_directional_mfe_r", "directional_mfe_r", 0.0)) for r in rnd_15)
            a1_avg_net_mfe_60 = _avg(float(_value(r, "net_directional_mfe_r", "directional_mfe_r", 0.0)) for r in a1_60)
            rnd_avg_net_mfe_60 = _avg(float(_value(r, "net_directional_mfe_r", "directional_mfe_r", 0.0)) for r in rnd_60)
            a1_net_hit_1r_15 = _rate(_value(r, "net_hit_plus_1r", "hit_plus_1r", False) for r in a1_15)
            rnd_net_hit_1r_15 = _rate(_value(r, "net_hit_plus_1r", "hit_plus_1r", False) for r in rnd_15)
            a1_avg_raw_mfe_15 = _avg(float(r.get("directional_mfe_r", 0.0)) for r in a1_15)
            rnd_avg_raw_mfe_15 = _avg(float(r.get("directional_mfe_r", 0.0)) for r in rnd_15)
            a1_avg_raw_mfe_60 = _avg(float(r.get("directional_mfe_r", 0.0)) for r in a1_60)
            rnd_avg_raw_mfe_60 = _avg(float(r.get("directional_mfe_r", 0.0)) for r in rnd_60)
            a1_raw_hit_1r_15 = _rate(r.get("hit_plus_1r") for r in a1_15)
            rnd_raw_hit_1r_15 = _rate(r.get("hit_plus_1r") for r in rnd_15)
            a1_range_15 = _avg(float(r.get("total_range_u", 0.0)) for r in a1_15)
            rnd_range_15 = _avg(float(r.get("total_range_u", 0.0)) for r in rnd_15)
            sample_count = len({r.get("event_key") for r in a1_15})
            directional = (
                a1_avg_net_mfe_15 > rnd_avg_net_mfe_15 * 1.25
                and a1_net_hit_1r_15 > rnd_net_hit_1r_15 + 0.05
            )
            volatility = a1_range_15 > rnd_range_15 * 1.25 if rnd_range_15 > 0 else a1_range_15 > 0
            if sample_count < self.min_group_sample_size:
                label = "INSUFFICIENT_SAMPLE"
            elif directional:
                label = "STRONG_DIRECTIONAL_EDGE"
            elif volatility:
                label = "VOLATILITY_EDGE"
            else:
                label = "NO_EDGE"
            summaries.append(
                {
                    "dimension": dimension,
                    "group": group,
                    "a1_sample_count": sample_count,
                    "random_sample_count": len(rnd_15),
                    "a1_avg_net_mfe_r_15m": a1_avg_net_mfe_15,
                    "random_avg_net_mfe_r_15m": rnd_avg_net_mfe_15,
                    "a1_avg_net_mfe_r_60m": a1_avg_net_mfe_60,
                    "random_avg_net_mfe_r_60m": rnd_avg_net_mfe_60,
                    "a1_net_hit_1r_rate_15m": a1_net_hit_1r_15,
                    "random_net_hit_1r_rate_15m": rnd_net_hit_1r_15,
                    "a1_net_hit_2r_rate_15m": _rate(_value(r, "net_hit_plus_2r", "hit_plus_2r", False) for r in a1_15),
                    "random_net_hit_2r_rate_15m": _rate(_value(r, "net_hit_plus_2r", "hit_plus_2r", False) for r in rnd_15),
                    "a1_net_hit_3r_rate_60m": _rate(_value(r, "net_hit_plus_3r", "hit_plus_3r", False) for r in a1_60),
                    "random_net_hit_3r_rate_60m": _rate(_value(r, "net_hit_plus_3r", "hit_plus_3r", False) for r in rnd_60),
                    "net_mfe_edge_15m": a1_avg_net_mfe_15 - rnd_avg_net_mfe_15,
                    "net_mfe_edge_60m": a1_avg_net_mfe_60 - rnd_avg_net_mfe_60,
                    "net_hit_1r_edge_15m": a1_net_hit_1r_15 - rnd_net_hit_1r_15,
                    "a1_avg_fee_share_r_15m": _avg(float(r.get("fee_share_r", 0.0)) for r in a1_15),
                    "random_avg_fee_share_r_15m": _avg(float(r.get("fee_share_r", 0.0)) for r in rnd_15),
                    "a1_avg_raw_mfe_r_15m": a1_avg_raw_mfe_15,
                    "random_avg_raw_mfe_r_15m": rnd_avg_raw_mfe_15,
                    "raw_mfe_edge_15m": a1_avg_raw_mfe_15 - rnd_avg_raw_mfe_15,
                    "a1_raw_hit_1r_rate_15m": a1_raw_hit_1r_15,
                    "random_raw_hit_1r_rate_15m": rnd_raw_hit_1r_15,
                    "a1_avg_mfe_r_15m": a1_avg_raw_mfe_15,
                    "random_avg_mfe_r_15m": rnd_avg_raw_mfe_15,
                    "a1_avg_mfe_r_60m": a1_avg_raw_mfe_60,
                    "random_avg_mfe_r_60m": rnd_avg_raw_mfe_60,
                    "a1_hit_1r_rate_15m": a1_raw_hit_1r_15,
                    "random_hit_1r_rate_15m": rnd_raw_hit_1r_15,
                    "a1_hit_2r_rate_15m": _rate(r.get("hit_plus_2r") for r in a1_15),
                    "random_hit_2r_rate_15m": _rate(r.get("hit_plus_2r") for r in rnd_15),
                    "a1_hit_3r_rate_60m": _rate(r.get("hit_plus_3r") for r in a1_60),
                    "random_hit_3r_rate_60m": _rate(r.get("hit_plus_3r") for r in rnd_60),
                    "a1_avg_total_range_15m": a1_range_15,
                    "random_avg_total_range_15m": rnd_range_15,
                    "mfe_edge_15m": a1_avg_raw_mfe_15 - rnd_avg_raw_mfe_15,
                    "mfe_edge_60m": a1_avg_raw_mfe_60 - rnd_avg_raw_mfe_60,
                    "hit_1r_edge_15m": a1_raw_hit_1r_15 - rnd_raw_hit_1r_15,
                    "volatility_edge_15m": a1_range_15 - rnd_range_15,
                    "edge_label": label,
                }
            )
        return summaries

    def export(self, baseline: Iterable[RandomBaselineEvent], summary: Iterable[Mapping[str, Any]], out_dir: Path | str) -> None:
        out = Path(out_dir)
        baseline_rows = [row.to_dict() for row in baseline or []]
        summary_rows = [dict(row) for row in summary or []]
        write_csv(out / "a1_random_baseline.csv", baseline_rows, RANDOM_BASELINE_FIELDS)
        write_csv(out / "a1_vs_random_summary.csv", summary_rows, SUMMARY_FIELDS)
        md_lines = [
            "# A1 vs Random Summary",
            "",
            "Core edge labels use fee-aware net R. Raw fields are diagnostic only.",
            "",
            "| Dimension | Group | Edge Label | Net MFE Edge 15m | Net Hit 1R Edge 15m | Raw MFE Edge 15m | Vol Edge 15m |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
        for row in summary_rows:
            md_lines.append(
                f"| {row.get('dimension')} | {row.get('group')} | {row.get('edge_label')} | "
                f"{float(row.get('net_mfe_edge_15m', 0.0)):.4f} | {float(row.get('net_hit_1r_edge_15m', 0.0)):.4f} | "
                f"{float(row.get('raw_mfe_edge_15m', 0.0)):.4f} | "
                f"{float(row.get('volatility_edge_15m', 0.0)):.4f} |"
            )
        path = out / "a1_vs_random_summary.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
