#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from typing import Any, Dict, Mapping


def parse_timestamp(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            return ts / 1000.0
        return ts
    text = str(value).strip()
    if not text:
        return default
    try:
        ts = float(text)
        if ts > 10_000_000_000:
            return ts / 1000.0
        return ts
    except ValueError:
        pass
    iso = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return default
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def parse_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_direction(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"BUY", "LONG", "BID", "UP"}:
        return "BUY"
    if text in {"SELL", "SHORT", "ASK", "DOWN"}:
        return "SELL"
    return "UNKNOWN"


def _first_present(record: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in record and record.get(name) not in (None, ""):
            return record.get(name)
    return None


def build_event_key(
    zone_id: str,
    direction: str,
    event_ts: float,
    frozen_low: float,
    frozen_high: float,
    a1_reaction_type: str,
    reaction_event_kind: str,
) -> str:
    if zone_id:
        return f"{zone_id}|{a1_reaction_type}|{reaction_event_kind}|{event_ts}"
    return f"{direction}|{event_ts}|{frozen_low}|{frozen_high}|{a1_reaction_type}|{reaction_event_kind}"


@dataclass(frozen=True)
class A1EdgeEvent:
    zone_id: str = ""
    event_key: str = ""
    symbol: str = ""
    direction: str = "UNKNOWN"
    frozen_ts: float = 0.0
    reaction_event_ts: float = 0.0
    event_ts: float = 0.0
    frozen_low: float = 0.0
    frozen_high: float = 0.0
    last_price: float = 0.0
    a1_reaction_type: str = "A1_REACTION_UNKNOWN"
    reaction_event_kind: str = "UNKNOWN"
    legacy_phase2_type: str = "UNKNOWN_RESEARCH"
    frozen_reason: str = ""
    frozen_state: str = ""
    has_swept_boundary: bool = False
    has_reclaimed_boundary: bool = False
    has_retested_inside_zone: bool = False
    has_confirmed: bool = False
    has_failed: bool = False
    absorption_score: float = 0.0
    pressure_decay_score: float = 0.0
    reclaim_score: float = 0.0
    retest_score: float = 0.0
    reload_score: float = 0.0
    book_absorption_score: float = 0.0
    relevant_book_depth_available: bool = False
    iceberg_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    positive_score: float = 0.0
    negative_score: float = 0.0
    net_score: float = 0.0

    def __post_init__(self) -> None:
        if not self.event_key:
            object.__setattr__(
                self,
                "event_key",
                build_event_key(
                    self.zone_id,
                    self.direction,
                    self.event_ts,
                    self.frozen_low,
                    self.frozen_high,
                    self.a1_reaction_type,
                    self.reaction_event_kind,
                ),
            )

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "A1EdgeEvent":
        r = record or {}
        frozen_ts = parse_timestamp(_first_present(r, "frozen_ts", "phase2_registered_ts", "registered_ts"))
        reaction_ts = parse_timestamp(_first_present(r, "reaction_event_ts", "a1_reaction_confirmed_ts"))
        event_ts = parse_timestamp(
            _first_present(r, "reaction_event_ts", "confirmed_ts", "frozen_ts", "ts", "timestamp")
        )
        if event_ts <= 0:
            event_ts = reaction_ts or frozen_ts
        zone_id = str(_first_present(r, "zone_id", "frozen_event_id") or "")
        direction = normalize_direction(_first_present(r, "direction", "side"))
        frozen_low = parse_float(_first_present(r, "frozen_low", "zone_low", "low"))
        frozen_high = parse_float(_first_present(r, "frozen_high", "zone_high", "high"))
        a1_reaction_type = str(_first_present(r, "a1_reaction_type", "reaction_type") or "A1_REACTION_UNKNOWN")
        reaction_event_kind = str(_first_present(r, "reaction_event_kind", "event_kind") or "UNKNOWN")
        event_key = str(_first_present(r, "event_key") or "") or build_event_key(
            zone_id,
            direction,
            event_ts,
            frozen_low,
            frozen_high,
            a1_reaction_type,
            reaction_event_kind,
        )
        return cls(
            zone_id=zone_id,
            event_key=event_key,
            symbol=str(_first_present(r, "symbol", "instId", "instrument") or ""),
            direction=direction,
            frozen_ts=frozen_ts,
            reaction_event_ts=reaction_ts,
            event_ts=event_ts,
            frozen_low=frozen_low,
            frozen_high=frozen_high,
            last_price=parse_float(_first_present(r, "last_price", "reaction_event_price", "price", "close")),
            a1_reaction_type=a1_reaction_type,
            reaction_event_kind=reaction_event_kind,
            legacy_phase2_type=str(_first_present(r, "legacy_phase2_type", "phase2_type") or "UNKNOWN_RESEARCH"),
            frozen_reason=str(_first_present(r, "frozen_reason") or ""),
            frozen_state=str(_first_present(r, "frozen_state", "state") or ""),
            has_swept_boundary=parse_bool(_first_present(r, "has_swept_boundary")),
            has_reclaimed_boundary=parse_bool(_first_present(r, "has_reclaimed_boundary")),
            has_retested_inside_zone=parse_bool(_first_present(r, "has_retested_inside_zone")),
            has_confirmed=parse_bool(_first_present(r, "has_confirmed")),
            has_failed=parse_bool(_first_present(r, "has_failed")),
            absorption_score=parse_float(_first_present(r, "absorption_score")),
            pressure_decay_score=parse_float(_first_present(r, "pressure_decay_score")),
            reclaim_score=parse_float(_first_present(r, "reclaim_score")),
            retest_score=parse_float(_first_present(r, "retest_score")),
            reload_score=parse_float(_first_present(r, "reload_score")),
            book_absorption_score=parse_float(_first_present(r, "book_absorption_score")),
            relevant_book_depth_available=parse_bool(_first_present(r, "relevant_book_depth_available")),
            iceberg_count=parse_int(_first_present(r, "iceberg_count")),
            high_count=parse_int(_first_present(r, "high_count")),
            medium_count=parse_int(_first_present(r, "medium_count")),
            low_count=parse_int(_first_present(r, "low_count")),
            positive_score=parse_float(_first_present(r, "positive_score")),
            negative_score=parse_float(_first_present(r, "negative_score")),
            net_score=parse_float(_first_present(r, "net_score", "a1_reaction_score", "phase2_total_score")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


A1_EDGE_EVENT_FIELDS = [field.name for field in fields(A1EdgeEvent)]


@dataclass(frozen=True)
class ForwardMetricResult:
    zone_id: str = ""
    event_key: str = ""
    symbol: str = ""
    direction: str = "UNKNOWN"
    event_ts: float = 0.0
    entry_price: float = 0.0
    risk_u: float = 0.0
    risk_pct: float = 0.0
    a1_reaction_type: str = ""
    reaction_event_kind: str = ""
    legacy_phase2_type: str = ""
    frozen_reason: str = ""
    frozen_state: str = ""
    window_sec: int = 0
    future_bar_count: int = 0
    directional_mfe_u: float = 0.0
    directional_mae_u: float = 0.0
    directional_mfe_r: float = 0.0
    directional_mae_r: float = 0.0
    directional_mfe_pct: float = 0.0
    directional_mae_pct: float = 0.0
    upside_move_u: float = 0.0
    downside_move_u: float = 0.0
    upside_move_pct: float = 0.0
    downside_move_pct: float = 0.0
    total_range_u: float = 0.0
    total_range_pct: float = 0.0
    close_return_pct: float = 0.0
    hit_plus_1r: bool = False
    hit_plus_2r: bool = False
    hit_plus_3r: bool = False
    hit_minus_1r: bool = False
    first_hit_plus_1r: bool = False
    first_hit_minus_1r: bool = False
    time_to_plus_1r_sec: float = 0.0
    time_to_minus_1r_sec: float = 0.0
    partial_window: bool = False
    insufficient_future_data: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


FORWARD_METRIC_FIELDS = [field.name for field in fields(ForwardMetricResult)]


@dataclass(frozen=True)
class RandomBaselineEvent:
    baseline_id: str = ""
    source_zone_id: str = ""
    source_event_key: str = ""
    symbol: str = ""
    direction: str = "UNKNOWN"
    random_event_ts: float = 0.0
    entry_price: float = 0.0
    source_risk_u: float = 0.0
    risk_mode: str = "SOURCE_A1_RISK"
    window_sec: int = 0
    future_bar_count: int = 0
    insufficient_future_data: bool = False
    directional_mfe_r: float = 0.0
    directional_mae_r: float = 0.0
    hit_plus_1r: bool = False
    hit_plus_2r: bool = False
    hit_plus_3r: bool = False
    hit_minus_1r: bool = False
    total_range_u: float = 0.0
    total_range_pct: float = 0.0
    close_return_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


RANDOM_BASELINE_FIELDS = [field.name for field in fields(RandomBaselineEvent)]


@dataclass(frozen=True)
class HypothesisResult:
    hypothesis_id: str = ""
    zone_id: str = ""
    event_key: str = ""
    symbol: str = ""
    direction: str = "UNKNOWN"
    hypothesis_type: str = ""
    is_skipped: bool = False
    skip_reason: str = ""
    entry_ts: float = 0.0
    entry_price: float = 0.0
    stop_price: float = 0.0
    risk_u: float = 0.0
    risk_pct: float = 0.0
    fee_share_r: float = 0.0
    window_sec: int = 0
    future_bar_count: int = 0
    insufficient_future_data: bool = False
    mfe_r: float = 0.0
    mae_r: float = 0.0
    hit_1r: bool = False
    hit_2r: bool = False
    hit_3r: bool = False
    hit_stop: bool = False
    first_hit_plus_1r: bool = False
    first_hit_minus_1r: bool = False
    time_to_1r_sec: float = 0.0
    time_to_stop_sec: float = 0.0
    realized_r_proxy: float = 0.0
    a1_reaction_type: str = ""
    reaction_event_kind: str = ""
    legacy_phase2_type: str = ""
    frozen_reason: str = ""
    frozen_state: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


HYPOTHESIS_RESULT_FIELDS = [field.name for field in fields(HypothesisResult)]


@dataclass(frozen=True)
class A1EdgeReport:
    decision: str = "INSUFFICIENT_SAMPLE"
    total_a1_events: int = 0
    total_random_baseline_samples: int = 0
    total_hypothesis_rows: int = 0
    recommended_next_step: str = "Continue data collection. Do not make strategic conclusion."

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
