#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from src.research.a1_edge.schema import parse_bool, parse_float, parse_int, parse_timestamp


SCHEMA_VERSION = "v6.3.11.5.zone_truth.1"
MATCH_EXACT = "exact"
MATCH_FUZZY = "fuzzy"
MATCH_UNMATCHED = "unmatched"
SOURCE_REACTION = "a1_reaction"
SOURCE_CANDIDATE = "candidate_zone"
SOURCE_SYNTHETIC = "synthetic_from_pie"


def first_present(record: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in record and record.get(name) not in (None, ""):
            return record.get(name)
    return None


def normalize_direction(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"BUY", "LONG", "BID", "UP"}:
        return "BUY"
    if text in {"SELL", "SHORT", "ASK", "DOWN"}:
        return "SELL"
    return "UNKNOWN"


def local_session(ts: float, timezone: str = "Asia/Shanghai") -> dict[str, Any]:
    if not ts or ts <= 0:
        return {"local_time": "", "session_tag": "UNKNOWN", "is_weekend": False}
    dt = datetime.fromtimestamp(float(ts), tz=ZoneInfo(timezone))
    hour = dt.hour
    if 8 <= hour < 16:
        session = "ASIA"
    elif 16 <= hour < 21:
        session = "EUROPE"
    elif 21 <= hour or hour < 5:
        session = "US"
    else:
        session = "OFF_HOURS"
    return {
        "local_time": dt.isoformat(),
        "session_tag": session,
        "is_weekend": dt.weekday() >= 5,
    }


def truth_score(record: Mapping[str, Any]) -> float:
    score = record.get("truth_score")
    if isinstance(score, Mapping):
        return parse_float(score.get("truth_score_total"))
    return parse_float(first_present(record, "truth_score_total", "truth_score"))


def truth_label(record: Mapping[str, Any]) -> str:
    score = record.get("truth_score")
    nested = score.get("truth_label") if isinstance(score, Mapping) else None
    return str(first_present(record, "truth_label") or nested or "")


def score_warnings(record: Mapping[str, Any]) -> list[str]:
    score = record.get("truth_score")
    warnings = first_present(record, "score_warnings")
    if warnings is None and isinstance(score, Mapping):
        warnings = score.get("score_warnings")
    if isinstance(warnings, str):
        return [x.strip() for x in warnings.replace(",", "|").split("|") if x.strip()]
    if isinstance(warnings, (list, tuple, set)):
        return [str(x).strip() for x in warnings if str(x).strip()]
    return []


@dataclass
class ZoneReaction:
    zone_id: str
    symbol: str = ""
    direction: str = "UNKNOWN"
    zone_lower: float = 0.0
    zone_upper: float = 0.0
    first_seen_ts: float = 0.0
    last_seen_ts: float = 0.0
    frozen_ts: float = 0.0
    reaction_event_ts: float = 0.0
    reaction_type: str = "UNKNOWN"
    a1_reaction_type: str = "UNKNOWN"
    a1_reaction_reason: str = ""
    frozen_reason: str = ""
    zone_state: str = ""
    raw: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "ZoneReaction":
        frozen_low = parse_float(first_present(record, "frozen_zone_lower", "frozen_low", "zone_low", "low"))
        frozen_high = parse_float(first_present(record, "frozen_zone_upper", "frozen_high", "zone_high", "high"))
        live_low = parse_float(first_present(record, "live_zone_lower", "zone_lower"), frozen_low)
        live_high = parse_float(first_present(record, "live_zone_upper", "zone_upper"), frozen_high)
        lower = frozen_low or live_low
        upper = frozen_high or live_high
        if lower > upper:
            lower, upper = upper, lower
        reaction_ts = parse_timestamp(first_present(record, "reaction_event_ts", "a1_reaction_confirmed_ts", "confirmed_ts", "event_ts", "ts"))
        frozen_ts = parse_timestamp(first_present(record, "frozen_ts", "phase2_registered_ts", "registered_ts"))
        return cls(
            zone_id=str(first_present(record, "zone_id", "frozen_event_id") or ""),
            symbol=str(first_present(record, "symbol", "instId", "instrument") or ""),
            direction=normalize_direction(first_present(record, "direction", "side")),
            zone_lower=lower,
            zone_upper=upper,
            first_seen_ts=parse_timestamp(first_present(record, "first_seen_ts")),
            last_seen_ts=parse_timestamp(first_present(record, "last_seen_ts")),
            frozen_ts=frozen_ts,
            reaction_event_ts=reaction_ts,
            reaction_type=str(first_present(record, "reaction_type", "a1_reaction_type", "phase2_type") or "UNKNOWN"),
            a1_reaction_type=str(first_present(record, "a1_reaction_type", "reaction_type", "phase2_type") or "UNKNOWN"),
            a1_reaction_reason=str(first_present(record, "a1_reaction_reason", "phase2_reason") or ""),
            frozen_reason=str(first_present(record, "frozen_reason") or ""),
            zone_state=str(first_present(record, "frozen_state", "state", "zone_state") or ""),
            raw=dict(record),
        )


@dataclass
class ZoneTruthEvent:
    schema_version: str = SCHEMA_VERSION
    zone_id: str = ""
    zone_source: str = ""
    zone_match_method: str = ""
    match_score: float = 0.0
    symbol: str = ""
    direction: str = "UNKNOWN"
    zone_lower: float = 0.0
    zone_upper: float = 0.0
    zone_mid: float = 0.0
    zone_width: float = 0.0
    first_seen_ts: float = 0.0
    last_seen_ts: float = 0.0
    frozen_ts: float = 0.0
    reaction_event_ts: float = 0.0
    local_time: str = ""
    session_tag: str = "UNKNOWN"
    is_weekend: bool = False
    reaction_type: str = "UNKNOWN"
    a1_reaction_type: str = "UNKNOWN"
    a1_reaction_reason: str = ""
    frozen_reason: str = ""
    zone_state: str = ""
    pie_count: int = 0
    iceberg_pie_count: int = 0
    ignore_pie_count: int = 0
    spoofing_pie_count: int = 0
    cancel_pie_count: int = 0
    truth_score_max: float = 0.0
    truth_score_avg: float = 0.0
    truth_score_median: float = 0.0
    truth_score_min: float = 0.0
    truth_ge50_count: int = 0
    truth_ge65_count: int = 0
    truth_ge80_count: int = 0
    truth_not_iceberg_count: int = 0
    truth_insufficient_count: int = 0
    best_pie_event_key: str = ""
    best_pie_ts: float = 0.0
    best_pie_price: float = 0.0
    best_pie_truth_score: float = 0.0
    best_pie_truth_label: str = ""
    best_pie_quality: str = ""
    best_pie_behavior: str = ""
    sum_active_notional: float = 0.0
    max_active_notional: float = 0.0
    avg_active_notional: float = 0.0
    sum_hidden_volume: float = 0.0
    max_hidden_volume: float = 0.0
    avg_hidden_volume: float = 0.0
    avg_absorption_rate: float = 0.0
    max_absorption_rate: float = 0.0
    negative_hidden_cap_count: int = 0
    strong_negative_hidden_cap_count: int = 0
    negative_absorption_rate_cap_count: int = 0
    spoofing_withdrawal_cap_count: int = 0
    spoofing_result_cap_count: int = 0
    excessive_book_reduction_cap_count: int = 0
    has_any_hard_cap: bool = False
    hard_cap_warning_count: int = 0
    a2_pre_pool_eligible: bool = False
    a2_pre_pool_reason: str = "NO_ICEBERG_PIE"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


ZONE_TRUTH_EVENT_FIELDS = [field.name for field in fields(ZoneTruthEvent)]
FORWARD_FIELDS = [
    "mfe_15m_u", "mae_15m_u", "end_15m_u", "is_complete_15m",
    "mfe_1h_u", "mae_1h_u", "end_1h_u", "is_complete_1h",
    "mfe_4h_u", "mae_4h_u", "end_4h_u", "is_complete_4h",
]
ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS = ZONE_TRUTH_EVENT_FIELDS + FORWARD_FIELDS


def parse_candidate_bool(value: Any) -> bool:
    return parse_bool(value)


def parse_candidate_int(value: Any) -> int:
    return parse_int(value)
