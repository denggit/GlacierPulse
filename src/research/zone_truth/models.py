#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from src.research.a1_edge.schema import parse_bool, parse_float, parse_int, parse_timestamp


SCHEMA_VERSION = "v6.3.12.zone_truth.2"
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
    minute = dt.minute
    if 0 <= hour < 5:
        session = "US_LATE"
    elif 5 <= hour < 8:
        session = "ASIA_OFF"
    elif 8 <= hour < 16:
        session = "ASIA_DAY"
    elif 16 <= hour < 21 or (hour == 21 and minute < 30):
        session = "EUROPE_PRE_US"
    else:
        session = "US_OPEN"
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
    reaction_count: int = 1
    reaction_types: str = ""
    has_clean_hold: bool = False
    has_failed_reclaim: bool = False
    primary_reaction_type: str = "UNKNOWN"
    final_reaction_type: str = "UNKNOWN"
    final_reaction_ts: float = 0.0
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
        reaction_type = str(first_present(record, "reaction_type", "a1_reaction_type", "phase2_type") or "UNKNOWN")
        a1_reaction_type = str(first_present(record, "a1_reaction_type", "reaction_type", "phase2_type") or "UNKNOWN")
        reaction_types = _join_unique([reaction_type, a1_reaction_type])
        final_ts = reaction_ts or frozen_ts
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
            reaction_type=reaction_type,
            a1_reaction_type=a1_reaction_type,
            a1_reaction_reason=str(first_present(record, "a1_reaction_reason", "phase2_reason") or ""),
            frozen_reason=str(first_present(record, "frozen_reason") or ""),
            zone_state=str(first_present(record, "frozen_state", "state", "zone_state") or ""),
            reaction_count=1,
            reaction_types=reaction_types,
            has_clean_hold=_contains_reaction_token(reaction_types, "CLEAN_HOLD"),
            has_failed_reclaim=(
                _contains_reaction_token(reaction_types, "FAILED_RECLAIM")
                or _contains_reaction_token(reaction_types, "FAILED")
            ),
            primary_reaction_type=reaction_type,
            final_reaction_type=reaction_type,
            final_reaction_ts=final_ts,
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
    reaction_count: int = 0
    reaction_types: str = ""
    has_clean_hold: bool = False
    has_failed_reclaim: bool = False
    primary_reaction_type: str = "UNKNOWN"
    final_reaction_type: str = "UNKNOWN"
    final_reaction_ts: float = 0.0
    pie_count: int = 0
    iceberg_pie_count: int = 0
    ignore_pie_count: int = 0
    spoofing_pie_count: int = 0
    cancel_pie_count: int = 0
    pie_event_keys: str = ""
    iceberg_pie_event_keys: str = ""
    ignore_pie_event_keys: str = ""
    spoofing_pie_event_keys: str = ""
    cancel_pie_event_keys: str = ""
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
    structural_proxy_available: bool = False
    structural_proxy_reason: str = ""
    trade_sweep_low: float = 0.0
    trade_sweep_high: float = 0.0
    trade_sweep_width_u: float = 0.0
    iceberg_trade_sweep_low: float = 0.0
    iceberg_trade_sweep_high: float = 0.0
    iceberg_trade_sweep_width_u: float = 0.0
    best_pie_min_trade_price: float = 0.0
    best_pie_max_trade_price: float = 0.0
    first_pie_ts: float = 0.0
    first_pie_min_trade_price: float = 0.0
    first_pie_max_trade_price: float = 0.0
    first_iceberg_pie_ts: float = 0.0
    first_iceberg_pie_min_trade_price: float = 0.0
    first_iceberg_pie_max_trade_price: float = 0.0
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
    relevant_book_depth_available: Any = ""
    bid_depth_near_zone: Any = ""
    ask_depth_near_zone: Any = ""
    bid_depth_near_sweep: Any = ""
    ask_depth_near_sweep: Any = ""
    a2_state: str = ""
    a2_state_reason: str = ""
    a2_state_ts: float = 0.0
    a2_failed_reason: str = ""
    a2_book_depth_state: str = ""
    a2_context_alignment: str = ""
    a2_clean_hold_flag: bool = False
    a2_failed_reclaim_flag: bool = False
    a2_sweep_flag: bool = False
    a2_reclaim_flag: bool = False
    a2_retest_flag: bool = False
    a2_validated_candidate_flag: bool = False
    a2_validation_score: float = 0.0
    a2_observe_priority: str = ""
    a2_priority_reason: str = ""
    a2_block_reason: str = ""
    a2_risk_tier: str = ""
    a2_risk_reason: str = ""
    a2_reaction_latency_sec: float = 0.0
    a2_time_to_clean_hold_sec: float = 0.0
    a2_time_to_failed_reclaim_sec: float = 0.0
    a2_hold_duration_sec: float = 0.0
    a2_zone_age_sec: float = 0.0
    a2_sweep_reclaim_quality: str = ""
    a2_reclaim_success_flag: bool = False
    a2_retest_success_flag: bool = False
    a2_post_zone_range_15m_u: float = 0.0
    a2_post_zone_range_1h_u: float = 0.0
    a2_post_zone_range_4h_u: float = 0.0
    a2_compression_ratio_15m: float = 0.0
    a2_compression_ratio_1h: float = 0.0
    a2_compression_state: str = ""
    a2_compression_reason: str = ""
    a2_ready_for_a3_watch_flag: bool = False
    a2_ready_for_a3_reason: str = ""
    a3_watch_priority: str = ""
    a3_preview_breakout_after_a2_flag: bool = False
    a2_fee_reference_price: float = 0.0
    a2_risk_u: float = 0.0
    a2_fee_u: float = 0.0
    a2_fee_share_r: float = 0.0
    a2_net_mfe_15m_r: float = 0.0
    a2_net_mae_15m_r: float = 0.0
    a2_net_mfe_1h_r: float = 0.0
    a2_net_mae_1h_r: float = 0.0
    a2_net_mfe_4h_r: float = 0.0
    a2_net_mae_4h_r: float = 0.0
    a2_net_hit_1r_15m: bool = False
    a2_net_hit_1r_1h: bool = False
    a2_net_hit_2r_1h: bool = False
    a3_preview_latency_bucket: str = "NO_IGNITION"
    a3_preview_entry_ts: float = 0.0
    a3_preview_entry_price: float = 0.0
    a3_preview_entry_time_utc: str = ""
    a3_preview_risk_u: float = 0.0
    a3_preview_fee_u: float = 0.0
    a3_preview_fee_share_r: float = 0.0
    a3_preview_net_mfe_15m_r: float = 0.0
    a3_preview_net_mae_15m_r: float = 0.0
    a3_preview_net_mfe_1h_r: float = 0.0
    a3_preview_net_mae_1h_r: float = 0.0
    a3_preview_first_hit_1r_15m: bool = False
    a3_preview_first_hit_1r_1h: bool = False
    a3_preview_realized_r_proxy_15m: float = 0.0
    a3_preview_realized_r_proxy_1h: float = 0.0
    a3_preview_realized_outcome_15m: str = "NO_BREAKOUT"
    a3_preview_realized_outcome_1h: str = "NO_BREAKOUT"
    a3_structural_stop_price: float = 0.0
    a3_structural_risk_u: float = 0.0
    a3_structural_fee_u: float = 0.0
    a3_structural_fee_share_r: float = 0.0
    a3_structural_net_mfe_15m_r: float = 0.0
    a3_structural_net_mae_15m_r: float = 0.0
    a3_structural_net_mfe_1h_r: float = 0.0
    a3_structural_net_mae_1h_r: float = 0.0
    a3_structural_realized_r_proxy_15m: float = 0.0
    a3_structural_realized_r_proxy_1h: float = 0.0
    a3_structural_realized_outcome_15m: str = "NO_BREAKOUT"
    a3_structural_realized_outcome_1h: str = "NO_BREAKOUT"
    a3_structural_fee_positive_1h: bool = False
    a3_structural_realized_r_proxy_1h_bucket: str = "NO_BREAKOUT"
    a3_preview_breakout_volume: float = 0.0
    a3_preview_volume_median_20: float = 0.0
    a3_preview_volume_boost: float = 0.0
    a3_preview_body_strength: float = 0.0
    a3_preview_breakout_strength_r: float = 0.0
    a3_preview_persistence_3m_flag: bool = False
    a3_preview_persistence_5m_flag: bool = False
    a3_preview_no_quick_return_3m_flag: bool = False
    a3_preview_no_quick_return_5m_flag: bool = False
    a3_preview_ignition_quality: str = "NO_IGNITION"
    a2_pre_ignition_bar_count: int = 0
    a2_pre_ignition_window_sec: float = 0.0
    a2_pre_ignition_range_u: float = 0.0
    a2_pre_ignition_range_ratio: float = 0.0
    a2_pre_ignition_zone_stay_ratio: float = 0.0
    a2_pre_ignition_compression_state: str = "INSUFFICIENT_BARS"
    a3_preview_net_mfe_1h_bucket: str = "NO_BREAKOUT"
    a3_preview_realized_r_proxy_1h_bucket: str = "NO_BREAKOUT"
    a3_after_a2_net_mfe_15m_r: float = 0.0
    a3_after_a2_net_mae_15m_r: float = 0.0
    a3_after_a2_net_mfe_1h_r: float = 0.0
    a3_after_a2_net_mae_1h_r: float = 0.0
    a3_after_a2_realized_r_proxy_15m: float = 0.0
    a3_after_a2_realized_r_proxy_1h: float = 0.0
    a3_after_a2_realized_outcome_15m: str = "NO_BREAKOUT"
    a3_after_a2_realized_outcome_1h: str = "NO_BREAKOUT"
    a3_after_a2_fee_positive_1h: bool = False
    a3_after_a2_net_mfe_1h_bucket: str = "NO_BREAKOUT"
    a3_after_a2_realized_r_proxy_1h_bucket: str = "NO_BREAKOUT"
    a3_after_a2_structural_stop_price: float = 0.0
    a3_after_a2_structural_risk_u: float = 0.0
    a3_after_a2_structural_fee_share_r: float = 0.0
    a3_after_a2_structural_realized_r_proxy_15m: float = 0.0
    a3_after_a2_structural_realized_r_proxy_1h: float = 0.0
    a3_after_a2_structural_realized_outcome_15m: str = "NO_BREAKOUT"
    a3_after_a2_structural_realized_outcome_1h: str = "NO_BREAKOUT"
    a3_after_a2_structural_fee_positive_1h: bool = False
    a3_after_a2_structural_realized_r_proxy_1h_bucket: str = "NO_BREAKOUT"
    a3_after_a2_structural_vs_v1_delta_r_1h: float = 0.0
    a3_after_a2_structural_fee_share_delta_r: float = 0.0
    a3_after_a2_structural_improved_flag: bool = False
    a3_preview_breakout_after_a2_latency_sec: float = 0.0
    strong_a1_raw_flag: bool = False
    strong_a1_tier: str = ""
    strong_a1_reason: str = ""
    reaction_event_ts_valid: Any = ""
    reaction_event_ts_outside_kline_range: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


ZONE_TRUTH_EVENT_FIELDS = [field.name for field in fields(ZoneTruthEvent)]
FORWARD_FIELDS = [
    "forward_anchor_ts", "forward_anchor_source", "forward_anchor_local_time",
    "forward_entry_price", "forward_entry_price_source",
    "mfe_15m_u", "mae_15m_u", "end_15m_u", "is_complete_15m",
    "mfe_1h_u", "mae_1h_u", "end_1h_u", "is_complete_1h",
    "mfe_4h_u", "mae_4h_u", "end_4h_u", "is_complete_4h",
    "a3_preview_breakout_raw_flag",
    "a3_preview_breakout_raw_latency_sec",
    "a3_preview_breakout_direction",
    "a3_preview_breakout_threshold_u",
    "a3_preview_breakout_price",
    "a3_preview_max_extension_15m_u",
    "a3_preview_max_extension_1h_u",
]
MARKET_CONTEXT_FIELDS = [
    "market_context_anchor_ts",
    "market_context_anchor_local_time",
    "pre_15m_return_u",
    "pre_15m_return_pct",
    "pre_1h_return_u",
    "pre_1h_return_pct",
    "pre_4h_return_u",
    "pre_4h_return_pct",
    "is_complete_pre_15m",
    "is_complete_pre_1h",
    "is_complete_pre_4h",
    "pre_15m_range_u",
    "pre_1h_range_u",
    "pre_4h_range_u",
    "pre_15m_volume",
    "pre_1h_volume",
    "pre_4h_volume",
    "pre_1h_direction",
    "pre_4h_direction",
    "trend_regime_1h",
    "trend_regime_4h",
    "ema20_1m",
    "ema60_1m",
    "ema20_slope_15m_pct",
    "ema60_slope_15m_pct",
    "trend_efficiency_1h",
    "trend_efficiency_4h",
    "higher_high_count_1h",
    "higher_low_count_1h",
    "lower_high_count_1h",
    "lower_low_count_1h",
    "price_location_1h",
    "price_location_4h",
    "trend_score_1h",
    "trend_score_4h",
    "trend_confidence_1h",
    "trend_confidence_4h",
    "trend_regime_enhanced_1h",
    "trend_regime_enhanced_4h",
    "trend_alignment",
    "trend_alignment_score",
    "volatility_regime_1h",
    "volume_regime_1h",
    "distance_to_pre_1h_high_u",
    "distance_to_pre_1h_low_u",
    "distance_to_session_high_u",
    "distance_to_session_low_u",
    "session_open_price",
    "session_high",
    "session_low",
]
ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS = ZONE_TRUTH_EVENT_FIELDS + FORWARD_FIELDS
ZONE_TRUTH_EVENT_WITH_CONTEXT_FIELDS = ZONE_TRUTH_EVENT_WITH_FORWARD_FIELDS + MARKET_CONTEXT_FIELDS


def parse_candidate_bool(value: Any) -> bool:
    return parse_bool(value)


def parse_candidate_int(value: Any) -> int:
    return parse_int(value)


def _join_unique(values: list[Any]) -> str:
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return "|".join(result)


def _contains_reaction_token(reaction_types: str, token: str) -> bool:
    token_upper = token.upper()
    return any(token_upper in part.upper() for part in str(reaction_types or "").split("|"))
