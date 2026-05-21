from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    try:
        return bool(value)
    except Exception:
        return default


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


@dataclass(frozen=True)
class A1AbsorptionContext:
    zone_id: str
    direction: str
    is_frozen: bool
    frozen_ts: float | None
    frozen_reason: str
    frozen_state: str
    frozen_event_id: str
    frozen_low: float
    frozen_high: float
    live_low: float
    live_high: float
    event_count: int
    iceberg_count: int
    ignore_count: int
    spoof_count: int
    cancel_count: int
    high_count: int
    medium_count: int
    low_count: int
    positive_score: float
    negative_score: float
    net_score: float

    @classmethod
    def from_public_zone(cls, zone: Mapping[str, Any]) -> "A1AbsorptionContext":
        return cls(
            zone_id=_as_str(zone.get("zone_id")),
            direction=_as_str(zone.get("direction")),
            is_frozen=_as_bool(zone.get("is_frozen"), default=False),
            frozen_ts=_as_optional_float(zone.get("frozen_ts")),
            frozen_reason=_as_str(zone.get("frozen_reason")),
            frozen_state=_as_str(zone.get("frozen_state")),
            frozen_event_id=_as_str(zone.get("frozen_event_id")),
            frozen_low=_as_float(_first_present(zone, "frozen_low", "frozen_zone_lower")),
            frozen_high=_as_float(_first_present(zone, "frozen_high", "frozen_zone_upper")),
            live_low=_as_float(_first_present(zone, "live_low", "live_zone_lower")),
            live_high=_as_float(_first_present(zone, "live_high", "live_zone_upper")),
            event_count=_as_int(zone.get("event_count")),
            iceberg_count=_as_int(zone.get("iceberg_count")),
            ignore_count=_as_int(zone.get("ignore_count")),
            spoof_count=_as_int(zone.get("spoof_count")),
            cancel_count=_as_int(zone.get("cancel_count")),
            high_count=_as_int(zone.get("high_count")),
            medium_count=_as_int(zone.get("medium_count")),
            low_count=_as_int(zone.get("low_count")),
            positive_score=_as_float(zone.get("positive_score")),
            negative_score=_as_float(zone.get("negative_score")),
            net_score=_as_float(zone.get("net_score")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class A1ReactionSnapshot:
    zone_id: str
    direction: str
    state: str
    reaction_type: str
    legacy_phase2_type: str
    confirmed_ts: float | None
    last_price: float
    frozen_low: float
    frozen_high: float
    live_low: float
    live_high: float
    sweep_extreme: float
    suggested_stop: float
    risk_to_stop_u: float
    risk_to_stop_pct: float
    reaction_total_score: float
    legacy_phase2_total_score: float
    absorption_score: float
    pressure_decay_score: float
    reclaim_score: float
    retest_score: float
    book_absorption_score: float
    relevant_book_depth_available: bool
    reload_score: float
    has_swept_boundary: bool
    has_absorbed_after_sweep: bool
    has_reclaimed_boundary: bool
    has_retested_inside_zone: bool

    @classmethod
    def from_phase2_confirmed_event(cls, event: Mapping[str, Any]) -> "A1ReactionSnapshot":
        phase2_type = _as_str(event.get("phase2_type"))
        phase2_total_score = _as_float(event.get("phase2_total_score"))
        return cls(
            zone_id=_as_str(event.get("zone_id")),
            direction=_as_str(event.get("direction")),
            state=_as_str(event.get("state")),
            reaction_type=phase2_type,
            legacy_phase2_type=phase2_type,
            confirmed_ts=_as_optional_float(event.get("confirmed_ts")),
            last_price=_as_float(event.get("last_price")),
            frozen_low=_as_float(event.get("frozen_low")),
            frozen_high=_as_float(event.get("frozen_high")),
            live_low=_as_float(event.get("live_low")),
            live_high=_as_float(event.get("live_high")),
            sweep_extreme=_as_float(event.get("sweep_extreme")),
            suggested_stop=_as_float(event.get("suggested_stop")),
            risk_to_stop_u=_as_float(event.get("risk_to_stop_u")),
            risk_to_stop_pct=_as_float(event.get("risk_to_stop_pct")),
            reaction_total_score=phase2_total_score,
            legacy_phase2_total_score=phase2_total_score,
            absorption_score=_as_float(event.get("absorption_score")),
            pressure_decay_score=_as_float(event.get("pressure_decay_score")),
            reclaim_score=_as_float(event.get("reclaim_score")),
            retest_score=_as_float(event.get("retest_score")),
            book_absorption_score=_as_float(event.get("book_absorption_score")),
            relevant_book_depth_available=_as_bool(event.get("relevant_book_depth_available")),
            reload_score=_as_float(event.get("reload_score")),
            has_swept_boundary=_as_bool(event.get("has_swept_boundary")),
            has_absorbed_after_sweep=_as_bool(event.get("has_absorbed_after_sweep")),
            has_reclaimed_boundary=_as_bool(event.get("has_reclaimed_boundary")),
            has_retested_inside_zone=_as_bool(event.get("has_retested_inside_zone")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class A1OutcomeRecord:
    zone_id: str
    direction: str
    outcome_label: str
    close_reason: str
    reaction_type: str
    legacy_phase2_type: str
    candidate_type: str
    realized_pnl_u: float
    realized_r_multiple: float
    mfe_u: float
    mae_u: float
    frozen_reason: str
    frozen_state: str
    iceberg_count: int
    high_count: int
    medium_count: int
    low_count: int
    net_score: float

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "A1OutcomeRecord":
        phase2_type = _as_str(record.get("phase2_type"))
        return cls(
            zone_id=_as_str(record.get("zone_id")),
            direction=_as_str(record.get("direction")),
            outcome_label=_as_str(_first_present(record, "outcome_label", "label", "outcome_bucket")),
            close_reason=_as_str(record.get("close_reason")),
            reaction_type=_as_str(_first_present(record, "reaction_type", "phase2_type")),
            legacy_phase2_type=phase2_type,
            candidate_type=_as_str(record.get("candidate_type")),
            realized_pnl_u=_as_float(record.get("realized_pnl_u")),
            realized_r_multiple=_as_float(record.get("realized_r_multiple")),
            mfe_u=_as_float(_first_present(record, "mfe_u", "mfe", "max_favorable_u")),
            mae_u=_as_float(_first_present(record, "mae_u", "mae", "max_adverse_u")),
            frozen_reason=_as_str(record.get("frozen_reason")),
            frozen_state=_as_str(record.get("frozen_state")),
            iceberg_count=_as_int(record.get("iceberg_count")),
            high_count=_as_int(record.get("high_count")),
            medium_count=_as_int(record.get("medium_count")),
            low_count=_as_int(record.get("low_count")),
            net_score=_as_float(record.get("net_score")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = [
    "A1AbsorptionContext",
    "A1ReactionSnapshot",
    "A1OutcomeRecord",
]
