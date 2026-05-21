"""
Research-only A1 unified score normalization model.

This module combines existing A1 absorption context and A1 reaction snapshot
fields into a normalized research score. It does not participate in live trading,
candidate decisions, virtual position decisions, or order execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.strategy.a1_absorption.schema import (
    A1AbsorptionContext,
    A1ReactionSnapshot,
)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def bucket_for_score(score: float) -> str:
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "D"


@dataclass(frozen=True)
class A1ScoreBreakdown:
    absorption_component: float
    zone_quality_component: float
    reaction_component: float
    risk_location_component: float
    penalty_component: float

    def total(self) -> float:
        total_score = (
            self.absorption_component
            + self.zone_quality_component
            + self.reaction_component
            + self.risk_location_component
            + self.penalty_component
        )
        return round(_clamp(total_score, 0.0, 100.0), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "absorption_component": self.absorption_component,
            "zone_quality_component": self.zone_quality_component,
            "reaction_component": self.reaction_component,
            "risk_location_component": self.risk_location_component,
            "penalty_component": self.penalty_component,
            "total": self.total(),
        }


@dataclass(frozen=True)
class A1ScoreRecord:
    zone_id: str
    direction: str
    score_version: str
    a1_score: float
    a1_quality_bucket: str
    breakdown: A1ScoreBreakdown
    frozen_reason: str
    frozen_state: str
    reaction_type: str
    legacy_phase2_type: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "zone_id": self.zone_id,
            "direction": self.direction,
            "score_version": self.score_version,
            "a1_score": self.a1_score,
            "a1_quality_bucket": self.a1_quality_bucket,
            "breakdown": self.breakdown.to_dict(),
            "frozen_reason": self.frozen_reason,
            "frozen_state": self.frozen_state,
            "reaction_type": self.reaction_type,
            "legacy_phase2_type": self.legacy_phase2_type,
        }


class A1UnifiedScoreModel:
    SCORE_VERSION = "a1_score_v0_1"

    @classmethod
    def score(
        cls,
        absorption: A1AbsorptionContext,
        reaction: A1ReactionSnapshot,
    ) -> A1ScoreRecord:
        absorption_component = round(
            _clamp(
                min(12.0, _safe_float(absorption.iceberg_count) * 4.0)
                + min(8.0, _safe_float(absorption.high_count) * 4.0)
                + min(4.0, _safe_float(absorption.medium_count) * 1.5)
                + _clamp(_safe_float(absorption.net_score), 0.0, 6.0),
                0.0,
                30.0,
            ),
            4,
        )

        frozen_reason = absorption.frozen_reason or ""
        if frozen_reason == "HIGH_ICEBERG":
            reason_score = 8.0
        elif frozen_reason == "STATE_RELOADING":
            reason_score = 7.0
        elif frozen_reason == "STATE_ACTIVE":
            reason_score = 5.0
        elif frozen_reason:
            reason_score = 3.0
        else:
            reason_score = 0.0

        frozen_state = absorption.frozen_state or ""
        if frozen_state == "DISCOVERED":
            state_score = 5.0
        elif frozen_state == "RELOADING":
            state_score = 6.0
        elif frozen_state == "ACTIVE":
            state_score = 4.0
        elif frozen_state:
            state_score = 2.0
        else:
            state_score = 0.0

        zone_quality_component = round(
            _clamp(
                reason_score
                + state_score
                + _clamp(_safe_float(absorption.positive_score), 0.0, 6.0)
                - _clamp(_safe_float(absorption.negative_score), 0.0, 4.0)
                + min(4.0, _safe_float(absorption.event_count) * 0.5),
                0.0,
                25.0,
            ),
            4,
        )

        reaction_component = round(
            _clamp(
                _clamp(_safe_float(reaction.reaction_total_score), 0.0, 1.0) * 18.0
                + _clamp(_safe_float(reaction.absorption_score), 0.0, 1.0) * 4.0
                + _clamp(_safe_float(reaction.reclaim_score), 0.0, 1.0) * 3.0
                + _clamp(_safe_float(reaction.retest_score), 0.0, 1.0) * 3.0
                + _clamp(_safe_float(reaction.reload_score), 0.0, 1.0) * 2.0,
                0.0,
                30.0,
            ),
            4,
        )

        risk_to_stop_pct = _safe_float(reaction.risk_to_stop_pct)
        if risk_to_stop_pct <= 0:
            risk_location = 5.0
        elif risk_to_stop_pct <= 0.0015:
            risk_location = 15.0
        elif risk_to_stop_pct <= 0.0030:
            risk_location = 12.0
        elif risk_to_stop_pct <= 0.0060:
            risk_location = 8.0
        else:
            risk_location = 3.0
        if reaction.relevant_book_depth_available:
            risk_location += 2.0
        risk_location_component = round(_clamp(risk_location, 0.0, 15.0), 4)

        penalty_value = 0.0
        penalty_value -= min(6.0, _safe_float(absorption.spoof_count) * 3.0)
        penalty_value -= min(4.0, _safe_float(absorption.cancel_count) * 2.0)
        penalty_value -= min(3.0, _safe_float(absorption.ignore_count) * 1.0)
        if risk_to_stop_pct > 0.006:
            penalty_value -= 4.0
        elif risk_to_stop_pct > 0.003:
            penalty_value -= 2.0
        if not reaction.relevant_book_depth_available:
            penalty_value -= 2.0
        penalty_component = round(min(0.0, penalty_value), 4)

        breakdown = A1ScoreBreakdown(
            absorption_component=absorption_component,
            zone_quality_component=zone_quality_component,
            reaction_component=reaction_component,
            risk_location_component=risk_location_component,
            penalty_component=penalty_component,
        )

        a1_score = round(_clamp(breakdown.total(), 0.0, 100.0), 4)
        return A1ScoreRecord(
            zone_id=absorption.zone_id,
            direction=absorption.direction,
            score_version=cls.SCORE_VERSION,
            a1_score=a1_score,
            a1_quality_bucket=bucket_for_score(a1_score),
            breakdown=breakdown,
            frozen_reason=absorption.frozen_reason,
            frozen_state=absorption.frozen_state,
            reaction_type=reaction.reaction_type,
            legacy_phase2_type=reaction.legacy_phase2_type,
        )


__all__ = [
    "A1ScoreBreakdown",
    "A1ScoreRecord",
    "A1UnifiedScoreModel",
    "bucket_for_score",
]
