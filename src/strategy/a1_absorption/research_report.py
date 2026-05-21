"""
Research-only A1 report aggregation model.

This module aggregates A1 score records and A1 outcome records into deterministic
research summaries. It does not participate in live trading, candidate decisions,
virtual position decisions, or order execution.
"""

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from src.strategy.a1_absorption.schema import A1OutcomeRecord
from src.strategy.a1_absorption.score_model import A1ScoreRecord


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def _round6(value: float) -> float:
    return round(float(value), 6)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


@dataclass(frozen=True)
class A1ResearchSample:
    zone_id: str
    direction: str
    score_version: str
    a1_score: float
    a1_quality_bucket: str
    reaction_type: str
    legacy_phase2_type: str
    candidate_type: str
    frozen_reason: str
    frozen_state: str
    outcome_label: str
    close_reason: str
    realized_pnl_u: float
    realized_r_multiple: float
    mfe_u: float
    mae_u: float

    @classmethod
    def from_records(
        cls,
        score_record: A1ScoreRecord,
        outcome_record: A1OutcomeRecord,
    ) -> "A1ResearchSample":
        return cls(
            zone_id=score_record.zone_id or outcome_record.zone_id,
            direction=score_record.direction or outcome_record.direction,
            score_version=_safe_str(score_record.score_version),
            a1_score=_safe_float(score_record.a1_score),
            a1_quality_bucket=_safe_str(score_record.a1_quality_bucket),
            reaction_type=score_record.reaction_type or outcome_record.reaction_type,
            legacy_phase2_type=score_record.legacy_phase2_type or outcome_record.legacy_phase2_type,
            candidate_type=_safe_str(outcome_record.candidate_type),
            frozen_reason=score_record.frozen_reason or outcome_record.frozen_reason,
            frozen_state=score_record.frozen_state or outcome_record.frozen_state,
            outcome_label=_safe_str(outcome_record.outcome_label),
            close_reason=_safe_str(outcome_record.close_reason),
            realized_pnl_u=_safe_float(outcome_record.realized_pnl_u),
            realized_r_multiple=_safe_float(outcome_record.realized_r_multiple),
            mfe_u=_safe_float(outcome_record.mfe_u),
            mae_u=_safe_float(outcome_record.mae_u),
        )

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "A1ResearchSample":
        return cls(
            zone_id=_safe_str(record.get("zone_id")),
            direction=_safe_str(record.get("direction")),
            score_version=_safe_str(record.get("score_version")),
            a1_score=_safe_float(record.get("a1_score")),
            a1_quality_bucket=_safe_str(record.get("a1_quality_bucket")),
            reaction_type=_safe_str(record.get("reaction_type")),
            legacy_phase2_type=_safe_str(record.get("legacy_phase2_type")),
            candidate_type=_safe_str(record.get("candidate_type")),
            frozen_reason=_safe_str(record.get("frozen_reason")),
            frozen_state=_safe_str(record.get("frozen_state")),
            outcome_label=_safe_str(record.get("outcome_label")),
            close_reason=_safe_str(record.get("close_reason")),
            realized_pnl_u=_safe_float(record.get("realized_pnl_u")),
            realized_r_multiple=_safe_float(record.get("realized_r_multiple")),
            mfe_u=_safe_float(record.get("mfe_u")),
            mae_u=_safe_float(record.get("mae_u")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "zone_id": self.zone_id,
            "direction": self.direction,
            "score_version": self.score_version,
            "a1_score": self.a1_score,
            "a1_quality_bucket": self.a1_quality_bucket,
            "reaction_type": self.reaction_type,
            "legacy_phase2_type": self.legacy_phase2_type,
            "candidate_type": self.candidate_type,
            "frozen_reason": self.frozen_reason,
            "frozen_state": self.frozen_state,
            "outcome_label": self.outcome_label,
            "close_reason": self.close_reason,
            "realized_pnl_u": self.realized_pnl_u,
            "realized_r_multiple": self.realized_r_multiple,
            "mfe_u": self.mfe_u,
            "mae_u": self.mae_u,
        }


@dataclass(frozen=True)
class A1ResearchGroupStats:
    group_name: str
    group_value: str
    sample_count: int
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    avg_realized_r: float
    median_realized_r: float
    total_realized_pnl_u: float
    avg_mfe_u: float
    avg_mae_u: float
    avg_mae_abs_u: float
    profit_factor_r: float
    best_realized_r: float
    worst_realized_r: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_name": self.group_name,
            "group_value": self.group_value,
            "sample_count": self.sample_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "flat_count": self.flat_count,
            "win_rate": self.win_rate,
            "avg_realized_r": self.avg_realized_r,
            "median_realized_r": self.median_realized_r,
            "total_realized_pnl_u": self.total_realized_pnl_u,
            "avg_mfe_u": self.avg_mfe_u,
            "avg_mae_u": self.avg_mae_u,
            "avg_mae_abs_u": self.avg_mae_abs_u,
            "profit_factor_r": self.profit_factor_r,
            "best_realized_r": self.best_realized_r,
            "worst_realized_r": self.worst_realized_r,
        }


def _group_stats(group_name: str, group_value: str, samples: Sequence[A1ResearchSample]) -> A1ResearchGroupStats:
    r_values = [_safe_float(s.realized_r_multiple) for s in samples]
    pnl_values = [_safe_float(s.realized_pnl_u) for s in samples]
    mfe_values = [_safe_float(s.mfe_u) for s in samples]
    mae_values = [_safe_float(s.mae_u) for s in samples]

    sample_count = len(samples)
    win_count = sum(1 for r in r_values if r > 0)
    loss_count = sum(1 for r in r_values if r < 0)
    flat_count = sum(1 for r in r_values if r == 0)

    gross_win_r = sum(r for r in r_values if r > 0)
    gross_loss_abs_r = abs(sum(r for r in r_values if r < 0))
    if gross_loss_abs_r > 0:
        profit_factor_r = gross_win_r / gross_loss_abs_r
    elif gross_win_r > 0:
        profit_factor_r = 999.0
    else:
        profit_factor_r = 0.0

    avg_realized_r = (sum(r_values) / sample_count) if sample_count else 0.0
    avg_mfe_u = (sum(mfe_values) / sample_count) if sample_count else 0.0
    avg_mae_u = (sum(mae_values) / sample_count) if sample_count else 0.0
    avg_mae_abs_u = (sum(abs(v) for v in mae_values) / sample_count) if sample_count else 0.0

    return A1ResearchGroupStats(
        group_name=group_name,
        group_value=group_value,
        sample_count=sample_count,
        win_count=win_count,
        loss_count=loss_count,
        flat_count=flat_count,
        win_rate=_round6(win_count / sample_count) if sample_count else 0.0,
        avg_realized_r=_round6(avg_realized_r),
        median_realized_r=_round6(_median(r_values)),
        total_realized_pnl_u=_round6(sum(pnl_values)),
        avg_mfe_u=_round6(avg_mfe_u),
        avg_mae_u=_round6(avg_mae_u),
        avg_mae_abs_u=_round6(avg_mae_abs_u),
        profit_factor_r=_round6(profit_factor_r),
        best_realized_r=_round6(max(r_values) if r_values else 0.0),
        worst_realized_r=_round6(min(r_values) if r_values else 0.0),
    )


def _group_by(samples: Sequence[A1ResearchSample], attr_name: str, unknown_value: str) -> dict[str, list[A1ResearchSample]]:
    groups: dict[str, list[A1ResearchSample]] = {}
    for sample in samples:
        value = _safe_str(getattr(sample, attr_name, "")) or unknown_value
        groups.setdefault(value, []).append(sample)
    return groups


def _best_group_by_avg_r(groups: Mapping[str, A1ResearchGroupStats]) -> str:
    if not groups:
        return ""
    ordered = sorted(
        groups.items(),
        key=lambda item: (-item[1].avg_realized_r, -item[1].sample_count, item[0]),
    )
    return ordered[0][0]


@dataclass(frozen=True)
class A1ResearchReport:
    report_version: str
    sample_count: int
    global_stats: A1ResearchGroupStats
    by_quality_bucket: dict[str, A1ResearchGroupStats]
    by_reaction_type: dict[str, A1ResearchGroupStats]
    by_frozen_reason: dict[str, A1ResearchGroupStats]
    by_candidate_type: dict[str, A1ResearchGroupStats]
    best_quality_bucket_by_avg_r: str
    best_reaction_type_by_avg_r: str
    best_frozen_reason_by_avg_r: str
    best_candidate_type_by_avg_r: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "sample_count": self.sample_count,
            "global_stats": self.global_stats.to_dict(),
            "by_quality_bucket": {k: self.by_quality_bucket[k].to_dict() for k in sorted(self.by_quality_bucket)},
            "by_reaction_type": {k: self.by_reaction_type[k].to_dict() for k in sorted(self.by_reaction_type)},
            "by_frozen_reason": {k: self.by_frozen_reason[k].to_dict() for k in sorted(self.by_frozen_reason)},
            "by_candidate_type": {k: self.by_candidate_type[k].to_dict() for k in sorted(self.by_candidate_type)},
            "best_quality_bucket_by_avg_r": self.best_quality_bucket_by_avg_r,
            "best_reaction_type_by_avg_r": self.best_reaction_type_by_avg_r,
            "best_frozen_reason_by_avg_r": self.best_frozen_reason_by_avg_r,
            "best_candidate_type_by_avg_r": self.best_candidate_type_by_avg_r,
        }


class A1ResearchReportBuilder:
    REPORT_VERSION = "a1_research_report_v0_1"

    @classmethod
    def build(cls, samples: Iterable[A1ResearchSample]) -> A1ResearchReport:
        sample_list = list(samples)
        global_stats = _group_stats("ALL", "ALL", sample_list)

        grouped_quality = _group_by(sample_list, "a1_quality_bucket", "")
        by_quality_bucket = {
            k: _group_stats("a1_quality_bucket", k, v)
            for k, v in sorted(grouped_quality.items())
        }

        grouped_reaction = _group_by(sample_list, "reaction_type", "UNKNOWN_REACTION_TYPE")
        by_reaction_type = {
            k: _group_stats("reaction_type", k, v)
            for k, v in sorted(grouped_reaction.items())
        }

        grouped_frozen = _group_by(sample_list, "frozen_reason", "UNKNOWN_FROZEN_REASON")
        by_frozen_reason = {
            k: _group_stats("frozen_reason", k, v)
            for k, v in sorted(grouped_frozen.items())
        }

        grouped_candidate = _group_by(sample_list, "candidate_type", "UNKNOWN_CANDIDATE_TYPE")
        by_candidate_type = {
            k: _group_stats("candidate_type", k, v)
            for k, v in sorted(grouped_candidate.items())
        }

        return A1ResearchReport(
            report_version=cls.REPORT_VERSION,
            sample_count=len(sample_list),
            global_stats=global_stats,
            by_quality_bucket=by_quality_bucket,
            by_reaction_type=by_reaction_type,
            by_frozen_reason=by_frozen_reason,
            by_candidate_type=by_candidate_type,
            best_quality_bucket_by_avg_r=_best_group_by_avg_r(by_quality_bucket),
            best_reaction_type_by_avg_r=_best_group_by_avg_r(by_reaction_type),
            best_frozen_reason_by_avg_r=_best_group_by_avg_r(by_frozen_reason),
            best_candidate_type_by_avg_r=_best_group_by_avg_r(by_candidate_type),
        )

    @classmethod
    def build_from_records(
        cls,
        records: Iterable[tuple[A1ScoreRecord, A1OutcomeRecord]],
    ) -> A1ResearchReport:
        return cls.build(A1ResearchSample.from_records(score_record, outcome_record) for score_record, outcome_record in records)


__all__ = [
    "A1ResearchSample",
    "A1ResearchGroupStats",
    "A1ResearchReport",
    "A1ResearchReportBuilder",
]
