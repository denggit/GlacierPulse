"""Research-only A1 raw record report aggregation."""

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _round6(value: float) -> float:
    return round(float(value), 6)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _score_bucket(value: Any) -> str:
    score = _safe_float(value)
    if score >= 0.8:
        return "0.80_1.00"
    if score >= 0.6:
        return "0.60_0.80"
    if score >= 0.4:
        return "0.40_0.60"
    if score > 0:
        return "0.00_0.40"
    return "ZERO_OR_MISSING"


def _count_bucket(value: Any) -> str:
    count = _safe_int(value)
    if count >= 5:
        return "5_PLUS"
    if count >= 3:
        return "3_4"
    if count >= 1:
        return "1_2"
    return "ZERO"


def _net_score_bucket(value: Any) -> str:
    score = _safe_float(value)
    if score >= 3:
        return "3_PLUS"
    if score >= 1:
        return "1_3"
    if score > 0:
        return "0_1"
    if score == 0:
        return "ZERO"
    return "NEGATIVE"


@dataclass(frozen=True)
class A1ResearchSample:
    zone_id: str
    direction: str
    frozen_reason: str
    frozen_state: str
    a1_reaction_type: str
    legacy_phase2_type: str
    candidate_type: str
    iceberg_count_bucket: str
    high_count_bucket: str
    net_score_bucket: str
    relevant_book_depth_available: str
    reload_score_bucket: str
    absorption_score_bucket: str
    reclaim_score_bucket: str
    retest_score_bucket: str
    outcome_label: str
    close_reason: str
    realized_pnl_u: float
    realized_r_multiple: float
    mfe_u: float
    mae_u: float

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "A1ResearchSample":
        legacy_phase2_type = _safe_str(record.get("legacy_phase2_type", record.get("phase2_type")))
        a1_reaction_type = _safe_str(record.get("a1_reaction_type", record.get("reaction_type", legacy_phase2_type)))
        return cls(
            zone_id=_safe_str(record.get("zone_id")),
            direction=_safe_str(record.get("direction")).upper(),
            frozen_reason=_safe_str(record.get("frozen_reason"), "UNKNOWN_FROZEN_REASON"),
            frozen_state=_safe_str(record.get("frozen_state"), "UNKNOWN_FROZEN_STATE"),
            a1_reaction_type=a1_reaction_type or "UNKNOWN_A1_REACTION_TYPE",
            legacy_phase2_type=legacy_phase2_type or "UNKNOWN_PHASE2_TYPE",
            candidate_type=_safe_str(record.get("candidate_type"), "UNKNOWN_CANDIDATE_TYPE"),
            iceberg_count_bucket=_count_bucket(record.get("iceberg_count")),
            high_count_bucket=_count_bucket(record.get("high_count")),
            net_score_bucket=_net_score_bucket(record.get("net_score")),
            relevant_book_depth_available=str(bool(record.get("relevant_book_depth_available"))),
            reload_score_bucket=_score_bucket(record.get("reload_score")),
            absorption_score_bucket=_score_bucket(record.get("absorption_score")),
            reclaim_score_bucket=_score_bucket(record.get("reclaim_score")),
            retest_score_bucket=_score_bucket(record.get("retest_score")),
            outcome_label=_safe_str(record.get("outcome_label")),
            close_reason=_safe_str(record.get("close_reason")),
            realized_pnl_u=_safe_float(record.get("realized_pnl_u")),
            realized_r_multiple=_safe_float(record.get("realized_r_multiple")),
            mfe_u=_safe_float(record.get("mfe_u", record.get("max_favorable_u"))),
            mae_u=_safe_float(record.get("mae_u", record.get("max_adverse_u"))),
        )

    @classmethod
    def from_records(cls, *records: Mapping[str, Any]) -> "A1ResearchSample":
        merged: dict[str, Any] = {}
        for record in records:
            if isinstance(record, Mapping):
                merged.update(record)
            elif hasattr(record, "to_dict"):
                merged.update(record.to_dict())
            elif hasattr(record, "__dict__"):
                merged.update(vars(record))
        return cls.from_mapping(merged)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


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
        return dict(self.__dict__)


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
    return A1ResearchGroupStats(
        group_name=group_name,
        group_value=group_value,
        sample_count=sample_count,
        win_count=win_count,
        loss_count=loss_count,
        flat_count=flat_count,
        win_rate=_round6(win_count / sample_count) if sample_count else 0.0,
        avg_realized_r=_round6(sum(r_values) / sample_count) if sample_count else 0.0,
        median_realized_r=_round6(_median(r_values)),
        total_realized_pnl_u=_round6(sum(pnl_values)),
        avg_mfe_u=_round6(sum(mfe_values) / sample_count) if sample_count else 0.0,
        avg_mae_u=_round6(sum(mae_values) / sample_count) if sample_count else 0.0,
        avg_mae_abs_u=_round6(sum(abs(v) for v in mae_values) / sample_count) if sample_count else 0.0,
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


def _build_groups(samples: Sequence[A1ResearchSample], attr_name: str, unknown_value: str) -> dict[str, A1ResearchGroupStats]:
    return {
        key: _group_stats(attr_name, key, group_samples)
        for key, group_samples in sorted(_group_by(samples, attr_name, unknown_value).items())
    }


def _best_group_by_avg_r(groups: Mapping[str, A1ResearchGroupStats]) -> str:
    if not groups:
        return ""
    return sorted(groups.items(), key=lambda item: (-item[1].avg_realized_r, -item[1].sample_count, item[0]))[0][0]


@dataclass(frozen=True)
class A1ResearchReport:
    report_version: str
    sample_count: int
    global_stats: A1ResearchGroupStats
    groups: dict[str, dict[str, A1ResearchGroupStats]]
    best_groups_by_avg_r: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "sample_count": self.sample_count,
            "global_stats": self.global_stats.to_dict(),
            "groups": {
                group_name: {k: v.to_dict() for k, v in sorted(group_values.items())}
                for group_name, group_values in sorted(self.groups.items())
            },
            "best_groups_by_avg_r": dict(sorted(self.best_groups_by_avg_r.items())),
        }

    def __getattr__(self, name: str) -> Any:
        if name.startswith("by_"):
            group_name = name[3:]
            if group_name in self.groups:
                return self.groups[group_name]
        raise AttributeError(name)


class A1ResearchReportBuilder:
    REPORT_VERSION = "a1_raw_research_report_v0_2"
    GROUP_FIELDS = (
        ("frozen_reason", "UNKNOWN_FROZEN_REASON"),
        ("frozen_state", "UNKNOWN_FROZEN_STATE"),
        ("a1_reaction_type", "UNKNOWN_A1_REACTION_TYPE"),
        ("legacy_phase2_type", "UNKNOWN_PHASE2_TYPE"),
        ("candidate_type", "UNKNOWN_CANDIDATE_TYPE"),
        ("direction", "UNKNOWN_DIRECTION"),
        ("iceberg_count_bucket", "UNKNOWN_ICEBERG_COUNT"),
        ("high_count_bucket", "UNKNOWN_HIGH_COUNT"),
        ("net_score_bucket", "UNKNOWN_NET_SCORE"),
        ("relevant_book_depth_available", "UNKNOWN_BOOK_DEPTH"),
        ("reload_score_bucket", "UNKNOWN_RELOAD_SCORE"),
        ("absorption_score_bucket", "UNKNOWN_ABSORPTION_SCORE"),
        ("reclaim_score_bucket", "UNKNOWN_RECLAIM_SCORE"),
        ("retest_score_bucket", "UNKNOWN_RETEST_SCORE"),
    )

    @classmethod
    def build(cls, samples: Iterable[A1ResearchSample | Mapping[str, Any]]) -> A1ResearchReport:
        sample_list = [
            sample if isinstance(sample, A1ResearchSample) else A1ResearchSample.from_mapping(sample)
            for sample in samples
        ]
        groups = {
            field: _build_groups(sample_list, field, unknown)
            for field, unknown in cls.GROUP_FIELDS
        }
        return A1ResearchReport(
            report_version=cls.REPORT_VERSION,
            sample_count=len(sample_list),
            global_stats=_group_stats("ALL", "ALL", sample_list),
            groups=groups,
            best_groups_by_avg_r={field: _best_group_by_avg_r(groups[field]) for field, _unknown in cls.GROUP_FIELDS},
        )

    @classmethod
    def build_from_records(cls, records: Iterable[Any]) -> A1ResearchReport:
        samples: list[A1ResearchSample] = []
        for record in records:
            if isinstance(record, A1ResearchSample):
                samples.append(record)
            elif isinstance(record, Mapping):
                samples.append(A1ResearchSample.from_mapping(record))
            elif isinstance(record, tuple):
                samples.append(A1ResearchSample.from_records(*record))
            else:
                samples.append(A1ResearchSample.from_records(record))
        return cls.build(samples)


__all__ = [
    "A1ResearchSample",
    "A1ResearchGroupStats",
    "A1ResearchReport",
    "A1ResearchReportBuilder",
]
