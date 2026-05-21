from src.strategy.a1_absorption.event_schema import (
    A1AbsorptionContext,
    A1OutcomeRecord,
    A1ReactionSnapshot,
)
from src.strategy.a1_absorption.research_report import (
    A1ResearchGroupStats,
    A1ResearchReport,
    A1ResearchReportBuilder,
    A1ResearchSample,
)


def _record(
    zone_id="z1",
    a1_reaction_type="SWEEP_RECLAIM",
    frozen_reason="HIGH_ICEBERG",
    candidate_type="SWEEP_RECLAIM_RETEST_ENTRY",
    realized_r=1.5,
):
    return {
        "zone_id": zone_id,
        "direction": "LONG",
        "frozen_reason": frozen_reason,
        "frozen_state": "ACTIVE",
        "a1_reaction_type": a1_reaction_type,
        "phase2_type": a1_reaction_type,
        "candidate_type": candidate_type,
        "iceberg_count": 3,
        "high_count": 1,
        "net_score": 4.0,
        "relevant_book_depth_available": True,
        "reload_score": 0.4,
        "absorption_score": 0.7,
        "reclaim_score": 0.7,
        "retest_score": 0.6,
        "outcome_label": "WIN" if realized_r > 0 else "LOSS",
        "close_reason": "TP" if realized_r > 0 else "SL",
        "realized_pnl_u": realized_r * 10,
        "realized_r_multiple": realized_r,
        "mfe_u": 5.0,
        "mae_u": -1.0,
    }


def test_a1_research_report_empty_samples():
    report = A1ResearchReportBuilder.build([])
    assert isinstance(report, A1ResearchReport)
    assert report.sample_count == 0
    assert report.global_stats.sample_count == 0
    assert report.groups["frozen_reason"] == {}
    assert report.best_groups_by_avg_r["frozen_reason"] == ""
    assert report.to_dict()["report_version"] == "a1_raw_research_report_v0_2"


def test_a1_research_report_global_stats():
    report = A1ResearchReportBuilder.build([
        _record(realized_r=1.5),
        _record(zone_id="z2", realized_r=-1.0),
        _record(zone_id="z3", realized_r=0.5),
    ])
    gs = report.global_stats
    assert isinstance(gs, A1ResearchGroupStats)
    assert gs.sample_count == 3
    assert gs.win_count == 2
    assert gs.loss_count == 1
    assert gs.win_rate == 0.666667
    assert gs.avg_realized_r == 0.333333
    assert gs.median_realized_r == 0.5
    assert gs.profit_factor_r == 2.0


def test_required_raw_a1_group_dimensions_exist():
    report = A1ResearchReportBuilder.build([_record()])
    expected = {
        "frozen_reason",
        "frozen_state",
        "a1_reaction_type",
        "legacy_phase2_type",
        "candidate_type",
        "direction",
        "iceberg_count_bucket",
        "high_count_bucket",
        "net_score_bucket",
        "relevant_book_depth_available",
        "reload_score_bucket",
        "absorption_score_bucket",
        "reclaim_score_bucket",
        "retest_score_bucket",
    }
    assert expected.issubset(report.groups)


def test_group_by_reaction_type_and_frozen_reason():
    report = A1ResearchReportBuilder.build([
        _record(a1_reaction_type="SWEEP_RECLAIM", frozen_reason="HIGH_ICEBERG"),
        _record(zone_id="z2", a1_reaction_type="CLEAN_HOLD", frozen_reason="STATE_RELOADING"),
    ])
    assert report.groups["a1_reaction_type"]["SWEEP_RECLAIM"].sample_count == 1
    assert report.groups["a1_reaction_type"]["CLEAN_HOLD"].sample_count == 1
    assert "HIGH_ICEBERG" in report.groups["frozen_reason"]
    assert "STATE_RELOADING" in report.groups["frozen_reason"]


def test_unknown_group_values():
    report = A1ResearchReportBuilder.build([
        _record(a1_reaction_type="", frozen_reason="", candidate_type="")
    ])
    assert "UNKNOWN_A1_REACTION_TYPE" in report.groups["a1_reaction_type"]
    assert "UNKNOWN_FROZEN_REASON" in report.groups["frozen_reason"]
    assert "UNKNOWN_CANDIDATE_TYPE" in report.groups["candidate_type"]


def test_build_from_raw_records_without_score_model():
    absorption = A1AbsorptionContext.from_public_zone(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "frozen_reason": "HIGH_ICEBERG",
            "frozen_state": "ACTIVE",
            "iceberg_count": 3,
            "high_count": 2,
            "net_score": 4,
        }
    )
    reaction = A1ReactionSnapshot.from_phase2_confirmed_event(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "phase2_type": "SWEEP_RECLAIM",
            "phase2_total_score": 0.8,
            "absorption_score": 0.7,
            "reclaim_score": 0.7,
            "retest_score": 0.6,
            "reload_score": 0.4,
            "relevant_book_depth_available": True,
        }
    )
    outcome = A1OutcomeRecord.from_mapping(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "phase2_type": "SWEEP_RECLAIM",
            "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
            "outcome_label": "WIN",
            "realized_r_multiple": 1.2,
            "mfe_u": 6.0,
            "mae_u": -1.5,
        }
    )
    report = A1ResearchReportBuilder.build_from_records([
        (absorption.to_dict(), reaction.to_dict(), outcome.to_dict())
    ])
    assert report.sample_count == 1
    assert "SWEEP_RECLAIM" in report.groups["a1_reaction_type"]
    assert "SWEEP_RECLAIM_RETEST_ENTRY" in report.groups["candidate_type"]


def test_to_dict_is_deterministic():
    samples = [A1ResearchSample.from_mapping(_record()), A1ResearchSample.from_mapping(_record(zone_id="z2", realized_r=-1.0))]
    report1 = A1ResearchReportBuilder.build(samples)
    report2 = A1ResearchReportBuilder.build(samples)
    assert report1 == report2
    assert report1.to_dict() == report2.to_dict()
