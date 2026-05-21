from src.strategy.a1_absorption.research_report import (
    A1ResearchGroupStats,
    A1ResearchReport,
    A1ResearchReportBuilder,
    A1ResearchSample,
)
from src.strategy.a1_absorption.schema import (
    A1AbsorptionContext,
    A1OutcomeRecord,
    A1ReactionSnapshot,
)
from src.strategy.a1_absorption.score_model import A1UnifiedScoreModel


def _sample(
    zone_id="z1",
    bucket="A",
    reaction_type="SWEEP_RECLAIM",
    frozen_reason="HIGH_ICEBERG",
    candidate_type="SWEEP_RECLAIM_RETEST_ENTRY",
    realized_r=1.5,
    pnl=10.0,
    mfe=5.0,
    mae=-1.0,
):
    return A1ResearchSample(
        zone_id=zone_id,
        direction="LONG",
        score_version="a1_score_v0_1",
        a1_score=80.0,
        a1_quality_bucket=bucket,
        reaction_type=reaction_type,
        legacy_phase2_type=reaction_type,
        candidate_type=candidate_type,
        frozen_reason=frozen_reason,
        frozen_state="ACTIVE",
        outcome_label="WIN" if realized_r > 0 else "LOSS",
        close_reason="TP" if realized_r > 0 else "SL",
        realized_pnl_u=pnl,
        realized_r_multiple=realized_r,
        mfe_u=mfe,
        mae_u=mae,
    )


def test_a1_research_report_empty_samples():
    report = A1ResearchReportBuilder.build([])
    assert isinstance(report, A1ResearchReport)
    assert report.sample_count == 0
    assert report.global_stats.sample_count == 0
    assert report.by_quality_bucket == {}
    assert report.best_quality_bucket_by_avg_r == ""
    assert report.to_dict()["report_version"] == "a1_research_report_v0_1"


def test_a1_research_report_global_stats():
    report = A1ResearchReportBuilder.build([
        _sample(realized_r=1.5),
        _sample(zone_id="z2", realized_r=-1.0),
        _sample(zone_id="z3", realized_r=0.5),
    ])
    gs = report.global_stats
    assert isinstance(gs, A1ResearchGroupStats)
    assert gs.sample_count == 3
    assert gs.win_count == 2
    assert gs.loss_count == 1
    assert gs.flat_count == 0
    assert gs.win_rate == 0.666667
    assert gs.avg_realized_r == 0.333333
    assert gs.median_realized_r == 0.5
    assert gs.profit_factor_r == 2.0
    assert gs.best_realized_r == 1.5
    assert gs.worst_realized_r == -1.0


def test_group_by_quality_bucket():
    report = A1ResearchReportBuilder.build([
        _sample(bucket="A", realized_r=1.5),
        _sample(zone_id="z2", bucket="A", realized_r=-1.0),
        _sample(zone_id="z3", bucket="B", realized_r=0.5),
    ])
    assert report.by_quality_bucket["A"].sample_count == 2
    assert report.by_quality_bucket["B"].sample_count == 1
    assert report.best_quality_bucket_by_avg_r == "B"


def test_group_by_reaction_type_and_frozen_reason():
    report = A1ResearchReportBuilder.build([
        _sample(reaction_type="SWEEP_RECLAIM", frozen_reason="HIGH_ICEBERG"),
        _sample(zone_id="z2", reaction_type="CLEAN_HOLD", frozen_reason="STATE_RELOADING"),
    ])
    assert "SWEEP_RECLAIM" in report.by_reaction_type
    assert "CLEAN_HOLD" in report.by_reaction_type
    assert report.by_reaction_type["SWEEP_RECLAIM"].sample_count == 1
    assert report.by_reaction_type["CLEAN_HOLD"].sample_count == 1
    assert "HIGH_ICEBERG" in report.by_frozen_reason
    assert "STATE_RELOADING" in report.by_frozen_reason


def test_unknown_group_values():
    report = A1ResearchReportBuilder.build([
        _sample(reaction_type="", frozen_reason="", candidate_type="")
    ])
    assert "UNKNOWN_REACTION_TYPE" in report.by_reaction_type
    assert "UNKNOWN_FROZEN_REASON" in report.by_frozen_reason
    assert "UNKNOWN_CANDIDATE_TYPE" in report.by_candidate_type


def test_best_group_tie_break_is_deterministic():
    report = A1ResearchReportBuilder.build([
        _sample(bucket="A", realized_r=1.0),
        _sample(zone_id="z2", bucket="B", realized_r=1.0),
        _sample(zone_id="z3", bucket="B", realized_r=1.0),
    ])
    assert report.best_quality_bucket_by_avg_r == "B"

    report2 = A1ResearchReportBuilder.build([
        _sample(bucket="A", realized_r=1.0),
        _sample(zone_id="z2", bucket="B", realized_r=1.0),
    ])
    assert report2.best_quality_bucket_by_avg_r == "A"


def test_build_from_records():
    absorption = A1AbsorptionContext.from_public_zone(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "frozen_reason": "HIGH_ICEBERG",
            "frozen_state": "ACTIVE",
            "iceberg_count": 3,
            "high_count": 2,
            "medium_count": 1,
            "net_score": 4,
            "positive_score": 4,
            "negative_score": 1,
            "event_count": 6,
        }
    )
    reaction = A1ReactionSnapshot.from_phase2_confirmed_event(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "phase2_type": "SWEEP_RECLAIM",
            "phase2_total_score": 0.8,
            "risk_to_stop_pct": 0.002,
            "absorption_score": 0.7,
            "reclaim_score": 0.7,
            "retest_score": 0.6,
            "reload_score": 0.4,
            "relevant_book_depth_available": True,
        }
    )
    score_record = A1UnifiedScoreModel.score(absorption, reaction)
    outcome_record = A1OutcomeRecord.from_mapping(
        {
            "zone_id": "z1",
            "direction": "LONG",
            "reaction_type": "SWEEP_RECLAIM",
            "phase2_type": "SWEEP_RECLAIM",
            "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
            "outcome_label": "WIN",
            "close_reason": "TP",
            "realized_pnl_u": 12.0,
            "realized_r_multiple": 1.2,
            "mfe_u": 6.0,
            "mae_u": -1.5,
        }
    )
    report = A1ResearchReportBuilder.build_from_records([(score_record, outcome_record)])
    assert report.sample_count == 1
    assert report.global_stats.sample_count == 1
    assert score_record.a1_quality_bucket in report.by_quality_bucket
    assert "SWEEP_RECLAIM" in report.by_reaction_type
    assert "SWEEP_RECLAIM_RETEST_ENTRY" in report.by_candidate_type


def test_to_dict_is_deterministic():
    samples = [_sample(realized_r=1.5), _sample(zone_id="z2", realized_r=-1.0)]
    report1 = A1ResearchReportBuilder.build(samples)
    report2 = A1ResearchReportBuilder.build(samples)
    assert report1 == report2
    assert report1.to_dict() == report2.to_dict()


def test_no_runtime_side_effects(monkeypatch):
    import sys
    import types

    if "requests" not in sys.modules:
        sys.modules["requests"] = types.SimpleNamespace()

    from src.execution import trader
    from src.strategy import phase1_zone_engine
    from src.strategy import virtual_position_manager

    def _raise(*_args, **_kwargs):
        raise AssertionError("runtime side effect should not be called")

    monkeypatch.setattr(trader.IcebergTrader, "place_order", _raise, raising=False)
    monkeypatch.setattr(virtual_position_manager.VirtualPositionManager, "on_candidate", _raise, raising=False)
    monkeypatch.setattr(phase1_zone_engine.Phase1Engine, "on_trade", _raise, raising=False)

    report = A1ResearchReportBuilder.build([_sample()])
    assert report.sample_count == 1
