#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.a1_absorption.schema import (
    A1AbsorptionContext,
    A1ReactionSnapshot,
)
from src.strategy.a1_absorption.score_model import (
    A1ScoreBreakdown,
    A1ScoreRecord,
    A1UnifiedScoreModel,
    bucket_for_score,
)


def _build_absorption_zone(**overrides):
    base = {
        "zone_id": "iz-a1",
        "direction": "BUY",
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "DISCOVERED",
        "event_count": 6,
        "iceberg_count": 4,
        "ignore_count": 0,
        "spoof_count": 0,
        "cancel_count": 0,
        "high_count": 2,
        "medium_count": 1,
        "positive_score": 6,
        "negative_score": 0,
        "net_score": 6,
    }
    base.update(overrides)
    return A1AbsorptionContext.from_public_zone(base)


def _build_reaction_event(**overrides):
    base = {
        "zone_id": "iz-a1",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "phase2_total_score": 0.9,
        "absorption_score": 0.9,
        "reclaim_score": 0.9,
        "retest_score": 0.8,
        "reload_score": 0.5,
        "relevant_book_depth_available": True,
        "risk_to_stop_pct": 0.001,
    }
    base.update(overrides)
    return A1ReactionSnapshot.from_phase2_confirmed_event(base)


def test_a1_unified_score_high_quality_case_returns_a_bucket():
    absorption = _build_absorption_zone()
    reaction = _build_reaction_event()

    record = A1UnifiedScoreModel.score(absorption, reaction)

    assert isinstance(record, A1ScoreRecord)
    assert isinstance(record.breakdown, A1ScoreBreakdown)
    assert record.a1_score >= 80
    assert record.a1_quality_bucket == "A"
    assert record.score_version == "a1_score_v0_1"
    assert record.breakdown.total() == record.a1_score
    as_mapping = record.to_dict()
    assert "breakdown" in as_mapping
    assert "total" in as_mapping["breakdown"]


def test_a1_unified_score_low_quality_penalty_case_returns_d_bucket():
    absorption = _build_absorption_zone(
        iceberg_count=0,
        high_count=0,
        positive_score=0,
        negative_score=5,
        net_score=-2,
        spoof_count=2,
        cancel_count=2,
        ignore_count=3,
        frozen_reason="",
        frozen_state="",
    )
    reaction = _build_reaction_event(
        phase2_total_score=0.1,
        absorption_score=0.1,
        reclaim_score=0.0,
        retest_score=0.0,
        reload_score=0.0,
        relevant_book_depth_available=False,
        risk_to_stop_pct=0.01,
    )

    record = A1UnifiedScoreModel.score(absorption, reaction)

    assert record.a1_score < 50
    assert record.a1_quality_bucket == "D"
    assert record.breakdown.penalty_component < 0


def test_a1_score_bucket_boundaries():
    assert bucket_for_score(80) == "A"
    assert bucket_for_score(79.9999) == "B"
    assert bucket_for_score(65) == "B"
    assert bucket_for_score(64.9999) == "C"
    assert bucket_for_score(50) == "C"
    assert bucket_for_score(49.9999) == "D"


def test_a1_unified_score_is_deterministic():
    absorption = _build_absorption_zone()
    reaction = _build_reaction_event()
    record1 = A1UnifiedScoreModel.score(absorption, reaction)
    record2 = A1UnifiedScoreModel.score(absorption, reaction)
    assert record1 == record2
    assert record1.to_dict() == record2.to_dict()


def test_a1_unified_score_clamps_to_0_100():
    super_absorption = _build_absorption_zone(
        iceberg_count=999,
        high_count=999,
        medium_count=999,
        net_score=999,
        positive_score=999,
    )
    super_reaction = _build_reaction_event(
        phase2_total_score=9,
        absorption_score=9,
        reclaim_score=9,
        retest_score=9,
        reload_score=9,
        risk_to_stop_pct=0.0001,
        relevant_book_depth_available=True,
    )
    assert A1UnifiedScoreModel.score(super_absorption, super_reaction).a1_score <= 100

    awful_absorption = _build_absorption_zone(
        iceberg_count=-999,
        high_count=-999,
        medium_count=-999,
        net_score=-999,
        positive_score=-999,
        negative_score=999,
        spoof_count=999,
        cancel_count=999,
        ignore_count=999,
        frozen_reason="",
        frozen_state="",
    )
    awful_reaction = _build_reaction_event(
        phase2_total_score=-9,
        absorption_score=-9,
        reclaim_score=-9,
        retest_score=-9,
        reload_score=-9,
        risk_to_stop_pct=0.1,
        relevant_book_depth_available=False,
    )
    assert A1UnifiedScoreModel.score(awful_absorption, awful_reaction).a1_score >= 0


def test_a1_unified_score_does_not_require_runtime_components(monkeypatch):
    import sys
    import types

    sys.modules.setdefault("requests", types.SimpleNamespace())
    import src.execution.trader as trader_module
    import src.strategy.virtual_position_manager as virtual_module

    monkeypatch.setattr(
        trader_module.IcebergTrader,
        "place_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not be called")),
        raising=False,
    )
    monkeypatch.setattr(
        virtual_module.VirtualPositionManager,
        "on_candidate",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not be called")),
        raising=False,
    )

    absorption = _build_absorption_zone()
    reaction = _build_reaction_event()
    record = A1UnifiedScoreModel.score(absorption, reaction)
    assert record.score_version == "a1_score_v0_1"
