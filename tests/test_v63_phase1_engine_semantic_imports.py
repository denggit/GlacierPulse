#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy import phase1_zone_engine
from src.strategy.a1_reaction.reaction_evaluator import A1ReactionEvaluator
from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
)


def test_phase1_engine_uses_a1_reaction_semantic_class():
    assert phase1_zone_engine.A1ReactionEvaluator is A1ReactionEvaluator
    assert not hasattr(phase1_zone_engine, "Phase2OrderflowEvaluator")


def test_phase1_engine_uses_execution_research_semantic_class():
    assert (
        phase1_zone_engine.ExecutionResearchCandidateEvaluator
        is ExecutionResearchCandidateEvaluator
    )
    assert not hasattr(phase1_zone_engine, "Phase3CandidateEvaluator")


def test_phase1_engine_instantiates_with_semantic_imports_when_research_components_disabled(
    monkeypatch,
):
    monkeypatch.setattr(phase1_zone_engine, "PHASE2_ORDERFLOW_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "PHASE3_CANDIDATE_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "PHASE3_OUTCOME_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "VIRTUAL_POSITION_MANAGER_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "V62_STARTUP_SAFETY_CHECK_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "V62_INTEGRATION_HEARTBEAT_ENABLED", False)

    engine = phase1_zone_engine.Phase1Engine(
        market_context=object(),
        iceberg_detector=object(),
    )

    assert engine.phase2_orderflow_evaluator is None
    assert engine.phase3_candidate_evaluator is None
    assert engine.phase3_trade_outcome_evaluator is None
    assert engine.virtual_position_manager is None


def test_phase1_engine_instantiates_semantic_research_components(monkeypatch):
    monkeypatch.setattr(phase1_zone_engine, "PHASE2_ORDERFLOW_EVALUATOR_ENABLED", True)
    monkeypatch.setattr(phase1_zone_engine, "PHASE3_CANDIDATE_EVALUATOR_ENABLED", True)
    monkeypatch.setattr(phase1_zone_engine, "PHASE3_OUTCOME_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "VIRTUAL_POSITION_MANAGER_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "V62_STARTUP_SAFETY_CHECK_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine, "V62_INTEGRATION_HEARTBEAT_ENABLED", False)

    engine = phase1_zone_engine.Phase1Engine(
        market_context=object(),
        iceberg_detector=object(),
    )

    assert isinstance(engine.phase2_orderflow_evaluator, A1ReactionEvaluator)
    assert isinstance(
        engine.phase3_candidate_evaluator,
        ExecutionResearchCandidateEvaluator,
    )
