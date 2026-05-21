#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.a1_absorption import engine as a1_engine
from src.strategy.a1_absorption.reaction_evaluator import A1ReactionEvaluator
from src.strategy.execution_research.candidate_risk_evaluator import CandidateRiskEvaluator


def test_phase1_engine_uses_a1_reaction_semantic_class():
    assert a1_engine.A1ReactionEvaluator is A1ReactionEvaluator
    assert not hasattr(a1_engine, "Phase2OrderflowEvaluator")


def test_phase1_engine_uses_execution_research_semantic_class():
    assert (
        a1_engine.CandidateRiskEvaluator
        is CandidateRiskEvaluator
    )
    assert not hasattr(a1_engine, "Phase3CandidateEvaluator")


def test_phase1_engine_instantiates_with_semantic_imports_when_research_components_disabled(
    monkeypatch,
):
    monkeypatch.setattr(a1_engine, "PHASE2_ORDERFLOW_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(a1_engine, "PHASE3_CANDIDATE_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(a1_engine, "PHASE3_OUTCOME_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(a1_engine, "VIRTUAL_POSITION_MANAGER_ENABLED", False)
    monkeypatch.setattr(a1_engine, "V62_STARTUP_SAFETY_CHECK_ENABLED", False)
    monkeypatch.setattr(a1_engine, "V62_INTEGRATION_HEARTBEAT_ENABLED", False)

    engine = a1_engine.A1AbsorptionEngine(
        market_context=object(),
        iceberg_detector=object(),
    )

    assert engine.a1_reaction_evaluator is None
    assert engine.candidate_risk_evaluator is None
    assert engine.execution_outcome_evaluator is None
    assert engine.virtual_position_manager is None


def test_phase1_engine_instantiates_semantic_research_components(monkeypatch):
    monkeypatch.setattr(a1_engine, "PHASE2_ORDERFLOW_EVALUATOR_ENABLED", True)
    monkeypatch.setattr(a1_engine, "PHASE3_CANDIDATE_EVALUATOR_ENABLED", True)
    monkeypatch.setattr(a1_engine, "PHASE3_OUTCOME_EVALUATOR_ENABLED", False)
    monkeypatch.setattr(a1_engine, "VIRTUAL_POSITION_MANAGER_ENABLED", False)
    monkeypatch.setattr(a1_engine, "V62_STARTUP_SAFETY_CHECK_ENABLED", False)
    monkeypatch.setattr(a1_engine, "V62_INTEGRATION_HEARTBEAT_ENABLED", False)

    engine = a1_engine.A1AbsorptionEngine(
        market_context=object(),
        iceberg_detector=object(),
    )

    assert isinstance(engine.a1_reaction_evaluator, A1ReactionEvaluator)
    assert isinstance(
        engine.candidate_risk_evaluator,
        CandidateRiskEvaluator,
    )
