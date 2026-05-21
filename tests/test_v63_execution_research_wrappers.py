#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.phase3_candidate_evaluator import Phase3CandidateEvaluator
from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
)


def test_legacy_phase3_candidate_evaluator_wrapper_points_to_execution_research():
    assert Phase3CandidateEvaluator is ExecutionResearchCandidateEvaluator


def test_legacy_phase3_candidate_evaluator_exports_object_from_execution_research_module():
    assert (
        Phase3CandidateEvaluator.__module__
        == "src.strategy.execution_research.candidate_evaluator"
    )


def test_legacy_phase3_candidate_evaluator_can_instantiate():
    evaluator = Phase3CandidateEvaluator()
    assert evaluator is not None
    assert hasattr(evaluator, "evaluate")
    assert hasattr(evaluator, "evaluate_phase2_confirmed")


def test_legacy_phase3_candidate_evaluator_accepts_phase2_confirmed_smoke():
    evaluator = Phase3CandidateEvaluator()

    event = {
        "zone_id": "iz-smoke",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "last_price": 3000.0,
        "suggested_stop": 2998.0,
        "phase2_total_score": 0.95,
        "absorption_score": 0.9,
        "pressure_decay_score": 0.8,
        "reclaim_score": 0.9,
        "retest_score": 0.9,
        "book_absorption_score": 0.5,
        "relevant_book_depth_available": True,
        "reload_score": 0.5,
        "has_swept_boundary": True,
        "has_absorbed_after_sweep": True,
        "has_reclaimed_boundary": True,
        "has_retested_inside_zone": True,
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "DISCOVERED",
        "iceberg_count": 2,
        "high_count": 1,
        "medium_count": 0,
        "low_count": 0,
        "net_score": 3.0,
    }

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result is not None
    assert result["zone_id"] == "iz-smoke"
    assert result["phase2_type"] == "SWEEP_RECLAIM"
    assert result["candidate_type"] == "SWEEP_RECLAIM_RETEST_ENTRY"
    assert "decision" in result
    assert "suggested_stop" in result
    assert "final_margin_usage_pct" in result
    assert "phase2_total_score" in result
    assert result["frozen_reason"] == "HIGH_ICEBERG"
