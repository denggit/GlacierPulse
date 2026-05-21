import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.execution_research.candidate_risk_evaluator import CandidateRiskEvaluator


def test_candidate_log_contains_a1_reaction_fields(caplog):
    event = {
        "zone_id": "z-log-a1",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "a1_reaction_type": "SWEEP_RECLAIM",
        "phase2_total_score": 0.83,
        "a1_reaction_score": 0.83,
        "a1_reaction_reason": "test_reason",
        "confirmed_ts": 100.0,
        "a1_reaction_confirmed_ts": 123.456,
        "last_price": 3000.3,
        "suggested_stop": 2998.5,
        "absorption_score": 0.9,
        "pressure_decay_score": 0.8,
        "reclaim_score": 0.9,
        "retest_score": 0.85,
        "book_absorption_score": 0.4,
        "relevant_book_depth_available": True,
        "reload_score": 0.3,
    }

    with caplog.at_level(logging.INFO):
        result = CandidateRiskEvaluator().evaluate_phase2_confirmed(event)

    assert result["candidate_source"] == "A1_REACTION_RESEARCH"
    assert "candidate_source=A1_REACTION_RESEARCH" in caplog.text
    assert "a1_reaction_type=SWEEP_RECLAIM" in caplog.text
    assert "a1_reaction_score=0.83" in caplog.text
    assert "a1_reaction_reason=test_reason" in caplog.text
    assert "a1_reaction_confirmed_ts=123.456" in caplog.text
    assert "phase2_type=SWEEP_RECLAIM" in caplog.text
    assert "phase2_total_score=0.83" in caplog.text
