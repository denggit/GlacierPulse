import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.a1_absorption import engine as a1_engine
from src.strategy.a1_absorption.engine import A1AbsorptionEngine


class _CandidateRiskSpy:
    def __init__(self):
        self.events = []

    def evaluate_phase2_confirmed(self, event):
        self.events.append(event)
        return {
            "zone_id": event["zone_id"],
            "phase2_type": event["phase2_type"],
            "a1_reaction_type": event["a1_reaction_type"],
            "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
            "candidate_source": "A1_REACTION_RESEARCH",
            "decision": "ACCEPT_RESEARCH_CANDIDATE",
        }


class _VirtualSpy:
    def __init__(self):
        self.calls = []

    def on_candidate(self, candidate):
        self.calls.append(candidate)


def test_v638_a1_reaction_confirmed_research_only_by_default(monkeypatch, caplog):
    monkeypatch.setattr(a1_engine, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", False)
    monkeypatch.setattr(a1_engine.cfg, "V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED", True, raising=False)

    engine = object.__new__(A1AbsorptionEngine)
    event = {
        "zone_id": "z-v638",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "a1_reaction_type": "SWEEP_RECLAIM",
    }
    engine.phase2_orderflow_evaluator = type("Reaction", (), {"pop_confirmed_events": lambda self: [event]})()
    engine.phase3_candidate_evaluator = _CandidateRiskSpy()
    engine.virtual_position_manager = _VirtualSpy()
    engine.phase3_trade_outcome_evaluator = None

    with caplog.at_level(logging.INFO):
        engine._drain_phase2_confirmed_events()

    assert engine.phase3_candidate_evaluator.events == [event]
    assert engine.virtual_position_manager.calls == []
    assert "[VIRTUAL-POSITION-BLOCKED] reason=a1_reaction_to_virtual_position_disabled" in caplog.text
