import logging

from src.strategy.phase1_zone_engine import Phase1Engine
from src.strategy import phase1_zone_engine


class FakeCandidateEvaluator:
    def __init__(self):
        self.events = []

    def evaluate_phase2_confirmed(self, event):
        self.events.append(event)
        return {
            "zone_id": event.get("zone_id", "z1"),
            "phase2_type": event.get("phase2_type", "SWEEP_RECLAIM"),
            "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
            "decision": "ACCEPT_RESEARCH_CANDIDATE",
        }


class FakeVirtualPositionManager:
    def __init__(self):
        self.received = []
        self.closed_events = []

    def on_candidate(self, result):
        self.received.append(result)

    def pop_closed_events(self):
        return list(self.closed_events)


def _build_engine_for_drain_test():
    engine = object.__new__(Phase1Engine)
    engine.phase2_orderflow_evaluator = None
    engine.phase3_candidate_evaluator = FakeCandidateEvaluator()
    engine.virtual_position_manager = FakeVirtualPositionManager()
    engine.phase3_trade_outcome_evaluator = None
    return engine


def test_a1_reaction_to_virtual_position_disabled_still_evaluates_candidate(monkeypatch):
    monkeypatch.setattr(phase1_zone_engine, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", False)

    engine = _build_engine_for_drain_test()
    phase2_events = [{"zone_id": "z-disabled", "phase2_type": "SWEEP_RECLAIM"}]
    engine.phase2_orderflow_evaluator = type(
        "FakePhase2",
        (),
        {"pop_confirmed_events": lambda self: phase2_events},
    )()

    engine._drain_phase2_confirmed_events()

    assert engine.phase3_candidate_evaluator.events == phase2_events
    assert engine.virtual_position_manager.received == []


def test_a1_reaction_to_virtual_position_enabled_preserves_old_virtual_path(monkeypatch):
    monkeypatch.setattr(phase1_zone_engine, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", True)

    engine = _build_engine_for_drain_test()
    phase2_events = [{"zone_id": "z-enabled", "phase2_type": "SWEEP_RECLAIM"}]
    engine.phase2_orderflow_evaluator = type(
        "FakePhase2",
        (),
        {"pop_confirmed_events": lambda self: phase2_events},
    )()

    engine._drain_phase2_confirmed_events()

    assert engine.phase3_candidate_evaluator.events == phase2_events
    assert len(engine.virtual_position_manager.received) == 1
    assert engine.virtual_position_manager.received[0]["zone_id"] == "z-enabled"


def test_a1_reaction_to_virtual_position_disabled_logs_blocked_when_enabled(monkeypatch, caplog):
    monkeypatch.setattr(phase1_zone_engine, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine.cfg, "V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED", True, raising=False)

    engine = _build_engine_for_drain_test()
    phase2_events = [{"zone_id": "z-log", "phase2_type": "SWEEP_RECLAIM"}]
    engine.phase2_orderflow_evaluator = type(
        "FakePhase2",
        (),
        {"pop_confirmed_events": lambda self: phase2_events},
    )()

    with caplog.at_level(logging.INFO):
        engine._drain_phase2_confirmed_events()

    assert "[VIRTUAL-POSITION-BLOCKED]" in caplog.text
    assert "z-log" in caplog.text


def test_a1_reaction_to_virtual_position_disabled_respects_blocked_log_switch(monkeypatch, caplog):
    monkeypatch.setattr(phase1_zone_engine, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", False)
    monkeypatch.setattr(phase1_zone_engine.cfg, "V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED", False, raising=False)

    engine = _build_engine_for_drain_test()
    phase2_events = [{"zone_id": "z-no-log", "phase2_type": "SWEEP_RECLAIM"}]
    engine.phase2_orderflow_evaluator = type(
        "FakePhase2",
        (),
        {"pop_confirmed_events": lambda self: phase2_events},
    )()

    with caplog.at_level(logging.INFO):
        engine._drain_phase2_confirmed_events()

    assert "[VIRTUAL-POSITION-BLOCKED]" not in caplog.text
