from src.strategy.a1_absorption import reaction_evaluator as reaction_evaluator_module
from src.strategy.a1_absorption.reaction_evaluator import A1ReactionEvaluator, A1ReactionTrackedZone


def _zone(direction="BUY"):
    return A1ReactionTrackedZone(
        zone_id="z-ts",
        direction=direction,
        frozen_ts=10.0,
        frozen_low=99.0,
        frozen_high=101.0,
        zone_mid=100.0,
        live_low=99.0,
        live_high=101.0,
        state="PHASE2_WAITING",
        phase2_registered_ts=10.0,
        last_price=100.0,
    )


def test_append_research_event_uses_explicit_event_ts_not_wall_clock(monkeypatch):
    monkeypatch.setattr(reaction_evaluator_module.time, "time", lambda: 9_999_999_999.0)
    evaluator = A1ReactionEvaluator()
    zone = _zone()

    evaluator._append_research_event(
        zone,
        "A1_REACTION_CLEAN_HOLD",
        "CONFIRMED",
        event_ts=123.456,
    )

    event = evaluator.pop_research_events()[0]
    assert event["reaction_event_ts"] == 123.456
    assert event["reaction_event_ts"] != reaction_evaluator_module.time.time()
    assert event["reaction_event_ts_valid"] is True
    assert event["reaction_event_ts_source"] == "event_time"


def test_append_research_event_missing_ts_is_marked_invalid(monkeypatch):
    monkeypatch.setattr(reaction_evaluator_module.time, "time", lambda: 9_999_999_999.0)
    evaluator = A1ReactionEvaluator()
    zone = _zone()
    zone.frozen_ts = 0.0
    zone.phase2_registered_ts = 0.0

    evaluator._append_research_event(zone, "A1_REACTION_NO_RESPONSE", "NO_RESPONSE")

    event = evaluator.pop_research_events()[0]
    assert event["reaction_event_ts"] == 0.0
    assert event["reaction_event_ts_valid"] is False
    assert event["reaction_event_ts_source"] == "missing"


def test_missed_fast_move_uses_now_ts(monkeypatch):
    monkeypatch.setattr(reaction_evaluator_module.time, "time", lambda: 9_999_999_999.0)
    evaluator = A1ReactionEvaluator()
    zone = _zone("BUY")
    zone.phase2_registered_ts = 999.0
    zone.last_price = 103.0

    evaluator._maybe_append_fast_move_research_event(zone, now_ts=1000.0)

    event = evaluator.pop_research_events()[0]
    assert event["reaction_event_kind"] == "MISSED_FAST_MOVE"
    assert event["reaction_event_ts"] == 1000.0


def test_prune_without_now_ts_does_not_emit_wall_clock_reaction_ts(monkeypatch):
    monkeypatch.setattr(reaction_evaluator_module.time, "time", lambda: 9_999_999_999.0)
    evaluator = A1ReactionEvaluator(zone_ttl_seconds=0.1)
    zone = _zone()
    evaluator.active_zones[zone.zone_id] = zone

    evaluator._prune()

    event = evaluator.pop_research_events()[0]
    assert event["reaction_event_ts"] == 10.0
    assert event["reaction_event_ts"] != reaction_evaluator_module.time.time()
