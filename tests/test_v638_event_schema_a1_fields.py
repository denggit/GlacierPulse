import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.a1_absorption.event_schema import A1ReactionSnapshot


def test_a1_reaction_snapshot_prefers_a1_fields_over_legacy_phase2_fields():
    event = {
        "zone_id": "z1",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "LEGACY_TYPE",
        "a1_reaction_type": "A1_REACTION_SWEEP_RECLAIM",
        "phase2_total_score": 0.6,
        "a1_reaction_score": 0.8,
        "confirmed_ts": 100.0,
        "a1_reaction_confirmed_ts": 101.0,
    }

    snapshot = A1ReactionSnapshot.from_phase2_confirmed_event(event)

    assert snapshot.reaction_type == "A1_REACTION_SWEEP_RECLAIM"
    assert snapshot.legacy_phase2_type == "LEGACY_TYPE"
    assert snapshot.reaction_total_score == 0.8
    assert snapshot.legacy_phase2_total_score == 0.6
    assert snapshot.confirmed_ts == 101.0


def test_a1_reaction_snapshot_keeps_legacy_phase2_record_compatibility():
    event = {
        "zone_id": "z1",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "phase2_total_score": 0.6,
        "confirmed_ts": 100.0,
    }

    snapshot = A1ReactionSnapshot.from_phase2_confirmed_event(event)

    assert snapshot.reaction_type == "SWEEP_RECLAIM"
    assert snapshot.legacy_phase2_type == "SWEEP_RECLAIM"
    assert snapshot.reaction_total_score == 0.6
    assert snapshot.legacy_phase2_total_score == 0.6
    assert snapshot.confirmed_ts == 100.0
