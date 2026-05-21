from src.strategy.a1_absorption.reaction_taxonomy import *


def test_taxonomy_mapping():
    assert normalize_a1_reaction_type("CLEAN_HOLD") == A1_REACTION_CLEAN_HOLD
    assert normalize_a1_reaction_type("SWEEP_RECLAIM") == A1_REACTION_SWEEP_RECLAIM_RETEST
    assert normalize_a1_reaction_type(A1_REACTION_BREAKOUT_AWAY) == A1_REACTION_BREAKOUT_AWAY
    assert normalize_a1_reaction_type("") == A1_REACTION_UNKNOWN
    assert legacy_phase2_type_for_reaction(A1_REACTION_CLEAN_HOLD) == "CLEAN_HOLD"
