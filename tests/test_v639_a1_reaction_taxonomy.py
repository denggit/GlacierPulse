from src.strategy.a1_absorption.reaction_taxonomy import *


def test_taxonomy_mapping():
    assert normalize_a1_reaction_type("CLEAN_HOLD") == A1_REACTION_CLEAN_HOLD
    assert normalize_a1_reaction_type("SWEEP_RECLAIM") == A1_REACTION_SWEEP_RECLAIM_RETEST
    assert normalize_a1_reaction_type("BELOW_ZONE_ABSORPTION") == A1_REACTION_SWEEP_RECLAIM_DIRECT
    assert normalize_a1_reaction_type("A1_REACTION_SWEEP_RECLAIM_DIRECT", "SWEEP_RECLAIM") == A1_REACTION_SWEEP_RECLAIM_DIRECT
    assert normalize_a1_reaction_type("A1_REACTION_SWEEP_RECLAIM_NO_RETEST", "SWEEP_RECLAIM") == A1_REACTION_SWEEP_RECLAIM_NO_RETEST
    assert normalize_a1_reaction_type("") == A1_REACTION_UNKNOWN
    assert normalize_a1_reaction_type("legacy_unknown") == A1_REACTION_UNKNOWN


def test_legacy_phase2_mapping():
    assert legacy_phase2_type_for_reaction(A1_REACTION_CLEAN_HOLD) == "CLEAN_HOLD"
    assert legacy_phase2_type_for_reaction(A1_REACTION_SWEEP_RECLAIM_RETEST) == "SWEEP_RECLAIM"
    assert legacy_phase2_type_for_reaction(A1_REACTION_SWEEP_RECLAIM_DIRECT) == "SWEEP_RECLAIM"
    assert legacy_phase2_type_for_reaction(A1_REACTION_SWEEP_RECLAIM_NO_RETEST) == "SWEEP_RECLAIM"
    assert legacy_phase2_type_for_reaction("A1_REACTION_WHATEVER") == "UNKNOWN_RESEARCH"


def test_package_exports_research_components():
    import src.strategy.a1_absorption as pkg

    assert pkg.A1ReactionEventRecorder
    assert pkg.A1_REACTION_CLEAN_HOLD == A1_REACTION_CLEAN_HOLD
    assert pkg.normalize_a1_reaction_type("CLEAN_HOLD") == A1_REACTION_CLEAN_HOLD
