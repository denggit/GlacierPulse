from src.strategy.a1_absorption.reaction_taxonomy import *

def test_taxonomy_mapping():
    assert normalize_a1_reaction_type('BELOW_ZONE_ABSORPTION') == A1_REACTION_SWEEP_RECLAIM_DIRECT
    assert normalize_a1_reaction_type('A1_REACTION_SWEEP_RECLAIM_DIRECT','SWEEP_RECLAIM') == A1_REACTION_SWEEP_RECLAIM_DIRECT
    assert normalize_a1_reaction_type('A1_REACTION_SWEEP_RECLAIM_NO_RETEST','SWEEP_RECLAIM') == A1_REACTION_SWEEP_RECLAIM_NO_RETEST
    assert normalize_a1_reaction_type('legacy_unknown') == A1_REACTION_UNKNOWN
    assert legacy_phase2_type_for_reaction('A1_REACTION_WHATEVER') == 'UNKNOWN_RESEARCH'
