from src.strategy.a1_absorption.reaction_evaluator import A1ReactionEvaluator, A1ReactionTrackedZone


def _zone(direction='BUY'):
    return A1ReactionTrackedZone(zone_id='z1',direction=direction,frozen_ts=1,frozen_low=99,frozen_high=101,zone_mid=100,live_low=99,live_high=101,state='PHASE2_WAITING',phase2_registered_ts=1,last_price=100,metadata={})


def test_append_research_and_dedup_and_fields():
    e=A1ReactionEvaluator(); z=_zone(); z.phase2_type='CLEAN_HOLD'; z.phase2_total_score=0.8
    e._append_research_event(z,'CLEAN_HOLD','CONFIRMED')
    e._append_research_event(z,'CLEAN_HOLD','CONFIRMED')
    ev=e.pop_research_events(); assert len(ev)==1
    assert ev[0]['a1_reaction_type'] and ev[0]['legacy_phase2_type']=='CLEAN_HOLD'
    assert ev[0]['reaction_event_kind']=='CONFIRMED' and 'reaction_event_ts' in ev[0] and 'reaction_event_price' in ev[0]


def test_confirmed_goes_both_confirmed_and_research():
    e=A1ReactionEvaluator(); z=_zone(); z.state='PHASE2_CONFIRMED'; z.has_confirmed=True; z.confirmed_ts=2; z.phase2_type='CLEAN_HOLD'
    e._append_confirmed_event(z); e._append_research_event(z,'CLEAN_HOLD','CONFIRMED')
    assert len(e.pop_confirmed_events())==1
    assert len(e.pop_research_events())==1


def test_timeout_and_classifications_research_only():
    e=A1ReactionEvaluator(); z=_zone(); z.phase2_registered_ts=0
    e.active_zones[z.zone_id]=z
    e.zone_ttl_seconds=0.1
    e._prune(now_ts=10)
    assert len(e.pop_confirmed_events())==0
    rs=e.pop_research_events(); assert rs and rs[0]['reaction_event_kind'] in ('NO_RESPONSE','TIMEOUT','SWEEP_NO_RECLAIM','RECLAIM_NO_RETEST')


def test_fast_move_research_only():
    e=A1ReactionEvaluator(); z=_zone('BUY'); z.phase2_registered_ts=1; z.last_price=103
    e._maybe_append_fast_move_research_event(z, now_ts=2)
    assert len(e.pop_confirmed_events())==0
    rs=e.pop_research_events(); assert len(rs)==1 and rs[0]['reaction_event_kind']=='MISSED_FAST_MOVE'
