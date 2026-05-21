from src.strategy.execution_research.candidate_risk_evaluator import CandidateRiskEvaluator


def test_candidate_counters_and_summary():
    c=CandidateRiskEvaluator()
    c._update_counters({'decision':'ACCEPT_RESEARCH_CANDIDATE'})
    c._update_counters({'decision':'WAIT_RECLAIM_OR_MORE_FLOW'})
    c._update_counters({'decision':'REJECT_UNKNOWN_PHASE2_TYPE'})
    s=c.summary()
    assert s['total_candidates']==3
    assert s['accepted_candidates']==1
    assert s['wait_candidates']==1
    assert s['rejected_candidates']==1


def _event(zone_id='z1', state='PHASE2_CONFIRMED', phase2_type='CLEAN_HOLD', score=0.9):
    return {
        'zone_id': zone_id,
        'state': state,
        'direction': 'BUY',
        'phase2_type': phase2_type,
        'a1_reaction_type': 'A1_REACTION_CLEAN_HOLD',
        'phase2_total_score': score,
        'a1_reaction_score': score,
        'absorption_score': 0.9,
        'last_price': 100.0,
        'frozen_low': 100.2,
        'frozen_high': 101.0,
        'sweep_extreme': 99.8,
        'confirmed_ts': 10.0,
        'relevant_book_depth_available': True,
    }


def test_evaluate_phase2_confirmed_updates_counters_real_path():
    c=CandidateRiskEvaluator()
    high=c.evaluate_phase2_confirmed(_event(zone_id='high'))
    assert c.total_candidates==1
    if high['decision']=='ACCEPT_RESEARCH_CANDIDATE':
        assert c.accepted_candidates==1

    c.evaluate_phase2_confirmed(_event(zone_id='low', score=0.1))
    assert c.rejected_candidates>=1

    wait_event=_event(zone_id='wait', phase2_type='BELOW_ZONE_ABSORPTION', score=0.9)
    wait_event['a1_reaction_type']='A1_REACTION_SWEEP_RECLAIM_DIRECT'
    wait_event['relevant_book_depth_available']=False
    c.evaluate_phase2_confirmed(wait_event)
    assert c.wait_candidates==1

    c.evaluate_phase2_confirmed(_event(zone_id='bad-state', state='PHASE2_WAITING'))
    assert c.rejected_candidates>=2

    c.evaluate_phase2_confirmed(_event(zone_id='high'))
    assert c.rejected_candidates>=3
