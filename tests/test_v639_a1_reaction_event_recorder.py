from src.strategy.a1_absorption.reaction_event_recorder import A1ReactionEventRecorder

def test_recorder_summary_and_flags(tmp_path):
    p = tmp_path / 'x.jsonl'
    r = A1ReactionEventRecorder(enabled=False,write_jsonl=False,jsonl_path=str(p))
    assert r.record({'a1_reaction_type':'A1_REACTION_CLEAN_HOLD','reaction_event_kind':'CONFIRMED'}) is None
    assert r.summary()['active'] is False and r.summary()['total_events']==0
    r = A1ReactionEventRecorder(write_jsonl=False,jsonl_path=str(p))
    assert r.record_many(None)==0
    assert r.record({'a1_reaction_type':'A1_REACTION_CLEAN_HOLD','reaction_event_kind':'CONFIRMED'})
    assert r.record_many([{'a1_reaction_type':'A1_REACTION_SWEEP_NO_RECLAIM','reaction_event_kind':'SWEEP_NO_RECLAIM'}])==1
    s=r.summary(); assert s['by_a1_reaction_type']['A1_REACTION_CLEAN_HOLD']==1 and s['by_event_kind']['SWEEP_NO_RECLAIM']==1
    assert not p.exists()

def test_write_jsonl_true(tmp_path):
    p = tmp_path / 'x.jsonl'
    r = A1ReactionEventRecorder(write_jsonl=True,jsonl_path=str(p))
    r.record({'a1_reaction_type':'A1_REACTION_CLEAN_HOLD','reaction_event_kind':'CONFIRMED'})
    assert p.exists()
