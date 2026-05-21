from src.strategy.a1_absorption.reaction_event_recorder import A1ReactionEventRecorder

def test_recorder_summary(tmp_path):
    p = tmp_path / "x.jsonl"
    r = A1ReactionEventRecorder(write_jsonl=False, jsonl_path=str(p))
    assert r.record({"a1_reaction_type":"A1_REACTION_CLEAN_HOLD","reaction_event_kind":"CONFIRMED"})
    assert r.record_many([{"a1_reaction_type":"A1_REACTION_SWEEP_NO_RECLAIM","reaction_event_kind":"SWEEP_NO_RECLAIM"}]) == 1
    s = r.summary()
    assert s["total_events"] == 2 and s["total_confirmed"] == 1
    assert not p.exists()
