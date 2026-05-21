from src.strategy.a1_absorption.engine import A1AbsorptionEngine
from src.strategy.a1_absorption import engine as engine_module

class D: pass


def test_engine_drain_research_events_no_candidate_no_virtual(monkeypatch):
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.a1_reaction_evaluator:
        return
    called={'cand':0,'virt':0}
    if engine.candidate_risk_evaluator:
        monkeypatch.setattr(engine.candidate_risk_evaluator,'evaluate_phase2_confirmed',lambda e: called.__setitem__('cand',called['cand']+1))
    if engine.virtual_position_manager:
        monkeypatch.setattr(engine.virtual_position_manager,'on_candidate',lambda r: called.__setitem__('virt',called['virt']+1))
    engine.a1_reaction_evaluator.research_events.append({'zone_id':'z1','a1_reaction_type':'A1_REACTION_NO_RESPONSE','reaction_event_kind':'NO_RESPONSE'})
    engine._drain_a1_reaction_research_events()
    assert called['cand']==0 and called['virt']==0


def test_engine_drain_coverage_disabled_does_not_pop_record_or_log(monkeypatch, caplog):
    monkeypatch.setattr(engine_module.cfg, 'A1_REACTION_RESEARCH_COVERAGE_ENABLED', False)
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.a1_reaction_evaluator:
        return
    called={'record':0}
    monkeypatch.setattr(engine.a1_reaction_event_recorder,'record_many',lambda events: called.__setitem__('record',called['record']+1))
    engine.a1_reaction_evaluator.research_events.append({'zone_id':'z1','a1_reaction_type':'A1_REACTION_NO_RESPONSE','reaction_event_kind':'NO_RESPONSE'})
    engine._drain_a1_reaction_research_events()
    assert len(engine.a1_reaction_evaluator.research_events)==1
    assert called['record']==0
    assert 'A1-REACTION-RESEARCH-EVENT' not in caplog.text


def test_confirmed_path_still_to_candidate(monkeypatch):
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.candidate_risk_evaluator:
        return
    hits={'cand':0,'virt':0}
    monkeypatch.setattr(engine.candidate_risk_evaluator,'evaluate_phase2_confirmed',lambda e: hits.__setitem__('cand',hits['cand']+1) or {'zone_id':'z'})
    if engine.virtual_position_manager:
        monkeypatch.setattr(engine.virtual_position_manager,'on_candidate',lambda r: hits.__setitem__('virt',hits['virt']+1))
    engine.a1_reaction_evaluator.confirmed_events.append({'zone_id':'z','state':'PHASE2_CONFIRMED','direction':'BUY','phase2_type':'CLEAN_HOLD'})
    engine._drain_a1_reaction_confirmed_events()
    assert hits['cand']==1


def test_research_events_do_not_call_candidate_or_virtual(monkeypatch):
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.a1_reaction_evaluator:
        return
    hits={'cand':0,'virt':0}
    if engine.candidate_risk_evaluator:
        monkeypatch.setattr(engine.candidate_risk_evaluator,'evaluate_phase2_confirmed',lambda e: hits.__setitem__('cand',hits['cand']+1))
    if engine.virtual_position_manager:
        monkeypatch.setattr(engine.virtual_position_manager,'on_candidate',lambda r: hits.__setitem__('virt',hits['virt']+1))
    engine.a1_reaction_evaluator.research_events.append({'zone_id':'z1','a1_reaction_type':'A1_REACTION_NO_RESPONSE','reaction_event_kind':'NO_RESPONSE'})
    engine._drain_a1_reaction_research_events()
    assert hits=={'cand':0,'virt':0}


def test_confirmed_candidate_blocked_from_virtual_by_default(monkeypatch):
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.candidate_risk_evaluator:
        return
    hits={'virt':0}
    monkeypatch.setattr(engine.candidate_risk_evaluator,'evaluate_phase2_confirmed',lambda e: {'zone_id':'z','phase2_type':'CLEAN_HOLD','candidate_type':'CLEAN_HOLD_LOW_RISK','decision':'ACCEPT_RESEARCH_CANDIDATE'})
    if engine.virtual_position_manager:
        monkeypatch.setattr(engine.virtual_position_manager,'on_candidate',lambda r: hits.__setitem__('virt',hits['virt']+1))
    engine.a1_reaction_evaluator.confirmed_events.append({'zone_id':'z','state':'PHASE2_CONFIRMED','direction':'BUY','phase2_type':'CLEAN_HOLD'})
    engine._drain_a1_reaction_confirmed_events()
    assert hits['virt']==0
