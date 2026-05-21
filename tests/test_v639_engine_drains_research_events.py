from src.strategy.a1_absorption.engine import A1AbsorptionEngine

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
