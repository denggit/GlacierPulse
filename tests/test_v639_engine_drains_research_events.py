from src.strategy.a1_absorption.engine import A1AbsorptionEngine

class D: pass

def test_engine_drain_research_events():
    ctx=D(); ctx.bids=[]; ctx.asks=[]; ctx.current_price=100
    engine=A1AbsorptionEngine(ctx, D())
    if not engine.a1_reaction_evaluator:
        return
    engine.a1_reaction_evaluator.research_events.append({"zone_id":"z1","a1_reaction_type":"A1_REACTION_NO_RESPONSE","reaction_event_kind":"NO_RESPONSE"})
    engine._drain_a1_reaction_research_events()
    if engine.a1_reaction_event_recorder:
        assert engine.a1_reaction_event_recorder.summary()["total_events"] >= 1
