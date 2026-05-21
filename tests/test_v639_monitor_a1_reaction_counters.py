from src.monitoring.research_runtime_monitor import ResearchRuntimeMonitor

class E: pass
class R:
    enabled=True
    def summary(self): return {'total_events':3,'total_confirmed':1,'total_no_response':2}

def test_heartbeat_summary_has_a1_fields():
    e=E(); e.a1_reaction_event_recorder=R(); e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    m=ResearchRuntimeMonitor(e)
    s=m._heartbeat_summary(10)
    assert 'a1_reaction_research_recorder_active' in s and s['a1_reaction_research_total_events']==3

def test_no_recorder_and_exception_safe():
    e=E(); e.a1_reaction_event_recorder=None; e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    m=ResearchRuntimeMonitor(e); s=m._heartbeat_summary(10); assert s['a1_reaction_research_total_events']==0
    class Bad: 
        def summary(self): raise RuntimeError('x')
    e.a1_reaction_event_recorder=Bad(); s=m._heartbeat_summary(11); assert s['a1_reaction_research_total_events']==0
