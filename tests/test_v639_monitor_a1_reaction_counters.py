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


def test_config_snapshot_includes_v639_keys():
    e=E(); e.a1_reaction_event_recorder=None; e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    s=ResearchRuntimeMonitor(e)._collect_config_snapshot()
    for key in (
        'A1_REACTION_RESEARCH_COVERAGE_ENABLED',
        'A1_REACTION_FAST_MOVE_ENABLED',
        'A1_REACTION_FAST_MOVE_WINDOW_SEC',
        'A1_REACTION_FAST_MOVE_MIN_DISTANCE_U',
        'A1_REACTION_FAST_MOVE_MIN_DISTANCE_PCT',
        'A1_REACTION_FAST_MOVE_MIN_ACTIVE_NOTIONAL_3S',
        'A1_REACTION_EVENT_RECORDER_ENABLED',
        'A1_REACTION_EVENT_RECORDER_WRITE_JSONL',
        'A1_REACTION_EVENT_RECORDER_JSONL_PATH',
        'A1_REACTION_EVENT_RECORDER_MAX_RECENT_EVENTS',
        'V62_LOG_A1_REACTION_RESEARCH_EVENT_ENABLED',
    ):
        assert key in s


def test_recorder_disabled_heartbeat_inactive():
    class Disabled:
        enabled=False
        def summary(self): return {'total_events':5}
    e=E(); e.a1_reaction_event_recorder=Disabled(); e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    s=ResearchRuntimeMonitor(e)._heartbeat_summary(10)
    assert s['a1_reaction_research_recorder_active'] is False

def test_no_recorder_and_exception_safe():
    e=E(); e.a1_reaction_event_recorder=None; e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    m=ResearchRuntimeMonitor(e); s=m._heartbeat_summary(10); assert s['a1_reaction_research_total_events']==0
    class Bad: 
        def summary(self): raise RuntimeError('x')
    e.a1_reaction_event_recorder=Bad(); s=m._heartbeat_summary(11); assert s['a1_reaction_research_total_events']==0


def test_recorder_group_maps_do_not_flatten_into_heartbeat():
    class Grouped:
        enabled=True
        def summary(self):
            return {
                'total_events': 2,
                'by_a1_reaction_type': {'A1_REACTION_CLEAN_HOLD': 2},
                'by_event_kind': {'CONFIRMED': 2},
            }
    e=E(); e.a1_reaction_event_recorder=Grouped(); e.a1_reaction_evaluator=None; e.candidate_risk_evaluator=None; e.virtual_position_manager=None; e.execution_outcome_evaluator=None; e.zone_tracker=None; e.outcome_evaluator=None
    s=ResearchRuntimeMonitor(e)._heartbeat_summary(10)
    assert s['a1_reaction_research_total_events']==2
    assert 'by_a1_reaction_type' not in s
    assert 'by_event_kind' not in s
