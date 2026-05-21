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
