import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.a1_absorption.engine import A1AbsorptionEngine
from src.strategy.a1_absorption.outcome_evaluator import A1OutcomeEvaluator
from src.strategy.a1_absorption.reaction_evaluator import A1ReactionEvaluator
from src.strategy.a1_absorption.zone_tracker import A1ZoneTracker
from src.strategy.execution_research.candidate_risk_evaluator import CandidateRiskEvaluator
from src.strategy.execution_research.trade_outcome_evaluator import ExecutionResearchOutcomeEvaluator
from src.strategy.execution_research.virtual_position_manager import ResearchVirtualPositionManager


def test_v638_final_layout_imports():
    assert A1AbsorptionEngine
    assert A1ZoneTracker
    assert A1OutcomeEvaluator
    assert A1ReactionEvaluator
    assert CandidateRiskEvaluator
    assert ResearchVirtualPositionManager
    assert ExecutionResearchOutcomeEvaluator
